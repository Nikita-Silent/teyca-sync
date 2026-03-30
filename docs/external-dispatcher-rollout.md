# External Dispatcher Rollout

Цель: включить `external-dispatcher` в runtime так, чтобы webhook-consumers перестали держать `RabbitMQ unacked` на внешних вызовах, и при этом не потерять side effect'ы в Listmonk/Teyca.

## Что уже изменено в коде

- `CREATE` / `UPDATE` / `DELETE` больше не ходят в Listmonk/Teyca inline.
- Вместо этого consumers пишут операции в `external_call_outbox`.
- Новый worker `external-dispatcher` читает outbox, делает внешние вызовы и фиксирует локальный прогресс.

## Безопасный порядок выката

1. Применить миграцию `9bac67d00026_add_external_call_outbox`.
2. Выкатить код с новой архитектурой на `app` и `consumers`.
3. Одновременно добавить `external-dispatcher` в runtime.
4. На время первичного drain лучше поставить `consent-sync` на паузу или снизить его частоту, чтобы не делить Teyca rate-limit с dispatcher.
5. Убедиться, что новые webhook'и создают записи в `external_call_outbox`, а dispatcher их закрывает.
6. После стабилизации вернуть `consent-sync`.

Безопасный fallback:

- если migration уже применена, а новый код откатывается назад, старая версия просто игнорирует `external_call_outbox`;
- если новый код выкачен, а dispatcher еще не включен, side effect'ы не потеряются, но начнут копиться в `external_call_outbox`.

## Рекомендуемый rollout window

- rollout лучше делать в окно с минимальным входящим `queue-update`;
- перед выкладкой полезно убедиться, что `queue-update` не в стагнации;
- первый запуск dispatcher лучше делать с умеренным `EXTERNAL_DISPATCHER_BATCH_SIZE`, например `10` или `25`.

## Что смотреть во время выката

### RabbitMQ

- `queue-update.messages_unacknowledged`
- `queue-update.messages_ready`
- `queue-update.consumer_capacity`
- `queue-create` / `queue-delete` по тем же метрикам

Ожидаемое поведение после перехода:

- `unacked` перестает надолго упираться в `prefetch_count`
- `consumer_capacity` растет
- backlog drain'ится без зависания на 4 сообщениях

### Postgres

Проверка outbox:

```sql
select status, count(*)
from external_call_outbox
group by status
order by status;
```

Старые pending/failed:

```sql
select id, operation, user_id, status, attempts, next_retry_at, created_at, updated_at
from external_call_outbox
where status in ('pending', 'failed', 'processing', 'dead')
order by created_at asc
limit 100;
```

Самые старые незавершенные:

```sql
select id, operation, user_id, status, attempts, now() - created_at as age, last_error
from external_call_outbox
where status in ('pending', 'failed', 'processing', 'dead')
order by created_at asc
limit 20;
```

### Loki

Смотреть по компонентам:

- `component="consumers"`
- `component="external-dispatcher"`
- временно отдельно `component="consent-sync"`

Ключевые события:

- `create_consumer_listmonk_enqueue_done`
- `update_consumer_listmonk_enqueue_done`
- `create_merge_enqueued`
- `update_merge_enqueued`
- `delete_consumer_processed`
- `external_dispatcher_listmonk_upsert_done`
- `external_dispatcher_invalid_email_block_done`
- `external_dispatcher_merge_finalize_done`
- `external_dispatcher_job_retry_scheduled`
- `external_dispatcher_metrics`

## Runtime policy на первое включение

- `external-dispatcher` включить сразу вместе с новым code rollout.
- `consent-sync` на старте rollout лучше отключить или запускать реже.
- Если `external_call_outbox` быстро растет, а `done` не растет:
  - проверить heartbeat `external-dispatcher`
  - проверить `external_dispatcher_job_retry_scheduled`
  - проверить rate-limit pressure от `consent-sync`

## Признаки успешного перехода

- новые webhook-сообщения быстро ack'аются consumers;
- `queue-update` перестает застревать в `4 unacked`;
- `external_call_outbox.done` растет вместе с входящими webhook;
- `failed` / `dead` остаются штучными, а не массовыми;
- при паузе `consent-sync` dispatcher стабильно догоняет outbox.

## Если надо быстро откатиться

1. Остановить `external-dispatcher`.
2. Откатить `app` и `consumers` на предыдущий образ.
3. `external_call_outbox` не удалять: это журнал pending side effect'ов нового rollout.
4. Перед повторной попыткой rollout разобраться, какие outbox-строки остались в `pending` / `failed` / `dead`.

## Что не делать во время rollout

- не purge'ить Rabbit очереди;
- не удалять `external_call_outbox` вручную;
- не включать одновременно агрессивный `consent-sync` и большой `EXTERNAL_DISPATCHER_BATCH_SIZE`, если Teyca уже на лимите;
- не рестартовать RabbitMQ ради снятия `unacked`, если проблема уже вынесена в dispatcher/outbox.
