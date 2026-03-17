# DB Flow (куда смотреть и зачем)

Дата актуальности: 2026-03-17

## 1) Где лежат таблицы

- База: Postgres из `DATABASE_URL`
- Схема: по умолчанию `public` (отдельная custom-schema сейчас не используется)
- Основные таблицы:
  - `users`
  - `listmonk_users`
  - `merge_log`
  - `bonus_accrual_log`
  - `email_repair_log`
  - `sync_state` (используется в инкрементальном consent sync как watermark)

## 2) Таблицы и их назначение

1. `users`
- Главный профиль пользователя из webhook (`pass`) + merged поля.
- Ключ: `user_id` (PK).
- Сюда смотреть в первую очередь, чтобы понять: пользователь вообще создан/обновлен или нет.

2. `listmonk_users`
- Связка нашего `user_id` с Listmonk (`subscriber_id`), status, list_ids, attributes.
- Флаги consent-процесса:
  - `consent_pending`
  - `consent_checked_at`
  - `consent_confirmed_at`
- Сюда смотреть, если проблема с подпиской/consent.

3. `merge_log`
- Фиксирует, что merge со старой БД уже выполнялся.
- Нужна для защиты от повторного merge в `CREATE/UPDATE`.
- Если записи нет, consumer пытается сделать merge.

4. `bonus_accrual_log`
- Идемпотентность начислений бонусов (сейчас главным образом для `email_consent`).
- Ключ дедупликации: `idempotency_key` (unique).
- Статусы: `pending` / `done` / `failed`.
- Сюда смотреть, если бонус не начислился или начислился повторно.

5. `sync_state`
- Водяные знаки для инкрементальных sync-задач.
- Для `consent_sync_worker` это ключевая таблица прогресса по каждому `list_id`.

6. `email_repair_log`
- Очередь и аудит duplicate-email remediation.
- Пишется consumer'ами `CREATE/UPDATE`, когда локальный `listmonk_users.email` уже занят другим `user_id`.
- Полезные поля:
  - `normalized_email`
  - `incoming_user_id`
  - `existing_user_id`
  - `winner_user_id`
  - `winner_subscriber_id`
  - `status`
  - `attempts`
  - `next_retry_at`
  - `error_text`
- Сюда смотреть, если email "пропал" у loser-пользователя или если duplicate-email больше не ретраится через RabbitMQ.

## 3) Flow по событиям (что меняется в БД)

## CREATE (`queue-create`)

1. lock по `user_id`
2. `users`: `upsert`
3. `listmonk_users`: `upsert`
4. если merge применился:
- вызов Teyca bonuses API
- вызов Teyca `PUT key2=\"merge DD.MM.YYYY HH:MM\"`
- `merge_log`: `insert`
5. `listmonk_users.consent_pending = true`
6. commit

Если локальный email-дубликат:
1. `users`: upsert уже выполнен
2. проверяется локальный duplicate email в `listmonk_users` до вызова mutating Listmonk API
3. `email_repair_log`: insert `status='pending'`
4. `listmonk_users`: не обновляется этим сообщением
5. Listmonk этим сообщением не мутируется
6. consumer завершает обработку без `requeue`

Куда смотреть:
1. `users` (профиль)
2. `listmonk_users` (subscriber/status + consent_pending)
3. `merge_log` (был ли merge)
4. `email_repair_log` (если sync в Listmonk не прошёл из-за duplicate email)

## UPDATE (`queue-update`)

1. lock по `user_id`
2. проверка `merge_log`
3. `users`: `upsert`
4. `listmonk_users`: `upsert`
5. `listmonk_users.consent_pending = true`
6. commit

Если `merge_log` отсутствует, merge выполняется и логируется в `merge_log`.
При успешном merge дополнительно обновляется `key2` в Teyca.
Если локальный email-дубликат, вместо бесконечного retry создаётся запись в `email_repair_log`, а сообщение ack-ается.
Локальный duplicate pre-check выполняется до `listmonk_client.upsert_subscriber(...)`, поэтому сам Listmonk в этом сценарии не успевает обновиться.

Куда смотреть:
1. `users.updated_at`
2. `merge_log` (есть/нет записи)
3. `listmonk_users` (обновился ли subscriber и pending-флаг)
4. `email_repair_log` (если update "не дошёл" до `listmonk_users` из-за duplicate email)

## DELETE (`queue-delete`)

В транзакции:
1. `listmonk_users`: delete
2. `merge_log`: delete
3. `bonus_accrual_log`: delete
4. `users`: delete
5. commit

После commit:
6. delete subscriber в Listmonk (внешний вызов)

Куда смотреть:
1. проверить отсутствие строк в `users`, `listmonk_users`, `merge_log`, `bonus_accrual_log` по `user_id`

## Consent sync-worker

1. для каждого `list_id` из `LISTMONK_LIST_IDS` читает watermark из `sync_state`
2. запрашивает из Listmonk только подписчиков, изменившихся после watermark
3. маппит `subscriber_id` на `user_id` через `listmonk_users`
4. обрабатывает status
5. если status `blocked`:
- Teyca `PUT key1=blocked`
- обновляем `listmonk_users.status`
- `consent_pending = false` (или остается `true` при ошибке Teyca)
6. если status подтвержден (`confirmed/enabled/active`):
- `bonus_accrual_log`: reserve/get по `idempotency_key=email_consent:{user_id}`
- в `bonus_accrual_log.payload` ведем шаги:
  - `bonus_done`
  - `key1_done`
- если `bonus_done=false`: Teyca `POST /bonuses`, затем `bonus_done=true`
- если `key1_done=false`: Teyca `PUT key1=confirmed`, затем `key1_done=true`
- когда оба шага `true`: `bonus_accrual_log.status = done`, `consent_pending = false`
7. после батча обновляет watermark в `sync_state` (по последнему `(updated_at, subscriber_id)`)
8. при любой ошибке Teyca:
- сохраняем текущий step-progress в `bonus_accrual_log.payload`
- `consent_pending` остается `true` (для ретрая)

Куда смотреть:
1. `listmonk_users.consent_pending`
2. `bonus_accrual_log` по `reason='email_consent'`

## Email repair-worker

1. читает `email_repair_log` со статусами `pending` и `failed`, учитывая `next_retry_at`
2. ищет authoritative subscriber в Listmonk по `normalized_email`
3. находит winner по совпадению `subscriber_id` с одной из строк в `listmonk_users`
4. winner'у отправляет Teyca `PUT /passes/{user_id}` с `key6='bugs'`
4. loser'ам:
- `users.email = NULL`
- `listmonk_users.email = NULL`
- Teyca `PUT /passes/{user_id}` с `email=null`, `key1='bad email'`, `key6='bugs'`
5. `email_repair_log.status` меняется:
- `teyca_synced` при успехе
- `failed` при очередной retryable ошибке
- `manual_review` после исчерпания попыток

Подтверждённый контракт Teyca:
- `PUT /passes/{user_id}` ведёт себя как partial update, не как full replace.
- Проверка выполнена 2026-03-18 на тестовой карте `user_id=5722735`: `PUT {"key6":"put-check"}` сохранил остальные поля карты без изменений.

Куда смотреть:
1. `email_repair_log` по `normalized_email` или `incoming_user_id`
2. `users.email` и `listmonk_users.email` у winner/loser
3. логи `email_repair_*`

## 3.1) Маппинг old DB -> текущая БД

Текущая интеграция old DB читает таблицу `public.users` и использует алиасы колонок.

Фактический маппинг:

| old DB (`public.users`) | наше поле | куда пишется |
|---|---|---|
| `balance` (или `bonus`) | `bonus` | `users.bonus` |
| `check_sum` (или `check_summ`) | `check_summ` | `users.check_summ` |
| `check_count` (или `visits`) | `visits` | `users.visits` |
| `average_check` (или `summ_last`) | `summ_last` | `users.summ_last` |

Также поддерживаются прямые поля, если они есть в old DB:
- `summ` -> `users.summ`
- `summ_all` -> `users.summ_all`
- `visits_all` -> `users.visits_all`

Примечание:
- Если колонка отсутствует в old DB, она просто пропускается (ошибка не выбрасывается).
- Merge выполняется только по полям, которые реально найдены в old DB.

## 4) Быстрые SQL для дебага

Подставь `:user_id` вручную.

```sql
-- 1) Главный профиль
SELECT * FROM users WHERE user_id = :user_id;

-- 2) Состояние sync с Listmonk и consent
SELECT * FROM listmonk_users WHERE user_id = :user_id;

-- 3) Факт merge
SELECT * FROM merge_log WHERE user_id = :user_id ORDER BY merged_at DESC;

-- 4) Начисления и идемпотентность
SELECT *
FROM bonus_accrual_log
WHERE user_id = :user_id
ORDER BY created_at DESC;

-- 5) Duplicate email remediation
SELECT *
FROM email_repair_log
WHERE incoming_user_id = :user_id
ORDER BY created_at DESC;
```

## 5) Что проверять в первую очередь по типовым вопросам

1. "Webhook пришел, но пользователя нет"
- Проверить очередь/consumer-логи
- Проверить `users` по `user_id`

2. "Пользователь есть, но нет подписки/consent"
- Проверить `listmonk_users`
- Проверить `consent_pending`, `consent_checked_at`, `consent_confirmed_at`
- Проверить watermark в `sync_state` (не застрял ли курсор)

3. "Бонус не начислился"
- Проверить `bonus_accrual_log` (`status`, `error_text`)
- Проверить логи `consent_sync_worker`/consumer

4. "Почему merge не выполнился повторно"
- Проверить `merge_log` по `user_id` (если запись есть, повтор не делается)

5. "Почему бонус уже начислен, но key1 не обновился"
- Проверить `bonus_accrual_log.payload` (`bonus_done=true`, `key1_done=false`)
- Это штатный частичный прогресс; следующий retry догонит `key1`.

6. "Почему email очистился или не обновился"
- Проверить `email_repair_log` по `incoming_user_id` или `normalized_email`
- Проверить, кто стал `winner_user_id`
- Проверить, был ли статус `teyca_synced` или `manual_review`

## 6) Метрики и логи consent sync

События в логах `consent_sync_worker`:
- `consent_sync_list_processed`:
  - `list_id`
  - `deltas`
  - `watermark_updated_at`
  - `watermark_subscriber_id`
- `consent_sync_metrics` (агрегат за запуск):
  - `processed`
  - `batch_size`
  - `consent_bonus_amount`
  - `deltas_fetched`
  - `unmapped_subscribers`
  - `subscriber_not_found`
  - `blocked_done`
  - `not_confirmed`
  - `confirmed_done`
  - `accrual_resumed`
  - `operation_missing`
  - `teyca_errors`
- Ошибочные/диагностические события:
  - `consent_sync_subscriber_not_mapped`
  - `consent_sync_subscriber_not_found`
  - `consent_sync_blocked_key1_update_failed`
  - `consent_sync_confirmed_step_failed`

Пример запроса в Loki:
```logql
{service="app"} |= "consent_sync_metrics" | json
```

## 7) Reconcile-процесс (восстановление связей)

Задача: обработать `subscriber_id`, которые приходят из Listmonk, но отсутствуют в `listmonk_users`.

Как работает:
1. воркер `listmonk_reconcile_worker` читает изменения Listmonk инкрементально через watermark в `sync_state` с `source=listmonk_reconcile`
2. для каждого subscriber без маппинга пытается восстановить связь:
- сначала по `attributes.user_id` (безопасный приоритет)
- затем по `email`, только если найден ровно один `user_id`
3. при успешном восстановлении делает `upsert` в `listmonk_users` и ставит `consent_pending=true`
4. если безопасно сопоставить нельзя (ambiguous/not found) — только логирует кейс, без авто-маппинга

Зачем это нужно:
- не терять пользователей, у которых subscriber уже есть в Listmonk, а локальная связь отсутствует
- уменьшать `consent_sync_subscriber_not_mapped` в основном consent-воркере

Логи reconcile:
- `listmonk_reconcile_mapping_restored`
- `listmonk_reconcile_unmapped`
- `listmonk_reconcile_email_ambiguous`
- `listmonk_reconcile_metrics`

Политика восстановления удалённого subscriber в Listmonk:
- если в `listmonk_users` есть связь, но `subscriber_id` больше не существует в Listmonk, воркер пересоздаёт subscriber по `email` и `list_ids`
- затем пытается восстановить статус:
  - `blocked`/`blacklisted` -> `blocklisted`
  - `confirmed`/`active` -> `enabled`
  - `enabled`/`disabled`/`blocklisted` -> без изменений
- после восстановления ставится `consent_pending=true` для повторной проверки consent-потока
