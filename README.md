# teyca-sync

Webhook-to-RabbitMQ sync service for Teyca CRM loyalty. Replaces n8n Current-Loyalty workflow.

See [AGENTS.md](AGENTS.md), [docs/roadmap.md](docs/roadmap.md), and [docs/db-flow.md](docs/db-flow.md).

## Run

```bash
make up
# POST ${WEBHOOK:-/webhook} with Authorization header token and body {"type": "CREATE", "pass": {"user_id": 1}}
# make up запускает:
# - rabbitmq
# - migrate (alembic upgrade head)
# - app (FastAPI)
# - consumers
# - consent-sync (периодический worker)
# - reconcile (периодический worker)
```

## Test

```bash
make test
```

## Env

- `CONSENT_BONUS_AMOUNT` — количество бонусов за подтверждённый consent в sync-worker.
- `WEBHOOK` — HTTP path входящего webhook (по умолчанию `/webhook`).
- `DATABASE_URL` — внешняя Postgres БД (в compose локальная postgres больше не поднимается).
- `LOKI_URL` — URL Loki (обязателен, логирование только в Loki).
- `LOKI_USERNAME` / `LOKI_PASSWORD` — Basic Auth для Loki.

## Tracing

- Для входящего webhook можно передать:
  - `X-Trace-Id` — сквозной trace идентификатор.
  - `X-Event-Id` — идентификатор исходного события.
- Если заголовки не переданы, сервис сгенерирует их автоматически.
- Эти поля прокидываются в RabbitMQ (`correlation_id`, `message_id`) и в payload (`trace_id`, `source_event_id`), затем попадают в логи consumers и в `merge_log`.

## Email Validation

- В `CREATE/UPDATE` перед синком в Listmonk валидируется `pass.email`.
- Если email невалидный:
  - вызовов Listmonk SDK нет (не создаём и не обновляем subscriber),
  - в Teyca отправляется `PUT /passes/{user_id}` с `key1=blocked`,
  - если для `user_id` уже есть запись в `listmonk_users`, локально сохраняется `status=blocked` и `consent_pending=false`.
- Если email исправили и пришёл следующий `UPDATE` с валидным email, работает обычный flow: `upsert_subscriber` + `set_consent_pending=true`.
