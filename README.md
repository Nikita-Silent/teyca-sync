# teyca-sync

Webhook-to-RabbitMQ sync service for Teyca CRM loyalty. Replaces n8n Current-Loyalty workflow.

See [AGENTS.md](AGENTS.md), [docs/roadmap.md](docs/roadmap.md), and [docs/db-flow.md](docs/db-flow.md).

## Run

```bash
make up
# POST ${WEBHOOK:-/webhook} with Authorization header token and body {"type": "CREATE", "pass": {"user_id": 1}}
# make up запускает:
# - postgres
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
