# teyca-sync

Webhook-to-RabbitMQ sync service for Teyca CRM loyalty. Replaces n8n Current-Loyalty workflow.

See [AGENTS.md](AGENTS.md) and [docs/roadmap.md](docs/roadmap.md).

## Run

```bash
make up
make migrate
# POST /webhook with JWT and body {"type": "CREATE", "pass": {"user_id": 1}}
make consent-sync-once
```

## Test

```bash
make test
```
