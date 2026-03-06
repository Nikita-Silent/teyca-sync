.PHONY: up down migrate test test-unit test-integration consent-sync-once reconcile-once consumers

up:
	docker compose up -d --build

down:
	docker compose down

migrate:
	docker compose run --rm app alembic upgrade head

test:
	python -m pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

consent-sync-once:
	docker compose run --rm app python -m app.workers.run_consent_sync

reconcile-once:
	docker compose run --rm app python -m app.workers.run_listmonk_reconcile

consumers:
	docker compose run --rm app python -m app.workers.run_queue_consumers
