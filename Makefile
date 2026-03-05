.PHONY: up down migrate test test-unit test-integration consent-sync-once

up:
	docker compose up -d

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
	python -m app.workers.run_consent_sync
