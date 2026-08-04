.PHONY: up down migrate test test-unit test-integration coverage lint complexity deadcode deps-audit security docs-coverage refurb-check quality quality-report typecheck typecheck-tests consent-sync-once reconcile-once listmonk-refresh-subscriber-ids listmonk-refresh-subscriber-ids-apply external-dispatcher-once external-dispatcher-listmonk-once external-dispatcher-merge-once external-dispatcher-invalid-email-once external-dispatcher-consent-block-once consumers legacy-import legacy-import-dry-run consent-bonus-backfill consent-bonus-backfill-apply consent-block-backfill consent-block-backfill-apply

PYTHON ?= ./.venv/bin/python
PYTEST ?= ./.venv/bin/pytest
BASEDPYRIGHT ?= ./.venv/bin/basedpyright
RUFF ?= ./.venv/bin/ruff
VULTURE ?= ./.venv/bin/vulture
DEPTRY ?= ./.venv/bin/deptry
BANDIT ?= ./.venv/bin/bandit
INTERROGATE ?= ./.venv/bin/interrogate
RADON ?= ./.venv/bin/radon
XENON ?= ./.venv/bin/xenon
REFURB ?= ./.venv/bin/refurb
HEARTBEAT_DIR ?= /tmp/teyca-sync-heartbeat
COVERAGE_FAIL_UNDER ?= 80
QUALITY_TIMEOUT_SECONDS ?= 300

up:
	docker compose up -d --build

down:
	docker compose down

migrate:
	docker compose run --rm --build app alembic upgrade head

test:
	HEARTBEAT_DIR=$(HEARTBEAT_DIR) $(PYTHON) -m pytest tests/ -v

test-unit:
	HEARTBEAT_DIR=$(HEARTBEAT_DIR) $(PYTEST) tests/unit/ -v

test-integration:
	HEARTBEAT_DIR=$(HEARTBEAT_DIR) $(PYTEST) tests/integration/ -v

coverage:
	HEARTBEAT_DIR=$(HEARTBEAT_DIR) $(PYTEST) tests/ -v --cov=app --cov-report=term-missing --cov-report=html --cov-fail-under=$(COVERAGE_FAIL_UNDER)

lint:
	$(RUFF) check .

complexity:
	$(RUFF) check app tests --select C901,PLR0911,PLR0912,PLR0915
	$(RADON) cc app tests -s -a
	$(XENON) --max-absolute B --max-modules A --max-average A app tests

deadcode:
	$(VULTURE) app tests --min-confidence 80

deps-audit:
	$(DEPTRY) app --config pyproject.toml

security:
	$(BANDIT) -r app -c pyproject.toml

docs-coverage:
	$(INTERROGATE) app

refurb-check:
	$(REFURB) app tests

quality:
	$(MAKE) lint
	$(MAKE) typecheck
	$(MAKE) typecheck-tests
	$(MAKE) complexity
	$(MAKE) deadcode
	$(MAKE) deps-audit
	$(MAKE) security
	$(MAKE) docs-coverage
	$(MAKE) refurb-check
	$(MAKE) coverage

quality-report:
	@status=0; \
	for check in \
		"$(MAKE) lint" \
		"$(MAKE) typecheck" \
		"$(MAKE) typecheck-tests" \
		"$(MAKE) complexity" \
		"$(MAKE) deadcode" \
		"$(MAKE) deps-audit" \
		"$(MAKE) security" \
		"$(MAKE) docs-coverage" \
		"$(MAKE) refurb-check" \
		"$(MAKE) coverage"; \
	do \
		printf '\n===== %s =====\n' "$$check"; \
		if ! timeout $(QUALITY_TIMEOUT_SECONDS) $$check; then status=1; fi; \
	done; \
	exit $$status

typecheck:
	$(BASEDPYRIGHT)

typecheck-tests:
	$(BASEDPYRIGHT) --project pyrightconfig.tests.json

consent-sync-once:
	docker compose run --rm app python -m app.workers.run_consent_sync

reconcile-once:
	docker compose run --rm app python -m app.workers.run_listmonk_reconcile

listmonk-refresh-subscriber-ids:
	docker compose run --rm --build app python -m app.workers.run_listmonk_refresh_subscriber_ids --batch-size "$${BATCH_SIZE:-100}" --concurrency "$${CONCURRENCY:-10}"

listmonk-refresh-subscriber-ids-apply:
	docker compose run --rm --build app python -m app.workers.run_listmonk_refresh_subscriber_ids --apply --batch-size "$${BATCH_SIZE:-100}" --concurrency "$${CONCURRENCY:-10}"

external-dispatcher-once:
	docker compose run --rm app python -m app.workers.run_external_dispatcher

external-dispatcher-listmonk-once:
	docker compose run --rm app python -m app.workers.run_external_dispatcher_listmonk

external-dispatcher-merge-once:
	docker compose run --rm app python -m app.workers.run_external_dispatcher_merge

external-dispatcher-invalid-email-once:
	docker compose run --rm app python -m app.workers.run_external_dispatcher_invalid_email

external-dispatcher-consent-block-once:
	docker compose run --rm app python -m app.workers.run_external_dispatcher_consent_block

consumers:
	docker compose run --rm app python -m app.workers.run_queue_consumers

legacy-import:
	docker compose run --rm --build app python -m app.workers.run_legacy_snapshot_import --source-db-url "$$SOURCE_DB_URL" --batch-size "$${BATCH_SIZE:-500}"

legacy-import-dry-run:
	docker compose run --rm --build app python -m app.workers.run_legacy_snapshot_import --source-db-url "$$SOURCE_DB_URL" --dry-run --batch-size "$${BATCH_SIZE:-500}"

consent-bonus-backfill:
	docker compose run --rm --build app python -m app.workers.run_consent_bonus_backfill

consent-bonus-backfill-apply:
	docker compose run --rm --build app python -m app.workers.run_consent_bonus_backfill --apply

consent-block-backfill:
	docker compose run --rm --build app python -m app.workers.run_consent_block_backfill

consent-block-backfill-apply:
	docker compose run --rm --build app python -m app.workers.run_consent_block_backfill --apply
