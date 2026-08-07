# teyca-sync

Webhook-to-Postgres sync service for Teyca CRM loyalty. Replaces n8n Current-Loyalty workflow.

See [AGENTS.md](AGENTS.md), [docs/roadmap.md](docs/roadmap.md), and [docs/db-flow.md](docs/db-flow.md).

## Run

```bash
make up
# POST ${WEBHOOK:-/webhook} with Authorization header token and body {"type": "CREATE", "pass": {"user_id": 1}}
# make up запускает:
# - migrate (alembic upgrade head)
# - app (FastAPI: принимает webhook, пишет в webhook_inbox)
# - worker (все фоновые задачи в одном процессе, см. Process Flow)
```

- Для безопасного локального smoke-run можно переопределить env-файл: `COMPOSE_ENV_FILE=.env.safe docker compose up -d --build`.

## Migrations

```bash
make migrate
```

- `make migrate` запускает `docker compose run --rm --build app alembic upgrade head`.
- `--build` обязателен: Alembic работает внутри Docker-образа, и без пересборки контейнер может не увидеть свежие файлы в `migrations/versions`.

## Test

```bash
make test
```

## Quality Gates

```bash
./.venv/bin/ruff check .
./.venv/bin/basedpyright
./.venv/bin/basedpyright --project pyrightconfig.tests.json
make test
make coverage
make complexity
make deadcode
make deps-audit
make security
make docs-coverage
make refurb-check
make quality-report
```

Перед любым коммитом обязательны как минимум:

```bash
./.venv/bin/ruff check .
make typecheck
make test
```

- `ruff` — линт и формат правил проекта.
- `basedpyright` — основной type check в режиме `basic` по `app/` и `migrations/`.
- `basedpyright --project pyrightconfig.tests.json` — отдельный rollout для типизации `tests/`; это не основной gate runtime-кода.
- `make test` — полный unit/integration набор, доступный в текущем репозитории.
- `make coverage` — pytest с `pytest-cov` и порогом `COVERAGE_FAIL_UNDER` (по умолчанию `80`).
- `make complexity` — CCN/branch/statement checks через `ruff`, `radon`, `xenon`.
- `make deadcode` — поиск потенциально мёртвого кода через `vulture`.
- `make deps-audit` — проверка зависимостей через `deptry`.
- `make security` — статический security scan через `bandit`.
- `make docs-coverage` — покрытие docstring'ами через `interrogate`.
- `make refurb-check` — дополнительные modernization/refactor замечания через `refurb`.
- `make quality-report` — запускает все quality checks подряд и собирает общий список падений.
- `lefthook` — pre-commit запускает `ruff`, pre-push запускает `make quality-report`.

## Env

Полный список полей и дефолтов — `app/config.py`. Ключевые группы:

- `DATABASE_URL` — внешняя Postgres БД (в compose локальная postgres не поднимается). `DATABASE_POOL_SIZE`, `DATABASE_POOL_MAX_OVERFLOW`, `DATABASE_POOL_TIMEOUT_SECONDS`.
- `WEBHOOK_AUTH_ENABLED` — включает/выключает проверку `Authorization` для webhook (`true`/`false`).
- `WEBHOOK_AUTH_TOKEN` — статический токен, который присылает Teyca в `Authorization`.
- `WEBHOOK` — HTTP path входящего webhook (по умолчанию `/webhook`).
- `WEBHOOK_INBOX_*` — очередь входящих webhook-событий в Postgres (`webhook_inbox`, заменяет RabbitMQ): `WEBHOOK_INBOX_BATCH_SIZE`, `WEBHOOK_INBOX_POLL_INTERVAL_SECONDS`, `WEBHOOK_INBOX_MAX_CONCURRENCY`, `WEBHOOK_INBOX_STALE_CLAIM_SECONDS`, `WEBHOOK_INBOX_RETRY_*`, `WEBHOOK_INBOX_LOCK_BUSY_RETRY_*`, `WEBHOOK_INBOX_TEYCA_RATE_LIMIT_RETRY_*`.
- `EXTERNAL_DISPATCHER_*` — размер пачки, backoff и poll-интервал (`EXTERNAL_DISPATCHER_POLL_INTERVAL_SECONDS`) durable-dispatcher'ов для внешних вызовов Listmonk/Teyca; `EXTERNAL_DISPATCHER_TEYCA_RATE_LIMIT_MAX_WAIT_SECONDS` — сколько dispatcher готов ждать слот Teyca limiter перед deferred retry (по умолчанию `0`, то есть без inline wait).
- `CONSENT_SYNC_INTERVAL_SECONDS` (по умолчанию `3600`) / `LISTMONK_RECONCILE_INTERVAL_SECONDS` (по умолчанию `300`) — периодичность соответствующих фоновых задач внутри `worker`.
- `WORKER_SHUTDOWN_DRAIN_TIMEOUT_SECONDS` — сколько `worker` ждёт завершения текущих итераций всех задач при `SIGTERM`, прежде чем выйти всё равно с предупреждением в логах.
- `TEYCA_BASE_URL` / `TEYCA_API_KEY` / `TEYCA_TOKEN` — доступ к Teyca API; `TEYCA_RATE_LIMIT_PER_*` — исходящие лимиты (см. «Teyca API limits» ниже); `TEYCA_CIRCUIT_BREAKER_FAILURE_THRESHOLD` / `TEYCA_CIRCUIT_BREAKER_COOLDOWN_SECONDS` — см. «Circuit breaker» ниже.
- `LISTMONK_URL` / `LISTMONK_USER` / `LISTMONK_PASSWORD` / `LISTMONK_LIST_IDS` — доступ к Listmonk; `LISTMONK_CIRCUIT_BREAKER_FAILURE_THRESHOLD` / `LISTMONK_CIRCUIT_BREAKER_COOLDOWN_SECONDS` — см. «Circuit breaker».
- `CONSENT_BONUS_AMOUNT` — количество бонусов за подтверждённый consent в sync-worker; `CONSENT_BONUS_TTL_DAYS`, `CONSENT_SYNC_BATCH_SIZE`.
- `EXPORT_DB_URL` — старая read-only БД для merge при первом `CREATE`.
- `LOKI_URL` — URL Loki (обязателен, логирование только в Loki). `LOKI_USERNAME` / `LOKI_PASSWORD` — Basic Auth. `LOKI_REQUEST_TIMEOUT_SECONDS` — timeout на отправку одного batched log push в Loki, чтобы worker не зависал на shutdown.
- `LOG_COMPONENT` — label `component` для Loki (`app` или `worker`, соответствуют сервисам в `compose.yaml`).
- Все operational logs для диагностики нужно смотреть в Loki; `docker compose logs` не считать источником истины.

## Process Flow

- Teyca шлёт `CREATE` / `UPDATE` / `DELETE` webhook в FastAPI (`app` сервис).
- FastAPI валидирует `Authorization`, добавляет `trace_id` / `source_event_id` и пишет событие в Postgres-таблицу `webhook_inbox` (идемпотентно по `source_event_id` — повторная доставка того же события не создаёт вторую строку).
- Все фоновые задачи выполняются в одном процессе `worker` (`app/workers/run_worker.py`), каждая — периодическая `ScheduledTask` (`app/workers/scheduled_task.py`), опрашивающая свою Postgres-таблицу:
  - вычитывает `webhook_inbox`, обновляет локальную БД и пишет durable outbox (`external_call_outbox`) для внешних side effect'ов;
  - читает Listmonk-операции outbox и фиксирует локальный прогресс после успешного внешнего шага;
  - читает `merge_finalize` из outbox отдельным потоком, не деля FIFO с Listmonk-операциями;
  - читает `teyca_block_invalid_email` из outbox отдельно от Listmonk-операций;
  - периодически читает изменившихся подписчиков из Listmonk, подтверждает consent в Teyca и начисляет бонусы (`consent-sync`);
  - периодически восстанавливает потерянные связи `subscriber_id -> user_id` (`reconcile`).
- Каждая задача пишет heartbeat в `/var/run/teyca-sync/<task>.json` (смонтировано как volume, читаемо с хоста без `docker exec`); healthcheck `worker`-сервиса агрегирует свежесть всех задач через `app.service_health.all_worker_heartbeats_fresh()`.

## Circuit breaker

- Исходящие вызовы к Teyca и Listmonk защищены общим `CircuitBreaker` (`app/circuit_breaker.py`): closed → open после `N` подряд неудач → half-open (один пробный вызов) → closed или снова open.
- Пороги по умолчанию: `TEYCA_CIRCUIT_BREAKER_FAILURE_THRESHOLD=5` / `TEYCA_CIRCUIT_BREAKER_COOLDOWN_SECONDS=30`, `LISTMONK_CIRCUIT_BREAKER_FAILURE_THRESHOLD=5` / `LISTMONK_CIRCUIT_BREAKER_COOLDOWN_SECONDS=30`.
- Пока breaker открыт, вызовы отклоняются без реального обращения к сети (`TeycaAPIError`/`ListmonkClientError` с текстом `"Circuit breaker"`), что не тратит retry-бюджет и не блокирует остальную обработку.
- Поведение проверено интеграционными тестами: `tests/integration/test_teyca_fault_injection.py`, `tests/integration/test_listmonk_fault_injection.py`.

## Rollout

- `email-repair` разбирает duplicate email кейсы через `email_repair_log`, определяет winner по Listmonk и очищает loser'ов локально и в Teyca.
- `listmonk-duplicate-subscriber` запускается вручную как repair-flow для duplicate `subscriber_id` в `listmonk_users`: выбирает winner по `Listmonk attributes.user_id`, loser'ов архивирует и удаляет.

Подтверждённый контракт Teyca:
- `PUT /passes/{user_id}` ведёт себя как partial update.
- Это проверено живым запросом 2026-03-18 на тестовой карте `user_id=5722735`: `PUT {"key6":"put-check"}` изменил только `key6`, остальные поля сохранились.

## Teyca API limits

- Исходящие вызовы в Teyca ограничиваются скользящими окнами, посчитанными через Postgres-таблицу `teyca_call_budget` (общий для всех процессов бюджет, без Redis):
  - `5` запросов в секунду
  - `50` запросов в минуту
  - `500` запросов в час
  - `5000` запросов в день
- При достижении лимита запрос не падает, а откладывается до освобождения окна (`external_dispatcher_teyca_rate_limit_max_wait_seconds`/deferred retry).

## Ошибки Teyca API

- Если Teyca вернул `4xx/5xx`, `TeycaClient` бросает `TeycaAPIError`.
- При обработке `CREATE/UPDATE/DELETE` из `webhook_inbox`:
  - DB транзакция откатывается,
  - строка возвращается в `pending` с экспоненциальным backoff (`webhook_inbox.mark_retry`, счётчик `attempts`), после исчерпания `WEBHOOK_INBOX_MAX_RETRIES` — переходит в `dead`.
- Исключение: duplicate email в `listmonk_users` не считается transient-ошибкой обработки.
  - обработчик пишет запись в `email_repair_log`,
  - логирует `*_consumer_duplicate_email_scheduled`,
  - помечает событие `webhook_inbox` обработанным без повторов.
- В `consent-sync`:
  - ошибка логируется,
  - пользователь остаётся `consent_pending=true`,
  - обработка повторяется в следующих запусках.
- В `email-repair`:
  - ошибка resolution/Teyca cleanup не возвращает исходный webhook в обработку заново,
  - запись в `email_repair_log` переводится в `failed` или `manual_review` с bounded retry.

## Tracing

- Для входящего webhook можно передать:
  - `X-Trace-Id` — сквозной trace идентификатор.
  - `X-Event-Id` — идентификатор исходного события (используется как `source_event_id`, ключ идемпотентности `webhook_inbox`).
- Если заголовки не переданы, сервис сгенерирует их автоматически.
- Эти поля сохраняются в `webhook_inbox.trace_id`/строке payload (`trace_id`, `source_event_id`), затем попадают в логи и в `merge_log`.

## Email Validation

- В `CREATE/UPDATE` перед синком в Listmonk валидируется `pass.email`.
- Если email невалидный:
  - вызовов Listmonk SDK нет (не создаём и не обновляем subscriber),
  - в Teyca отправляется `PUT /passes/{user_id}` с `key1=blocked`,
  - если для `user_id` уже есть запись в `listmonk_users`, локально сохраняется `status=blocked` и `consent_pending=false`.
- Если email исправили и пришёл следующий `UPDATE` с валидным email, работает обычный flow: `upsert_subscriber` + `set_consent_pending=true`.

## Duplicate Email Remediation

- Если `CREATE/UPDATE` упирается в локальный duplicate email в `listmonk_users`, обработчик не делает бесконечный retry.
- Вместо этого создаётся `email_repair_log` со статусом `pending`.
- Local duplicate pre-check выполняется до mutating вызова в Listmonk, поэтому в этом сценарии сам Listmonk не успевает обновиться.
- Отдельный `email-repair` worker:
  - ищет authoritative subscriber в Listmonk по email,
  - выбирает winner по совпавшему `subscriber_id`,
  - loser'ам очищает `email` в `users` и `listmonk_users`,
  - winner'у отправляет в Teyca `PUT /passes/{user_id}` с `key6="bugs"`,
  - loser'у отправляет в Teyca `PUT /passes/{user_id}` с `email=null`, `key1="bad email"`, `key6="bugs"`,
  - помечает repair как `teyca_synced`, `failed` или `manual_review`.
- Это нужно, чтобы обработка одного и того же `CREATE`/`UPDATE`-конфликта не зацикливалась и данные могли актуализироваться дальше.

## Duplicate Subscriber Remediation

- `listmonk_users.subscriber_id` теперь защищён unique constraint на уровне БД.
- До финального constraint rollout текущие дубли очищаются отдельным repair-worker:
  ```bash
  docker compose run --rm app python -m service_workers.run_listmonk_duplicate_subscriber
  ```
- Worker:
  - находит `subscriber_id` с несколькими строками в `listmonk_users`,
  - читает subscriber из Listmonk через SDK,
  - берёт authoritative `attributes.user_id`,
  - если winner определяется однозначно, loser-строки пишет в `listmonk_user_archive` и удаляет из `listmonk_users`,
  - если winner не определяется, логирует `manual_review` и ничего не удаляет.
- Runtime-защита:
  - `CREATE/UPDATE` не создают вторую строку с тем же `subscriber_id`,
  - `consent-sync` и `reconcile` не зацикливаются на duplicate-subscriber кейсах и пропускают конфликтную запись.
- Для текущей диагностики:
  ```sql
  SELECT subscriber_id, COUNT(*)
  FROM public.listmonk_users
  GROUP BY subscriber_id
  HAVING COUNT(*) > 1;
  ```

## Listmonk Upsert Rules

- Перед вызовом Listmonk SDK:
  - `email` нормализуется (`strip`),
  - `list_ids` нормализуются (только положительные `int`, dedup, сортировка).
- Имя subscriber (`name`) строится по приоритету:
  - `pass.fio`,
  - `last_name first_name pat_name`,
  - fallback: `email`.
- Если `LISTMONK_LIST_IDS` пустой/невалидный, upsert не выполняется и бросается `ListmonkClientError`.
- Если при update прилетает конфликт уникальности email (`subscribers_email_key` / `409 conflict`):
  - выполняется fallback: поиск `subscriber_by_email(email)` и повторный update уже найденного subscriber.

## Grafana Dashboard

- Готовый dashboard JSON для Loki: [docs/grafana/teyca-sync-overview-dashboard.json](docs/grafana/teyca-sync-overview-dashboard.json)
- Импорт:
  - Grafana -> Dashboards -> New -> Import
  - вставь JSON из файла и выбери Loki datasource.
- Панели включают:
  - короткую инструкцию `How To Read This Dashboard`
  - summary stat: `Total Logs`, `Errors`, `Webhooks`, `Teyca Failures`
  - `Logs By Component` и `Errors By Event`
  - агрегированные графики `Consent Sync` и `Reconcile`
  - лог-панель `Recent Failures` для быстрого дебага.
