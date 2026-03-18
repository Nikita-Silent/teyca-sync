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
make test
```

- `ruff` — линт и формат правил проекта.
- `basedpyright` — type check в режиме `basic` по `app/` и `migrations/`.
- `make test` — полный unit/integration набор, доступный в текущем репозитории.

## Env

- `CONSENT_BONUS_AMOUNT` — количество бонусов за подтверждённый consent в sync-worker.
- `WEBHOOK_AUTH_ENABLED` — включает/выключает проверку `Authorization` для webhook (`true`/`false`).
- `WEBHOOK` — HTTP path входящего webhook (по умолчанию `/webhook`).
- `DATABASE_URL` — внешняя Postgres БД (в compose локальная postgres больше не поднимается).
- `LOKI_URL` — URL Loki (обязателен, логирование только в Loki).
- `LOKI_USERNAME` / `LOKI_PASSWORD` — Basic Auth для Loki.
- `LOG_COMPONENT` — label `component` для Loki (`app`, `consumers`, `reconcile`, `consent-sync`).

## Process Flow

- Teyca шлёт `CREATE` / `UPDATE` / `DELETE` webhook в FastAPI.
- FastAPI валидирует `Authorization`, добавляет `trace_id` / `source_event_id` и публикует сообщение в RabbitMQ.
- `queue-consumers` читают сообщение, обновляют `users`, `listmonk_users`, `merge_log` и синхронизируют Listmonk через Python SDK.
- `consent-sync` периодически читает изменившихся подписчиков из Listmonk, подтверждает consent в Teyca и начисляет бонусы.
- `listmonk-reconcile` восстанавливает потерянные связи `subscriber_id -> user_id`.
- `email-repair` разбирает duplicate email кейсы через `email_repair_log`, определяет winner по Listmonk и очищает loser'ов локально и в Teyca.
- `listmonk-duplicate-subscriber` запускается вручную как repair-flow для duplicate `subscriber_id` в `listmonk_users`: выбирает winner по `Listmonk attributes.user_id`, loser'ов архивирует и удаляет.

Подтверждённый контракт Teyca:
- `PUT /passes/{user_id}` ведёт себя как partial update.
- Это проверено живым запросом 2026-03-18 на тестовой карте `user_id=5722735`: `PUT {"key6":"put-check"}` изменил только `key6`, остальные поля сохранились.

## Teyca API limits

- Исходящие вызовы в Teyca ограничиваются в клиенте скользящими окнами:
  - `5` запросов в секунду
  - `150` запросов в минуту
  - `1000` запросов в час
  - `5000` запросов в день
- При достижении лимита запрос не падает, а ждёт до освобождения окна и отправляется позже.

## Ошибки Teyca API

- Если Teyca вернул `4xx/5xx`, `TeycaClient` бросает `TeycaAPIError`.
- В queue-consumers (`CREATE/UPDATE/DELETE`):
  - DB транзакция откатывается,
  - сообщение `reject(requeue=true)` и будет обработано повторно.
- Исключение: duplicate email в `listmonk_users` не считается transient-ошибкой обработки.
  - consumer пишет запись в `email_repair_log`,
  - логирует `*_consumer_duplicate_email_scheduled`,
  - завершает обработку без `requeue`, чтобы не клинить очередь.
- В `consent-sync`:
  - ошибка логируется,
  - пользователь остаётся `consent_pending=true`,
  - обработка повторяется в следующих запусках.
- В `email-repair`:
  - ошибка resolution/Teyca cleanup не возвращает исходный webhook в очередь,
  - запись в `email_repair_log` переводится в `failed` или `manual_review` с bounded retry.

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

## Duplicate Email Remediation

- Если `CREATE/UPDATE` упирается в локальный duplicate email в `listmonk_users`, consumer не делает бесконечный retry.
- Вместо этого создаётся `email_repair_log` со статусом `pending`.
- Local duplicate pre-check выполняется до mutating вызова в Listmonk, поэтому в этом сценарии сам Listmonk не успевает обновиться.
- Отдельный `email-repair` worker:
  - ищет authoritative subscriber в Listmonk по email,
  - выбирает winner по совпавшему `subscriber_id`,
  - loser'ам очищает `email` в `users` и `listmonk_users`,
  - winner'у отправляет в Teyca `PUT /passes/{user_id}` с `key6="bugs"`,
  - loser'у отправляет в Teyca `PUT /passes/{user_id}` с `email=null`, `key1="bad email"`, `key6="bugs"`,
  - помечает repair как `teyca_synced`, `failed` или `manual_review`.
- Это нужно, чтобы очередь `queue-update`/`queue-create` не зацикливалась на одном конфликте и данные могли актуализироваться дальше.

## Duplicate Subscriber Remediation

- `listmonk_users.subscriber_id` теперь защищён unique constraint на уровне БД.
- До финального constraint rollout текущие дубли очищаются отдельным repair-worker:
  ```bash
  docker compose run --rm app python -m app.workers.run_listmonk_duplicate_subscriber
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
