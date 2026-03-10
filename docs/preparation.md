# Подготовка к реализации

Что должно быть готово до начала реализации срезов (implementer + tester). Ответственный за пункты — **infra**, если не указано иное.

---

## 1. Инфраструктура

| # | Что | Статус | Примечание |
|---|-----|--------|------------|
| 1.1 | **pyproject.toml** | ⬜ | Зависимости: FastAPI, uvicorn, sqlalchemy[asyncio], alembic, asyncpg или psycopg, pika/aio-pika (RabbitMQ), httpx, pydantic-settings, structlog, listmonk SDK. Dev: pytest, pytest-asyncio, respx, testcontainers. `[tool.pytest.ini_options] asyncio_mode = "auto"`. |
| 1.2 | **Docker: compose.yaml** | ⬜ | Сервисы: app (FastAPI), postgres (17+), rabbitmq (4+). Сети и переменные для подключения. |
| 1.3 | **Makefile** | ⬜ | `up`, `down`, `migrate`, `test`, `test-unit`, `test-integration` по AGENTS.md. |
| 1.4 | **.env.example** | ⬜ | DATABASE_URL, RABBITMQ_URL, LISTMONK_*, WEBHOOK_AUTH_TOKEN, TEYCA_API_KEY, TEYCA_TOKEN, URL старой БД (export_db), при необходимости Loki. Все переменные, которые читает app/config. |
| 1.5 | **Alembic** | ⬜ | `migrations/` и env для async Postgres. Миграции создавать только через `--autogenerate` после изменения `app/db/models.py`. |

После 1.1–1.5: `make up` и `make migrate` должны выполняться без ошибок.

---

## 2. Каркас приложения (структура и заглушки)

Чтобы implementer мог начать **Срез 1** без «придумывания» путей и имён.

| # | Что | Статус | Примечание |
|---|-----|--------|------------|
| 2.1 | **app/config.py** | ⬜ | Pydantic-settings: чтение ENV (DATABASE_URL, RABBITMQ_*, LISTMONK_*, WEBHOOK_AUTH_TOKEN, EXPORT_DB_URL и т.д.). |
| 2.2 | **app/mq/queues.py** | ⬜ | Константы имён очередей, например: `QUEUE_CREATE = "queue-create"`, `QUEUE_UPDATE = "queue-update"`, `QUEUE_DELETE = "queue-delete"`. Использовать только их, не строки в коде. |
| 2.3 | **app/db/models.py** | ⬜ | ORM-модели: `User` (users), `ListmonkUser` (listmonk_users), `MergeLog` (merge_log) по описанию из roadmap.md. Без CASCADE на удаление. |
| 2.4 | **app/schemas/webhook.py** | ⬜ | Pydantic-модели входящего payload: тип события (`type`: CREATE/UPDATE/DELETE) и тело `pass` (user_id, email, fio, summ, check_summ и др.). Минимум для роутинга и Среза 1: `type`, `pass.user_id`. |
| 2.5 | **app/api/webhook.py** | ⬜ | Роут POST на путь из `WEBHOOK` (по умолчанию `/webhook`): приём JSON, валидация `Authorization` header (token), парсинг body в схему, публикация в RabbitMQ в очередь по `type`. |
| 2.6 | **app/main.py** | ⬜ | Подключить роут webhook, при необходимости CORS/middleware. |

После 2.1–2.6: POST на путь из `WEBHOOK` (по умолчанию `/webhook`) с валидным `Authorization` token и body `{\"type\": \"CREATE\", \"pass\": {\"user_id\": 1, ...}}` должен публиковать сообщение в нужную очередь (проверка через RabbitMQ Management или тест).

---

## 3. Данные и внешние системы

| # | Что | Статус | Примечание |
|---|-----|--------|------------|
| 3.1 | **API Teyca** | ⬜ | Спецификация: **teyca-api.json** (Postman-коллекция в корне репо). Выжимка по эндпоинтам и полям — [docs/teyca-api.md](teyca-api.md). Формат входящего webhook (type + pass) — по teyca-api.md; использовать для app/schemas/webhook.py и тестов. ENV: TEYCA_API_KEY, TEYCA_TOKEN для исходящих вызовов. |
| 3.2 | **Подключение к старой БД** | ⬜ | URL/учётные данные (read-only) в ENV; при Срезе 2 понадобится клиент/репозиторий для чтения исторических данных по user_id. |
| 3.3 | **Listmonk** | ⬜ | URL и параметры доступа для SDK в ENV; list_ids из конфига (LISTMONK_LIST_IDS). |

---

## 4. Тесты (минимальный запуск)

| # | Что | Статус | Примечание |
|---|-----|--------|------------|
| 4.1 | **tests/conftest.py** | ⬜ | Общие fixtures (pytest-asyncio, при необходимости testcontainers для integration). |
| 4.2 | **make test** | ⬜ | Запуск pytest; хотя бы один пустой/заглушечный тест, чтобы пайплайн не падал. |

---

## 5. Порядок работ

1. **Infra:** 1.1 → 1.2 → 1.4 → 1.3. Затем 1.5 после появления `app/db/models.py`.
2. **Каркас:** 2.1, 2.2, 2.3, 2.4, 2.5, 2.6 (можно параллельно с пунктом 1 после pyproject и compose).
3. **Данные:** 3.1 — до уточнения схемы webhook; 3.2 и 3.3 — до начала Среза 2.
4. **Тесты:** 4.1 и 4.2 — до того как tester начнёт писать тесты к Срезу 1.

После выполнения пунктов 1 (кроме 1.5, если модели ещё не зафиксированы), 2 и 4 можно считать подготовку к **Срезу 1** завершённой. Модели и миграции (1.5) нужны к началу **Среза 2**.
