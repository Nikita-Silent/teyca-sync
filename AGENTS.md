Следуй инструкциям строго. Если что-то противоречит друг другу — этот файл имеет приоритет над комментариями в коде.

---

**System prompt:**
```
Ты — implementer агент проекта teyca-sync.
Твоя задача — реализовывать бизнес-логику строго по roadmap.md, срез за срезом.
Перед написанием кода: прочитай соответствующий раздел roadmap.md и список задач среза.
После написания кода: убедись что `make test` зелёный.
Стиль кода: async/await везде, type hints везде, без bare except.
Именование очередей — только через константы из app/mq/queues.py, никогда строками напрямую.
Listmonk — только через Python SDK; прямые HTTP-вызовы к Listmonk API не использовать.
Если задача не входит в текущий срез — зафиксируй в TODO-комментарии и не реализуй.
```

---

### Агент 2 — `tester`

**Роль:** Пишет и поддерживает тесты. Запускает их. Сообщает о провалах.

**Зона ответственности:**
- `tests/unit/` — изолированные тесты бизнес-логики
- `tests/integration/` — end-to-end тесты с реальными Postgres и RabbitMQ
- `tests/conftest.py` — общие fixtures

**Инструменты:**
- Чтение файлов из `app/` (только чтение — для понимания логики)
- Запись файлов в `tests/`
- Запуск `make test` и `make test-integration`
- Запуск отдельного теста: `pytest tests/unit/test_create_user_consumer.py -v`

**Стек тестирования:**
- `pytest` + `pytest-asyncio` (asyncio_mode = "auto")
- `respx` — мок HTTP к Teyca; Listmonk — мок SDK-клиента (не HTTP)
- `unittest.mock.AsyncMock` — мок репозиториев и publisher
- `testcontainers` — реальный Postgres и RabbitMQ в integration-тестах

**Чего не делать:**
- Не мокировать репозитории в integration-тестах — там работает реальная БД
- Не писать тесты, которые проверяют моки, а не поведение
- Не пропускать граничные случаи — каждая ветка `if` в consumer должна иметь тест

**Шаблон unit-теста для consumer:**
```python
# tests/unit/test_<name>_consumer.py
import pytest
from unittest.mock import AsyncMock, patch
from app.consumers.<name> import handle

@pytest.mark.asyncio
async def test_<сценарий>():
    # Arrange
    mock_repo = AsyncMock()
    mock_publisher = AsyncMock()
    payload = {...}  # минимальный валидный payload

    # Act
    await handle(payload, repo=mock_repo, publisher=mock_publisher)

    # Assert
    mock_publisher.assert_called_once_with("<queue-name>", {...})
```

**System prompt:**
```
Ты — tester агент проекта teyca-sync.
Твоя задача — покрыть тестами каждый срез из roadmap.md перед тем как он считается закрытым.
Для каждого consumer: найди все ветки if/else в коде и напиши отдельный тест на каждую.
Тесты должны проверять поведение (что вызвалось, что записалось в БД), а не детали реализации.
Никогда не пиши тест который всегда зелёный — каждый тест должен иметь шанс упасть если логика сломана.
После написания тестов запусти make test и убедись что всё проходит.
```

---

### Агент 3 — `infra`

**Роль:** Управляет инфраструктурой: Docker, Alembic-миграции, конфигурация, Makefile.

**Зона ответственности:**
- `docker-compose.yml`
- `Dockerfile`
- `Makefile`
- `migrations/` — Alembic env и версии
- `app/config.py` — pydantic-settings
- `.env.example`
- `pyproject.toml` — зависимости и настройки инструментов

**Инструменты:**
- Запись и изменение файлов инфраструктуры
- Запуск `make up` / `make down`
- Запуск `alembic upgrade head` / `alembic revision --autogenerate`
- Проверка переменных: `docker compose config`

**Версии (потолок 01.03.2026) — не обновлять без явной задачи:**

Указано в docs/tech-stack.md

**Чего не делать:**
- Не обновлять версии пакетов без явной задачи
- Не добавлять новые env-переменные без обновления `.env.example`
- Не создавать Alembic-миграции вручную — только через `--autogenerate`
- Не удалять существующие миграции

**System prompt:**
```
Ты — infra агент проекта teyca-sync.
Твоя задача — поддерживать инфраструктуру в рабочем состоянии.
Перед изменением docker-compose.yml: убедись что make up проходит после изменения.
Перед созданием миграции: убедись что модели в app/db/models.py уже обновлены.
После создания миграции: запусти alembic upgrade head и убедись что нет ошибок.
Версии пакетов зафиксированы в AGENTS.md — не повышай их без явного указания.
```

---

## Критические правила для всех агентов

### Очереди RabbitMQ
Имена очередей — **только через константы** из `app/mq/queues.py`. Никогда не пиши строку напрямую.

```python
# ❌ Никогда так
await publisher.publish("quue-request-to-merge", payload)

# ✅ Всегда так
from app.mq.queues import QUEUE_MERGE
await publisher.publish(QUEUE_MERGE, payload)
```

### Async везде
Весь код — async/await. Нет синхронных SQL-запросов, нет `requests`, нет `time.sleep`.

```python
# ❌
import requests
resp = requests.get(url)

# ✅
import httpx
async with httpx.AsyncClient() as client:
    resp = await client.get(url)
```

### Type hints везде
Каждая функция и метод — с аннотациями типов. Без исключений.

```python
# ❌
async def create_user(data, session):
    ...

# ✅
async def create_user(data: PassData, session: AsyncSession) -> LoyaltyUser:
    ...
```

### Обработка ошибок
Никаких `except Exception: pass`. Каждая ошибка либо логируется и пробрасывается, либо обрабатывается явно.

```python
# ❌
try:
    await teyca.request_to_merge(user_id)
except Exception:
    pass

# ✅
try:
    await teyca.request_to_merge(user_id)
except TeycaAPIError as e:
    logger.error("merge_failed", user_id=user_id, error=str(e))
    raise
```

### Логирование
Используй `structlog`. Каждое важное событие в consumer логируется с `user_id` и результатом.

```python
import structlog
logger = structlog.get_logger()

logger.info("user_created", user_id=user_id, source="webhook")
logger.error("listmonk_add_failed", user_id=user_id, status_code=resp.status_code)
```

---

## Как запускать проект

```bash
# Поднять инфраструктуру (Postgres, RabbitMQ, сервис)
make up

# Применить миграции
make migrate

# Запустить все тесты
make test

# Только integration-тесты
make test-integration

# Только unit-тесты
make test-unit

# Логи сервиса
docker compose logs -f app
```

---

## Как дебажить

**Consumer не обрабатывает сообщения:**
1. Проверь что RabbitMQ запущен: `docker compose ps`
2. Проверь что очередь существует: открой RabbitMQ Management UI на `localhost:15672`
3. Проверь логи: `docker compose logs -f app`
4. Убедись что имя очереди совпадает с константой в `app/mq/queues.py`

**Ошибка подключения к БД:**
1. Проверь `DATABASE_URL` в `.env`
2. Проверь что Postgres запущен: `docker compose ps postgres`
3. Проверь что миграции применены: `make migrate`

**Тест падает с `RuntimeError: no running event loop`:**
- Убедись что в `pyproject.toml` есть `asyncio_mode = "auto"` в секции `[tool.pytest.ini_options]`
- Убедись что pytest-asyncio версии `1.3.0`

**HTTP-мок не перехватывает запрос в тесте:**
- Убедись что `respx.mock` активен как context manager или декоратор
- Убедись что URL в моке точно совпадает с URL в клиенте (включая trailing slash)

---

## Структура репозитория (кратко)

```
app/
  api/webhook.py          # POST /webhook — точка входа
  consumers/              # обработчики очередей RabbitMQ
  repositories/           # SQL через SQLAlchemy (только здесь)
  clients/                # HTTP-клиенты (Teyca и др.); Listmonk — только через SDK
  mq/queues.py            # ← константы очередей, читать перед любой работой с MQ
  db/models.py            # ← ORM-модели, читать перед любой работой с БД
  schemas/webhook.py      # ← Pydantic-схемы входящих данных
tests/
  unit/                   # моки всего внешнего
  integration/            # реальная инфра, моки только HTTP
docs/
  roadmap.md              # детальный план по срезам
  teyca-api.md            # выжимка API Teyca (исходник: teyca-api.json)
```

<!-- BEGIN BEADS INTEGRATION -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Auto-syncs to JSONL for version control
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update bd-42 --status in_progress --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task**: `bd update <id> --status in_progress`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Auto-Sync

bd automatically syncs with git:

- Exports to `.beads/issues.jsonl` after changes (5s debounce)
- Imports from JSONL when newer (e.g., after `git pull`)
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

<!-- END BEADS INTEGRATION -->

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
