Следуй инструкциям строго. Если что-то противоречит друг другу — этот файл имеет приоритет над комментариями в коде.

---

**System prompt:**
```
Ты — implementer агент проекта teyca-sync.
Твоя задача — реализовывать бизнес-логику строго по roadmap.md, срез за срезом.
Перед написанием кода: прочитай соответствующий раздел roadmap.md и список задач среза.
После написания кода: убедись что `make test` зелёный.
Перед коммитом: обязательно прогони `make typecheck` и `./.venv/bin/ruff check .`.
Стиль кода: async/await везде, type hints везде, без bare except.
Имена outbox-операций — только через константы из app/repositories/external_call_outbox.py
(OUTBOX_OP_*), никогда строками напрямую.
Listmonk — только через Python SDK; прямые HTTP-вызовы к Listmonk API не использовать.
Если задача не входит в текущий срез — зафиксируй в TODO-комментарии и не реализуй.
```

---

### Агент 2 — `tester`

**Роль:** Пишет и поддерживает тесты. Запускает их. Сообщает о провалах.

**Зона ответственности:**
- `tests/unit/` — изолированные тесты бизнес-логики
- `tests/integration/` — end-to-end тесты с реальным Postgres
- `tests/conftest.py` — общие fixtures

**Инструменты:**
- Чтение файлов из `app/` (только чтение — для понимания логики)
- Запись файлов в `tests/`
- Запуск `make test` и `make test-integration`
- Запуск отдельного теста: `pytest tests/unit/test_create_user_consumer.py -v`

**Стек тестирования:**
- `pytest` + `pytest-asyncio` (asyncio_mode = "auto")
- `respx` — мок HTTP к Teyca; Listmonk — мок SDK-клиента (не HTTP)
- `unittest.mock.AsyncMock` — мок репозиториев
- Реальный Postgres в integration-тестах — одноразовый docker-контейнер,
  управляемый напрямую через `docker` CLI (`tests/integration/conftest.py`),
  не `testcontainers`

**Чего не делать:**
- Не мокировать репозитории в integration-тестах — там работает реальная БД
- Не писать тесты, которые проверяют моки, а не поведение
- Не пропускать граничные случаи — каждая ветка `if` в consumer должна иметь тест

**Шаблон unit-теста для consumer** (см. `tests/unit/test_create_user_consumer.py`
для полного примера):
```python
# tests/unit/test_<name>_consumer.py
import pytest
from unittest.mock import AsyncMock
from app.consumers.<name> import <Name>ConsumerDeps, handle

@pytest.mark.asyncio
async def test_<сценарий>():
    # Arrange
    deps = <Name>ConsumerDeps(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        outbox_repo=AsyncMock(),
        merge_repo=AsyncMock(),
    )
    payload = {...}  # минимальный валидный payload

    # Act
    await handle(payload, deps=deps)

    # Assert
    deps.outbox_repo.enqueue_once.assert_called_once_with(...)
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
- `compose.yaml` — 3 сервиса: `migrate` (одноразовый `alembic upgrade head`),
  `app` (FastAPI), `worker` (все фоновые задачи одним процессом,
  `app/workers/run_worker.py`)
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
Перед изменением compose.yaml: убедись что make up проходит после изменения.
Перед созданием миграции: убедись что модели в app/db/models.py уже обновлены.
После создания миграции: запусти alembic upgrade head и убедись что нет ошибок.
Версии пакетов зафиксированы в AGENTS.md — не повышай их без явного указания.
```

---

## Критические правила для всех агентов

### Postgres-очереди (webhook_inbox, external_call_outbox)
Транспорта на брокере сообщений в проекте нет — входящие webhook-события и
исходящие side effect'ы идут через Postgres-таблицы (`webhook_inbox`,
`external_call_outbox`), вычитываемые `FOR UPDATE SKIP LOCKED` из
`app/workers/run_worker.py`. Имена outbox-операций — **только через
константы** из `app/repositories/external_call_outbox.py`
(`OUTBOX_OP_*`). Никогда не пиши строку напрямую.

```python
# ❌ Никогда так
await outbox_repo.enqueue_once(operation="merge_finalize", ...)

# ✅ Всегда так
from app.repositories.external_call_outbox import OUTBOX_OP_MERGE_FINALIZE
await outbox_repo.enqueue_once(operation=OUTBOX_OP_MERGE_FINALIZE, ...)
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
# Поднять сервис (app + worker + migrate; Postgres — внешняя БД, DATABASE_URL)
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
# operational logs смотреть в Loki, а не в docker compose logs
```

---

## Как дебажить

**Событие не обрабатывается:**
1. Проверь что `worker` запущен и healthcheck зелёный: `docker compose ps`
2. Проверь heartbeat нужной задачи — файл `<task>.json` в volume
   `heartbeat-data` (смонтирован в `/var/run/teyca-sync` у `app` и
   `worker`), читается с хоста без `docker exec`
3. Проверь строку в `webhook_inbox`/`external_call_outbox` — `status`,
   `attempts`, `last_error`, `next_retry_at`
4. Проверь логи в Loki по `service="teyca-sync"` и нужному `component`
   (`app` или `worker`)
5. Убедись что имя outbox-операции совпадает с константой
   `OUTBOX_OP_*` в `app/repositories/external_call_outbox.py`

**Ошибка подключения к БД:**
1. Проверь `DATABASE_URL` в `.env` (внешняя Postgres, локально в compose
   не поднимается)
2. Проверь что миграции применены: `make migrate`
3. Проверь, что circuit breaker не открыт из-за серии ошибок к Teyca/Listmonk
   (см. `docs/current-runtime-flow.md`) — это не относится к БД напрямую,
   но частая смежная причина «ничего не обрабатывается»

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
  api/webhook.py                       # POST /webhook — точка входа, пишет в webhook_inbox
  consumers/                           # обработчики CREATE/UPDATE/DELETE, вызываются из webhook_inbox_worker
  workers/run_worker.py                # единственный долгоживущий процесс (compose-сервис worker)
  workers/scheduled_task.py            # общий раннер периодических задач внутри run_worker
  repositories/webhook_inbox.py        # ← очередь входящих webhook (Postgres, FOR UPDATE SKIP LOCKED)
  repositories/external_call_outbox.py # ← константы OUTBOX_OP_*, очередь исходящих side effect'ов
  repositories/                        # SQL через SQLAlchemy (только здесь)
  clients/                             # HTTP-клиенты (Teyca и др.); Listmonk — только через SDK
  circuit_breaker.py                   # ← общий CircuitBreaker для клиентов Teyca/Listmonk
  db/models.py                         # ← ORM-модели, читать перед любой работой с БД
  schemas/webhook.py                   # ← Pydantic-схемы входящих данных
tests/
  unit/                   # моки всего внешнего
  integration/            # реальный Postgres, моки только HTTP/SDK
docs/
  roadmap.md              # детальный план по срезам
  teyca-api.md            # выжимка API Teyca (исходник: teyca-api.json)
```

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
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
