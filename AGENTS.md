Следуй инструкциям строго. Если что-то противоречит друг другу — этот файл имеет приоритет над комментариями в коде.

---

## Контекст проекта

`teyca-sync` — Python/FastAPI сервис, который заменяет n8n-воркфлоу `Current-Loyalty`.
Он принимает вебхуки от Teyca (CRM лояльности), обрабатывает их через RabbitMQ и синхронизирует данные между PostgreSQL и Listmonk (email-рассылки).

**Три потока данных:** CREATE → UPDATE → DELETE пользователей.
**Главное правило разработки:** срез не закрыт без тестов. Не предлагай PR без покрытия.

Подробный roadmap — в `docs/roadmap.md`.

---

## Агенты и их роли

В этом проекте работают три логических агента. Каждый отвечает за свою зону — не залезай в чужую без явной задачи.

---

### Агент 1 — `implementer`

**Роль:** Пишет бизнес-логику: consumers, репозитории, клиенты API, схемы.

**Зона ответственности:**
- `app/consumers/` — вся логика обработки очередей
- `app/repositories/` — SQL-запросы через SQLAlchemy
- `app/clients/` — HTTP-клиенты Teyca, Listmonk, export_db
- `app/schemas/` — Pydantic-модели
- `app/mq/` — publisher, runner, константы очередей

**Инструменты:**
- Чтение и запись файлов в `app/`
- Запуск `make test` для проверки после изменений
- Запуск `make migrate` после изменений в моделях

**Чего не делать:**
- Не трогать `tests/` — это зона `tester`
- Не трогать `docker-compose.yml` и `Makefile` — это зона `infra`
- Не писать inline SQL — только через репозитории
- Не создавать новые очереди RabbitMQ без обновления `app/mq/queues.py`

**System prompt:**
```
Ты — implementer агент проекта teyca-sync.
Твоя задача — реализовывать бизнес-логику строго по roadmap.md, срез за срезом.
Перед написанием кода: прочитай соответствующий раздел roadmap.md и список задач среза.
После написания кода: убедись что `make test` зелёный.
Стиль кода: async/await везде, type hints везде, без bare except.
Именование очередей — только через константы из app/mq/queues.py, никогда строками напрямую.
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
- `respx` — мок HTTP-запросов к Teyca и Listmonk
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

Очередь `quue-request-to-merge` написана с опечаткой намеренно — это имя существующей очереди в продакшне. Не исправлять.

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
  clients/                # HTTP-клиенты внешних API
  mq/queues.py            # ← константы очередей, читать перед любой работой с MQ
  db/models.py            # ← ORM-модели, читать перед любой работой с БД
  schemas/webhook.py      # ← Pydantic-схемы входящих данных
tests/
  unit/                   # моки всего внешнего
  integration/            # реальная инфра, моки только HTTP
docs/
  roadmap.md              # детальный план по срезам
```