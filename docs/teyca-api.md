# API Teyca — выжимка для teyca-sync

**Источник:** Postman-коллекция [teyca-api.json](../teyca-api.json).

**Базовый URL:** `https://api.teyca.ru`

**Авторизация:** заголовок `Authorization` с основным ключом API. Входящий webhook от Teyca верифицируется статическим токеном в том же заголовке (ENV: `WEBHOOK_AUTH_TOKEN`).

---

## Входящие вызовы (Teyca → наш сервис)

Teyca шлёт webhook на наш URL (POST). В теле запроса:

- `type` — тип события: `CREATE` | `UPDATE` | `DELETE`
- `pass` — объект карты лояльности: `user_id`, `email`, `phone`, `first_name`, `last_name`, `pat_name`, `birthday`, `gender`, `barcode`, `discount`, `bonus`, `loyalty_level`, `summ`, `summ_all`, `summ_last`, `visits`, `visits_all`, `date_last`, `city` и др. (схема в `app/schemas/webhook.py`).

---

## Синхронизация с Listmonk (важно для CREATE/UPDATE)

- Работа с Listmonk идёт через Python SDK (без прямого HTTP).
- Перед upsert:
  - `email` нормализуется (`strip`),
  - `list_ids` берутся из ENV и нормализуются (dedup, только положительные id).
- Если при update/create в Listmonk возникает конфликт уникальности email (`subscribers_email_key` / `409 conflict`), сервис делает fallback:
  - `subscriber_by_email(email)` -> `update_subscriber` найденного subscriber.
- Если `LISTMONK_LIST_IDS` пустой/невалидный, upsert не выполняется (ошибка конфигурации).
- Если локальная БД `listmonk_users` уже содержит тот же email у другого `user_id`, consumer не уходит в бесконечный retry:
  - создаётся запись в `email_repair_log`,
  - отдельный repair-worker позже определяет winner через Listmonk,
  - loser'ам email очищается локально и в Teyca.

---

## Исходящие вызовы (наш сервис → Teyca)

Во всех запросах в path нужен `token` (ENV: `TEYCA_API_KEY`, `TEYCA_TOKEN`). Авторизация: заголовок `Authorization` с основным ключом API.

### Лимиты Teyca API

При работе с Teyca API в `teyca-sync` применяются лимиты:

- `5` запросов в секунду
- `150` запросов в минуту
- `1000` запросов в час
- `5000` запросов в день

Лимиты применяются в клиенте как скользящие окна. В production они координируются через Redis, чтобы `consumers`, `consent-sync` и другие процессы делили один общий бюджет запросов к Teyca. При достижении лимита запрос ожидает свободное окно и выполняется позже.

### Что происходит при ошибке запроса в Teyca

- Если Teyca возвращает `4xx/5xx`, клиент выбрасывает `TeycaAPIError`.
- Для `CREATE/UPDATE/DELETE` consumers это приводит к rollback транзакции и `reject(requeue=true)` в RabbitMQ.
- Для `consent-sync` это приводит к сохранению `consent_pending=true` и повторной попытке на следующем запуске worker.
- Для `email-repair` это не возвращает исходное webhook-сообщение в RabbitMQ; ошибка сохраняется в `email_repair_log` с bounded retry.

### Карты (passes)

| Действие | Метод | Путь | Примечание |
|----------|--------|------|------------|
| Получение карты по `user_id` | GET | `/v1/{token}/passes/userid/{user_id}` | Чтение актуальной карты. |
| Создание карты | POST | `/v1/{token}/passes` | Body: `template`, `first_name`, `last_name`, `pat_name`, `phone`, `email`, `birthday`, `gender`, `barcode`, `discount`, `bonus`, `loyalty_level`. |
| Обновление карты (в т.ч. merge) | PUT | `/v1/{token}/passes/{user_id}` | Partial update полей карты (не начисление). Body: `bonus`, `loyalty_level`, `summ`, `summ_all`, `key2` и др. Пример: `{"bonus": 300, "loyalty_level": "Золотой"}`. |
| Удаление карты | DELETE | `/v1/{token}/passes/{user_id}` | — |

Подтверждённое поведение `PUT /passes/{user_id}`:

- На 2026-03-18 поведение проверено живым запросом на тестовой карте `user_id=5722735`.
- `PUT {"key6":"put-check"}` изменил только `key6`; `email`, `key1`, `bonus`, `key2` и остальные поля сохранились.
- Для `teyca-sync` это означает, что `PUT /passes/{user_id}` можно использовать как partial update без предварительного `GET`.

### Начисление бонусов: механизм в FastAPI (teyca-sync)

В сервисе (FastAPI) бонусы участвуют так:

1. **Входящий webhook** — Teyca присылает POST на наш путь из `WEBHOOK` (по умолчанию `/webhook`) с телом `{ "type": "CREATE"|"UPDATE"|"DELETE", "pass": { ... } }`. В `pass.bonus` приходит **уже актуальный баланс** бонусов по карте (Teyca сама начисляет/списывает при покупках и т.д.). Сервис парсит payload, проверяет Authorization, публикует сообщение в RabbitMQ в очередь по `type` (CREATE/UPDATE/DELETE). Consumers сохраняют данные в Postgres и синхронизируют Listmonk; поле `bonus` хранится в таблице `users` как часть профиля.

2. **Когда нужно начислить бонусы из нашего сервиса** (например при merge с кассовой БД или по внутреннему правилу) — вызываем **API Teyca**, а не выставляем число через PUT карты:
   - **POST** `https://api.teyca.ru/v1/{token}/passes/{user_id}/bonuses`  
   - Body: `{ "bonus": [ { "value": "500.0", "active_from": "2025-07-19T11:21:35.779+00:00", "expires_at": "2025-08-03T11:21:35.779+00:00" }, ... ] }`  
   Каждый элемент массива — одна операция начисления: сумма `value` и период действия `active_from`, `expires_at`. Teyca добавляет их к балансу и ведёт историю. В коде (consumers или отдельный клиент) использовать httpx и конфиг `TEYCA_TOKEN`, `TEYCA_API_KEY`.

3. **Получение истории бонусов** — **GET** `https://api.teyca.ru/v1/{token}/passes/{user_id}/bonuses` (без body). Нужно для отчётов или проверки.

4. **PUT карты** (`/v1/{token}/passes/{user_id}`) с полем `bonus` — это **не начисление**, а установка итогового значения поля на карте (например после merge: сложили бонусы из кассы и CRM и один раз обновили карту). Для добавления новой операции начисления используется только POST `.../bonuses` с массивом.

5. **Duplicate email remediation** — отдельный repair-worker при конфликте email в локальной БД отправляет:
   - winner'у: `PUT /passes/{user_id}` с `{"key6": "bugs"}`
   - **PUT** `https://api.teyca.ru/v1/{token}/passes/{user_id}`
   - loser'у Body: `{"email": null, "key1": "bad email", "key6": "bugs"}`
   Это помечает loser-пользователя как невалидный email-кейс и убирает email из Teyca, чтобы следующий `UPDATE` снова не возвращал конфликт.

| Действие | Метод | Путь | Body |
|----------|--------|------|------|
| История бонусов | GET | `/v1/{token}/passes/{user_id}/bonuses` | — |
| Начисление бонусов | POST | `/v1/{token}/passes/{user_id}/bonuses` | `{ "bonus": [ { "value": "...", "active_from": "ISO8601", "expires_at": "ISO8601" } ] }` |

---

## Webhook в Teyca (управление подписками)

- GET `/v1/{token}/webhook` — список вебхуков
- POST `/v1/{token}/webhook` — создание (body: `type`, `url`), например `{"type": "CREATE", "url": "https://example.com/webhook/create"}`
- PUT/DELETE по `hook_id` — обновление/удаление

Нужны для настройки доставки событий CREATE/UPDATE/DELETE на наш POST путь из `WEBHOOK` (по умолчанию `/webhook`).
