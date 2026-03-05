
---

## Контекст и роли

**Инициатор:** CRM
**Триггер:** Webhook в FastAPI (CREATE/UPDATE/DELETE). Верификация запроса — **JWT токен** (заголовок или параметр).
**Транспорт:** FastAPI принимает webhook, публикует payload в RabbitMQ в очередь по типу события; обработка — асинхронно в consumers.
**Очереди RabbitMQ:** имена только через константы (см. `app/mq/queues.py`). Роутинг: CREATE → `queue-create`, UPDATE → `queue-update`, DELETE → `queue-delete`.
**Сторедж:** Postgres (ACID транзакции)
**История:** Старая БД (read-only источник исторических полей)
**Email/маркетинг:** Listmonk — **только через Python SDK** (прямые вызовы API не используются)
**ID пользователя:** `user.id` — главный ключ/корреляция

---

## Правила
* признак merge = **в `merge_log`**
* merge-правило для `summ/check_summ` и подобных = **всегда сложение**
* merge-правило для бонусов из старой БД: переносим в Teyca **операциями начисления** через `POST /v1/{token}/passes/{user_id}/bonuses`, а не через `PUT /passes/{user_id}`.
* Listmonk list_id **не из CRM**, а берётся из **ENV/конфига** (то есть маппинг "куда подписывать" — системный)
* **Верификация webhook:** входящий запрос проверяется по **JWT токену** (настройка в ENV). Без валидного токена запрос отклоняется.
* Источник истины по согласию на email-рассылку — **Listmonk** (статус подписчика в целевом `list_id`), а не webhook payload CRM.
* Начисление бонусов за согласие на email-рассылку выполняется отдельной операцией в Teyca `POST /v1/{token}/passes/{user_id}/bonuses` после подтверждения согласия в Listmonk.
* Webhook от Listmonk не используется: подтверждение consent обрабатывает отдельный `sync-worker` периодическим опросом Listmonk.
* Любое начисление бонусов должно быть идемпотентным (уникальный ключ операции в БД, чтобы не начислить повторно при ретраях).
---

## Данные и таблицы

### `users` (master)

Содержит:

* CRM профиль (основные поля)
* “некоторые данные сверху” (агрегаты/слитые значения вроде `summ`, `check_summ`, …)
* служебные поля (рекомендовано): `updated_at`, `created_at`, `last_event_id` (если есть), `last_event_ts`, `version`

### `listmonk_users`

Отражает фактическое состояние в Listmonk:

* `user_id`
* `subscriber_id`
* `email`
* `status`
* `list_ids` (те, что применили)
* `attributes` (какие атрибуты передали/актуальны)
* timestamps

### `merge_log`

Фиксирует факт мерджа и защиту от повтора:

* `user_id`
* `merged_at` (timestamp)
* (рекомендовано) `source_event_type`, `source_event_id`/`trace_id`
* (рекомендовано) `merge_version` или `hash` (на случай эволюции правил)

---

## Общие правила обработки (для всех событий)

1. **Все DB-операции — в одной транзакции Postgres (ACID).**
2. **Сериализация по пользователю:** при обработке одного `user.id` одновременно должен работать один поток.
   Практично: `SELECT ... FOR UPDATE` по `users` (если есть) +/или advisory lock на `user_id`.
3. **Listmonk list_id берём из ENV/конфига** (один/несколько списков).
4. **Merge данных:** если есть исторические значения и CRM значения, то для заданных полей применяется **сложение**:
   `result = crm_value + old_value` (правило “всегда идёт сложение”).
5. **Listmonk операции идемпотентны по смыслу:** если subscriber уже существует — делаем update, если нет — create.
6. **Работа с Listmonk — только через Python SDK;** прямые HTTP-запросы к Listmonk API не используются.

---

# Бизнес-процесс 1: CREATE пользователя

### Вход

Webhook `CREATE` от CRM с данными пользователя и `user.id`.

### Выход

* `users` содержит актуальный master-профиль (CRM + merged поля)
* пользователь создан/обновлён в Listmonk
* `listmonk_users` актуализирована
* (если был merge) `merge_log` записан

### Шаги (алгоритм)

**Транзакция Postgres:**

1. Принять payload, извлечь `user.id`.
2. Взять lock по `user.id` (чтобы не было параллельных create/update).
3. Прочитать исторические данные из старой БД по `user.id` (если есть).
4. Собрать `merged_profile`:

   * базовые поля = CRM
   * для полей “на сложение” (например `summ`, `check_summ`):
     `merged = crm + old`
5. `UPSERT` в `users` (даже для CREATE безопаснее делать upsert).
6. Проверить `listmonk_users` по `user.id`:

   * если запись есть → считаем, что subscriber существует (или хотя бы был создан ранее)
7. Синхронизация Listmonk:

   * если subscriber есть → **update** через SDK
   * иначе → **create** через SDK
   * применить list_ids из ENV
8. Обновить/вставить `listmonk_users` (subscriber_id, status, list_ids, attributes).
9. Если шаг 3 вернул историю и merge реально применился:

   * выполнить начисление бонусов из старой БД в Teyca через `POST /v1/{token}/passes/{user_id}/bonuses` (пакет операций `bonus[]`)
   * вставить запись в `merge_log` (user_id, merged_at=now()).
10. Если пользователь подлежит проверке consent, пометить его как `consent_pending` для фоновой обработки `sync-worker`.
11. `COMMIT`.

**Важно (про внешние вызовы):** Listmonk — не часть ACID транзакции. Если хочешь максимально правильно, это делается через outbox. Но по твоей схеме — допускаем прямой вызов внутри обработки, при этом обеспечиваем идемпотентность и наличие `listmonk_users` как “контрольной точки”.

---

# Бизнес-процесс 2: UPDATE пользователя

### Вход

Webhook `UPDATE` от CRM с `user.id` и данными.

### Выход

* `users` обновлён
* Listmonk subscriber обновлён (или создан, если отсутствует)
* merge выполняется **только если `merge_log` отсутствует** (защита от повторного merge)

### Шаги

**Транзакция Postgres:**

1. Принять payload, взять lock по `user.id`.
2. Проверить наличие пользователя в `users`:


   * если нет → выполнить “создание отсутствующего” (по сути как CREATE, но остаётся событием UPDATE).
3. Проверить `listmonk_users`:

   * если нет → создать subscriber в Listmonk + вставить `listmonk_users`.
4. Проверка “прошло ли слияние?”:

   * `SELECT 1 FROM merge_log WHERE user_id = $1 LIMIT 1`
5. Если merge_log **не найден**:

   * прочитать старую БД по `user.id`
   * применить merge-правило **сложения** для заданных полей
   * вставить `merge_log` с timestamp
6. Обновить `users` итоговыми данными (CRM + merged поля).
7. Обновить subscriber в Listmonk через SDK (`PATCH`/update), применить list_ids из ENV.
8. Если пользователь подлежит проверке consent, пометить его как `consent_pending` для фоновой обработки `sync-worker`.
9. Обновить `listmonk_users`.
10. `COMMIT`.

---

# Бизнес-процесс 4: SYNC-WORKER согласий (Listmonk -> бонусы)

### Вход

Периодический запуск воркера (например, каждые N минут) + пользователи со статусом `consent_pending`.

### Выход

* подтверждённые consent получают бонус через `POST /v1/{token}/passes/{user_id}/bonuses`
* начисления не дублируются (идемпотентность)
* `consent_pending` очищается для обработанных пользователей

### Шаги

1. Выбрать батч пользователей `consent_pending`.
2. Для каждого `user_id` взять lock/идемпотентный reservation.
3. Через Listmonk SDK прочитать актуальный статус подписчика в целевом `list_id`.
4. Если status не подтверждён — оставить `consent_pending`, перейти к следующему.
5. Если status подтверждён и начисление `email_consent` ещё не выполнялось:
   * вызвать Teyca `POST /v1/{token}/passes/{user_id}/bonuses`
   * записать журнал начисления (`reason=email_consent`)
   * снять `consent_pending`.
6. При ошибке Teyca сохранить ошибку, оставить запись для следующего ретрая без потери идемпотентности.

---

# Бизнес-процесс 3: DELETE пользователя

### Вход

Webhook `DELETE` от CRM с `user.id`.

### Выход

* пользователь удалён из `users`
* удалены все связанные записи во **всех** таблицах master DB (явными DELETE, без CASCADE)
* удалён из `listmonk_users`
* удалён subscriber в Listmonk
* удалены записи `merge_log` по `user.id`. Старая БД не трогаем (по твоим словам “из всех БД” — значит да)

### Шаги

**Транзакция Postgres:**

Таблицы master DB **не** связаны каскадным удалением; данные пользователя удаляются явно в каждой таблице (кроме старой БД — её не изменяем).

1. Принять payload, взять lock по `user.id`.
2. Достать данные для удаления в Listmonk (например `subscriber_id`) из `listmonk_users`.
3. Удалить данные пользователя во всех таблицах master DB в нужном порядке (чтобы не нарушать FK, если есть):
   * `DELETE FROM listmonk_users WHERE user_id = $1`
   * `DELETE FROM merge_log WHERE user_id = $1`
   * любые другие таблицы, хранящие данные по `user_id` (подписки, сегменты, профили, события и т.д.)
   * `DELETE FROM users WHERE user_id = $1`
4. `COMMIT`.
5. Вызвать SDK Listmonk: delete subscriber (после коммита; при сбое — ретраи/ручная сверка).

---

## Конфигурация Listmonk (ENV)

Так как CRM не отдаёт list_id:

* в ENV задаётся:

  * `LISTMONK_LIST_IDS` (один/несколько)
  * базовые параметры подключения
* логика назначения списков:

  * минимум: “всегда подписывать в эти списки”
  * если есть дополнительные правила сегментации — их можно добавить позже, но сейчас их на схеме не требуется.

---

## Контрольные точки и ошибки (коротко)

* Если Listmonk недоступен:

  * DB транзакция может пройти, а Listmonk — нет → появится рассинхрон.
  * Минимальное решение: сохранять статус синка в `listmonk_users` (`sync_status`, `last_error`, `last_try_at`) и ретраить.
  * “Правильное” решение: outbox + воркер.

---

# Срезы реализации

Срез считается закрытым, когда реализована логика, пройдены тесты (unit + при необходимости integration) и обновлена документация по необходимости.

| Срез | Содержание | Критерий закрытия |
|------|-------------|-------------------|
| **Срез 1** | Точка входа: webhook API, JWT-верификация, публикация в RabbitMQ (очереди CREATE/UPDATE/DELETE). Константы очередей в `app/mq/queues.py`. | POST /webhook принимает payload, проверяет JWT, публикует в нужную очередь; тесты на роутинг и отказ без токена. |
| **Срез 2** | Consumer CREATE: блокировка по `user_id`, чтение старой БД, merge, upsert `users`, синхронизация Listmonk, `listmonk_users`, `merge_log`, начисление бонусов из старой БД через `POST .../bonuses`. | Все шаги бизнес-процесса 1 покрыты; unit-тесты на ветки (есть/нет история, есть/нет subscriber) и на начисление merge-бонусов. |
| **Срез 3** | Consumer UPDATE: блокировка, проверка `users`/создание при отсутствии, проверка `merge_log`, условный merge, обновление `users` и Listmonk, постановка `consent_pending`. | Все шаги бизнес-процесса 2 покрыты; тесты на «merge уже был»/«merge не был» и «пользователь поставлен в consent_pending». |
| **Срез 4** | Consumer DELETE: блокировка, чтение `subscriber_id`, явное удаление из всех таблиц master DB (без CASCADE), затем удаление в Listmonk. | Все шаги бизнес-процесса 3 покрыты; тесты на порядок удаления и вызов Listmonk после commit. |
| **Срез 5** | Sync-worker consent: периодический опрос Listmonk, начисление `email_consent` через Teyca bonuses API, снятие `consent_pending`. | Есть тесты на ветки: consent подтверждён/не подтверждён, начисление выполнено/уже было, ошибка Teyca с ретраем. |
| **Срез 6** | Идемпотентность начислений бонусов (merge + consent): журнал операций начисления, защита от дублей, ретраи. | Повторная доставка события или повторный запуск worker не приводит к повторному начислению; есть unit/integration тесты на дедупликацию. |

Дальнейшие срезы (по необходимости): retry/outbox для Listmonk, мониторинг, доработки по результатам эксплуатации.

**Подготовка к реализации:** чек-лист инфраструктуры, каркаса приложения и данных — в [docs/preparation.md](preparation.md).

---
2️⃣ **BPMN-описание процессов**
3️⃣ **Sequence diagram взаимодействия систем**

---

# 2. BPMN модель процессов

## CREATE

```
CRM
  │
  ▼
Webhook API
  │
  ▼
Начать транзакцию
  │
  ▼
Проверка пользователя
  │
  ▼
Получение исторических данных
  │
  ▼
Merge данных
  │
  ▼
Upsert users
  │
  ▼
Проверка listmonk_users
  │
 ┌───────────────┐
 │subscriber есть?│
 └───────┬───────┘
        │
   Да ──┘ └── Нет
        │
 update subscriber
        │
 create subscriber
        │
        ▼
Обновить listmonk_users
        │
        ▼
Записать merge_log
        │
        ▼
COMMIT
```

---

## UPDATE

```
CRM
 │
 ▼
Webhook
 │
 ▼
BEGIN TRANSACTION
 │
 ▼
Проверка users
 │
 ▼
Проверка merge_log
 │
 ┌───────────────┐
 │merge выполнен?│
 └───────┬───────┘
        │
  Да ───┘ └── Нет
        │
  SELECT old DB
        │
  Merge данных
        │
  INSERT merge_log
        │
        ▼
Update users
        │
        ▼
PATCH subscriber
        │
        ▼
Update listmonk_users
        │
        ▼
COMMIT
```

---

## DELETE

```
CRM
 │
 ▼
Webhook
 │
 ▼
BEGIN
 │
 ▼
Получение subscriber_id
 │
 ▼
DELETE listmonk_users
 │
 ▼
DELETE merge_log
 │
 ▼
DELETE прочие таблицы по user_id
 │
 ▼
DELETE users
 │
 ▼
COMMIT
 │
 ▼
Delete subscriber (Listmonk)
```

---

# 3. Sequence Diagram

## CREATE

```
CRM
 │
 │ webhook CREATE
 ▼
FastAPI
 │
 │ BEGIN
 ▼
Postgres
 │
 │ SELECT old DB
 ▼
Old Database
 │
 │ historical data
 ▼
FastAPI
 │
 │ UPSERT users
 ▼
Postgres
 │
 │ check listmonk_users
 ▼
Postgres
 │
 │ create/update subscriber
 ▼
Listmonk
 │
 │ subscriber_id
 ▼
FastAPI
 │
 │ update listmonk_users
 ▼
Postgres
 │
 │ commit
 ▼
END
```

## UPDATE

```
CRM
 │
 │ webhook UPDATE
 ▼
FastAPI
 │
 │ publish → queue-update
 ▼
Consumer
 │
 │ BEGIN, lock user_id
 ▼
Postgres
 │
 │ SELECT users, merge_log
 ▼
Postgres
 │
 │ если merge не был: SELECT old DB
 ▼
Old Database
 │
 │ historical data
 ▼
Consumer
 │
 │ merge, INSERT merge_log, UPDATE users
 ▼
Postgres
 │
 │ PATCH subscriber
 ▼
Listmonk
 │
 │ update listmonk_users
 ▼
Postgres
 │
 │ commit
 ▼
END
```

## DELETE

```
CRM
 │
 │ webhook DELETE
 ▼
FastAPI
 │
 │ publish → queue-delete
 ▼
Consumer
 │
 │ BEGIN, lock, SELECT subscriber_id
 ▼
Postgres
 │
 │ DELETE listmonk_users, merge_log, users (и др.)
 ▼
Postgres
 │
 │ commit
 ▼
Consumer
 │
 │ delete subscriber
 ▼
Listmonk
 │
 ▼
END
```

---

# Финальный результат архитектуры

```
        CRM
         │
         │ webhook (JWT)
         ▼
      FastAPI
         │
         │ publish по типу события
         ▼
    RabbitMQ  (queue-create / queue-update / queue-delete)
         │
         ▼
   Consumers
         │
 ┌───────┼────────┐
 │       │        │
 ▼       ▼        ▼
Old DB  Postgres  Listmonk
         │
         ▼
      users
      listmonk_users
      merge_log
```

---
