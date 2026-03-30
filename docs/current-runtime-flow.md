# Runtime Flow (Mermaid)

Источник:
- текущий код `app/` (факт на 2026-03-17)
- `docs/roadmap.md` (план/срезы)

## 1) Текущее состояние (реально в коде)

```mermaid
flowchart LR
    Teyca["Teyca sender"] -->|POST ${WEBHOOK}| API["FastAPI"]
    API --> Auth["verify_webhook_token (Authorization header)"]
    Auth --> Webhook["parse payload type+pass"]
    Webhook --> Pub["publish_webhook"]
    Pub --> Qc["queue-create"]
    Pub --> Qu["queue-update"]
    Pub --> Qd["queue-delete"]

    Qc --> CCreate["CREATE consumer"]
    Qu --> CUpdate["UPDATE consumer"]
    Qd --> CDelete["DELETE consumer"]

    CCreate --> PG["Postgres"]
    CUpdate --> PG
    CDelete --> PG

    CCreate --> LM["Listmonk SDK"]
    CUpdate --> LM
    CDelete --> LM

    CCreate --> TY["Teyca bonuses API"]
    CUpdate --> TY

    ER["email repair-worker"] --> PG
    ER --> LM
    ER --> TY

    SW["consent sync-worker"] --> PG
    SW --> LM
    SW --> TY

    RW["listmonk reconcile-worker"] --> PG
    RW --> LM
```

## 2) Sequence: CREATE

```mermaid
sequenceDiagram
    participant RMQ as RabbitMQ
    participant C as CREATE consumer
    participant DB as Postgres
    participant ODB as Old DB
    participant LM as Listmonk SDK
    participant TY as Teyca API

    RMQ->>C: message CREATE(user_id)
    C->>DB: advisory lock(user_id)
    C->>DB: check merge_log
    alt merge absent
        C->>ODB: read historical data
        C->>DB: upsert users (merged profile)
        alt old bonus > 0
            C->>TY: POST /passes/{user_id}/bonuses
        end
        C->>TY: PUT /passes/{user_id} {key2: "merge DD.MM.YYYY HH:MM"}
        C->>DB: insert merge_log
    else merge already exists
        C->>DB: upsert users
    end
    alt email valid
        C->>DB: check local duplicate email in listmonk_users
        alt local duplicate found
            C->>DB: insert email_repair_log(status=pending)
            C->>C: log create_consumer_duplicate_email_scheduled
            Note over C,DB: consumer exits without requeue and without mutating Listmonk
        else no local duplicate
            C->>LM: upsert subscriber (preserve current status)
            alt update/create returned email conflict (subscribers_email_key/409)
                C->>LM: subscriber_by_email(email)
                C->>LM: update found subscriber by email
            end
            C->>DB: upsert listmonk_users
            C->>DB: set consent_pending=true
        end
    else email invalid
        C->>TY: PUT /passes/{user_id} {key1: "blocked"}
        opt listmonk_users row already exists
            C->>DB: mark checked status=blocked, consent_pending=false
        end
    end
    C->>DB: commit
```

## 3) Sequence: UPDATE

```mermaid
sequenceDiagram
    participant RMQ as RabbitMQ
    participant U as UPDATE consumer
    participant DB as Postgres
    participant ODB as Old DB
    participant LM as Listmonk SDK
    participant TY as Teyca API

    RMQ->>U: message UPDATE(user_id)
    U->>DB: advisory lock(user_id)
    U->>DB: check merge_log
    alt merge absent
        U->>ODB: read historical data
        U->>DB: upsert users (merged profile)
        alt old bonus > 0
            U->>TY: POST /passes/{user_id}/bonuses
        end
        U->>TY: PUT /passes/{user_id} {key2: "merge DD.MM.YYYY HH:MM"}
        U->>DB: insert merge_log
    else merge exists
        U->>DB: upsert users
    end
    alt email valid
        U->>DB: check local duplicate email in listmonk_users
        alt local duplicate found
            U->>DB: insert email_repair_log(status=pending)
            U->>U: log update_consumer_duplicate_email_scheduled
            Note over U,DB: consumer exits without requeue and without mutating Listmonk
        else no local duplicate
            U->>LM: upsert subscriber (preserve current status)
            alt update/create returned email conflict (subscribers_email_key/409)
                U->>LM: subscriber_by_email(email)
                U->>LM: update found subscriber by email
            end
            U->>DB: upsert listmonk_users
            U->>DB: set consent_pending=true
        end
    else email invalid
        U->>TY: PUT /passes/{user_id} {key1: "blocked"}
        opt listmonk_users row already exists
            U->>DB: mark checked status=blocked, consent_pending=false
        end
    end
    U->>DB: commit
```

## 4) Sequence: DELETE

```mermaid
sequenceDiagram
    participant RMQ as RabbitMQ
    participant D as DELETE consumer
    participant DB as Postgres
    participant LM as Listmonk SDK

    RMQ->>D: message DELETE(user_id)
    D->>DB: advisory lock(user_id)
    D->>DB: read listmonk_users.subscriber_id
    D->>DB: delete listmonk_users
    D->>DB: delete merge_log
    D->>DB: delete bonus_accrual_log
    D->>DB: delete users
    D->>DB: commit
    alt subscriber_id exists
        D->>LM: delete subscriber
    end
```

## 5) Sequence: consent sync-worker

```mermaid
sequenceDiagram
    participant SCH as Scheduler
    participant W as Sync-worker
    participant DB as Postgres
    participant LM as Listmonk SDK
    participant TY as Teyca API

    SCH->>W: periodic tick
    loop by list_id
        W->>DB: read sync_state watermark(source=listmonk_consent,list_id)
        W->>LM: fetch subscribers updated since watermark
        loop by changed subscriber
            W->>DB: map subscriber_id -> user_id via listmonk_users
            W->>LM: use current subscriber status snapshot

            alt status is blocked
                W->>TY: PUT /passes/{user_id} {key1: "blocked"}
                W->>DB: mark checked, status=blocked
                W->>DB: set consent_pending=false
            else status not confirmed
                W->>DB: mark checked, keep consent_pending=true
            else status confirmed/active
                W->>DB: reserve/get bonus_accrual_log by idempotency_key
                W->>DB: read payload steps {bonus_done, key1_done}

                alt bonus_done is false
                    W->>TY: POST /passes/{user_id}/bonuses
                    W->>DB: save payload bonus_done=true
                end

                alt key1_done is false
                    W->>TY: PUT /passes/{user_id} {key1: "confirmed"}
                    W->>DB: save payload key1_done=true
                end

                W->>DB: mark bonus_accrual_log done
                W->>DB: set consent_pending=false, confirmed_at=now
            end
        end
        W->>DB: update sync_state watermark to last processed (updated_at,id)
    end
    W->>DB: commit
```

Примечание по текущему поведению Listmonk на 2026-03-18:
- найден баг в интеграции `app/clients/listmonk.py`: при обычном `UPDATE` мы вызываем SDK-метод `update_subscriber(...)`
- в используемой версии Python SDK этот метод всегда отправляет `preconfirm_subscriptions=true`
- следствие: повторный `UPDATE` может автоматически подтвердить подписку в double opt-in списке без действия клиента
- issue: `teyca-sync-b7j`

Примечание по delivery semantics:
- RabbitMQ consumer делает `ack` только после успешного завершения handler: см. `ConsumersRunner._callback`
- при любой ошибке до `ack` сообщение уходит в `reject(requeue=true)` или в delayed retry/dead-letter path для rate limit / lock contention
- это at-least-once обработка, а не exactly-once
- внешний вызов в Teyca/Listmonk может выполниться повторно, если процесс/соединение упадёт после внешнего side effect, но до `ack`
- для `consent_sync_worker` RabbitMQ не участвует; там повторяемость зависит от `bonus_accrual_log` и сохранённого step-progress
- `consent` бонус защищён частично: есть `idempotency_key=email_consent:{user_id}` и шаги `bonus_done/key1_done`, но если процесс упадёт после `POST /bonuses` и до `save_progress(bonus_done=true)`, повторное начисление всё ещё возможно
- `merge` бонусы в CREATE/UPDATE сейчас не имеют отдельного idempotency log, поэтому повторная доставка сообщения после внешнего начисления и до `ack` может привести к повторному начислению

## 6) Sequence: email repair-worker

```mermaid
sequenceDiagram
    participant SCH as Scheduler
    participant E as Email repair-worker
    participant DB as Postgres
    participant LM as Listmonk SDK
    participant TY as Teyca API

    SCH->>E: periodic tick
    E->>DB: read pending/failed rows from email_repair_log
    loop by repair row
        E->>DB: mark row processing
        E->>LM: subscriber_by_email(normalized_email)
        alt winner resolved by subscriber_id -> user_id
            E->>DB: clear users.email for losers
            E->>DB: clear listmonk_users.email for losers
            E->>TY: PUT /passes/{winner_user_id} {key6: "bugs"}
            loop by loser user_id
                E->>TY: PUT /passes/{user_id} {email: null, key1: "bad email", key6: "bugs"}
            end
            E->>DB: mark email_repair_log teyca_synced
        else resolution/Teyca failed
            E->>DB: increment attempts, set next_retry_at/status
            alt attempts exhausted
                E->>DB: mark manual_review
            else retry allowed
                E->>DB: mark failed
            end
        end
    end
```

## 7) Метрики и логи (runtime)

Подтверждённый контракт Teyca:

- `PUT /v1/{token}/passes/{user_id}` в живой системе ведёт себя как partial update.
- Проверка выполнена 2026-03-18 на тестовой карте `user_id=5722735`: запрос `PUT {"key6":"put-check"}` изменил только `key6`, остальные поля карты сохранились.

В конце каждого запуска `consent_sync_worker` пишется агрегированный лог:

- `event=consent_sync_metrics`
- `processed`
- `batch_size`
- `consent_bonus_amount`
- `deltas_fetched`
- `unmapped_subscribers`
- `subscriber_not_found`
- `blocked_done`
- `not_confirmed`
- `confirmed_done`
- `accrual_resumed`
- `operation_missing`
- `teyca_errors`

Дополнительные логи:
- `consent_sync_list_processed` — сколько deltas обработано по конкретному `list_id` и до какого watermark дошли.
- `consent_sync_subscriber_not_mapped` — в Listmonk есть subscriber, но нет связи с `user_id` в нашей БД.
- `listmonk_upsert_subscriber_request` / `listmonk_upsert_subscriber_done` — upsert subscriber в Listmonk (включая fallback по email при конфликте `subscribers_email_key`).
- `email_repair_metrics` — агрегированные счётчики запуска repair-worker.
- `email_repair_synced` — duplicate email разрешён и loser синхронизирован с Teyca.
- `email_repair_failed` — repair не завершился и переведён в `failed`/`manual_review`.

## 8) Sequence: listmonk reconcile-worker

```mermaid
sequenceDiagram
    participant SCH as Scheduler
    participant R as Reconcile-worker
    participant DB as Postgres
    participant LM as Listmonk SDK

    SCH->>R: periodic tick
    loop by list_id
        R->>DB: read sync_state watermark(source=listmonk_reconcile,list_id)
        R->>LM: fetch subscribers updated since watermark
        loop by changed subscriber
            R->>DB: check mapping by subscriber_id in listmonk_users
            alt mapping exists
                R->>R: skip
            else mapping missing
                alt attributes.user_id is valid and user exists
                    R->>DB: upsert listmonk_users(user_id, subscriber_id, status, email)
                    R->>DB: set consent_pending=true
                else unique user match by email
                    R->>DB: upsert listmonk_users(user_id, subscriber_id, status, email)
                    R->>DB: set consent_pending=true
                else no safe match
                    R->>R: log unmapped/ambiguous
                end
            end
        end
        R->>DB: update sync_state watermark to last processed (updated_at,id)
    end
    R->>DB: commit
```

Логи reconcile:
- `listmonk_reconcile_list_processed`
- `listmonk_reconcile_mapping_restored`
- `listmonk_reconcile_unmapped`
- `listmonk_reconcile_metrics` (агрегированные счётчики за запуск)
