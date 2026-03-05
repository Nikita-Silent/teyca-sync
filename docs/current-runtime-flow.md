# Runtime Flow (Mermaid)

Источник:
- текущий код `app/` (факт)
- обновлённый `docs/roadmap.md` (целевая логика)

## 1) Текущее состояние (что уже работает)

```mermaid
flowchart LR
    Teyca["Teyca sender"] -->|POST /webhook| API["FastAPI"]
    API --> Auth["verify_webhook_token"]
    Auth --> Webhook["parse payload type+pass"]
    Webhook --> Pub["publish_webhook"]
    Pub --> Qc["queue-create"]
    Pub --> Qu["queue-update"]
    Pub --> Qd["queue-delete"]

    Qc -. no consumers yet .-> Todo1["TODO consumers"]
    Qu -. no consumers yet .-> Todo1
    Qd -. no consumers yet .-> Todo1
```

```mermaid
flowchart TD
    A["POST /webhook"] --> B{"WEBHOOK_AUTH_TOKEN set"}
    B -- no --> E503["503"]
    B -- yes --> C{"Authorization header present"}
    C -- no --> E401["401"]
    C -- yes --> D{"token valid"}
    D -- no --> E403["403"]
    D -- yes --> P["validate body: type + pass"]
    P --> T{"type"}
    T -- CREATE --> Q1["publish queue-create"]
    T -- UPDATE --> Q2["publish queue-update"]
    T -- DELETE --> Q3["publish queue-delete"]
    Q1 --> OK["200 OK"]
    Q2 --> OK
    Q3 --> OK
```

## 2) Целевая схема по roadmap (после доработок)

```mermaid
flowchart LR
    CRM["Teyca webhook"] --> API["POST /webhook"]
    API --> MQ["RabbitMQ queues"]

    MQ --> CCreate["Consumer CREATE"]
    MQ --> CUpdate["Consumer UPDATE"]
    MQ --> CDelete["Consumer DELETE"]
    Sched["Scheduler/Cron"] --> SW["Consent sync-worker"]

    CCreate --> PG["Postgres users/listmonk_users/merge_log"]
    CCreate --> OldDB["Old DB read-only"]
    CCreate --> LM["Listmonk SDK"]
    CCreate --> TeycaBonus["Teyca POST /v1/{token}/passes/{user_id}/bonuses"]

    CUpdate --> PG
    CUpdate --> LM
    CUpdate --> Pending["mark consent_pending"]

    CDelete --> PG
    CDelete --> LM

    SW --> LM
    LM --> Consent["Consent source of truth"]
    Consent --> SW
    SW --> TeycaBonus
    SW --> PG
```

## 3) Логика начислений бонусов (целевая)

```mermaid
flowchart TD
    S["Consumer event CREATE or UPDATE"] --> M{"Need merge from Old DB"}
    M -- yes --> B1["Build bonus operations from Old DB"]
    B1 --> T1["POST bonuses to Teyca"]
    T1 --> L1["Write idempotency log reason=merge_old_db"]

    M -- no --> C["Set consent_pending for sync-worker"]
    L1 --> C
    C --> End["Finish consumer flow"]
```

## 4) Sequence: UPDATE без ожидания consent

```mermaid
sequenceDiagram
    participant RMQ as RabbitMQ
    participant U as UPDATE consumer
    participant DB as Postgres
    participant LM as Listmonk SDK

    RMQ->>U: message UPDATE(user_id)
    U->>DB: lock user + read user/listmonk/merge state
    U->>LM: upsert subscriber
    U->>DB: set consent_pending=true
    U->>DB: commit changes
```

## 5) Sequence: sync-worker для consent

```mermaid
sequenceDiagram
    participant SCH as Scheduler
    participant W as Sync-worker
    participant DB as Postgres
    participant LM as Listmonk SDK
    participant TY as Teyca bonuses API

    SCH->>W: periodic tick
    W->>DB: select users where consent_pending=true
    loop by user
        W->>LM: get subscriber status in target list
        alt consent not confirmed
            W->>DB: keep consent_pending=true
        else confirmed and not accrued
            W->>TY: POST /v1/{token}/passes/{user_id}/bonuses
            TY-->>W: 200
            W->>DB: write accrual log(email_consent)
            W->>DB: set consent_pending=false
        else confirmed and already accrued
            W->>DB: set consent_pending=false
        end
    end
```
