# 2. Create User Flow

## Назначение

Создание нового пользователя в `loyalty_users` и регистрация в Listmonk при получении события CREATE из Teyca.

## Цепочка узлов

```
get-queue-create → create_user → [success: select_to_check_exists] [error: No Operation]
                                       ↓
                              exists_in_external_db?
                              /                    \
                    true: select-user2         false: select-user
                              ↓                         ↓
                    quue-request-to-merge      queue-create-listmonk-user
                              ↓                         ↓
                    [Merge flow]               queue-create-listmonk-user2
                                                              ↓
                                                    add_user_to_listmonk
                                                              ↓
                                                    add_to_listmonk_users_table
```

## Ключевая логика

### create_user (Postgres INSERT)

- Таблица: `loyalty_users`
- Источник: `$json.body.pass` (данные из webhook)
- Маппинг полей: `pass.*` → колонки БД

### select_to_check_exists

Проверяет наличие телефона в внешней БД (`export_db`, таблица `users`):

```sql
SELECT EXISTS(
  SELECT 1 FROM users WHERE phone = '{{ $json.phone.replace(/7/, '') }}'
) AS found;
```

### exists_in_external_db?

- **true** (пользователь есть в export_db) → Merge flow (слияние с кассой)
- **false** (новый пользователь) → Создание в Listmonk

## Проверки
