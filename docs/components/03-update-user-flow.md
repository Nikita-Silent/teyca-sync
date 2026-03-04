# 3. Update User Flow

## Назначение

Обновление данных пользователя в `loyalty_users` при событии UPDATE из Teyca. При необходимости — синхронизация с Listmonk и merge с export_db.

## Цепочка узлов

```
get-queue-update → select_person (проверка user_id в loyalty_users)
                         ↓
                   exists_user (If)
                   /           \
          есть: update_user_info    нет: select_person3 → go-to-queue-create1
                   ↓
            select_merge_row (проверка merge_history)
                   ↓
            If1 (есть запись о merge?)
                   /                    \
         да: select_person2        нет: select_to_check_exists2
                   ↓                         ↓
         [Listmonk update]         exists_in_external_db?2
         [Dividends cron]                 /        \
                                   да: select_person1   нет: select_person2
                                         ↓                   ↓
                                 quue-request-to-merge1   [Listmonk]
                                         ↓
                                 [Merge subflow]
```

## Ключевая логика

### update_user_info (Postgres UPDATE)

- Таблица: `loyalty_users`
- Источник: `$('get-queue-update').item.json.body.pass`
- Matching: `user_id`

### select_merge_row

Проверяет таблицу `merge_history` — был ли пользователь уже смержен с export_db.

### select_to_check_exists2

Проверка телефона в export_db:

```sql
SELECT EXISTS(
  SELECT 1 FROM users WHERE phone = '{{ $('get-queue-update').item.json.body.pass.phone.replace(/7/, '') }}'
) AS found;
```

