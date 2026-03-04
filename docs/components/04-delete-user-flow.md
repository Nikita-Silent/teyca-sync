# 4. Delete User Flow

## Назначение

Удаление пользователя из `loyalty_users` и Listmonk при событии DELETE из Teyca.

## Цепочка узлов

```
get-queue-delete → select_id_from_listmonk_to_delete → get_info_from_listmonk_to_delete
                                                              ↓
                                                    delete_user_in_listmonk
                                                              ↓
                                                    delete_user (Postgres DELETE)
```

## Ключевая логика

### select_id_from_listmonk_to_delete

```sql
SELECT * FROM listmonk_users WHERE user_id = {{ $json.body.pass.user_id }};
```

Получает `listmonk_user_id` для последующего удаления в Listmonk API.

### get_info_from_listmonk_to_delete

```
GET https://listmonk.mirteck.ru/api/subscribers/{{ $json.listmonk_user_id }}
```

### delete_user_in_listmonk

```
DELETE https://listmonk.mirteck.ru/api/subscribers/{{ $json.data.id }}
```

Используется `data.id` из ответа GET — может отличаться от `listmonk_user_id` в нашей БД. Проверить соответствие.

### delete_user (Postgres)

```sql
DELETE FROM loyalty_users WHERE user_id = {{ $('get-queue-delete').item.json.body.pass.user_id }};
```

## Проверки

- [ ] Если пользователя нет в `listmonk_users` — цепочка может сломаться (нет `listmonk_user_id`)
- [ ] Порядок: сначала Listmonk, потом Postgres — корректен (иначе потеряем listmonk_user_id)
