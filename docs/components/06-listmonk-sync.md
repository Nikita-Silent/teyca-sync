# 6. Listmonk Sync

## Назначение

Синхронизация пользователей с Listmonk (email-рассылки). Создание, обновление, обработка дубликатов.

## Сценарии

### 6.1 Создание в Listmonk (Create flow)

```
queue-create-listmonk-user2 → add_user_to_listmonk → add_to_listmonk_users_table
```

**add_user_to_listmonk** (POST):
```
POST https://listmonk.mirteck.ru/api/subscribers
```

Body:
```json
{
  "email": "{{ $json.email.toLowerCase() }}",
  "name": "{{ $json.fio }}",
  "status": "enabled",
  "lists": [2],
  "attribs": {
    "phone": "{{ $json.phone }}",
    "birthday": "{{ $json.birthday }}",
    "gender": "{{ $json.gender }}",
    "barcode": "{{ $json.barcode }}",
    "link": "https://cards.teyca.ru/download/ {{ $json.link }}"
  }
}
```

При ошибке (409 — email уже существует): `Switch` → `get_info_from_listmonk1` → `add_to_listmonk_users_table1` (upsert по существующему).

### 6.2 Обновление в Listmonk (Update flow)

```
select_id_from_listmonk → If (есть в listmonk?)
                    /                    \
          да: get_info_from_listmonk    нет: select-user1 → go-to-queue-create-listmonk-user
                    ↓
          update_user_in_listmonk1 / No Operation
```