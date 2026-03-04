# 5. Merge Flow

## Назначение

Слияние данных из кассовой БД (`export_db`) с картой лояльности Teyca. Вызывается когда пользователь уже есть в export_db (касса) и нужно объединить бонусы/суммы.

## Цепочка узлов

```
get-quue-request-to-merge → select_from_export_db → req_to_merge (PUT Teyca API)
                                                              ↓
                                                    update_loyalty_users
                                                              ↓
                                                    insert_infow_about_merge
```

## Ключевая логика

### select_from_export_db

```sql
SELECT * FROM users
WHERE phone = '{{ $('get-quue-request-to-merge').item.json.phone.replace(/7/, '') }}'
LIMIT 1;
```

БД: `export_db` (касса). Ожидаемые поля: `balance`, `summ`, `summ_all`.

### req_to_merge (Teyca API)

```
PUT https://api.teyca.ru/v1/.../passes/{{ user_id }}
```

Body:
```json
{
  "bonus": {{ balance/100 + bonus }},
  "summ": {{ summ || 0 }},
  "summ_all": {{ summ_all || 0 }},
  "key2": "merge {{ $now }}"
}
```

Источник: `$('select_from_export_db').item.json` — поля `balance`, `summ`, `summ_all`.

### update_loyalty_users (Postgres UPDATE)

Обновляет `loyalty_users` после merge:

| Поле | Текущий код | Источник |
|------|-------------|----------|
| user_id | get-quue-request-to-merge | |
| summ | `Number($json.check_sum) / 100 \|\| 0` | **Ошибка**: `$json` = ответ Teyca, в нём нет `check_sum` |
| summ_all | `Number($json.check_sum) / 100 \|\| 0` | **Ошибка**: то же |
| bonus | `$('req_to_merge').item.json.bonus` | ✓ ответ Teyca |
| key2 | "merge {{ $now }}" | ✓ |

### insert_infow_about_merge

Запись в `merge_history` (user_id, merged_at) для предотвращения повторного merge.