# 1. Webhook Entry

## Назначение

Точка входа в воркфлоу. Принимает webhook от Teyca, определяет тип события и направляет в соответствующую RabbitMQ-очередь.

## Узлы

| Узел | Тип | Описание |
|------|-----|----------|
| Webhook | webhook | Принимает POST от Teyca |
| type_of_webhook1 | switch | Роутинг по `body.type` |

## Логика

- **CREATE** → `go-to-queue-create` → очередь `queue-create`
- **UPDATE** → `go-to-queue-update` → очередь `queue-update`
- **DELETE** → `go-to-queue-delete` → очередь `queue-delete`

## Входные данные (body)

```json
{
  "type": "CREATE" | "UPDATE" | "DELETE",
  "pass": { /* данные карты лояльности */ }
}
```

## Проверки

- [ ] Условия Switch корректно различают типы
- [ ] Webhook URL и авторизация настроены
