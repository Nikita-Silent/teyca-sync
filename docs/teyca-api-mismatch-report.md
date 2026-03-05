# Несоответствия кода источнику истины `teyca-api.json`

Дата проверки: 2026-03-05

## 1) Исходящие вызовы Teyca API из коллекции не реализованы в коде

В `teyca-api.json` есть рабочие эндпоинты Teyca (например `/v1/:token/passes`, `/v1/:token/passes/:user_id`, `/v1/:token/passes/:user_id/bonuses`, `/v1/:token/webhook`, `/v1/authorization`),
но в `app/` отсутствует клиент/сервис, который делает эти вызовы.

Факт по коду:
- Есть только входящий webhook-роут: `app/api/webhook.py`.
- Настройки `teyca_api_key` и `teyca_token` объявлены, но не используются для HTTP-вызовов: `app/config.py`.
- Поиск по `app/` не находит использования URL `api.teyca.ru` и маршрутов из коллекции.

## 2) Входящий `/webhook` в коде требует `Authorization`, а в `teyca-api.json` это не описано для callback-а

Код жёстко отклоняет входящий webhook без `Authorization`:
- `app/api/auth.py` — 401 при отсутствии, 403 при неверном токене.

В `teyca-api.json` есть управление webhook-подписками (`POST /v1/:token/webhook` с полями `type` и `url`),
но нет параметра/поля для настройки секретного заголовка callback-а.

Итог: текущее требование заголовка `Authorization` для входящего callback-а не подтверждается источником истины `teyca-api.json`.
