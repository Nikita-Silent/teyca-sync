# RabbitMQ Volume Normalization

Цель: исключить повторение ситуации, когда `docker compose up` поднимает новый RabbitMQ с пустой очередью из-за смены volume.

## Что случилось на проде 2026-03-30

- Исторический контейнер RabbitMQ жил на anonymous Docker volume.
- `compose.yaml` уже ожидал named volume `rabbitmq-data`.
- При rollout compose создал новый volume и пересоздал broker на пустом каталоге.
- Сообщения пришлось восстанавливать из старого anonymous volume через временный recovery-broker.

## Что нормализовано в репозитории

- В `compose.yaml` volume для RabbitMQ теперь закреплен по явному имени:

```yaml
volumes:
  rabbitmq-data:
    name: ${RABBITMQ_VOLUME_NAME:-teyca-sync_rabbitmq-data}
```

- Это убирает зависимость от compose project name и защищает следующий deploy от случайного создания нового RabbitMQ volume.

## Обязательная проверка перед deploy

Проверить, что текущий broker уже смонтирован на ожидаемый volume:

```bash
docker inspect teyca-sync-rabbitmq-1 --format '{{json .Mounts}}'
```

Ожидаемое значение:

- `Destination=/var/lib/rabbitmq`
- `Name=teyca-sync_rabbitmq-data` или значение из `RABBITMQ_VOLUME_NAME`

Проверить compose config без запуска сервисов:

```bash
docker compose config | grep -A3 'rabbitmq-data:'
```

## Что делать, если active broker снова не на named volume

1. Не запускать `docker compose up` для `rabbitmq` вслепую.
2. Сначала зафиксировать current mount:

```bash
docker inspect teyca-sync-rabbitmq-1 --format '{{json .Mounts}}'
```

3. Если там anonymous volume — поднимать recovery-broker на этом volume и переносить сообщения/данные до любого recreate.
4. Только после этого переводить broker на `RABBITMQ_VOLUME_NAME`.

## Cleanup старого anonymous volume

Старый volume удалять только когда одновременно выполнены все условия:

- текущий production broker использует `RABBITMQ_VOLUME_NAME`
- очереди и сообщения проверены после deploy
- recovery больше не нужен
- есть отдельная запись в incident/issue, что backup volume можно удалять

Удаление старого anonymous volume — отдельное сознательное действие, не часть обычного deploy.

## Что не делать

- не менять `RABBITMQ_VOLUME_NAME` на проде без миграционного плана
- не делать `docker compose down -v` для production stack
- не удалять anonymous volume сразу после recovery
- не полагаться на compose project name как на защиту от пересоздания broker storage
