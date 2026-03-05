"""Async RabbitMQ publisher. Use queue constants from app.mq.queues."""

import json
from typing import Any

import aio_pika
from aio_pika import Message

from app.mq.queues import QUEUE_CREATE, QUEUE_DELETE, QUEUE_UPDATE


class MQPublisher:
    """Publish messages to RabbitMQ queues. Queues are declared on first use."""

    def __init__(self, connection: aio_pika.RobustConnection) -> None:
        self._connection = connection
        self._channel: aio_pika.Channel | None = None

    async def _get_channel(self) -> aio_pika.Channel:
        if self._channel is None or self._channel.is_closed:
            self._channel = await self._connection.channel()
        return self._channel

    async def publish(self, queue_name: str, payload: dict[str, Any]) -> None:
        """Publish JSON payload to the named queue. Declares queue if needed."""
        channel = await self._get_channel()
        await channel.declare_queue(queue_name, durable=True)
        body = json.dumps(payload).encode()
        await channel.default_exchange.publish(
            Message(body=body),
            routing_key=queue_name,
        )

    async def publish_webhook(self, event_type: str, payload: dict[str, Any]) -> None:
        """Route by event_type (CREATE/UPDATE/DELETE) to the correct queue."""
        if event_type == "CREATE":
            await self.publish(QUEUE_CREATE, payload)
        elif event_type == "UPDATE":
            await self.publish(QUEUE_UPDATE, payload)
        elif event_type == "DELETE":
            await self.publish(QUEUE_DELETE, payload)
        else:
            raise ValueError(f"Unknown event type: {event_type}")
