"""Async RabbitMQ publisher. Use queue constants from app.mq.queues."""

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import structlog
from aio_pika import Message
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from app.mq.queues import QUEUE_CREATE, QUEUE_DELETE, QUEUE_UPDATE
from app.utils import to_optional_str

logger = structlog.get_logger()


class MQPublisher:
    """Publish messages to RabbitMQ queues. Queues are declared on first use."""

    def __init__(self, connection: AbstractRobustConnection) -> None:
        self._connection = connection
        self._channel: AbstractChannel | None = None
        self._declared_queues: set[str] = set()
        self._declare_lock = asyncio.Lock()

    async def _get_channel(self) -> AbstractChannel:
        if self._channel is None or self._channel.is_closed:
            self._channel = await self._connection.channel()
            self._declared_queues.clear()
        return self._channel

    async def publish(self, queue_name: str, payload: dict[str, Any]) -> None:
        """Publish JSON payload to the named queue. Declares queue if needed."""
        channel = await self._get_channel()
        await self._ensure_queue_declared(channel=channel, queue_name=queue_name)
        trace_id = to_optional_str(payload.get("trace_id"))
        source_event_id = to_optional_str(payload.get("source_event_id"))
        user_id = _extract_user_id(payload)
        body = json.dumps(payload).encode()
        await channel.default_exchange.publish(
            Message(
                body=body,
                content_type="application/json",
                correlation_id=trace_id,
                message_id=source_event_id,
                timestamp=datetime.now(UTC),
            ),
            routing_key=queue_name,
        )
        logger.info(
            "mq_published",
            queue_name=queue_name,
            trace_id=trace_id,
            source_event_id=source_event_id,
            user_id=user_id,
            payload_bytes=len(body),
        )

    async def _ensure_queue_declared(self, *, channel: AbstractChannel, queue_name: str) -> None:
        if queue_name in self._declared_queues:
            return
        async with self._declare_lock:
            if queue_name in self._declared_queues:
                return
            await channel.declare_queue(queue_name, durable=True)
            self._declared_queues.add(queue_name)

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


def _extract_user_id(payload: dict[str, Any]) -> int | None:
    pass_obj = payload.get("pass")
    if not isinstance(pass_obj, dict):
        return None
    raw = pass_obj.get("user_id")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return None
