"""Async RabbitMQ publisher. Use queue constants from app.mq.queues."""

import json
from datetime import UTC, datetime
from typing import Any

import aio_pika
import structlog
from aio_pika import Message

from app.mq.queues import QUEUE_CREATE, QUEUE_DELETE, QUEUE_UPDATE

logger = structlog.get_logger()


class MQPublisher:
    """Publish messages to RabbitMQ queues. Queues are declared on first use."""

    def __init__(self, connection: aio_pika.RobustConnection) -> None:
        """
        Initialize the MQPublisher with a RabbitMQ connection and prepare lazy channel state.
        
        Parameters:
            connection (aio_pika.RobustConnection): An existing RobustConnection to RabbitMQ used to create channels for publishing.
        """
        self._connection = connection
        self._channel: aio_pika.Channel | None = None

    async def _get_channel(self) -> aio_pika.Channel:
        """
        Retrieve the publisher's active RabbitMQ channel, creating and caching one if none exists or the current is closed.
        
        Returns:
            aio_pika.Channel: An open channel ready for publishing messages.
        """
        if self._channel is None or self._channel.is_closed:
            self._channel = await self._connection.channel()
        return self._channel

    async def publish(self, queue_name: str, payload: dict[str, Any]) -> None:
        """
        Publish a JSON-encoded payload to the specified RabbitMQ queue.
        
        The queue is declared durable on first use. If present in the payload, `trace_id`
        and `source_event_id` are attached to the AMQP message as `correlation_id` and
        `message_id` respectively; the message content type is set to "application/json"
        and the timestamp is set to the current UTC time. The method extracts a `user_id`
        from payload["pass"] when available for logging, and logs an `mq_published` event
        including queue name, trace id, source event id, user id, and payload size in bytes.
        
        Parameters:
            queue_name (str): Name of the target RabbitMQ queue.
            payload (dict[str, Any]): JSON-serializable payload to send. Optional keys:
                - "trace_id": string used as the message `correlation_id`.
                - "source_event_id": string used as the message `message_id`.
                - "pass": optional mapping that may contain "user_id" (int or digit string).
        """
        channel = await self._get_channel()
        await channel.declare_queue(queue_name, durable=True)
        trace_id = _to_optional_str(payload.get("trace_id"))
        source_event_id = _to_optional_str(payload.get("source_event_id"))
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

    async def publish_webhook(self, event_type: str, payload: dict[str, Any]) -> None:
        """
        Route an incoming webhook event to its corresponding RabbitMQ queue.
        
        Maps event_type "CREATE", "UPDATE", and "DELETE" to the module queues QUEUE_CREATE, QUEUE_UPDATE, and QUEUE_DELETE respectively and publishes the given payload to the matched queue.
        
        Parameters:
            event_type (str): The webhook event type; expected values are "CREATE", "UPDATE", or "DELETE".
            payload (dict[str, Any]): The message payload to publish.
        
        Raises:
            ValueError: If event_type is not one of "CREATE", "UPDATE", or "DELETE".
        """
        if event_type == "CREATE":
            await self.publish(QUEUE_CREATE, payload)
        elif event_type == "UPDATE":
            await self.publish(QUEUE_UPDATE, payload)
        elif event_type == "DELETE":
            await self.publish(QUEUE_DELETE, payload)
        else:
            raise ValueError(f"Unknown event type: {event_type}")


def _extract_user_id(payload: dict[str, Any]) -> int | None:
    """
    Extract an integer user_id from payload["pass"] when present and well-formed.
    
    Parameters:
        payload (dict[str, Any]): Message payload that may contain a "pass" mapping with a "user_id" entry. The "user_id" may be an int or a numeric string.
    
    Returns:
        int | None: The extracted `user_id` as an integer if present and valid, `None` otherwise.
    """
    pass_obj = payload.get("pass")
    if not isinstance(pass_obj, dict):
        return None
    raw = pass_obj.get("user_id")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return None


def _to_optional_str(raw: object) -> str | None:
    """
    Normalize a value into a trimmed string when it contains non-whitespace characters, otherwise return None.
    
    Parameters:
        raw (object): Input value to convert.
    
    Returns:
        str | None: The input stripped of surrounding whitespace when `raw` is a string with at least one non-whitespace character, `None` otherwise.
    """
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    return value or None
