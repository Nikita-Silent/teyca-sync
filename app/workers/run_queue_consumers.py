"""Run RabbitMQ consumers for CREATE/UPDATE/DELETE queues."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import aio_pika
import structlog
from structlog import contextvars as log_contextvars
from aio_pika.abc import AbstractIncomingMessage

from app.clients.listmonk import ListmonkSDKClient
from app.clients.teyca import TeycaClient
from app.config import Settings, get_settings
from app.consumers.create_user import CreateConsumerDeps, handle as handle_create
from app.consumers.delete_user import DeleteConsumerDeps, handle as handle_delete
from app.consumers.update_user import UpdateConsumerDeps, handle as handle_update
from app.db.session import SessionLocal
from app.logging_config import configure_logging, shutdown_logging
from app.mq.queues import QUEUE_CREATE, QUEUE_DELETE, QUEUE_UPDATE
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.merge_log import MergeLogRepository
from app.repositories.old_db import OldDBRepository
from app.repositories.users import UsersRepository

logger = structlog.get_logger()


@dataclass(slots=True)
class ConsumersRunner:
    """Queue consumers lifecycle manager."""

    settings: Settings
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient
    old_db_repo: OldDBRepository

    async def _parse_payload(self, message: AbstractIncomingMessage) -> dict[str, Any]:
        """
        Parse an AMQP message body as JSON and return the resulting dictionary.
        
        Parameters:
            message (AbstractIncomingMessage): Incoming AMQP message whose body contains UTF-8 JSON bytes.
        
        Returns:
            dict[str, Any]: The parsed JSON object.
        
        Raises:
            ValueError: If the message body cannot be decoded as UTF-8 or is not valid JSON.
        """
        try:
            return json.loads(message.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid message body JSON") from exc

    async def _consume_create(self, payload: dict[str, Any]) -> None:
        """
        Process a "create user" payload and persist resulting changes within a database transaction.
        
        Processes the provided payload using the create-user consumer logic and commits any database changes if processing succeeds; rolls back the transaction if processing fails.
        
        Parameters:
            payload (dict[str, Any]): Parsed message payload containing create-user data.
        """
        async with SessionLocal() as session:
            deps = CreateConsumerDeps(
                settings=self.settings,
                users_repo=UsersRepository(session),
                listmonk_repo=ListmonkUsersRepository(session),
                merge_repo=MergeLogRepository(session),
                old_db_repo=self.old_db_repo,
                listmonk_client=self.listmonk_client,
                teyca_client=self.teyca_client,
            )
            try:
                await handle_create(payload, deps=deps)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def _consume_update(self, payload: dict[str, Any]) -> None:
        """
        Execute update-user consumer logic within a transactional database session using the runner's configured repositories and external clients.
        
        Parameters:
            payload (dict[str, Any]): The update-user event payload to process.
        
        Raises:
            Exception: Any exception raised by the update handler is re-raised after the database transaction is rolled back.
        """
        async with SessionLocal() as session:
            deps = UpdateConsumerDeps(
                settings=self.settings,
                users_repo=UsersRepository(session),
                listmonk_repo=ListmonkUsersRepository(session),
                merge_repo=MergeLogRepository(session),
                old_db_repo=self.old_db_repo,
                listmonk_client=self.listmonk_client,
                teyca_client=self.teyca_client,
            )
            try:
                await handle_update(payload, deps=deps)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def _consume_delete(self, payload: dict[str, Any]) -> None:
        """
        Process a delete-user payload using a transactional database session.
        
        Constructs the dependencies required by the delete consumer handler, invokes the delete handler with the provided payload, and ensures the database session is rolled back if an error occurs.
        
        Parameters:
            payload (dict[str, Any]): The delete event payload parsed from the incoming message.
        """
        async with SessionLocal() as session:
            deps = DeleteConsumerDeps(
                users_repo=UsersRepository(session),
                listmonk_repo=ListmonkUsersRepository(session),
                merge_repo=MergeLogRepository(session),
                bonus_accrual_repo=BonusAccrualRepository(session),
                listmonk_client=self.listmonk_client,
                session=session,
            )
            try:
                await handle_delete(payload, deps=deps)
            except Exception:
                await session.rollback()
                raise

    async def _process(self, message: AbstractIncomingMessage, queue_name: str) -> None:
        """
        Process a single incoming AMQP message by parsing its JSON payload, binding structured logging context (trace, source event, user, event type, queue), and dispatching the payload to the appropriate queue-specific consumer.
        
        Parameters:
            message (AbstractIncomingMessage): The incoming AMQP message to process.
            queue_name (str): The name of the queue the message was received from. Expected values: QUEUE_CREATE, QUEUE_UPDATE, or QUEUE_DELETE.
        
        Raises:
            ValueError: If `queue_name` is not one of the supported queues.
        """
        payload = await self._parse_payload(message)
        with log_contextvars.bound_contextvars(
            trace_id=_resolve_trace_id(payload=payload, message=message),
            source_event_id=_resolve_source_event_id(payload=payload, message=message),
            user_id=_extract_user_id(payload),
            event_type=_to_optional_str(payload.get("type")),
            queue_name=queue_name,
        ):
            logger.info(
                "consumer_message_processing_started",
                redelivered=getattr(message, "redelivered", None),
            )
            if queue_name == QUEUE_CREATE:
                await self._consume_create(payload)
                return
            if queue_name == QUEUE_UPDATE:
                await self._consume_update(payload)
                return
            if queue_name == QUEUE_DELETE:
                await self._consume_delete(payload)
                return
            raise ValueError(f"Unsupported queue: {queue_name}")

    async def _callback(self, message: AbstractIncomingMessage, queue_name: str) -> None:
        """
        Handle a delivered AMQP message by processing it, acknowledging on success, or rejecting with requeue on failure while logging the outcome.
        
        On success, acknowledges the message and logs an informational event including queue name, message_id, correlation_id, and delivery_tag. On exception, logs the exception with the same metadata and rejects the message with requeue=True.
        """
        try:
            await self._process(message, queue_name)
            await message.ack()
            logger.info(
                "consumer_message_acked",
                queue_name=queue_name,
                message_id=getattr(message, "message_id", None),
                correlation_id=getattr(message, "correlation_id", None),
                delivery_tag=getattr(message, "delivery_tag", None),
            )
        except Exception as exc:
            logger.exception(
                "consumer_message_failed",
                queue_name=queue_name,
                error=str(exc),
                message_id=getattr(message, "message_id", None),
                correlation_id=getattr(message, "correlation_id", None),
                delivery_tag=getattr(message, "delivery_tag", None),
            )
            await message.reject(requeue=True)

    async def run(self) -> None:
        """Connect to RabbitMQ and consume all queues indefinitely."""
        connection = await aio_pika.connect_robust(self.settings.rabbitmq_url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=32)

        queue_create = await channel.declare_queue(QUEUE_CREATE, durable=True)
        queue_update = await channel.declare_queue(QUEUE_UPDATE, durable=True)
        queue_delete = await channel.declare_queue(QUEUE_DELETE, durable=True)

        await queue_create.consume(lambda msg: self._callback(msg, QUEUE_CREATE))
        await queue_update.consume(lambda msg: self._callback(msg, QUEUE_UPDATE))
        await queue_delete.consume(lambda msg: self._callback(msg, QUEUE_DELETE))

        logger.info(
            "consumers_started",
            queues=[QUEUE_CREATE, QUEUE_UPDATE, QUEUE_DELETE],
        )
        try:
            await asyncio.Event().wait()
        finally:
            await self.old_db_repo.close()
            await connection.close()


def _extract_user_id(payload: dict[str, Any]) -> int | None:
    """
    Extracts an integer user ID from the payload's "pass" object when present.
    
    Parameters:
        payload (dict): Message payload expected to contain an optional "pass" mapping with a "user_id" field.
    
    Returns:
        int: The extracted user ID when present as an int or as a numeric string; `None` otherwise.
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
    Normalize a value to an optional trimmed string.
    
    Parameters:
        raw (object): Input value to normalize; non-string inputs are treated as absent.
    
    Returns:
        str | None: The input trimmed of leading/trailing whitespace if it is a non-empty string, otherwise `None`.
    """
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    return value or None


def _resolve_trace_id(*, payload: dict[str, Any], message: AbstractIncomingMessage) -> str | None:
    """
    Resolve the trace identifier from the payload or the AMQP message.
    
    The payload's "trace_id" (normalized to a non-empty string) is preferred; if absent, the message's `correlation_id` (normalized) is returned.
    
    Parameters:
        payload (dict[str, Any]): Incoming message payload where a "trace_id" may be present.
        message (AbstractIncomingMessage): AMQP message object used as a fallback source for `correlation_id`.
    
    Returns:
        str | None: The trace identifier as a non-empty string if found, `None` otherwise.
    """
    payload_trace_id = _to_optional_str(payload.get("trace_id"))
    if payload_trace_id:
        return payload_trace_id
    return _to_optional_str(getattr(message, "correlation_id", None))


def _resolve_source_event_id(*, payload: dict[str, Any], message: AbstractIncomingMessage) -> str | None:
    """
    Determine the source event identifier for a message, preferring the payload value and falling back to the AMQP message ID.
    
    Parameters:
        payload (dict[str, Any]): The decoded message payload; may contain a `source_event_id` field.
        message (AbstractIncomingMessage): The incoming AMQP message; `message.message_id` is used as a fallback.
    
    Returns:
        str | None: The `source_event_id` as a trimmed non-empty string if present in the payload or the message's `message_id` if present and non-empty; otherwise `None`.
    """
    payload_event_id = _to_optional_str(payload.get("source_event_id"))
    if payload_event_id:
        return payload_event_id
    return _to_optional_str(getattr(message, "message_id", None))


async def _run() -> None:
    """
    Bootstraps logging, constructs the ConsumersRunner with concrete clients and repositories, and starts the consumer event loop.
    
    Configures structured logging (optionally with Loki), creates the runner with settings, Listmonk and Teyca clients, and the old DB repository, then awaits the runner's run loop. Ensures logging is shut down on exit.
    """
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        component=getattr(settings, "log_component", "consumers"),
    )
    runner = ConsumersRunner(
        settings=settings,
        listmonk_client=ListmonkSDKClient(settings),
        teyca_client=TeycaClient(settings),
        old_db_repo=OldDBRepository(settings.export_db_url),
    )
    try:
        await runner.run()
    finally:
        shutdown_logging()


def main() -> None:
    """
    Start the consumers runner and block until it finishes.
    
    Executes the module's startup, runs the queue consumers, and ensures orderly shutdown of resources when the run completes or is interrupted.
    """
    asyncio.run(_run())


if __name__ == "__main__":
    main()
