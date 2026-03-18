"""Run RabbitMQ consumers for CREATE/UPDATE/DELETE queues."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import aio_pika
import structlog
from aio_pika.abc import AbstractChannel, AbstractIncomingMessage
from structlog import contextvars as log_contextvars

from app.clients.listmonk import ListmonkSDKClient
from app.clients.teyca import TeycaClient
from app.config import Settings, get_settings
from app.consumers.create_user import CreateConsumerDeps
from app.consumers.create_user import handle as handle_create
from app.consumers.delete_user import DeleteConsumerDeps
from app.consumers.delete_user import handle as handle_delete
from app.consumers.update_user import UpdateConsumerDeps
from app.consumers.update_user import handle as handle_update
from app.db.session import SessionLocal
from app.logging_config import configure_logging, shutdown_logging
from app.mq.queues import (
    QUEUE_CREATE,
    QUEUE_CREATE_DEAD,
    QUEUE_CREATE_RETRY,
    QUEUE_DELETE,
    QUEUE_DELETE_DEAD,
    QUEUE_DELETE_RETRY,
    QUEUE_UPDATE,
    QUEUE_UPDATE_DEAD,
    QUEUE_UPDATE_RETRY,
)
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.email_repair_log import EmailRepairLogRepository
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.merge_log import MergeLogRepository
from app.repositories.old_db import OldDBRepository
from app.repositories.users import UserLockNotAcquiredError, UsersRepository
from app.service_health import write_heartbeat
from app.utils import to_optional_str

logger = structlog.get_logger()
LOCK_BUSY_RETRY_HEADER = "x-lock-busy-retry-count"
LOCK_BUSY_ORIGINAL_QUEUE_HEADER = "x-original-queue"
LOCK_BUSY_BASE_DELAY_MS = 1_000
LOCK_BUSY_MAX_DELAY_MS = 30_000
LOCK_BUSY_MAX_RETRIES = 5
RETRY_QUEUE_BY_MAIN_QUEUE = {
    QUEUE_CREATE: QUEUE_CREATE_RETRY,
    QUEUE_UPDATE: QUEUE_UPDATE_RETRY,
    QUEUE_DELETE: QUEUE_DELETE_RETRY,
}
DEAD_QUEUE_BY_MAIN_QUEUE = {
    QUEUE_CREATE: QUEUE_CREATE_DEAD,
    QUEUE_UPDATE: QUEUE_UPDATE_DEAD,
    QUEUE_DELETE: QUEUE_DELETE_DEAD,
}


@dataclass(slots=True)
class ConsumersRunner:
    """Queue consumers lifecycle manager."""

    settings: Settings
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient
    old_db_repo: OldDBRepository
    _process_semaphore: asyncio.Semaphore | None = None
    _channel: AbstractChannel | None = None

    async def _parse_payload(self, message: AbstractIncomingMessage) -> dict[str, Any]:
        try:
            return json.loads(message.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Invalid message body JSON") from exc

    async def _consume_create(
        self, payload: dict[str, Any], *, wait_for_lock: bool = False
    ) -> None:
        async with SessionLocal() as session:
            deps = CreateConsumerDeps(
                settings=self.settings,
                users_repo=UsersRepository(session),
                listmonk_repo=ListmonkUsersRepository(session),
                email_repair_repo=EmailRepairLogRepository(session),
                merge_repo=MergeLogRepository(session),
                old_db_repo=self.old_db_repo,
                listmonk_client=self.listmonk_client,
                teyca_client=self.teyca_client,
                commit_checkpoint=session.commit,
            )
            try:
                await handle_create(payload, deps=deps, wait_for_lock=wait_for_lock)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def _consume_update(
        self, payload: dict[str, Any], *, wait_for_lock: bool = False
    ) -> None:
        async with SessionLocal() as session:
            deps = UpdateConsumerDeps(
                settings=self.settings,
                users_repo=UsersRepository(session),
                listmonk_repo=ListmonkUsersRepository(session),
                email_repair_repo=EmailRepairLogRepository(session),
                merge_repo=MergeLogRepository(session),
                old_db_repo=self.old_db_repo,
                listmonk_client=self.listmonk_client,
                teyca_client=self.teyca_client,
                commit_checkpoint=session.commit,
            )
            try:
                await handle_update(payload, deps=deps, wait_for_lock=wait_for_lock)
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def _consume_delete(
        self, payload: dict[str, Any], *, wait_for_lock: bool = False
    ) -> None:
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
                await handle_delete(payload, deps=deps, wait_for_lock=wait_for_lock)
            except Exception:
                await session.rollback()
                raise

    async def _process(self, message: AbstractIncomingMessage, queue_name: str) -> None:
        payload = await self._parse_payload(message)
        wait_for_lock = (
            _coerce_retry_count((getattr(message, "headers", {}) or {}).get(LOCK_BUSY_RETRY_HEADER))
            > 0
        )
        with log_contextvars.bound_contextvars(
            trace_id=_resolve_trace_id(payload=payload, message=message),
            source_event_id=_resolve_source_event_id(payload=payload, message=message),
            user_id=_extract_user_id(payload),
            event_type=to_optional_str(payload.get("type")),
            queue_name=queue_name,
        ):
            logger.info(
                "consumer_message_processing_started",
                redelivered=getattr(message, "redelivered", None),
            )
            if queue_name == QUEUE_CREATE:
                await self._consume_create(payload, wait_for_lock=wait_for_lock)
                return
            if queue_name == QUEUE_UPDATE:
                await self._consume_update(payload, wait_for_lock=wait_for_lock)
                return
            if queue_name == QUEUE_DELETE:
                await self._consume_delete(payload, wait_for_lock=wait_for_lock)
                return
            raise ValueError(f"Unsupported queue: {queue_name}")

    async def _schedule_lock_retry(
        self,
        *,
        message: AbstractIncomingMessage,
        queue_name: str,
        user_id: int,
    ) -> None:
        channel = self._channel
        if channel is None:
            raise RuntimeError("Retry channel is not initialized")

        headers = dict(getattr(message, "headers", {}) or {})
        retry_count = _coerce_retry_count(headers.get(LOCK_BUSY_RETRY_HEADER)) + 1
        headers[LOCK_BUSY_RETRY_HEADER] = retry_count
        headers[LOCK_BUSY_ORIGINAL_QUEUE_HEADER] = queue_name

        retry_queue = RETRY_QUEUE_BY_MAIN_QUEUE[queue_name]
        dead_queue = DEAD_QUEUE_BY_MAIN_QUEUE[queue_name]
        target_queue = retry_queue
        expiration_ms: int | None = _compute_lock_retry_delay_ms(retry_count)
        expiration: timedelta | None = None
        result = "user_lock_busy"
        log_event = "consumer_message_requeued_user_lock_busy"

        if retry_count > LOCK_BUSY_MAX_RETRIES:
            target_queue = dead_queue
            expiration_ms = None
            result = "user_lock_busy_dead_lettered"
            log_event = "consumer_message_dead_lettered_user_lock_busy"
        elif expiration_ms is not None:
            expiration = timedelta(milliseconds=expiration_ms)

        await channel.default_exchange.publish(
            aio_pika.Message(
                body=message.body,
                headers=headers,
                content_type=getattr(message, "content_type", None),
                content_encoding=getattr(message, "content_encoding", None),
                correlation_id=getattr(message, "correlation_id", None),
                message_id=getattr(message, "message_id", None),
                timestamp=getattr(message, "timestamp", None),
                type=getattr(message, "type", None),
                app_id=getattr(message, "app_id", None),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                expiration=expiration,
            ),
            routing_key=target_queue,
        )
        logger.warning(
            log_event,
            result=result,
            queue_name=queue_name,
            user_id=user_id,
            retry_count=retry_count,
            retry_delay_ms=expiration_ms,
            retry_queue_name=target_queue,
            message_id=getattr(message, "message_id", None),
            correlation_id=getattr(message, "correlation_id", None),
            delivery_tag=getattr(message, "delivery_tag", None),
        )
        await message.ack()

    async def _callback(self, message: AbstractIncomingMessage, queue_name: str) -> None:
        try:
            semaphore = self._process_semaphore
            if semaphore is None:
                await self._process(message, queue_name)
            else:
                async with semaphore:
                    await self._process(message, queue_name)
            await message.ack()
            logger.info(
                "consumer_message_acked",
                queue_name=queue_name,
                message_id=getattr(message, "message_id", None),
                correlation_id=getattr(message, "correlation_id", None),
                delivery_tag=getattr(message, "delivery_tag", None),
            )
        except UserLockNotAcquiredError as exc:
            try:
                await self._schedule_lock_retry(
                    message=message,
                    queue_name=queue_name,
                    user_id=exc.user_id,
                )
            except Exception as retry_exc:
                logger.exception(
                    "consumer_message_lock_retry_failed",
                    result="user_lock_retry_failed",
                    queue_name=queue_name,
                    user_id=exc.user_id,
                    error=str(retry_exc),
                    message_id=getattr(message, "message_id", None),
                    correlation_id=getattr(message, "correlation_id", None),
                    delivery_tag=getattr(message, "delivery_tag", None),
                )
                await message.reject(requeue=True)
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
        self._channel = channel
        prefetch_count = max(1, int(getattr(self.settings, "rabbitmq_consumer_prefetch_count", 4)))
        await channel.set_qos(prefetch_count=prefetch_count)
        db_capacity = max(
            1,
            int(getattr(self.settings, "database_pool_size", 5))
            + int(getattr(self.settings, "database_pool_max_overflow", 10)),
        )
        max_concurrency = max(
            1,
            int(getattr(self.settings, "rabbitmq_consumer_max_concurrency", prefetch_count)),
        )
        max_concurrency = min(max_concurrency, prefetch_count)
        max_concurrency = min(max_concurrency, db_capacity)
        self._process_semaphore = asyncio.Semaphore(max_concurrency)
        heartbeat_task = _start_heartbeat_task("consumers", interval_seconds=15)

        queue_create = await channel.declare_queue(QUEUE_CREATE, durable=True)
        queue_update = await channel.declare_queue(QUEUE_UPDATE, durable=True)
        queue_delete = await channel.declare_queue(QUEUE_DELETE, durable=True)
        await channel.declare_queue(
            QUEUE_CREATE_RETRY,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": QUEUE_CREATE,
            },
        )
        await channel.declare_queue(QUEUE_CREATE_DEAD, durable=True)
        await channel.declare_queue(
            QUEUE_UPDATE_RETRY,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": QUEUE_UPDATE,
            },
        )
        await channel.declare_queue(QUEUE_UPDATE_DEAD, durable=True)
        await channel.declare_queue(
            QUEUE_DELETE_RETRY,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": QUEUE_DELETE,
            },
        )
        await channel.declare_queue(QUEUE_DELETE_DEAD, durable=True)

        await queue_create.consume(lambda msg: self._callback(msg, QUEUE_CREATE))
        await queue_update.consume(lambda msg: self._callback(msg, QUEUE_UPDATE))
        await queue_delete.consume(lambda msg: self._callback(msg, QUEUE_DELETE))

        logger.info(
            "consumers_started",
            queues=[QUEUE_CREATE, QUEUE_UPDATE, QUEUE_DELETE],
            prefetch_count=prefetch_count,
            max_concurrency=max_concurrency,
            db_capacity=db_capacity,
        )
        try:
            await asyncio.Event().wait()
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            await self.old_db_repo.close()
            await connection.close()
            self._channel = None


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


def _resolve_trace_id(*, payload: dict[str, Any], message: AbstractIncomingMessage) -> str | None:
    payload_trace_id = to_optional_str(payload.get("trace_id"))
    if payload_trace_id:
        return payload_trace_id
    return to_optional_str(getattr(message, "correlation_id", None))


def _resolve_source_event_id(
    *, payload: dict[str, Any], message: AbstractIncomingMessage
) -> str | None:
    payload_event_id = to_optional_str(payload.get("source_event_id"))
    if payload_event_id:
        return payload_event_id
    return to_optional_str(getattr(message, "message_id", None))


def _start_heartbeat_task(service_name: str, *, interval_seconds: int) -> asyncio.Task[None]:
    async def _run() -> None:
        while True:
            try:
                await write_heartbeat(service_name)
            except Exception as exc:
                logger.error(
                    "service_heartbeat_write_failed",
                    service_name=service_name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            await asyncio.sleep(interval_seconds)

    return asyncio.create_task(_run())


def _coerce_retry_count(raw: object) -> int:
    if isinstance(raw, bool):
        return 0
    if isinstance(raw, int):
        return max(0, raw)
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped.isdigit():
            return int(stripped)
    return 0


def _compute_lock_retry_delay_ms(retry_count: int) -> int:
    bounded_retry_count = max(1, retry_count)
    delay_ms = LOCK_BUSY_BASE_DELAY_MS * (2 ** (bounded_retry_count - 1))
    return min(delay_ms, LOCK_BUSY_MAX_DELAY_MS)


async def _run() -> None:
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
    asyncio.run(_run())


if __name__ == "__main__":
    main()
