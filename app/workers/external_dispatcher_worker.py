"""Durable dispatcher for external Listmonk/Teyca side effects."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import httpx
import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog import contextvars as log_contextvars

from app.clients.listmonk import ListmonkClientError, ListmonkSDKClient, SubscriberState
from app.clients.teyca import (
    BonusOperation,
    TeycaAPIError,
    TeycaClient,
    TeycaRateLimitBusyError,
    build_teyca_client,
)
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.email_repair_log import EmailRepairLogRepository
from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_DELETE,
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_MERGE_FINALIZE,
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
    ExternalCallOutboxRepository,
    OutboxClaim,
)
from app.repositories.listmonk_users import (
    DuplicateListmonkSubscriberIdError,
    DuplicateListmonkUserEmailError,
    ListmonkUsersRepository,
)
from app.repositories.merge_log import MergeLogRepository
from app.repositories.users import UsersRepository

logger = structlog.get_logger()

DEFAULT_OUTBOX_OPERATIONS = [
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_LISTMONK_DELETE,
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
    OUTBOX_OP_MERGE_FINALIZE,
]


@dataclass(slots=True)
class ExternalDispatcherMetrics:
    batch_size: int
    processed: int = 0
    done: int = 0
    retried: int = 0
    dead: int = 0
    skipped: int = 0


@dataclass(slots=True)
class ExternalDispatcherWorker:
    """Polls external_call_outbox and executes side effects after DB commit."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient
    worker_id: str

    async def _run_in_session(
        self,
        operation: Callable[[AsyncSession], Awaitable[Any]],
    ) -> Any:
        async with self.session_factory() as session:
            try:
                result = await operation(session)
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        return result

    async def _claim_batch(self, *, limit: int) -> list[OutboxClaim]:
        async def operation(session: AsyncSession) -> list[OutboxClaim]:
            repo = ExternalCallOutboxRepository(session)
            return await repo.claim_batch(
                operations=DEFAULT_OUTBOX_OPERATIONS,
                limit=limit,
                worker_id=self.worker_id,
            )

        return await self._run_in_session(operation)

    async def _mark_done(self, *, outbox_id: int, payload: dict[str, Any] | None = None) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = ExternalCallOutboxRepository(session)
            await repo.mark_done(outbox_id=outbox_id, payload=payload)

        await self._run_in_session(operation)

    async def _save_progress(
        self,
        *,
        outbox_id: int,
        payload: dict[str, Any],
        error_text: str | None = None,
    ) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = ExternalCallOutboxRepository(session)
            await repo.save_progress(outbox_id=outbox_id, payload=payload, error_text=error_text)

        await self._run_in_session(operation)

    async def _mark_retry(self, *, outbox_id: int, attempts: int, error_text: str) -> str:
        async def operation(session: AsyncSession) -> str:
            repo = ExternalCallOutboxRepository(session)
            return await repo.mark_retry(
                outbox_id=outbox_id,
                attempts=attempts,
                error_text=error_text,
                max_attempts=max(1, self.settings.external_dispatcher_max_retries),
                base_delay_ms=max(1, self.settings.external_dispatcher_retry_base_delay_ms),
                max_delay_ms=max(1, self.settings.external_dispatcher_retry_max_delay_ms),
            )

        return str(await self._run_in_session(operation))

    async def _defer_rate_limit_busy(
        self,
        *,
        outbox_id: int,
        wait_seconds: float,
        error_text: str,
    ) -> None:
        async def operation(session: AsyncSession) -> None:
            repo = ExternalCallOutboxRepository(session)
            await repo.defer(
                outbox_id=outbox_id,
                delay_seconds=max(wait_seconds, 0.0),
                error_text=error_text,
            )

        await self._run_in_session(operation)

    def _teyca_rate_limit_max_wait_seconds(self) -> float:
        configured = float(
            getattr(self.settings, "external_dispatcher_teyca_rate_limit_max_wait_seconds", 0.0)
        )
        return max(0.0, configured)

    async def run_once(self) -> int:
        batch_size = max(1, self.settings.external_dispatcher_batch_size)
        metrics = ExternalDispatcherMetrics(batch_size=batch_size)
        claims = await self._claim_batch(limit=batch_size)
        if not claims:
            logger.info("external_dispatcher_no_pending_jobs", batch_size=batch_size)
            return 0

        for claim in claims:
            metrics.processed += 1
            log_contextvars.bind_contextvars(
                trace_id=claim.trace_id,
                source_event_id=claim.source_event_id,
                user_id=claim.user_id,
                queue_name=claim.queue_name,
                outbox_id=claim.id,
                outbox_operation=claim.operation,
            )
            try:
                await self._process_claim(claim=claim, metrics=metrics)
            finally:
                log_contextvars.unbind_contextvars(
                    "trace_id",
                    "source_event_id",
                    "user_id",
                    "queue_name",
                    "outbox_id",
                    "outbox_operation",
                )

        logger.info(
            "external_dispatcher_metrics",
            batch_size=metrics.batch_size,
            processed=metrics.processed,
            done=metrics.done,
            retried=metrics.retried,
            dead=metrics.dead,
            skipped=metrics.skipped,
        )
        return metrics.processed

    async def _process_claim(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        try:
            if claim.operation == OUTBOX_OP_LISTMONK_UPSERT:
                await self._process_listmonk_upsert(claim=claim, metrics=metrics)
                return
            if claim.operation == OUTBOX_OP_LISTMONK_DELETE:
                await self._process_listmonk_delete(claim=claim, metrics=metrics)
                return
            if claim.operation == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL:
                await self._process_invalid_email_block(claim=claim, metrics=metrics)
                return
            if claim.operation == OUTBOX_OP_MERGE_FINALIZE:
                await self._process_merge_finalize(claim=claim, metrics=metrics)
                return
            raise RuntimeError(f"Unsupported outbox operation: {claim.operation}")
        except TeycaRateLimitBusyError as exc:
            await self._defer_rate_limit_busy(
                outbox_id=claim.id,
                wait_seconds=exc.wait_seconds,
                error_text=str(exc),
            )
            metrics.retried += 1
            logger.warning(
                "external_dispatcher_job_rate_limit_deferred",
                outbox_id=claim.id,
                operation=claim.operation,
                wait_seconds=round(exc.wait_seconds, 3),
                max_wait_seconds=round(exc.max_wait_seconds, 3),
                backend=exc.backend,
            )
        except (ListmonkClientError, TeycaAPIError, httpx.HTTPError, RuntimeError) as exc:
            status = await self._mark_retry(
                outbox_id=claim.id,
                attempts=claim.attempts + 1,
                error_text=str(exc),
            )
            if status == "dead":
                metrics.dead += 1
            else:
                metrics.retried += 1
            logger.error(
                "external_dispatcher_job_retry_scheduled",
                outbox_id=claim.id,
                operation=claim.operation,
                attempts=claim.attempts + 1,
                status=status,
                error=str(exc),
                error_type=type(exc).__name__,
            )

    async def _process_listmonk_upsert(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        if not await self._user_exists(user_id=claim.user_id):
            await self._mark_done(outbox_id=claim.id, payload=claim.payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_listmonk_upsert_user_missing", outbox_id=claim.id)
            return
        email = _payload_text(claim.payload, key="email")
        attributes = dict(claim.payload.get("attributes") or {})
        list_ids = _payload_int_list(claim.payload, key="list_ids")
        subscriber_id = _payload_optional_int(claim.payload, key="subscriber_id")
        event_type = _payload_text(claim.payload, key="event_type") or "UPDATE"

        state = await self.listmonk_client.upsert_subscriber(
            email=email,
            list_ids=list_ids,
            attributes=attributes,
            subscriber_id=subscriber_id,
        )
        await self._apply_listmonk_upsert_success(
            claim=claim,
            state=state,
            event_type=event_type,
        )
        await self._mark_done(outbox_id=claim.id)
        metrics.done += 1
        logger.info(
            "external_dispatcher_listmonk_upsert_done",
            outbox_id=claim.id,
            subscriber_id=state.subscriber_id,
            status=state.status,
            list_ids=state.list_ids,
        )

    async def _apply_listmonk_upsert_success(
        self,
        *,
        claim: OutboxClaim,
        state: SubscriberState,
        event_type: str,
    ) -> None:
        async def operation(session: AsyncSession) -> None:
            users_repo = UsersRepository(session)
            listmonk_repo = ListmonkUsersRepository(session)
            email_repair_repo = EmailRepairLogRepository(session)

            current_user = await users_repo.get_by_user_id(user_id=claim.user_id)
            if current_user is None:
                return
            try:
                await listmonk_repo.upsert(
                    user_id=claim.user_id,
                    subscriber_id=state.subscriber_id,
                    email=_payload_text(claim.payload, key="email"),
                    status=state.status,
                    list_ids=state.list_ids,
                    attributes=dict(claim.payload.get("attributes") or {}),
                )
            except DuplicateListmonkSubscriberIdError as exc:
                logger.error(
                    "external_dispatcher_duplicate_subscriber_id",
                    outbox_id=claim.id,
                    user_id=claim.user_id,
                    subscriber_id=exc.subscriber_id,
                    existing_user_ids=exc.user_ids,
                )
                return
            except DuplicateListmonkUserEmailError as exc:
                for existing_user_id in exc.existing_user_ids:
                    await email_repair_repo.create_pending(
                        normalized_email=exc.normalized_email,
                        incoming_user_id=claim.user_id,
                        existing_user_id=existing_user_id,
                        source_event_type=event_type,
                        source_event_id=claim.source_event_id,
                        trace_id=claim.trace_id,
                    )
                logger.error(
                    "external_dispatcher_duplicate_email_scheduled",
                    outbox_id=claim.id,
                    user_id=claim.user_id,
                    email=exc.normalized_email,
                    existing_user_ids=exc.existing_user_ids,
                )
                return
            await listmonk_repo.set_consent_pending(user_id=claim.user_id)

        await self._run_in_session(operation)

    async def _process_listmonk_delete(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        subscriber_id = _payload_optional_int(claim.payload, key="subscriber_id")
        if subscriber_id is None:
            await self._mark_done(outbox_id=claim.id)
            metrics.skipped += 1
            logger.info("external_dispatcher_listmonk_delete_skipped", outbox_id=claim.id)
            return
        await self.listmonk_client.delete_subscriber(subscriber_id=subscriber_id)
        await self._mark_done(outbox_id=claim.id)
        metrics.done += 1
        logger.info(
            "external_dispatcher_listmonk_delete_done",
            outbox_id=claim.id,
            subscriber_id=subscriber_id,
        )

    async def _process_invalid_email_block(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        if not await self._user_exists(user_id=claim.user_id):
            await self._mark_done(outbox_id=claim.id, payload=claim.payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_invalid_email_block_user_missing", outbox_id=claim.id)
            return
        status = _payload_text(claim.payload, key="status") or "blocked"
        await self.teyca_client.update_pass_fields(
            user_id=claim.user_id,
            fields={"key1": status},
            rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
        )
        await self._apply_invalid_email_block_success(user_id=claim.user_id, status=status)
        await self._mark_done(outbox_id=claim.id)
        metrics.done += 1
        logger.info(
            "external_dispatcher_invalid_email_block_done",
            outbox_id=claim.id,
            status=status,
        )

    async def _apply_invalid_email_block_success(self, *, user_id: int, status: str) -> None:
        async def operation(session: AsyncSession) -> None:
            listmonk_repo = ListmonkUsersRepository(session)
            current = await listmonk_repo.get_by_user_id(user_id=user_id)
            if current is None:
                return
            await listmonk_repo.mark_checked(
                user_id=user_id,
                pending=False,
                confirmed=False,
                status=status,
            )

        await self._run_in_session(operation)

    async def _process_merge_finalize(
        self,
        *,
        claim: OutboxClaim,
        metrics: ExternalDispatcherMetrics,
    ) -> None:
        payload = _normalize_merge_payload(claim.payload)
        if await self._merge_already_logged(user_id=claim.user_id):
            payload["merge_logged"] = True
            await self._mark_done(outbox_id=claim.id, payload=payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_merge_already_done", outbox_id=claim.id)
            return
        if not await self._user_exists(user_id=claim.user_id):
            await self._mark_done(outbox_id=claim.id, payload=payload)
            metrics.skipped += 1
            logger.info("external_dispatcher_merge_user_missing", outbox_id=claim.id)
            return

        old_bonus_value = _payload_optional_float(payload, key="old_bonus_value")
        if not payload["bonus_done"] and old_bonus_value is not None and old_bonus_value > 0:
            await self.teyca_client.accrue_bonuses(
                user_id=claim.user_id,
                bonuses=[BonusOperation.one_shot(value=str(old_bonus_value))],
                rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
            )
            payload["bonus_done"] = True
            await self._save_progress(outbox_id=claim.id, payload=payload)

        if not payload["key2_done"]:
            await self.teyca_client.update_pass_fields(
                user_id=claim.user_id,
                fields={"key2": _payload_text(payload, key="merge_key2_value")},
                rate_limit_max_wait_seconds=self._teyca_rate_limit_max_wait_seconds(),
            )
            payload["key2_done"] = True
            await self._save_progress(outbox_id=claim.id, payload=payload)

        if not payload["merge_logged"]:
            await self._write_merge_log(
                user_id=claim.user_id,
                source_event_type=_payload_text(payload, key="source_event_type") or "UPDATE",
                source_event_id=claim.source_event_id,
                trace_id=claim.trace_id,
            )
            payload["merge_logged"] = True

        await self._mark_done(outbox_id=claim.id, payload=payload)
        metrics.done += 1
        logger.info(
            "external_dispatcher_merge_finalize_done",
            outbox_id=claim.id,
            bonus_done=payload["bonus_done"],
            key2_done=payload["key2_done"],
            merge_logged=payload["merge_logged"],
        )

    async def _user_exists(self, *, user_id: int) -> bool:
        async def operation(session: AsyncSession) -> bool:
            users_repo = UsersRepository(session)
            return await users_repo.get_by_user_id(user_id=user_id) is not None

        return bool(await self._run_in_session(operation))

    async def _merge_already_logged(self, *, user_id: int) -> bool:
        async def operation(session: AsyncSession) -> bool:
            merge_repo = MergeLogRepository(session)
            return await merge_repo.exists(user_id=user_id)

        return bool(await self._run_in_session(operation))

    async def _write_merge_log(
        self,
        *,
        user_id: int,
        source_event_type: str,
        source_event_id: str | None,
        trace_id: str | None,
    ) -> None:
        async def operation(session: AsyncSession) -> None:
            merge_repo = MergeLogRepository(session)
            if await merge_repo.exists(user_id=user_id):
                return
            await merge_repo.create(
                user_id=user_id,
                source_event_type=source_event_type,
                source_event_id=source_event_id,
                trace_id=trace_id,
            )

        await self._run_in_session(operation)


def _payload_text(payload: dict[str, Any], *, key: str) -> str | None:
    value = payload.get(key)
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _payload_optional_int(payload: dict[str, Any], *, key: str) -> int | None:
    value = payload.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _payload_optional_float(payload: dict[str, Any], *, key: str) -> float | None:
    value = payload.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _payload_int_list(payload: dict[str, Any], *, key: str) -> list[int]:
    raw = payload.get(key)
    if not isinstance(raw, list):
        return []
    result: list[int] = []
    for item in raw:
        if isinstance(item, bool):
            continue
        if isinstance(item, int):
            result.append(item)
            continue
        if isinstance(item, str) and item.strip().isdigit():
            result.append(int(item.strip()))
    return result


def _normalize_merge_payload(raw_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "bonus_done": bool(raw_payload.get("bonus_done", False)),
        "key2_done": bool(raw_payload.get("key2_done", False)),
        "merge_logged": bool(raw_payload.get("merge_logged", False)),
        "old_bonus_value": raw_payload.get("old_bonus_value"),
        "merge_key2_value": _payload_text(raw_payload, key="merge_key2_value"),
        "source_event_type": _payload_text(raw_payload, key="source_event_type") or "UPDATE",
    }


def build_external_dispatcher_worker() -> ExternalDispatcherWorker:
    settings = get_settings()
    return ExternalDispatcherWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
        teyca_client=build_teyca_client(settings),
        worker_id=f"external-dispatcher:{uuid4().hex}",
    )
