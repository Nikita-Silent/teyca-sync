"""Repository for durable external side effects outbox."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Select, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ExternalCallOutbox

OUTBOX_STATUS_PENDING = "pending"
OUTBOX_STATUS_PROCESSING = "processing"
OUTBOX_STATUS_FAILED = "failed"
OUTBOX_STATUS_DONE = "done"
OUTBOX_STATUS_DEAD = "dead"

OUTBOX_OP_LISTMONK_UPSERT = "listmonk_upsert"
OUTBOX_OP_LISTMONK_DELETE = "listmonk_delete"
OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL = "teyca_block_invalid_email"
OUTBOX_OP_MERGE_FINALIZE = "merge_finalize"


def dedupe_key_for_listmonk_sync(*, user_id: int) -> str:
    return f"listmonk-sync:{user_id}"


def dedupe_key_for_listmonk_delete(*, user_id: int) -> str:
    return f"listmonk-delete:{user_id}"


def dedupe_key_for_invalid_email_block(*, user_id: int) -> str:
    return f"invalid-email-block:{user_id}"


def dedupe_key_for_merge_finalize(*, user_id: int) -> str:
    return f"merge-finalize:{user_id}"


@dataclass(slots=True)
class OutboxClaim:
    id: int
    operation: str
    dedupe_key: str
    user_id: int
    payload: dict[str, Any]
    attempts: int
    trace_id: str | None
    source_event_id: str | None
    queue_name: str | None


class ExternalCallOutboxRepository:
    """Data access helpers for external_call_outbox."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def enqueue_latest(
        self,
        *,
        operation: str,
        dedupe_key: str,
        user_id: int,
        payload: dict[str, Any] | None,
        trace_id: str | None,
        source_event_id: str | None,
        queue_name: str | None,
    ) -> None:
        """Insert or replace the latest desired state for a dedupe key."""
        stmt = insert(ExternalCallOutbox).values(
            operation=operation,
            dedupe_key=dedupe_key,
            user_id=user_id,
            status=OUTBOX_STATUS_PENDING,
            payload=payload,
            attempts=0,
            next_retry_at=None,
            locked_at=None,
            locked_by=None,
            last_error=None,
            trace_id=trace_id,
            source_event_id=source_event_id,
            queue_name=queue_name,
            processed_at=None,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_external_call_outbox_dedupe_key",
            set_={
                "operation": operation,
                "user_id": user_id,
                "status": OUTBOX_STATUS_PENDING,
                "payload": payload,
                "attempts": 0,
                "next_retry_at": None,
                "locked_at": None,
                "locked_by": None,
                "last_error": None,
                "trace_id": trace_id,
                "source_event_id": source_event_id,
                "queue_name": queue_name,
                "processed_at": None,
            },
        )
        await self._session.execute(stmt)

    async def enqueue_once(
        self,
        *,
        operation: str,
        dedupe_key: str,
        user_id: int,
        payload: dict[str, Any] | None,
        trace_id: str | None,
        source_event_id: str | None,
        queue_name: str | None,
    ) -> bool:
        """Insert a new outbox item when the dedupe key is absent."""
        stmt = (
            insert(ExternalCallOutbox)
            .values(
                operation=operation,
                dedupe_key=dedupe_key,
                user_id=user_id,
                status=OUTBOX_STATUS_PENDING,
                payload=payload,
                attempts=0,
                next_retry_at=None,
                locked_at=None,
                locked_by=None,
                last_error=None,
                trace_id=trace_id,
                source_event_id=source_event_id,
                queue_name=queue_name,
                processed_at=None,
            )
            .on_conflict_do_nothing(constraint="uq_external_call_outbox_dedupe_key")
        )
        result = await self._session.execute(stmt)
        return int(getattr(result, "rowcount", 0) or 0) > 0

    async def claim_batch(
        self,
        *,
        operations: list[str],
        limit: int,
        worker_id: str,
    ) -> list[OutboxClaim]:
        """Claim due jobs in a short transaction using SKIP LOCKED."""
        if not operations:
            return []
        now = datetime.now(UTC)
        stmt: Select[tuple[ExternalCallOutbox]] = (
            select(ExternalCallOutbox)
            .where(
                ExternalCallOutbox.operation.in_(operations),
                ExternalCallOutbox.status.in_((OUTBOX_STATUS_PENDING, OUTBOX_STATUS_FAILED)),
                or_(
                    ExternalCallOutbox.next_retry_at.is_(None),
                    ExternalCallOutbox.next_retry_at <= now,
                ),
            )
            .order_by(ExternalCallOutbox.created_at.asc(), ExternalCallOutbox.id.asc())
            .limit(max(1, limit))
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        claims: list[OutboxClaim] = []
        for row in rows:
            row.status = OUTBOX_STATUS_PROCESSING
            row.locked_at = now
            row.locked_by = worker_id
            claims.append(
                OutboxClaim(
                    id=int(row.id),
                    operation=str(row.operation),
                    dedupe_key=str(row.dedupe_key),
                    user_id=int(row.user_id),
                    payload=dict(row.payload or {}),
                    attempts=int(row.attempts),
                    trace_id=row.trace_id,
                    source_event_id=row.source_event_id,
                    queue_name=row.queue_name,
                )
            )
        return claims

    async def get_by_dedupe_key(self, *, dedupe_key: str) -> ExternalCallOutbox | None:
        """Return one outbox row by unique dedupe key."""
        stmt: Select[tuple[ExternalCallOutbox]] = select(ExternalCallOutbox).where(
            ExternalCallOutbox.dedupe_key == dedupe_key
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def save_progress(
        self,
        *,
        outbox_id: int,
        payload: dict[str, Any],
        error_text: str | None = None,
    ) -> None:
        """Persist intermediate payload progress for a claimed job."""
        stmt = (
            update(ExternalCallOutbox)
            .where(ExternalCallOutbox.id == outbox_id)
            .values(payload=payload, last_error=error_text)
        )
        await self._session.execute(stmt)

    async def mark_done(
        self,
        *,
        outbox_id: int,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """Mark job done and release any processing lock."""
        now = datetime.now(UTC)
        values: dict[str, Any] = {
            "status": OUTBOX_STATUS_DONE,
            "locked_at": None,
            "locked_by": None,
            "last_error": None,
            "next_retry_at": None,
            "processed_at": now,
        }
        if payload is not None:
            values["payload"] = payload
        stmt = update(ExternalCallOutbox).where(ExternalCallOutbox.id == outbox_id).values(**values)
        await self._session.execute(stmt)

    async def mark_retry(
        self,
        *,
        outbox_id: int,
        attempts: int,
        error_text: str,
        max_attempts: int,
        base_delay_ms: int,
        max_delay_ms: int,
    ) -> str:
        """Schedule retry or move the job to dead state."""
        status = OUTBOX_STATUS_FAILED
        next_retry_at = datetime.now(UTC) + timedelta(
            milliseconds=_compute_retry_delay_ms(
                retry_count=attempts,
                base_delay_ms=base_delay_ms,
                max_delay_ms=max_delay_ms,
            )
        )
        if attempts >= max_attempts:
            status = OUTBOX_STATUS_DEAD
            next_retry_at = None
        stmt = (
            update(ExternalCallOutbox)
            .where(ExternalCallOutbox.id == outbox_id)
            .values(
                status=status,
                attempts=attempts,
                next_retry_at=next_retry_at,
                last_error=error_text,
                locked_at=None,
                locked_by=None,
            )
        )
        await self._session.execute(stmt)
        return status

    async def release_claim(self, *, outbox_id: int, error_text: str) -> None:
        """Release processing lock without consuming an attempt."""
        stmt = (
            update(ExternalCallOutbox)
            .where(ExternalCallOutbox.id == outbox_id)
            .values(
                status=OUTBOX_STATUS_PENDING,
                locked_at=None,
                locked_by=None,
                last_error=error_text,
            )
        )
        await self._session.execute(stmt)

    async def count_by_status(self) -> dict[str, int]:
        """Return aggregated counts grouped by status."""
        stmt = select(ExternalCallOutbox.status, func.count(ExternalCallOutbox.id)).group_by(
            ExternalCallOutbox.status
        )
        result = await self._session.execute(stmt)
        return {str(status): int(count) for status, count in result.all()}


def _compute_retry_delay_ms(*, retry_count: int, base_delay_ms: int, max_delay_ms: int) -> int:
    bounded_retry_count = max(1, retry_count)
    delay_ms = max(1, base_delay_ms) * (2 ** (bounded_retry_count - 1))
    return min(delay_ms, max(1, max_delay_ms))
