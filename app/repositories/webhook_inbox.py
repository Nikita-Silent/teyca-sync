"""Repository for the durable webhook inbox (replaces RabbitMQ, teyca-sync-8ib)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import Select, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import WebhookInbox
from app.retry_backoff import compute_retry_delay_ms

INBOX_STATUS_PENDING = "pending"
INBOX_STATUS_PROCESSING = "processing"
INBOX_STATUS_FAILED = "failed"
INBOX_STATUS_DONE = "done"
INBOX_STATUS_DEAD = "dead"


@dataclass(slots=True)
class InboxClaim:
    id: int
    source_event_id: str
    event_type: str
    payload: dict[str, Any]
    attempts: int
    trace_id: str | None


@dataclass(slots=True)
class DeadInboxRow:
    """Summary of a dead webhook_inbox row, for the replay CLI (teyca-sync-iil.6)."""

    id: int
    source_event_id: str
    event_type: str
    attempts: int
    last_error: str | None
    created_at: datetime


class WebhookInboxRepository:
    """Data access helpers for webhook_inbox."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def enqueue(
        self,
        *,
        source_event_id: str,
        event_type: str,
        payload: dict[str, Any],
        trace_id: str | None,
    ) -> bool:
        """Insert a new inbox row. Returns False when source_event_id already exists
        (redelivered webhook) — dedupe is idempotent, caller still responds 200."""
        stmt = (
            insert(WebhookInbox)
            .values(
                source_event_id=source_event_id,
                event_type=event_type,
                payload=payload,
                status=INBOX_STATUS_PENDING,
                attempts=0,
                next_retry_at=None,
                locked_at=None,
                locked_by=None,
                last_error=None,
                trace_id=trace_id,
                processed_at=None,
            )
            .on_conflict_do_nothing(constraint="uq_webhook_inbox_source_event_id")
        )
        result = await self._session.execute(stmt)
        return int(getattr(result, "rowcount", 0) or 0) > 0

    async def claim_batch(self, *, limit: int, worker_id: str) -> list[InboxClaim]:
        """Claim due rows in a short transaction using SKIP LOCKED."""
        if limit <= 0:
            return []
        now = datetime.now(UTC)
        stmt: Select[tuple[WebhookInbox]] = (
            select(WebhookInbox)
            .where(
                WebhookInbox.status.in_((INBOX_STATUS_PENDING, INBOX_STATUS_FAILED)),
                or_(
                    WebhookInbox.next_retry_at.is_(None),
                    WebhookInbox.next_retry_at <= now,
                ),
            )
            .order_by(WebhookInbox.created_at.asc(), WebhookInbox.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        claims: list[InboxClaim] = []
        for row in rows:
            row.status = INBOX_STATUS_PROCESSING
            row.locked_at = now
            row.locked_by = worker_id
            claims.append(
                InboxClaim(
                    id=int(row.id),
                    source_event_id=str(row.source_event_id),
                    event_type=str(row.event_type),
                    payload=dict(row.payload or {}),
                    attempts=int(row.attempts),
                    trace_id=row.trace_id,
                )
            )
        return claims

    async def mark_done(self, *, inbox_id: int) -> None:
        """Mark row done and release any processing lock."""
        now = datetime.now(UTC)
        stmt = (
            update(WebhookInbox)
            .where(WebhookInbox.id == inbox_id)
            .values(
                status=INBOX_STATUS_DONE,
                locked_at=None,
                locked_by=None,
                last_error=None,
                next_retry_at=None,
                processed_at=now,
            )
        )
        await self._session.execute(stmt)

    async def mark_retry(
        self,
        *,
        inbox_id: int,
        attempts: int,
        error_text: str,
        max_attempts: int,
        base_delay_ms: int,
        max_delay_ms: int,
    ) -> str:
        """Schedule retry or move the row to dead state."""
        status = INBOX_STATUS_FAILED
        next_retry_at = datetime.now(UTC) + timedelta(
            milliseconds=compute_retry_delay_ms(
                retry_count=attempts,
                base_delay_ms=base_delay_ms,
                max_delay_ms=max_delay_ms,
            )
        )
        if attempts >= max_attempts:
            status = INBOX_STATUS_DEAD
            next_retry_at = None
        stmt = (
            update(WebhookInbox)
            .where(WebhookInbox.id == inbox_id)
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

    async def get_dead_batch(
        self,
        *,
        limit: int,
        since: datetime | None = None,
        event_type: str | None = None,
    ) -> list[DeadInboxRow]:
        """List dead rows for inspection/replay, newest first. `since` filters on
        `created_at` (when the original event arrived, not when it died)."""
        stmt: Select[tuple[WebhookInbox]] = (
            select(WebhookInbox)
            .where(WebhookInbox.status == INBOX_STATUS_DEAD)
            .order_by(WebhookInbox.created_at.desc())
            .limit(limit)
        )
        if since is not None:
            stmt = stmt.where(WebhookInbox.created_at >= since)
        if event_type is not None:
            stmt = stmt.where(WebhookInbox.event_type == event_type)
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        return [
            DeadInboxRow(
                id=int(row.id),
                source_event_id=str(row.source_event_id),
                event_type=str(row.event_type),
                attempts=int(row.attempts),
                last_error=row.last_error,
                created_at=row.created_at,
            )
            for row in rows
        ]

    async def replay_dead(self, *, inbox_ids: list[int]) -> int:
        """Reset dead rows back to pending with a clean retry slate. Only rows
        still `dead` are touched — a row already reclaimed by the normal worker
        loop (e.g. released from a stale `processing` claim) is left alone."""
        if not inbox_ids:
            return 0
        stmt = (
            update(WebhookInbox)
            .where(
                WebhookInbox.id.in_(inbox_ids),
                WebhookInbox.status == INBOX_STATUS_DEAD,
            )
            .values(
                status=INBOX_STATUS_PENDING,
                attempts=0,
                next_retry_at=None,
                last_error=None,
                locked_at=None,
                locked_by=None,
            )
        )
        result = await self._session.execute(stmt)
        return int(getattr(result, "rowcount", 0) or 0)

    async def release_stale_processing_claims(
        self,
        *,
        stale_after_seconds: float = 300.0,
    ) -> int:
        """Reset PROCESSING rows stuck longer than stale_after_seconds back to PENDING."""
        cutoff = datetime.now(UTC) - timedelta(seconds=max(1.0, stale_after_seconds))
        stmt = (
            update(WebhookInbox)
            .where(
                WebhookInbox.status == INBOX_STATUS_PROCESSING,
                WebhookInbox.locked_at.is_not(None),
                WebhookInbox.locked_at < cutoff,
            )
            .values(
                status=INBOX_STATUS_PENDING,
                locked_at=None,
                locked_by=None,
            )
        )
        result = await self._session.execute(stmt)
        return int(getattr(result, "rowcount", 0) or 0)
