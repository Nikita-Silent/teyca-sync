"""Repository for duplicate-email remediation scheduling."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import Select, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import EmailRepairLog


class EmailRepairLogRepository:
    """Data access for email_repair_log scheduling state."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create_pending(
        self,
        *,
        normalized_email: str,
        incoming_user_id: int,
        existing_user_id: int,
        source_event_type: str,
        source_event_id: str | None,
        trace_id: str | None,
    ) -> None:
        """Insert or refresh a pending duplicate-email remediation row."""
        stmt = insert(EmailRepairLog).values(
            normalized_email=normalized_email,
            incoming_user_id=incoming_user_id,
            existing_user_id=existing_user_id,
            source_event_type=source_event_type,
            source_event_id=source_event_id,
            trace_id=trace_id,
            status="pending",
            attempts=0,
            next_retry_at=None,
            error_text=None,
            processed_at=None,
            winner_user_id=None,
            winner_subscriber_id=None,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_email_repair_log_email_user_pair",
            set_={
                "source_event_type": source_event_type,
                "source_event_id": source_event_id,
                "trace_id": trace_id,
                "status": "pending",
                "attempts": 0,
                "next_retry_at": None,
                "error_text": None,
                "processed_at": None,
                "winner_user_id": None,
                "winner_subscriber_id": None,
            },
        )
        await self._session.execute(stmt)

    async def create_db_applied(
        self,
        *,
        normalized_email: str,
        incoming_user_id: int,
        existing_user_id: int,
        winner_user_id: int,
        winner_subscriber_id: int | None,
        source_event_id: str | None,
        trace_id: str | None,
    ) -> None:
        """Persist a loser/winner plan after local DB normalization is committed."""
        now = datetime.now(UTC)
        stmt = insert(EmailRepairLog).values(
            normalized_email=normalized_email,
            incoming_user_id=incoming_user_id,
            existing_user_id=existing_user_id,
            winner_user_id=winner_user_id,
            winner_subscriber_id=winner_subscriber_id,
            source_event_type="BACKFILL",
            source_event_id=source_event_id,
            trace_id=trace_id,
            status="db_applied",
            attempts=0,
            next_retry_at=None,
            error_text=None,
            processed_at=now,
        )
        stmt = stmt.on_conflict_do_update(
            constraint="uq_email_repair_log_email_user_pair",
            set_={
                "winner_user_id": winner_user_id,
                "winner_subscriber_id": winner_subscriber_id,
                "source_event_type": "BACKFILL",
                "source_event_id": source_event_id,
                "trace_id": trace_id,
                "status": "db_applied",
                "attempts": 0,
                "next_retry_at": None,
                "error_text": None,
                "processed_at": now,
            },
        )
        await self._session.execute(stmt)

    async def get_pending_batch(self, *, limit: int) -> list[EmailRepairLog]:
        """Return rows ready for remediation processing."""
        now = datetime.now(UTC)
        stmt: Select[tuple[EmailRepairLog]] = (
            select(EmailRepairLog)
            .where(
                EmailRepairLog.status.in_(("pending", "failed")),
                (EmailRepairLog.next_retry_at.is_(None)) | (EmailRepairLog.next_retry_at <= now),
            )
            .order_by(EmailRepairLog.created_at.asc(), EmailRepairLog.id.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_db_applied_batch(self, *, limit: int) -> list[EmailRepairLog]:
        """Return rows whose local DB cleanup is done and Teyca sync is pending."""
        now = datetime.now(UTC)
        stmt: Select[tuple[EmailRepairLog]] = (
            select(EmailRepairLog)
            .where(
                EmailRepairLog.winner_user_id.is_not(None),
                EmailRepairLog.status.in_(("db_applied", "failed")),
                (EmailRepairLog.next_retry_at.is_(None)) | (EmailRepairLog.next_retry_at <= now),
            )
            .order_by(EmailRepairLog.created_at.asc(), EmailRepairLog.id.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def mark_processing(self, *, repair_id: int) -> None:
        """Mark a remediation row as being processed."""
        stmt = (
            update(EmailRepairLog)
            .where(EmailRepairLog.id == repair_id)
            .values(status="processing", error_text=None)
        )
        await self._session.execute(stmt)

    async def mark_teyca_synced(
        self,
        *,
        repair_id: int,
        winner_user_id: int,
        winner_subscriber_id: int | None,
    ) -> None:
        """Mark remediation row as fully applied, including Teyca sync."""
        now = datetime.now(UTC)
        stmt = (
            update(EmailRepairLog)
            .where(EmailRepairLog.id == repair_id)
            .values(
                status="teyca_synced",
                winner_user_id=winner_user_id,
                winner_subscriber_id=winner_subscriber_id,
                processed_at=now,
                next_retry_at=None,
                error_text=None,
            )
        )
        await self._session.execute(stmt)

    async def mark_retry(
        self,
        *,
        repair_id: int,
        attempts: int,
        error_text: str,
        max_attempts: int,
    ) -> str:
        """Schedule another attempt or terminal manual review."""
        status = "manual_review" if attempts >= max_attempts else "failed"
        next_retry_at = None
        if status == "failed":
            next_retry_at = datetime.now(UTC) + timedelta(minutes=min(60, 5 * attempts))
        stmt = (
            update(EmailRepairLog)
            .where(EmailRepairLog.id == repair_id)
            .values(
                status=status,
                attempts=attempts,
                error_text=error_text,
                next_retry_at=next_retry_at,
                processed_at=None,
            )
        )
        await self._session.execute(stmt)
        return status
