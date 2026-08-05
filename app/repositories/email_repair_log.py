"""Repository for duplicate-email remediation scheduling."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import Select, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.db.models import EmailRepairLog, User


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
        mark_bad_email: bool = True,
    ) -> int:
        """Persist a loser/winner plan after local DB normalization is committed.

        `mark_bad_email=False` is the Р5/Р6 "same person" case (teyca-sync-37z):
        the loser's email is cleared but Teyca sync must not set key1=bad email.
        Returns the row id, used as the outbox dedupe key for Teyca sync.
        """
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
            mark_bad_email=mark_bad_email,
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
                "mark_bad_email": mark_bad_email,
            },
        )
        stmt = stmt.returning(EmailRepairLog.id)
        result = await self._session.execute(stmt)
        return int(result.scalar_one())

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

    async def get_stale_pending_batch(self, *, limit: int) -> list[EmailRepairLog]:
        """Return pending rows whose recorded conflict no longer exists.

        A pending row is stale once either side's current `users.email` no
        longer matches `normalized_email` (`IS DISTINCT FROM`, so a missing
        user or a NULL email counts as a mismatch too) — the conflict it
        describes was already resolved by other means, most likely this
        row's own duplicate group getting cleared by the y1c policy
        backfill under a different email_repair_log row. The never-scheduled
        email_repair_worker never marked the original row done, so it was
        left pending indefinitely (teyca-sync-y1c).
        """
        incoming_user = aliased(User)
        existing_user = aliased(User)
        normalized_incoming = func.lower(func.trim(incoming_user.email))
        normalized_existing = func.lower(func.trim(existing_user.email))
        stmt: Select[tuple[EmailRepairLog]] = (
            select(EmailRepairLog)
            .outerjoin(incoming_user, incoming_user.user_id == EmailRepairLog.incoming_user_id)
            .outerjoin(existing_user, existing_user.user_id == EmailRepairLog.existing_user_id)
            .where(
                EmailRepairLog.status == "pending",
                or_(
                    normalized_incoming.is_distinct_from(EmailRepairLog.normalized_email),
                    normalized_existing.is_distinct_from(EmailRepairLog.normalized_email),
                ),
            )
            .order_by(EmailRepairLog.id.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def mark_stale(self, *, repair_id: int, reason: str) -> None:
        """Terminally mark a pending row whose recorded conflict no longer exists."""
        now = datetime.now(UTC)
        stmt = (
            update(EmailRepairLog)
            .where(EmailRepairLog.id == repair_id)
            .values(status="stale", error_text=reason, processed_at=now, next_retry_at=None)
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
