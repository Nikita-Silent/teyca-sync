"""Repository for bonus accrual idempotency and status."""

from datetime import datetime, UTC
from typing import Any

from sqlalchemy import Select, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import BonusAccrualLog


class BonusAccrualRepository:
    """Data access for bonus accrual log."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def reserve(
        self,
        *,
        user_id: int,
        reason: str,
        idempotency_key: str,
        payload: dict[str, Any] | None,
    ) -> bool:
        """Insert pending operation if idempotency key does not exist."""
        stmt = (
            insert(BonusAccrualLog)
            .values(
                user_id=user_id,
                reason=reason,
                idempotency_key=idempotency_key,
                status="pending",
                payload=payload,
            )
            .on_conflict_do_nothing(constraint="uq_bonus_accrual_idempotency_key")
        )
        result = await self._session.execute(stmt)
        return result.rowcount > 0

    async def mark_done(self, *, idempotency_key: str) -> None:
        """Mark operation as successfully processed."""
        stmt = (
            update(BonusAccrualLog)
            .where(BonusAccrualLog.idempotency_key == idempotency_key)
            .values(
                status="done",
                error_text=None,
                processed_at=datetime.now(UTC),
            )
        )
        await self._session.execute(stmt)

    async def mark_failed(self, *, idempotency_key: str, error_text: str) -> None:
        """Mark operation as failed with diagnostic text."""
        stmt = (
            update(BonusAccrualLog)
            .where(BonusAccrualLog.idempotency_key == idempotency_key)
            .values(
                status="failed",
                error_text=error_text,
            )
        )
        await self._session.execute(stmt)

    async def get_by_key(self, *, idempotency_key: str) -> BonusAccrualLog | None:
        """Return operation by idempotency key."""
        stmt: Select[tuple[BonusAccrualLog]] = select(BonusAccrualLog).where(
            BonusAccrualLog.idempotency_key == idempotency_key
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
