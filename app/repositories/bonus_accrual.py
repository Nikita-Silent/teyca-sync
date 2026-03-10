"""Repository for bonus accrual idempotency and status."""

from datetime import datetime, UTC
from typing import Any

from sqlalchemy import Select, delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import BonusAccrualLog


class BonusAccrualRepository:
    """Data access for bonus accrual log."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the repository with the provided AsyncSession for database access.
        
        Stores the session on self._session for use by repository methods.
        """
        self._session = session

    async def reserve(
        self,
        *,
        user_id: int,
        reason: str,
        idempotency_key: str,
        payload: dict[str, Any] | None,
    ) -> bool:
        """
        Ensure a pending bonus-accrual log exists for the given idempotency key.
        
        Inserts a new BonusAccrualLog with status "pending" only if no existing row uses the same idempotency key.
        
        Returns:
            True if a new row was inserted, False otherwise.
        """
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
        """
        Mark the idempotent operation identified by the given idempotency key as completed.
        
        Sets `status` to "done", clears `error_text`, and updates `processed_at` to the current UTC time for the matching BonusAccrualLog row.
        
        Parameters:
            idempotency_key (str): The idempotency key that identifies the operation to mark done.
        """
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

    async def save_progress(
        self,
        *,
        idempotency_key: str,
        payload: dict[str, Any],
        status: str = "pending",
        error_text: str | None = None,
    ) -> None:
        """
        Update an existing bonus accrual log's status, payload, and error text by idempotency key.
        
        Parameters:
            idempotency_key (str): Idempotency key that identifies the log entry to update.
            payload (dict[str, Any]): Progress payload to persist to the log.
            status (str): Status value to set on the log (default "pending").
            error_text (str | None): Optional diagnostic error text; set to `None` to clear existing text.
        """
        stmt = (
            update(BonusAccrualLog)
            .where(BonusAccrualLog.idempotency_key == idempotency_key)
            .values(
                status=status,
                payload=payload,
                error_text=error_text,
            )
        )
        await self._session.execute(stmt)

    async def mark_done_with_payload(
        self,
        *,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> None:
        """
        Mark the idempotent operation as done and store its final payload.
        
        Parameters:
            idempotency_key (str): Unique idempotency key identifying the operation to update.
            payload (dict[str, Any]): Final payload/state to persist for the operation.
        """
        stmt = (
            update(BonusAccrualLog)
            .where(BonusAccrualLog.idempotency_key == idempotency_key)
            .values(
                status="done",
                payload=payload,
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
        """
        Retrieve the BonusAccrualLog matching the given idempotency key.
        
        Returns:
            The matching BonusAccrualLog if found, `None` otherwise.
        """
        stmt: Select[tuple[BonusAccrualLog]] = select(BonusAccrualLog).where(
            BonusAccrualLog.idempotency_key == idempotency_key
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def delete_by_user_id(self, *, user_id: int) -> None:
        """Delete all bonus accrual rows for user."""
        stmt = delete(BonusAccrualLog).where(BonusAccrualLog.user_id == user_id)
        await self._session.execute(stmt)
