"""Repository for merge_log table operations."""

from __future__ import annotations

from sqlalchemy import Select, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import MergeLog


class MergeLogRepository:
    """Data access for merge_log table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def exists(self, *, user_id: int) -> bool:
        """Return True when merge record exists for user."""
        stmt: Select[tuple[MergeLog]] = select(MergeLog.id).where(MergeLog.user_id == user_id).limit(1)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def create(
        self,
        *,
        user_id: int,
        source_event_type: str,
        source_event_id: str | None = None,
        trace_id: str | None = None,
    ) -> None:
        """Insert merge fact row."""
        self._session.add(
            MergeLog(
                user_id=user_id,
                source_event_type=source_event_type,
                source_event_id=source_event_id,
                trace_id=trace_id,
            )
        )

    async def delete_by_user_id(self, *, user_id: int) -> None:
        """Delete merge log rows by user_id."""
        stmt = delete(MergeLog).where(MergeLog.user_id == user_id)
        await self._session.execute(stmt)
