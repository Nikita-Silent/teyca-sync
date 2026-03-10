"""Repository for merge_log table operations."""

from __future__ import annotations

from sqlalchemy import Select, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import MergeLog


class MergeLogRepository:
    """Data access for merge_log table."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the repository with an asynchronous database session for use by its methods.
        """
        self._session = session

    async def exists(self, *, user_id: int) -> bool:
        """
        Check whether a merge_log record exists for the given user.
        
        Returns:
            `true` if a merge_log record exists for the given user, `false` otherwise.
        """
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
        """
        Create a new merge_log record for the given user.
        
        Parameters:
            user_id (int): ID of the user the merge log will reference.
            source_event_type (str): Type or category of the source event that triggered the merge.
            source_event_id (str | None): Identifier of the source event, if available.
            trace_id (str | None): Optional trace identifier for cross-request correlation.
        """
        self._session.add(
            MergeLog(
                user_id=user_id,
                source_event_type=source_event_type,
                source_event_id=source_event_id,
                trace_id=trace_id,
            )
        )

    async def delete_by_user_id(self, *, user_id: int) -> None:
        """
        Delete all MergeLog rows for the specified user ID.
        
        Removes every record in the merge_log table whose MergeLog.user_id equals the provided user_id.
        """
        stmt = delete(MergeLog).where(MergeLog.user_id == user_id)
        await self._session.execute(stmt)
