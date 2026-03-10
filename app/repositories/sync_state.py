"""Repository for incremental sync watermark state."""

from datetime import UTC, datetime

from sqlalchemy import Select, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import SyncState


class SyncStateRepository:
    """Data access for sync state records."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the repository with an async database session.
        
        Parameters:
            session (AsyncSession): Async SQLAlchemy session used for database operations.
        """
        self._session = session

    async def get_or_create(self, *, source: str, list_id: int) -> SyncState:
        """
        Retrieve the SyncState row for the given source and list_id, creating it if missing.
        
        Returns:
            SyncState: The existing or newly created SyncState row matching the provided source and list_id.
        
        Raises:
            RuntimeError: If the row cannot be loaded after attempting to insert it.
        """
        stmt: Select[tuple[SyncState]] = select(SyncState).where(
            SyncState.source == source,
            SyncState.list_id == list_id,
        )
        result = await self._session.execute(stmt)
        current = result.scalar_one_or_none()
        if current is not None:
            return current

        insert_stmt = (
            insert(SyncState)
            .values(source=source, list_id=list_id)
            .on_conflict_do_nothing(constraint="uq_sync_state_source_list")
        )
        await self._session.execute(insert_stmt)
        result = await self._session.execute(stmt)
        created = result.scalar_one_or_none()
        if created is None:
            raise RuntimeError("Unable to load sync_state after insert")
        return created

    async def update_watermark(
        self,
        *,
        source: str,
        list_id: int,
        updated_at: datetime | None,
        subscriber_id: int | None,
    ) -> None:
        """
        Update the watermark timestamps and subscriber identifier for the SyncState row matching the given source and list_id.
        
        Parameters:
            source (str): Sync source identifier used to locate the SyncState row.
            list_id (int): Numeric list identifier used to locate the SyncState row.
            updated_at (datetime | None): New watermark timestamp to store; use `None` to clear the watermark timestamp.
            subscriber_id (int | None): New watermark subscriber identifier to store; use `None` to clear the subscriber id.
        """
        stmt = (
            update(SyncState)
            .where(SyncState.source == source, SyncState.list_id == list_id)
            .values(
                watermark_updated_at=updated_at,
                watermark_subscriber_id=subscriber_id,
                updated_at=datetime.now(UTC),
            )
        )
        await self._session.execute(stmt)
