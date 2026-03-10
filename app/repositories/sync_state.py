"""Repository for incremental sync watermark state."""

from datetime import UTC, datetime

from sqlalchemy import Select, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import SyncState


class SyncStateRepository:
    """Data access for sync state records."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_or_create(self, *, source: str, list_id: int) -> SyncState:
        """Get watermark state row, create if missing."""
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
        """Update watermark fields for given source/list."""
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
