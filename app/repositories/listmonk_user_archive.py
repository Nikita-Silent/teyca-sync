"""Repository for archived loser rows from duplicate subscriber repair."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ListmonkUser, ListmonkUserArchive


class ListmonkUserArchiveRepository:
    """Data access for listmonk_user_archive table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def archive_loser(
        self,
        *,
        row: ListmonkUser,
        winner_user_id: int,
        winner_subscriber_id: int,
        archive_reason: str,
    ) -> None:
        archived = ListmonkUserArchive(
            user_id=int(row.user_id),
            subscriber_id=int(row.subscriber_id),
            email=row.email,
            status=row.status,
            list_ids=row.list_ids,
            attributes=row.attributes,
            consent_pending=bool(row.consent_pending),
            consent_checked_at=row.consent_checked_at,
            consent_confirmed_at=row.consent_confirmed_at,
            original_created_at=row.created_at,
            original_updated_at=row.updated_at,
            archive_reason=archive_reason,
            winner_user_id=winner_user_id,
            winner_subscriber_id=winner_subscriber_id,
        )
        self._session.add(archived)
