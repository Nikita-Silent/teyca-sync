"""Repository for listmonk user sync state."""

from datetime import UTC, datetime

from sqlalchemy import Select, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ListmonkUser


class ListmonkUsersRepository:
    """Data access for listmonk_users table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def set_consent_pending(self, *, user_id: int) -> None:
        """Mark user for consent sync worker processing."""
        stmt = (
            update(ListmonkUser)
            .where(ListmonkUser.user_id == user_id)
            .values(consent_pending=True)
        )
        await self._session.execute(stmt)

    async def get_pending_batch(self, *, limit: int) -> list[ListmonkUser]:
        """Return batch of pending users ordered by user_id."""
        stmt: Select[tuple[ListmonkUser]] = (
            select(ListmonkUser)
            .where(ListmonkUser.consent_pending.is_(True))
            .order_by(ListmonkUser.user_id.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def mark_checked(self, *, user_id: int, pending: bool, confirmed: bool) -> None:
        """Store consent check timestamps and pending state."""
        now = datetime.now(UTC)
        values: dict[str, object] = {
            "consent_pending": pending,
            "consent_checked_at": now,
        }
        if confirmed:
            values["consent_confirmed_at"] = now
        stmt = update(ListmonkUser).where(ListmonkUser.user_id == user_id).values(**values)
        await self._session.execute(stmt)
