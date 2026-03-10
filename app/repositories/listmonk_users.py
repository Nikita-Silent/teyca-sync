"""Repository for listmonk user sync state."""

from datetime import UTC, datetime

from sqlalchemy import Select, delete, select, update
from sqlalchemy.dialects.postgresql import insert
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

    async def get_by_user_id(self, *, user_id: int) -> ListmonkUser | None:
        """Return listmonk state by user_id."""
        stmt: Select[tuple[ListmonkUser]] = select(ListmonkUser).where(ListmonkUser.user_id == user_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_subscriber_id(self, *, subscriber_id: int) -> ListmonkUser | None:
        """Return listmonk state by subscriber_id."""
        stmt: Select[tuple[ListmonkUser]] = select(ListmonkUser).where(
            ListmonkUser.subscriber_id == subscriber_id
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def upsert(
        self,
        *,
        user_id: int,
        subscriber_id: int,
        email: str | None,
        status: str | None,
        list_ids: list[int],
        attributes: dict[str, object] | None,
    ) -> None:
        """Insert/update listmonk state row."""
        list_ids_text = ",".join(str(item) for item in list_ids)
        insert_stmt = insert(ListmonkUser).values(
            user_id=user_id,
            subscriber_id=subscriber_id,
            email=email,
            status=status,
            list_ids=list_ids_text,
            attributes=attributes,
        )
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[ListmonkUser.user_id],
            set_={
                "subscriber_id": subscriber_id,
                "email": email,
                "status": status,
                "list_ids": list_ids_text,
                "attributes": attributes,
            },
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

    async def get_batch_after_user_id(self, *, last_user_id: int, limit: int) -> list[ListmonkUser]:
        """Return users batch ordered by user_id for consistency scans."""
        stmt: Select[tuple[ListmonkUser]] = (
            select(ListmonkUser)
            .where(ListmonkUser.user_id > last_user_id)
            .order_by(ListmonkUser.user_id.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def mark_checked(
        self,
        *,
        user_id: int,
        pending: bool,
        confirmed: bool,
        status: str | None = None,
    ) -> None:
        """Store consent check timestamps and pending state."""
        now = datetime.now(UTC)
        values: dict[str, object] = {
            "consent_pending": pending,
            "consent_checked_at": now,
        }
        if status is not None:
            values["status"] = status
        if confirmed:
            values["consent_confirmed_at"] = now
        stmt = update(ListmonkUser).where(ListmonkUser.user_id == user_id).values(**values)
        await self._session.execute(stmt)

    async def delete_by_user_id(self, *, user_id: int) -> None:
        """Delete listmonk state row."""
        stmt = delete(ListmonkUser).where(ListmonkUser.user_id == user_id)
        await self._session.execute(stmt)
