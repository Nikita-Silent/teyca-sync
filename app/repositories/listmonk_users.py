"""Repository for listmonk user sync state."""

from datetime import UTC, datetime

from sqlalchemy import Select, delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ListmonkUser


class ListmonkUsersRepository:
    """Data access for listmonk_users table."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the repository with an async SQLAlchemy session for database operations.
        
        Parameters:
            session (AsyncSession): AsyncSession instance used by repository methods to execute queries and transactions.
        """
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
        """
        Fetch the ListmonkUser record for the given user_id.
        
        Parameters:
            user_id (int): The application user ID to look up.
        
        Returns:
            ListmonkUser | None: The matching ListmonkUser instance, or None if no record exists.
        """
        stmt: Select[tuple[ListmonkUser]] = select(ListmonkUser).where(ListmonkUser.user_id == user_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_subscriber_id(self, *, subscriber_id: int) -> ListmonkUser | None:
        """
        Fetches the ListmonkUser record matching the given subscriber_id.
        
        Returns:
            ListmonkUser | None: The ListmonkUser with the matching subscriber_id, or None if no record exists.
        """
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
        """
        Insert or update the ListmonkUser row for the given user.
        
        Performs an upsert keyed on user_id: inserts a new row or updates subscriber_id, email, status, list_ids, and attributes when a row for user_id already exists.
        
        Parameters:
            list_ids (list[int]): List IDs; converted to a comma-separated string for storage.
            attributes (dict[str, object] | None): Optional arbitrary attributes stored on the row.
        """
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
        """
        Retrieve pending ListmonkUser records ordered by ascending user_id.
        
        Parameters:
            limit (int): Maximum number of users to return.
        
        Returns:
            list[ListmonkUser]: List of ListmonkUser objects where `consent_pending` is True, ordered by `user_id` ascending.
        """
        stmt: Select[tuple[ListmonkUser]] = (
            select(ListmonkUser)
            .where(ListmonkUser.consent_pending.is_(True))
            .order_by(ListmonkUser.user_id.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_batch_after_user_id(self, *, last_user_id: int, limit: int) -> list[ListmonkUser]:
        """
        Fetch a batch of ListmonkUser rows with user_id greater than a given value, ordered by user_id.
        
        Parameters:
            last_user_id (int): Exclusive lower bound for user_id; only rows with user_id > last_user_id are returned.
            limit (int): Maximum number of rows to return.
        
        Returns:
            list[ListmonkUser]: List of ListmonkUser instances ordered by ascending user_id.
        """
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
        """
        Record consent check results for a user and update corresponding timestamps and status in the database.
        
        Parameters:
            user_id (int): ID of the user to update.
            pending (bool): Whether the user's consent is pending; sets `consent_pending`.
            confirmed (bool): If True, sets `consent_confirmed_at` to the current UTC time.
            status (str | None): Optional status to store; if None, the user's status is not modified.
        """
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
        """
        Delete the ListmonkUser row for the specified user.
        
        Parameters:
            user_id (int): The application's user identifier whose Listmonk state should be removed.
        """
        stmt = delete(ListmonkUser).where(ListmonkUser.user_id == user_id)
        await self._session.execute(stmt)
