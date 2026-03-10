"""Repository for users table operations."""

from __future__ import annotations

from typing import Any

from sqlalchemy import Select, delete, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import User


USER_UPSERT_FIELDS: tuple[str, ...] = (
    "email",
    "phone",
    "first_name",
    "last_name",
    "pat_name",
    "birthday",
    "gender",
    "barcode",
    "discount",
    "bonus",
    "loyalty_level",
    "summ",
    "summ_all",
    "summ_last",
    "check_summ",
    "visits",
    "visits_all",
    "date_last",
    "city",
)


class UsersRepository:
    """Data access for users table."""

    def __init__(self, session: AsyncSession) -> None:
        """
        Initialize the repository with an asynchronous SQLAlchemy session used for database operations.
        
        Stores the provided AsyncSession on the instance for use by repository methods.
        """
        self._session = session

    async def lock_user(self, *, user_id: int) -> None:
        """Acquire transaction-scoped advisory lock for user_id."""
        await self._session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": user_id})

    async def get_by_user_id(self, *, user_id: int) -> User | None:
        """
        Retrieve a user by its primary key.
        
        Returns:
            The User instance if found, otherwise None.
        """
        stmt: Select[tuple[User]] = select(User).where(User.user_id == user_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_user_ids_by_email(self, *, email: str, limit: int = 2) -> list[int]:
        """
        Find user IDs whose email matches the provided address (case-insensitive).
        
        The input email is trimmed and lowercased before matching; if it is empty after trimming an empty list is returned. Results are ordered by user_id ascending and limited to at most `limit` (the method enforces a minimum limit of 1).
        
        Parameters:
            email (str): Email address to match; will be normalized by trimming and lowercasing.
            limit (int): Maximum number of user IDs to return (minimum honored value is 1).
        
        Returns:
            list[int]: List of matching user_id integers; may be empty.
        """
        normalized = email.strip().lower()
        if not normalized:
            return []
        stmt = (
            select(User.user_id)
            .where(func.lower(User.email) == normalized)
            .order_by(User.user_id.asc())
            .limit(max(1, limit))
        )
        result = await self._session.execute(stmt)
        return [int(value) for value in result.scalars().all()]

    async def upsert(self, *, user_id: int, profile: dict[str, Any]) -> None:
        """
        Insert or update a user's profile identified by user_id.
        
        Builds a values mapping from the provided `profile` for all fields in `USER_UPSERT_FIELDS`
        (missing fields are set to None) and performs an upsert: inserts a new row or updates the
        existing row when a record with the same `user_id` already exists.
        
        Parameters:
            user_id (int): Primary key of the user to insert or update.
            profile (dict[str, Any]): Mapping of user profile fields to values; keys correspond to
                fields listed in `USER_UPSERT_FIELDS`.
        """
        values: dict[str, Any] = {"user_id": user_id}
        for field in USER_UPSERT_FIELDS:
            values[field] = profile.get(field)

        insert_stmt = insert(User).values(**values)
        update_fields = {field: values[field] for field in USER_UPSERT_FIELDS}
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[User.user_id],
            set_=update_fields,
        )
        await self._session.execute(stmt)

    async def delete_by_user_id(self, *, user_id: int) -> None:
        """
        Delete the user row identified by user_id from the users table.
        
        Parameters:
            user_id (int): Primary key of the user to delete.
        """
        stmt = delete(User).where(User.user_id == user_id)
        await self._session.execute(stmt)
