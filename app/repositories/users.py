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
    "referal",
    "tags",
)


class UsersRepository:
    """Data access for users table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def lock_user(self, *, user_id: int) -> None:
        """Acquire transaction-scoped advisory lock for user_id."""
        await self._session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": user_id}
        )

    async def get_by_user_id(self, *, user_id: int) -> User | None:
        """Return user by primary key."""
        stmt: Select[tuple[User]] = select(User).where(User.user_id == user_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_user_ids_by_email(self, *, email: str, limit: int = 2) -> list[int]:
        """Return user ids matched by normalized email."""
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
        """Insert or update user profile by user_id."""
        values: dict[str, Any] = {"user_id": user_id}
        for field in USER_UPSERT_FIELDS:
            values[field] = profile.get(field)
        values["email"] = _normalize_email(values.get("email"))
        values["referal"] = _normalize_text(values.get("referal"))
        values["tags"] = _normalize_int_list(values.get("tags"))

        insert_stmt = insert(User).values(**values)
        update_fields = {field: values[field] for field in USER_UPSERT_FIELDS}
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[User.user_id],
            set_=update_fields,
        )
        await self._session.execute(stmt)

    async def delete_by_user_id(self, *, user_id: int) -> None:
        """Delete user by primary key."""
        stmt = delete(User).where(User.user_id == user_id)
        await self._session.execute(stmt)


def _normalize_email(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    normalized = raw.strip().lower()
    return normalized or None


def _normalize_text(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    normalized = raw.strip()
    return normalized or None


def _normalize_int_list(raw: object) -> list[int] | None:
    if isinstance(raw, dict):
        raw = raw.get("values")
    if not isinstance(raw, list):
        return None
    normalized: list[int] = []
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, int):
            return None
        normalized.append(item)
    return normalized
