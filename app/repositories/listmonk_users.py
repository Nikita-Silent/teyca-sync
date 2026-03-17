"""Repository for listmonk user sync state."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog
from sqlalchemy import Select, delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import ListmonkUser

logger = structlog.get_logger()


class DuplicateListmonkUserEmailError(RuntimeError):
    """Raised when one email is linked to multiple users in listmonk_users."""

    def __init__(
        self, *, normalized_email: str, user_id: int, existing_user_ids: list[int]
    ) -> None:
        self.normalized_email = normalized_email
        self.user_id = user_id
        self.existing_user_ids = existing_user_ids
        super().__init__(
            f"Email {normalized_email} already linked to another user in listmonk_users"
        )


class ListmonkUsersRepository:
    """Data access for listmonk_users table."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def set_consent_pending(self, *, user_id: int) -> None:
        """Mark user for consent sync worker processing."""
        stmt = (
            update(ListmonkUser).where(ListmonkUser.user_id == user_id).values(consent_pending=True)
        )
        await self._session.execute(stmt)

    async def get_by_user_id(self, *, user_id: int) -> ListmonkUser | None:
        """Return listmonk state by user_id."""
        stmt: Select[tuple[ListmonkUser]] = select(ListmonkUser).where(
            ListmonkUser.user_id == user_id
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_subscriber_id(self, *, subscriber_id: int) -> ListmonkUser | None:
        """Return listmonk state by subscriber_id."""
        stmt: Select[tuple[ListmonkUser]] = (
            select(ListmonkUser)
            .where(ListmonkUser.subscriber_id == subscriber_id)
            .order_by(ListmonkUser.updated_at.desc().nullslast(), ListmonkUser.user_id.desc())
            .limit(2)
        )
        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        if not rows:
            return None
        if len(rows) > 1:
            logger.warning(
                "listmonk_users_duplicate_subscriber_id",
                subscriber_id=subscriber_id,
                duplicate_rows=len(rows),
                picked_user_id=int(rows[0].user_id),
            )
        return rows[0]

    async def get_by_email(self, *, email: str) -> list[ListmonkUser]:
        """Return all rows mapped to normalized email."""
        normalized_email = _normalize_email(email)
        if normalized_email is None:
            return []
        stmt: Select[tuple[ListmonkUser]] = (
            select(ListmonkUser)
            .where(ListmonkUser.email == normalized_email)
            .order_by(ListmonkUser.user_id.asc())
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def get_duplicate_emails(self, *, limit: int | None = None) -> list[str]:
        """Return normalized emails that currently map to multiple users."""
        stmt: Select[tuple[str | None]] = (
            select(ListmonkUser.email)
            .where(ListmonkUser.email.is_not(None))
            .group_by(ListmonkUser.email)
            .having(func.count(ListmonkUser.user_id) > 1)
            .order_by(ListmonkUser.email.asc())
        )
        if limit is not None:
            stmt = stmt.limit(max(1, limit))
        result = await self._session.execute(stmt)
        return [str(value) for value in result.scalars().all()]

    async def get_other_user_ids_by_email(self, *, user_id: int, email: str) -> list[int]:
        """Return other user ids already mapped to the same normalized email."""
        normalized_email = _normalize_email(email)
        if normalized_email is None:
            return []
        return await self._find_other_user_ids_by_email(user_id=user_id, email=normalized_email)

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
        normalized_email = _normalize_email(email)
        if normalized_email is not None:
            duplicate_user_ids = await self._find_other_user_ids_by_email(
                user_id=user_id,
                email=normalized_email,
            )
            if duplicate_user_ids:
                logger.error(
                    "listmonk_users_duplicate_email",
                    email=normalized_email,
                    user_id=user_id,
                    duplicate_rows=len(duplicate_user_ids),
                    existing_user_ids=duplicate_user_ids,
                )
                raise DuplicateListmonkUserEmailError(
                    normalized_email=normalized_email,
                    user_id=user_id,
                    existing_user_ids=duplicate_user_ids,
                )
        list_ids_text = ",".join(str(item) for item in list_ids)
        insert_stmt = insert(ListmonkUser).values(
            user_id=user_id,
            subscriber_id=subscriber_id,
            email=normalized_email,
            status=status,
            list_ids=list_ids_text,
            attributes=attributes,
        )
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[ListmonkUser.user_id],
            set_={
                "subscriber_id": subscriber_id,
                "email": normalized_email,
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

    async def clear_email(self, *, user_id: int) -> None:
        """Clear stored email for a listmonk mapping."""
        stmt = update(ListmonkUser).where(ListmonkUser.user_id == user_id).values(email=None)
        await self._session.execute(stmt)

    async def _find_other_user_ids_by_email(self, *, user_id: int, email: str) -> list[int]:
        stmt: Select[tuple[int]] = (
            select(ListmonkUser.user_id)
            .where(ListmonkUser.email == email, ListmonkUser.user_id != user_id)
            .order_by(ListmonkUser.user_id.asc())
            .limit(2)
        )
        result = await self._session.execute(stmt)
        return [int(item) for item in result.scalars().all()]


def _normalize_email(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    normalized = raw.strip().lower()
    return normalized or None
