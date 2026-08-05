"""teyca-sync-eh8: runtime resolution of the users.email race.

Once teyca-sync-4wh's unique index exists, two concurrent writers for
different user_ids racing on the same email produce a real DB constraint
violation instead of both passing a check-then-act read. This tests that
the violation is resolved via the same Р5/Р6 policy the y1c one-time
cleanup used, and via the same outbox operation its dispatcher drains.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from app.consumers.email_conflict import (
    EMAIL_UNIQUE_CONSTRAINT,
    is_email_unique_violation,
    resolve_users_email_conflict,
)


class _FakeUniqueViolation(Exception):
    def __init__(self, constraint_name: str) -> None:
        super().__init__(constraint_name)
        self.constraint_name = constraint_name


def _violation(constraint_name: str = EMAIL_UNIQUE_CONSTRAINT) -> IntegrityError:
    return IntegrityError("INSERT ...", {}, _FakeUniqueViolation(constraint_name))


def test_is_email_unique_violation_matches_constraint_name() -> None:
    assert is_email_unique_violation(_violation()) is True
    assert is_email_unique_violation(_violation("some_other_constraint")) is False


def test_is_email_unique_violation_falls_back_to_message_text() -> None:
    exc = IntegrityError(
        "INSERT ...", {}, RuntimeError(f'duplicate key violates "{EMAIL_UNIQUE_CONSTRAINT}"')
    )
    assert is_email_unique_violation(exc) is True


@pytest.mark.asyncio
async def test_resolve_retries_plain_upsert_when_conflict_already_gone() -> None:
    """The other side's email changed again before we got here — no real
    conflict left, just retry the original write."""
    session = AsyncMock()
    users_repo = AsyncMock()
    users_repo.get_user_ids_by_email.return_value = [10]  # only ourselves

    with patch("app.consumers.email_conflict.UsersRepository", return_value=users_repo):
        won = await resolve_users_email_conflict(
            session=session,
            user_id=10,
            profile={"email": "dup@example.com", "phone": "+7900"},
            source_event_type="UPDATE",
            source_event_id="event-1",
            trace_id="trace-1",
        )

    session.rollback.assert_awaited_once()
    users_repo.upsert.assert_awaited_once_with(
        user_id=10, profile={"email": "dup@example.com", "phone": "+7900"}
    )
    assert won is True


@pytest.mark.asyncio
async def test_resolve_winner_keeps_email_loser_cleared_and_outbox_enqueued() -> None:
    """teyca-sync-37z: different phones -> different people, more recent
    activity wins, loser cleared and marked bad email via outbox."""
    session = AsyncMock()
    users_repo = AsyncMock()
    listmonk_repo = AsyncMock()
    repair_repo = AsyncMock()
    outbox_repo = AsyncMock()

    users_repo.get_user_ids_by_email.return_value = [10, 20]
    users_repo.get_by_user_id.return_value = SimpleNamespace(
        phone="+7901", date_last="2020-01-01", updated_at=None
    )
    repair_repo.create_db_applied.return_value = 555

    with (
        patch("app.consumers.email_conflict.UsersRepository", return_value=users_repo),
        patch("app.consumers.email_conflict.ListmonkUsersRepository", return_value=listmonk_repo),
        patch("app.consumers.email_conflict.EmailRepairLogRepository", return_value=repair_repo),
        patch(
            "app.consumers.email_conflict.ExternalCallOutboxRepository", return_value=outbox_repo
        ),
    ):
        won = await resolve_users_email_conflict(
            session=session,
            user_id=10,
            profile={"email": "dup@example.com", "phone": "+7900", "date_last": "2026-01-01"},
            source_event_type="UPDATE",
            source_event_id="event-1",
            trace_id="trace-1",
        )

    assert won is True
    users_repo.clear_email.assert_awaited_once_with(user_id=20)
    listmonk_repo.clear_email.assert_awaited_once_with(user_id=20)
    repair_repo.create_db_applied.assert_awaited_once()
    create_kwargs = repair_repo.create_db_applied.await_args.kwargs
    assert create_kwargs["incoming_user_id"] == 20
    assert create_kwargs["existing_user_id"] == 10
    assert create_kwargs["winner_user_id"] == 10
    assert create_kwargs["mark_bad_email"] is True
    outbox_repo.enqueue_once.assert_awaited_once()
    enqueue_kwargs = outbox_repo.enqueue_once.await_args.kwargs
    assert enqueue_kwargs["operation"] == "teyca_email_repair_sync"
    assert enqueue_kwargs["dedupe_key"] == "email-repair-sync:555"
    assert enqueue_kwargs["user_id"] == 20
    assert enqueue_kwargs["payload"]["mark_bad_email"] is True
    users_repo.upsert.assert_awaited_once_with(
        user_id=10,
        profile={"email": "dup@example.com", "phone": "+7900", "date_last": "2026-01-01"},
    )


@pytest.mark.asyncio
async def test_resolve_loser_email_cleared_before_upsert_and_outbox_enqueued() -> None:
    """The incoming request loses: its own upsert must still happen (for
    the rest of the profile), just with email cleared."""
    session = AsyncMock()
    users_repo = AsyncMock()
    listmonk_repo = AsyncMock()
    repair_repo = AsyncMock()
    outbox_repo = AsyncMock()

    users_repo.get_user_ids_by_email.return_value = [10, 20]
    users_repo.get_by_user_id.return_value = SimpleNamespace(
        phone="+7900", date_last="2026-06-01", updated_at=None
    )
    repair_repo.create_db_applied.return_value = 777

    with (
        patch("app.consumers.email_conflict.UsersRepository", return_value=users_repo),
        patch("app.consumers.email_conflict.ListmonkUsersRepository", return_value=listmonk_repo),
        patch("app.consumers.email_conflict.EmailRepairLogRepository", return_value=repair_repo),
        patch(
            "app.consumers.email_conflict.ExternalCallOutboxRepository", return_value=outbox_repo
        ),
    ):
        won = await resolve_users_email_conflict(
            session=session,
            user_id=10,
            profile={"email": "dup@example.com", "phone": "+7900", "date_last": "2020-01-01"},
            source_event_type="UPDATE",
            source_event_id="event-1",
            trace_id="trace-1",
        )

    assert won is False
    users_repo.clear_email.assert_not_awaited()  # loser is user_id itself, handled via upsert
    users_repo.upsert.assert_awaited_once_with(
        user_id=10,
        profile={"email": None, "phone": "+7900", "date_last": "2020-01-01"},
    )
    enqueue_kwargs = outbox_repo.enqueue_once.await_args.kwargs
    assert enqueue_kwargs["user_id"] == 10
    create_kwargs = repair_repo.create_db_applied.await_args.kwargs
    assert create_kwargs["incoming_user_id"] == 10
    assert create_kwargs["existing_user_id"] == 20
    assert create_kwargs["winner_user_id"] == 20


@pytest.mark.asyncio
async def test_resolve_same_phone_clears_loser_without_bad_email_mark() -> None:
    """teyca-sync-37z: same phone -> same person, loser cleared but no
    key1=bad email."""
    session = AsyncMock()
    users_repo = AsyncMock()
    listmonk_repo = AsyncMock()
    repair_repo = AsyncMock()
    outbox_repo = AsyncMock()

    users_repo.get_user_ids_by_email.return_value = [10, 20]
    users_repo.get_by_user_id.return_value = SimpleNamespace(
        phone="+7900", date_last="2020-01-01", updated_at=None
    )
    repair_repo.create_db_applied.return_value = 42

    with (
        patch("app.consumers.email_conflict.UsersRepository", return_value=users_repo),
        patch("app.consumers.email_conflict.ListmonkUsersRepository", return_value=listmonk_repo),
        patch("app.consumers.email_conflict.EmailRepairLogRepository", return_value=repair_repo),
        patch(
            "app.consumers.email_conflict.ExternalCallOutboxRepository", return_value=outbox_repo
        ),
    ):
        await resolve_users_email_conflict(
            session=session,
            user_id=10,
            profile={"email": "dup@example.com", "phone": "+7900", "date_last": "2026-01-01"},
            source_event_type="UPDATE",
            source_event_id="event-1",
            trace_id="trace-1",
        )

    create_kwargs = repair_repo.create_db_applied.await_args.kwargs
    assert create_kwargs["mark_bad_email"] is False
    enqueue_kwargs = outbox_repo.enqueue_once.await_args.kwargs
    assert enqueue_kwargs["payload"]["mark_bad_email"] is False
