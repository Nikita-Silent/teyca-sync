from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.db.models import MergeLog
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.email_repair_log import EmailRepairLogRepository
from app.repositories.listmonk_user_archive import ListmonkUserArchiveRepository
from app.repositories.listmonk_users import (
    DuplicateListmonkSubscriberIdError,
    DuplicateListmonkUserEmailError,
    ListmonkUsersRepository,
)
from app.repositories.merge_log import MergeLogRepository
from app.repositories.sync_state import SyncStateRepository
from app.repositories.users import UsersRepository


@pytest.mark.asyncio
async def test_users_repository_paths() -> None:
    session = AsyncMock()
    repo = UsersRepository(session)

    await repo.lock_user(user_id=1)
    session.execute.assert_awaited()

    session.execute.reset_mock()
    session.execute.return_value = SimpleNamespace(
        scalar_one_or_none=lambda: SimpleNamespace(user_id=1)
    )
    user = await repo.get_by_user_id(user_id=1)
    assert user is not None

    session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: [1, "2"])
    )
    ids = await repo.get_user_ids_by_email(email=" USER@example.com ", limit=0)
    assert ids == [1, 2]
    assert await repo.get_user_ids_by_email(email="  ") == []

    session.execute.reset_mock()
    await repo.upsert(
        user_id=1,
        profile={
            "email": "  X@Y.Z  ",
            "summ": 10,
            "referal": "  4243447  ",
            "tags": [892, 899],
        },
    )
    users_upsert_stmt = session.execute.await_args_list[0].args[0]
    assert users_upsert_stmt.compile().params["email"] == "x@y.z"
    assert users_upsert_stmt.compile().params["referal"] == "4243447"
    assert users_upsert_stmt.compile().params["tags"] == [892, 899]

    await repo.delete_by_user_id(user_id=1)
    assert session.execute.await_count >= 2


@pytest.mark.asyncio
async def test_listmonk_users_repository_paths() -> None:
    session = MagicMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    repo = ListmonkUsersRepository(session)

    await repo.set_consent_pending(user_id=1)

    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: "row")
    assert await repo.get_by_user_id(user_id=1) == "row"
    session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: ["row"])
    )
    assert await repo.get_by_subscriber_id(subscriber_id=10) == "row"
    session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(
            all=lambda: [
                SimpleNamespace(user_id=2),
                SimpleNamespace(user_id=1),
            ]
        )
    )
    with pytest.raises(DuplicateListmonkSubscriberIdError) as exc_info:
        await repo.get_by_subscriber_id(subscriber_id=10)
    assert exc_info.value.subscriber_id == 10
    assert exc_info.value.user_ids == [2, 1]

    session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: [10, "20"])
    )
    assert await repo.get_duplicate_subscriber_ids() == [10, 20]

    session.execute.reset_mock()
    session.execute.side_effect = [
        SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [])),
        SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [])),
        SimpleNamespace(),
    ]
    await repo.upsert(
        user_id=1,
        subscriber_id=10,
        email="  U@Example.COM  ",
        status="enabled",
        list_ids=[1, 2],
        attributes={"user_id": 1},
    )
    listmonk_upsert_stmt = session.execute.await_args_list[2].args[0]
    assert listmonk_upsert_stmt.compile().params["email"] == "u@example.com"

    session.execute.reset_mock()
    session.execute.side_effect = [
        SimpleNamespace(
            scalars=lambda: SimpleNamespace(
                all=lambda: [SimpleNamespace(user_id=2), SimpleNamespace(user_id=1)]
            )
        ),
    ]
    with (
        patch("app.repositories.listmonk_users.logger.error") as logger_error,
        pytest.raises(DuplicateListmonkSubscriberIdError) as exc_info,
    ):
        await repo.upsert(
            user_id=1,
            subscriber_id=10,
            email="duplicate@example.com",
            status="enabled",
            list_ids=[1],
            attributes={"user_id": 1},
        )
    assert exc_info.value.user_ids == [2]
    logger_error.assert_called_once_with(
        "listmonk_users_duplicate_subscriber_id",
        subscriber_id=10,
        user_id=1,
        duplicate_rows=1,
        existing_user_ids=[2],
    )

    session.execute.reset_mock()
    session.execute.side_effect = [
        SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [])),
        SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: [2])),
    ]
    with (
        patch("app.repositories.listmonk_users.logger.error") as logger_error,
        pytest.raises(DuplicateListmonkUserEmailError),
    ):
        await repo.upsert(
            user_id=1,
            subscriber_id=10,
            email="duplicate@example.com",
            status="enabled",
            list_ids=[1],
            attributes={"user_id": 1},
        )
    logger_error.assert_called_once_with(
        "listmonk_users_duplicate_email",
        email="duplicate@example.com",
        user_id=1,
        duplicate_rows=1,
        existing_user_ids=[2],
    )

    session.execute.side_effect = None
    session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: ["a", "b"])
    )
    assert await repo.get_pending_batch(limit=10) == ["a", "b"]
    assert await repo.get_batch_after_user_id(last_user_id=1, limit=10) == ["a", "b"]

    await repo.mark_checked(user_id=1, pending=False, confirmed=True, status="confirmed")
    await repo.mark_checked(user_id=2, pending=True, confirmed=False)
    await repo.delete_by_user_id(user_id=1)
    await repo.delete_by_user_ids(user_ids=[1, 2])


@pytest.mark.asyncio
async def test_merge_log_repository_paths() -> None:
    session = MagicMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    repo = MergeLogRepository(session)

    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: 1)
    assert await repo.exists(user_id=1) is True
    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: None)
    assert await repo.exists(user_id=2) is False

    await repo.create(user_id=1, source_event_type="CREATE", source_event_id="e", trace_id="t")
    session.add.assert_called_once()
    added = session.add.call_args.args[0]
    assert isinstance(added, MergeLog)
    assert added.user_id == 1

    await repo.delete_by_user_id(user_id=1)


@pytest.mark.asyncio
async def test_email_repair_log_repository_paths() -> None:
    session = AsyncMock()
    repo = EmailRepairLogRepository(session)

    await repo.create_pending(
        normalized_email="duplicate@example.com",
        incoming_user_id=10,
        existing_user_id=20,
        source_event_type="UPDATE",
        source_event_id="event-1",
        trace_id="trace-1",
    )

    stmt = session.execute.await_args.args[0]
    params = stmt.compile().params
    assert params["normalized_email"] == "duplicate@example.com"
    assert params["incoming_user_id"] == 10
    assert params["existing_user_id"] == 20
    assert params["source_event_type"] == "UPDATE"
    assert params["source_event_id"] == "event-1"
    assert params["trace_id"] == "trace-1"

    session.execute.reset_mock()

    await repo.create_db_applied(
        normalized_email="duplicate@example.com",
        incoming_user_id=10,
        existing_user_id=20,
        winner_user_id=20,
        winner_subscriber_id=33,
        source_event_id="backfill-1",
        trace_id="trace-1",
    )
    params = session.execute.await_args.args[0].compile().params
    assert params["status"] == "db_applied"
    assert params["winner_user_id"] == 20
    assert params["winner_subscriber_id"] == 33
    session.execute.reset_mock()

    session.execute.return_value = SimpleNamespace()
    assert (
        await repo.mark_retry(
            repair_id=1,
            attempts=3,
            error_text="boom",
            max_attempts=3,
        )
        == "manual_review"
    )
    session.execute.return_value = SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: ["row"])
    )
    assert await repo.get_db_applied_batch(limit=10) == ["row"]


@pytest.mark.asyncio
async def test_listmonk_user_archive_repository_paths() -> None:
    session = AsyncMock()
    session.add = MagicMock()
    repo = ListmonkUserArchiveRepository(session)
    row = SimpleNamespace(
        user_id=10,
        subscriber_id=777,
        email="dup@example.com",
        status="blocked",
        list_ids="2,5",
        attributes={"user_id": 10},
        consent_pending=True,
        consent_checked_at=None,
        consent_confirmed_at=None,
        created_at=None,
        updated_at=None,
    )

    await repo.archive_loser(
        row=row,
        winner_user_id=20,
        winner_subscriber_id=777,
        archive_reason="duplicate_subscriber_id",
    )

    archived = session.add.call_args.args[0]
    assert archived.user_id == 10
    assert archived.subscriber_id == 777
    assert archived.winner_user_id == 20
    assert archived.archive_reason == "duplicate_subscriber_id"


@pytest.mark.asyncio
async def test_bonus_accrual_repository_paths() -> None:
    session = AsyncMock()
    repo = BonusAccrualRepository(session)

    session.execute.return_value = SimpleNamespace(rowcount=1)
    assert (
        await repo.reserve(
            user_id=1,
            reason="consent",
            idempotency_key="k1",
            payload={"a": 1},
        )
        is True
    )
    session.execute.return_value = SimpleNamespace(rowcount=0)
    assert (
        await repo.reserve(
            user_id=1,
            reason="consent",
            idempotency_key="k1",
            payload={"a": 1},
        )
        is False
    )

    await repo.mark_done(idempotency_key="k1")
    await repo.save_progress(idempotency_key="k1", payload={"bonus_done": True})
    await repo.mark_done_with_payload(idempotency_key="k1", payload={"key1_done": True})
    await repo.mark_failed(idempotency_key="k1", error_text="boom")

    session.execute.return_value = SimpleNamespace(scalar_one_or_none=lambda: "row")
    assert await repo.get_by_key(idempotency_key="k1") == "row"

    await repo.delete_by_user_id(user_id=1)


@pytest.mark.asyncio
async def test_sync_state_repository_paths() -> None:
    session = AsyncMock()
    repo = SyncStateRepository(session)

    session.execute.side_effect = [
        SimpleNamespace(scalar_one_or_none=lambda: "existing"),
    ]
    assert await repo.get_or_create(source="s", list_id=1) == "existing"

    session.execute.side_effect = [
        SimpleNamespace(scalar_one_or_none=lambda: None),
        SimpleNamespace(),
        SimpleNamespace(scalar_one_or_none=lambda: "created"),
    ]
    assert await repo.get_or_create(source="s", list_id=2) == "created"

    session.execute.side_effect = [
        SimpleNamespace(scalar_one_or_none=lambda: None),
        SimpleNamespace(),
        SimpleNamespace(scalar_one_or_none=lambda: None),
    ]
    with pytest.raises(RuntimeError):
        await repo.get_or_create(source="s", list_id=3)

    session.execute.side_effect = None
    session.execute.return_value = SimpleNamespace()

    await repo.update_watermark(
        source="s",
        list_id=1,
        updated_at=None,
        subscriber_id=1,
    )
