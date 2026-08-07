from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from app.config import Settings
from app.consumers.common import QUEUE_CREATE
from app.consumers.create_user import CreateConsumerDeps, handle
from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_MERGE_FINALIZE,
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
)
from app.repositories.old_db import OldUserData


def _payload(user_id: int = 10, email: str = "user@example.com") -> dict[str, object]:
    return {
        "type": "CREATE",
        "pass": {
            "user_id": user_id,
            "email": email,
            "phone": "79039859055",
            "summ": 100,
            "summ_all": 1000,
            "visits": 2,
        },
    }


class _FakeUniqueViolation(Exception):
    def __init__(self, constraint_name: str) -> None:
        super().__init__(constraint_name)
        self.constraint_name = constraint_name


def _email_unique_violation() -> IntegrityError:
    return IntegrityError(
        "INSERT ...",
        {},
        _FakeUniqueViolation("uq_users_email_lower_trim"),
    )


class _FakeNestedTransaction:
    """Stand-in for AsyncSession.begin_nested()'s SAVEPOINT context manager.

    Real begin_nested() is a plain (sync) call returning an async context
    manager (teyca-sync-x1g.1) — AsyncMock would make the call itself a
    coroutine, breaking `async with`.
    """

    async def __aenter__(self) -> _FakeNestedTransaction:
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        return False


def _fake_session() -> MagicMock:
    session = MagicMock()
    session.begin_nested = MagicMock(return_value=_FakeNestedTransaction())
    return session


@dataclass(slots=True)
class _Mocks:
    users_repo: AsyncMock
    listmonk_repo: AsyncMock
    outbox_repo: AsyncMock
    merge_repo: AsyncMock
    old_db_repo: AsyncMock


def _deps() -> tuple[CreateConsumerDeps, _Mocks]:
    mocks = _Mocks(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        outbox_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    deps = CreateConsumerDeps(
        settings=cast(Settings, SimpleNamespace(listmonk_list_ids="1,2")),
        session=_fake_session(),
        users_repo=mocks.users_repo,
        listmonk_repo=mocks.listmonk_repo,
        outbox_repo=mocks.outbox_repo,
        merge_repo=mocks.merge_repo,
        old_db_repo=mocks.old_db_repo,
    )
    return deps, mocks


@pytest.mark.asyncio
async def test_create_without_old_data_enqueues_only_listmonk_sync() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = None
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    mocks.users_repo.lock_user.assert_awaited_once_with(user_id=10, wait=False)
    mocks.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    mocks.users_repo.upsert.assert_awaited_once()
    mocks.outbox_repo.enqueue_latest.assert_awaited_once()
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_LISTMONK_UPSERT
    assert latest_kwargs["queue_name"] == QUEUE_CREATE
    assert latest_kwargs["payload"]["subscriber_id"] is None
    assert latest_kwargs["payload"]["list_ids"] == [1, 2]
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_with_old_data_and_existing_subscriber_enqueues_merge_finalize() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = OldUserData(
        bonus=55.0,
        summ=10,
        check_summ=5,
    )
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=777)
    mocks.outbox_repo.enqueue_once.return_value = True

    await handle(_payload(), deps=deps)

    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["payload"]["subscriber_id"] == 777
    once_kwargs = mocks.outbox_repo.enqueue_once.await_args.kwargs
    assert once_kwargs["operation"] == OUTBOX_OP_MERGE_FINALIZE
    assert once_kwargs["queue_name"] == QUEUE_CREATE
    assert once_kwargs["payload"]["old_bonus_value"] == 55.0
    assert once_kwargs["payload"]["bonus_done"] is False
    assert once_kwargs["payload"]["key2_done"] is False


@pytest.mark.asyncio
async def test_create_skips_merge_if_merge_log_appears_after_old_db_prefetch() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.side_effect = [False, True]
    mocks.old_db_repo.get_user_data.return_value = OldUserData(bonus=55.0, summ=10)
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    mocks.outbox_repo.enqueue_latest.assert_awaited_once()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_skips_merge_when_merge_already_exists() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_not_awaited()
    mocks.outbox_repo.enqueue_latest.assert_awaited_once()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_invalid_email_enqueues_block_and_skips_listmonk_sync() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = None
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(email="bad.mail@"), deps=deps)

    mocks.outbox_repo.enqueue_once.assert_not_awaited()
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL
    assert latest_kwargs["payload"] == {"status": "blocked"}


@pytest.mark.asyncio
async def test_create_retry_waits_for_user_lock() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps, wait_for_lock=True)

    mocks.users_repo.lock_user.assert_awaited_once_with(user_id=10, wait=True)


@pytest.mark.asyncio
async def test_create_invalid_email_keeps_existing_mapping_for_worker_follow_up() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=500)

    await handle(_payload(email="bad"), deps=deps)

    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL
    assert latest_kwargs["queue_name"] == QUEUE_CREATE


@pytest.mark.asyncio
async def test_create_email_race_won_proceeds_to_listmonk_sync() -> None:
    """teyca-sync-eh8: users.upsert raising the unique-email constraint is
    resolved in-process via the Р5/Р6 policy, not scheduled for a
    never-run worker. Winning the race continues normally."""
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = None
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.users_repo.upsert.side_effect = [_email_unique_violation(), None]

    with patch(
        "app.consumers.create_user.resolve_users_email_conflict",
        new=AsyncMock(return_value=True),
    ) as resolver:
        await handle(_payload(email="duplicate@example.com"), deps=deps)

    resolver.assert_awaited_once()
    resolver_await_args = resolver.await_args
    assert resolver_await_args is not None
    assert resolver_await_args.kwargs["user_id"] == 10
    mocks.outbox_repo.enqueue_latest.assert_awaited_once()
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_LISTMONK_UPSERT


@pytest.mark.asyncio
async def test_create_email_race_lost_stops_without_listmonk_sync() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = None
    mocks.users_repo.upsert.side_effect = _email_unique_violation()

    with patch(
        "app.consumers.create_user.resolve_users_email_conflict",
        new=AsyncMock(return_value=False),
    ) as resolver:
        await handle(_payload(email="duplicate@example.com"), deps=deps)

    resolver.assert_awaited_once()
    mocks.outbox_repo.enqueue_latest.assert_not_awaited()
    mocks.listmonk_repo.get_by_user_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_non_email_integrity_error_propagates() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = None
    mocks.users_repo.upsert.side_effect = IntegrityError(
        "INSERT ...", {}, _FakeUniqueViolation("some_other_constraint")
    )

    with pytest.raises(IntegrityError):
        await handle(_payload(), deps=deps)
