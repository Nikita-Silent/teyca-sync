from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from app.config import Settings
from app.consumers.update_user import UpdateConsumerDeps, handle
from app.mq.queues import QUEUE_UPDATE
from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_MERGE_FINALIZE,
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
)
from app.repositories.old_db import OldUserData


def _payload(user_id: int = 20, email: str = "up@example.com") -> dict[str, object]:
    return {
        "type": "UPDATE",
        "pass": {
            "user_id": user_id,
            "email": email,
            "phone": "79039859055",
            "summ": 90,
            "visits": 3,
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


def _deps() -> tuple[UpdateConsumerDeps, _Mocks]:
    mocks = _Mocks(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        outbox_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    deps = UpdateConsumerDeps(
        settings=cast(Settings, SimpleNamespace(listmonk_list_ids="3")),
        session=_fake_session(),
        users_repo=mocks.users_repo,
        listmonk_repo=mocks.listmonk_repo,
        outbox_repo=mocks.outbox_repo,
        merge_repo=mocks.merge_repo,
        old_db_repo=mocks.old_db_repo,
    )
    mocks.users_repo.get_by_user_id.return_value = None
    return deps, mocks


@pytest.mark.asyncio
async def test_update_when_merge_already_exists_enqueues_only_listmonk_sync() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_not_awaited()
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_LISTMONK_UPSERT
    assert latest_kwargs["queue_name"] == QUEUE_UPDATE
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_when_merge_missing_and_old_data_exists_enqueues_merge_finalize() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = OldUserData(bonus=40.0, summ=15)
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)
    mocks.outbox_repo.enqueue_once.return_value = True

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["payload"]["subscriber_id"] == 902
    once_kwargs = mocks.outbox_repo.enqueue_once.await_args.kwargs
    assert once_kwargs["operation"] == OUTBOX_OP_MERGE_FINALIZE
    assert once_kwargs["payload"]["old_bonus_value"] == 40.0


@pytest.mark.asyncio
async def test_update_skips_merge_if_merge_log_appears_after_old_db_prefetch() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.side_effect = [False, True]
    mocks.old_db_repo.get_user_data.return_value = OldUserData(bonus=40.0, summ=15)
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    mocks.outbox_repo.enqueue_latest.assert_awaited_once()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_when_merge_missing_but_old_data_empty() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = None
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    mocks.outbox_repo.enqueue_latest.assert_awaited_once()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_invalid_email_enqueues_block_and_skips_listmonk_sync() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(email="not-an-email"), deps=deps)

    mocks.outbox_repo.enqueue_once.assert_not_awaited()
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL
    assert latest_kwargs["payload"] == {"status": "blocked"}


@pytest.mark.asyncio
async def test_update_retry_waits_for_user_lock() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.users_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps, wait_for_lock=True)

    mocks.users_repo.lock_user.assert_awaited_once_with(user_id=20, wait=True)


@pytest.mark.asyncio
async def test_update_emits_step_logs_for_major_phases() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.users_repo.get_by_user_id.return_value = None
    mocks.old_db_repo.get_user_data.return_value = OldUserData(bonus=40.0, summ=15)
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)
    mocks.outbox_repo.enqueue_once.return_value = True

    with patch("app.consumers.update_user.logger") as logger:
        await handle(_payload(), deps=deps)

    step_events = [call.args[0] for call in logger.info.call_args_list]
    assert "update_consumer_lock_start" in step_events
    assert "update_consumer_lock_done" in step_events
    assert "update_consumer_old_db_read_start" in step_events
    assert "update_consumer_old_db_read_done" in step_events
    assert "update_consumer_users_upsert_start" in step_events
    assert "update_consumer_users_upsert_done" in step_events
    assert "update_consumer_listmonk_enqueue_start" in step_events
    assert "update_consumer_listmonk_enqueue_done" in step_events
    assert "update_consumer_merge_enqueue_start" in step_events
    assert "update_consumer_merge_enqueue_done" in step_events


@pytest.mark.asyncio
async def test_update_email_race_won_proceeds_to_listmonk_sync() -> None:
    """teyca-sync-eh8: users.upsert raising the unique-email constraint is
    resolved in-process via the Р5/Р6 policy, not scheduled for a
    never-run worker. Winning the race continues normally."""
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.users_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.users_repo.upsert.side_effect = [_email_unique_violation(), None]

    with patch(
        "app.consumers.update_user.resolve_users_email_conflict",
        new=AsyncMock(return_value=True),
    ) as resolver:
        await handle(_payload(email="duplicate@example.com"), deps=deps)

    resolver.assert_awaited_once()
    resolver_await_args = resolver.await_args
    assert resolver_await_args is not None
    assert resolver_await_args.kwargs["user_id"] == 20
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_LISTMONK_UPSERT


@pytest.mark.asyncio
async def test_update_email_race_lost_stops_without_listmonk_sync() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.users_repo.get_by_user_id.return_value = None
    mocks.users_repo.upsert.side_effect = _email_unique_violation()

    with patch(
        "app.consumers.update_user.resolve_users_email_conflict",
        new=AsyncMock(return_value=False),
    ) as resolver:
        await handle(_payload(email="duplicate@example.com"), deps=deps)

    resolver.assert_awaited_once()
    mocks.outbox_repo.enqueue_latest.assert_not_awaited()
    mocks.listmonk_repo.get_by_user_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_invalid_email_with_existing_mapping_still_enqueues_worker_action() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.users_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)

    await handle(_payload(email="bad@"), deps=deps)

    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL
    assert latest_kwargs["queue_name"] == QUEUE_UPDATE


@pytest.mark.asyncio
async def test_update_preserves_existing_tags_when_payload_omits_them() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.users_repo.get_by_user_id.return_value = SimpleNamespace(tags=[7, 8])
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    upsert_kwargs = mocks.users_repo.upsert.await_args.kwargs
    assert upsert_kwargs["profile"]["tags"] == [7, 8]
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert "tags" not in latest_kwargs["payload"]["attributes"]


@pytest.mark.asyncio
async def test_update_overwrites_tags_when_payload_provides_them() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.users_repo.get_by_user_id.return_value = SimpleNamespace(tags=[7, 8])
    mocks.listmonk_repo.get_by_user_id.return_value = None

    payload = _payload()
    cast(dict[str, object], payload["pass"])["tags"] = [1, 2, 3]

    await handle(payload, deps=deps)

    upsert_kwargs = mocks.users_repo.upsert.await_args.kwargs
    assert upsert_kwargs["profile"]["tags"] == [1, 2, 3]
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["payload"]["attributes"]["tags"] == [1, 2, 3]
