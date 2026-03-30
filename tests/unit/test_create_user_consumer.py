from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest

from app.config import Settings
from app.consumers.create_user import CreateConsumerDeps, handle
from app.mq.queues import QUEUE_CREATE
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


@dataclass(slots=True)
class _Mocks:
    users_repo: AsyncMock
    listmonk_repo: AsyncMock
    email_repair_repo: AsyncMock
    outbox_repo: AsyncMock
    merge_repo: AsyncMock
    old_db_repo: AsyncMock


def _deps() -> tuple[CreateConsumerDeps, _Mocks]:
    mocks = _Mocks(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        email_repair_repo=AsyncMock(),
        outbox_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    deps = CreateConsumerDeps(
        settings=cast(Settings, SimpleNamespace(listmonk_list_ids="1,2")),
        users_repo=mocks.users_repo,
        listmonk_repo=mocks.listmonk_repo,
        email_repair_repo=mocks.email_repair_repo,
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
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []

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
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
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
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    mocks.outbox_repo.enqueue_latest.assert_awaited_once()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_skips_merge_when_merge_already_exists() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []

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

    mocks.listmonk_repo.get_other_user_ids_by_email.assert_not_awaited()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL
    assert latest_kwargs["payload"] == {"status": "blocked"}


@pytest.mark.asyncio
async def test_create_retry_waits_for_user_lock() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []

    await handle(_payload(), deps=deps, wait_for_lock=True)

    mocks.users_repo.lock_user.assert_awaited_once_with(user_id=10, wait=True)


@pytest.mark.asyncio
async def test_create_duplicate_email_schedules_repair_and_stops() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = [77, 88]

    await handle(_payload(email="duplicate@example.com"), deps=deps)

    assert mocks.email_repair_repo.create_pending.await_count == 2
    mocks.outbox_repo.enqueue_latest.assert_not_awaited()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_invalid_email_keeps_existing_mapping_for_worker_follow_up() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=500)

    await handle(_payload(email="bad"), deps=deps)

    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL
    assert latest_kwargs["queue_name"] == QUEUE_CREATE
