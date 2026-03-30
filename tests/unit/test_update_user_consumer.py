from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import pytest

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


@dataclass(slots=True)
class _Mocks:
    users_repo: AsyncMock
    listmonk_repo: AsyncMock
    email_repair_repo: AsyncMock
    outbox_repo: AsyncMock
    merge_repo: AsyncMock
    old_db_repo: AsyncMock


def _deps() -> tuple[UpdateConsumerDeps, _Mocks]:
    mocks = _Mocks(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        email_repair_repo=AsyncMock(),
        outbox_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        old_db_repo=AsyncMock(),
    )
    deps = UpdateConsumerDeps(
        settings=cast(Settings, SimpleNamespace(listmonk_list_ids="3")),
        users_repo=mocks.users_repo,
        listmonk_repo=mocks.listmonk_repo,
        email_repair_repo=mocks.email_repair_repo,
        outbox_repo=mocks.outbox_repo,
        merge_repo=mocks.merge_repo,
        old_db_repo=mocks.old_db_repo,
    )
    return deps, mocks


@pytest.mark.asyncio
async def test_update_when_merge_already_exists_enqueues_only_listmonk_sync() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []

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
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
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
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []

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
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []

    await handle(_payload(), deps=deps)

    mocks.outbox_repo.enqueue_latest.assert_awaited_once()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_invalid_email_enqueues_block_and_skips_listmonk_sync() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(email="not-an-email"), deps=deps)

    mocks.listmonk_repo.get_other_user_ids_by_email.assert_not_awaited()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()
    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL
    assert latest_kwargs["payload"] == {"status": "blocked"}


@pytest.mark.asyncio
async def test_update_retry_waits_for_user_lock() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []

    await handle(_payload(), deps=deps, wait_for_lock=True)

    mocks.users_repo.lock_user.assert_awaited_once_with(user_id=20, wait=True)


@pytest.mark.asyncio
async def test_update_emits_step_logs_for_major_phases() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = OldUserData(bonus=40.0, summ=15)
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
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
    assert "update_consumer_email_conflict_check_start" in step_events
    assert "update_consumer_email_conflict_check_done" in step_events
    assert "update_consumer_listmonk_enqueue_start" in step_events
    assert "update_consumer_listmonk_enqueue_done" in step_events
    assert "update_consumer_merge_enqueue_start" in step_events
    assert "update_consumer_merge_enqueue_done" in step_events


@pytest.mark.asyncio
async def test_update_duplicate_email_schedules_repair_and_skips_outbox() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = [21]

    await handle(_payload(email="duplicate@example.com"), deps=deps)

    mocks.email_repair_repo.create_pending.assert_awaited_once_with(
        normalized_email="duplicate@example.com",
        incoming_user_id=20,
        existing_user_id=21,
        source_event_type="UPDATE",
        source_event_id=None,
        trace_id=None,
    )
    mocks.outbox_repo.enqueue_latest.assert_not_awaited()
    mocks.outbox_repo.enqueue_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_invalid_email_with_existing_mapping_still_enqueues_worker_action() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)

    await handle(_payload(email="bad@"), deps=deps)

    latest_kwargs = mocks.outbox_repo.enqueue_latest.await_args.kwargs
    assert latest_kwargs["operation"] == OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL
    assert latest_kwargs["queue_name"] == QUEUE_UPDATE
