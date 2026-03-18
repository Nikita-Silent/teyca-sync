from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import pytest

from app.config import Settings
from app.consumers.update_user import UpdateConsumerDeps, handle
from app.repositories.listmonk_users import DuplicateListmonkSubscriberIdError
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
    merge_repo: AsyncMock
    old_db_repo: AsyncMock
    listmonk_client: AsyncMock
    teyca_client: AsyncMock
    commit_checkpoint: AsyncMock


def _deps() -> tuple[UpdateConsumerDeps, _Mocks]:
    mocks = _Mocks(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        email_repair_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        old_db_repo=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
        commit_checkpoint=AsyncMock(),
    )
    deps = UpdateConsumerDeps(
        settings=cast(
            Settings,
            SimpleNamespace(
                listmonk_list_ids="3",
            ),
        ),
        users_repo=mocks.users_repo,
        listmonk_repo=mocks.listmonk_repo,
        email_repair_repo=mocks.email_repair_repo,
        merge_repo=mocks.merge_repo,
        old_db_repo=mocks.old_db_repo,
        listmonk_client=mocks.listmonk_client,
        teyca_client=mocks.teyca_client,
        commit_checkpoint=mocks.commit_checkpoint,
    )
    return deps, mocks


@pytest.mark.asyncio
async def test_update_when_merge_already_exists() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
    mocks.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=901,
        status="enabled",
        list_ids=[3],
    )

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_not_awaited()
    mocks.teyca_client.accrue_bonuses.assert_not_awaited()
    mocks.teyca_client.update_pass_fields.assert_not_awaited()
    mocks.merge_repo.create.assert_not_awaited()
    mocks.listmonk_repo.set_consent_pending.assert_awaited_once_with(user_id=20)


@pytest.mark.asyncio
async def test_update_when_merge_missing_and_old_data_exists() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = OldUserData(
        bonus=40.0,
        summ=15,
    )
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
    mocks.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=902,
        status="enabled",
        list_ids=[3],
    )

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    mocks.teyca_client.accrue_bonuses.assert_awaited_once()
    mocks.teyca_client.update_pass_fields.assert_awaited_once()
    mocks.merge_repo.create.assert_awaited_once_with(
        user_id=20,
        source_event_type="UPDATE",
        source_event_id=None,
        trace_id=None,
    )


@pytest.mark.asyncio
async def test_update_skips_merge_if_merge_log_appears_after_old_db_prefetch() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.side_effect = [False, True]
    mocks.old_db_repo.get_user_data.return_value = OldUserData(
        bonus=40.0,
        summ=15,
    )
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
    mocks.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=903,
        status="enabled",
        list_ids=[3],
    )

    await handle(_payload(), deps=deps)

    mocks.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    mocks.merge_repo.create.assert_not_awaited()
    mocks.teyca_client.accrue_bonuses.assert_not_awaited()
    mocks.teyca_client.update_pass_fields.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_when_merge_missing_but_old_data_empty() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = None
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
    mocks.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=903,
        status="enabled",
        list_ids=[3],
    )

    await handle(_payload(), deps=deps)

    mocks.merge_repo.create.assert_not_awaited()
    mocks.teyca_client.accrue_bonuses.assert_not_awaited()
    mocks.teyca_client.update_pass_fields.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_invalid_email_blocks_and_skips_listmonk() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(email="not-an-email"), deps=deps)

    mocks.listmonk_client.upsert_subscriber.assert_not_awaited()
    mocks.listmonk_repo.upsert.assert_not_awaited()
    mocks.listmonk_repo.get_other_user_ids_by_email.assert_not_awaited()
    mocks.listmonk_repo.set_consent_pending.assert_not_awaited()
    mocks.listmonk_repo.mark_checked.assert_not_awaited()
    mocks.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=20,
        fields={"key1": "blocked"},
    )


@pytest.mark.asyncio
async def test_update_retry_waits_for_user_lock() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
    mocks.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=903,
        status="enabled",
        list_ids=[3],
    )

    await handle(_payload(), deps=deps, wait_for_lock=True)

    mocks.users_repo.lock_user.assert_awaited_once_with(user_id=20, wait=True)


@pytest.mark.asyncio
async def test_update_commits_before_external_calls() -> None:
    deps, mocks = _deps()
    events: list[str] = []
    mocks.commit_checkpoint.side_effect = lambda: events.append("commit")
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = OldUserData(bonus=40.0, summ=15)
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
    mocks.listmonk_client.upsert_subscriber.side_effect = lambda **_: (
        events.append("listmonk")
        or SimpleNamespace(
            subscriber_id=902,
            status="enabled",
            list_ids=[3],
        )
    )
    mocks.teyca_client.accrue_bonuses.side_effect = lambda **_: events.append("teyca_bonus")

    await handle(_payload(), deps=deps)

    assert mocks.commit_checkpoint.await_count == 2
    assert events[:4] == ["commit", "listmonk", "commit", "teyca_bonus"]


@pytest.mark.asyncio
async def test_update_emits_step_logs_for_major_phases() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = False
    mocks.old_db_repo.get_user_data.return_value = OldUserData(bonus=40.0, summ=15)
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
    mocks.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=902,
        status="enabled",
        list_ids=[3],
    )

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
    assert "update_consumer_listmonk_upsert_start" in step_events
    assert "update_consumer_listmonk_upsert_done" in step_events
    assert "update_consumer_merge_external_start" in step_events
    assert "update_consumer_merge_external_done" in step_events


@pytest.mark.asyncio
async def test_update_invalid_email_blocks_and_marks_existing_subscriber() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)

    await handle(_payload(email="bad@"), deps=deps)

    mocks.listmonk_client.upsert_subscriber.assert_not_awaited()
    mocks.listmonk_repo.get_other_user_ids_by_email.assert_not_awaited()
    mocks.listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=20,
        pending=False,
        confirmed=False,
        status="blocked",
    )


@pytest.mark.asyncio
async def test_update_duplicate_email_schedules_repair_and_skips_requeue_path() -> None:
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
    mocks.listmonk_client.upsert_subscriber.assert_not_awaited()
    mocks.listmonk_repo.upsert.assert_not_awaited()
    mocks.listmonk_repo.set_consent_pending.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_duplicate_subscriber_id_stops_retry_path() -> None:
    deps, mocks = _deps()
    mocks.merge_repo.exists.return_value = True
    mocks.listmonk_repo.get_by_user_id.return_value = None
    mocks.listmonk_repo.get_other_user_ids_by_email.return_value = []
    mocks.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=903,
        status="enabled",
        list_ids=[3],
    )
    mocks.listmonk_repo.upsert.side_effect = DuplicateListmonkSubscriberIdError(
        subscriber_id=903,
        rows=[],
    )

    await handle(_payload(), deps=deps)

    mocks.email_repair_repo.create_pending.assert_not_awaited()
    mocks.listmonk_repo.set_consent_pending.assert_not_awaited()
