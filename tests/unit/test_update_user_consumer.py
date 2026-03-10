from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.consumers.update_user import UpdateConsumerDeps, handle
from app.repositories.old_db import OldUserData


def _payload(user_id: int = 20, email: str = "up@example.com") -> dict[str, object]:
    """
    Builds a test payload representing an UPDATE event containing user pass data.
    
    Parameters:
        user_id (int): User identifier to include in the payload's pass section.
        email (str): Email address to include in the payload's pass section.
    
    Returns:
        payload (dict[str, object]): Dictionary with keys "type" (set to "UPDATE") and "pass" (a mapping that includes `user_id`, `email`, `phone`, `summ`, and `visits`).
    """
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


def _deps() -> UpdateConsumerDeps:
    """
    Create a default UpdateConsumerDeps populated with test doubles.
    
    Returns:
        UpdateConsumerDeps: Instance where `settings` is a SimpleNamespace with `listmonk_list_ids="3"`, and repository/client attributes (`users_repo`, `listmonk_repo`, `merge_repo`, `old_db_repo`, `listmonk_client`, `teyca_client`) are AsyncMock instances.
    """
    return UpdateConsumerDeps(
        settings=SimpleNamespace(
            listmonk_list_ids="3",
        ),
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        old_db_repo=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_update_when_merge_already_exists() -> None:
    deps = _deps()
    deps.merge_repo.exists.return_value = True
    deps.listmonk_repo.get_by_user_id.return_value = None
    deps.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=901,
        status="enabled",
        list_ids=[3],
    )

    await handle(_payload(), deps=deps)

    deps.old_db_repo.get_user_data.assert_not_awaited()
    deps.teyca_client.accrue_bonuses.assert_not_awaited()
    deps.teyca_client.update_pass_fields.assert_not_awaited()
    deps.merge_repo.create.assert_not_awaited()
    deps.listmonk_repo.set_consent_pending.assert_awaited_once_with(user_id=20)


@pytest.mark.asyncio
async def test_update_when_merge_missing_and_old_data_exists() -> None:
    deps = _deps()
    deps.merge_repo.exists.return_value = False
    deps.old_db_repo.get_user_data.return_value = OldUserData(
        bonus=40.0,
        summ=15,
    )
    deps.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)
    deps.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=902,
        status="enabled",
        list_ids=[3],
    )

    await handle(_payload(), deps=deps)

    deps.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    deps.teyca_client.accrue_bonuses.assert_awaited_once()
    deps.teyca_client.update_pass_fields.assert_awaited_once()
    deps.merge_repo.create.assert_awaited_once_with(user_id=20, source_event_type="UPDATE")


@pytest.mark.asyncio
async def test_update_when_merge_missing_but_old_data_empty() -> None:
    """
    Verify that when a merge record is missing and no old user data exists, the consumer does not create a merge and does not call teyca-related update functions.
    
    The test sets up dependencies to simulate: merge_repo.exists -> False, old_db_repo.get_user_data -> None, and no existing listmonk subscriber. After handling an UPDATE payload, it asserts that merge_repo.create, teyca_client.accrue_bonuses, and teyca_client.update_pass_fields were not awaited.
    """
    deps = _deps()
    deps.merge_repo.exists.return_value = False
    deps.old_db_repo.get_user_data.return_value = None
    deps.listmonk_repo.get_by_user_id.return_value = None
    deps.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=903,
        status="enabled",
        list_ids=[3],
    )

    await handle(_payload(), deps=deps)

    deps.merge_repo.create.assert_not_awaited()
    deps.teyca_client.accrue_bonuses.assert_not_awaited()
    deps.teyca_client.update_pass_fields.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_invalid_email_blocks_and_skips_listmonk() -> None:
    deps = _deps()
    deps.merge_repo.exists.return_value = True
    deps.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(email="not-an-email"), deps=deps)

    deps.listmonk_client.upsert_subscriber.assert_not_awaited()
    deps.listmonk_repo.upsert.assert_not_awaited()
    deps.listmonk_repo.set_consent_pending.assert_not_awaited()
    deps.listmonk_repo.mark_checked.assert_not_awaited()
    deps.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=20,
        fields={"key1": "blocked"},
    )


@pytest.mark.asyncio
async def test_update_invalid_email_blocks_and_marks_existing_subscriber() -> None:
    """
    Verifies that an invalid email blocks the user and marks an existing ListMonk subscriber as blocked.
    
    Sets up a scenario where a merge already exists and a ListMonk subscriber is present; ensures no upsert is performed and that listmonk_repo.mark_checked is called with pending=False, confirmed=False, and status="blocked".
    """
    deps = _deps()
    deps.merge_repo.exists.return_value = True
    deps.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=902)

    await handle(_payload(email="bad@"), deps=deps)

    deps.listmonk_client.upsert_subscriber.assert_not_awaited()
    deps.listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=20,
        pending=False,
        confirmed=False,
        status="blocked",
    )
