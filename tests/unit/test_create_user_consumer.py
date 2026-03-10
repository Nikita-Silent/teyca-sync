from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.consumers.create_user import CreateConsumerDeps, handle
from app.repositories.old_db import OldUserData


def _payload(user_id: int = 10, email: str = "user@example.com") -> dict[str, object]:
    """
    Builds a test CREATE event payload for a user.
    
    Parameters:
        user_id (int): User identifier to include in the payload's `pass.user_id`. Defaults to 10.
        email (str): Email address to include in the payload's `pass.email`. Defaults to "user@example.com".
    
    Returns:
        dict[str, object]: A dictionary representing a CREATE event payload with a nested `pass` block containing `user_id`, `email`, `phone`, `summ`, `summ_all`, and `visits`.
    """
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


def _deps() -> CreateConsumerDeps:
    """
    Create a default CreateConsumerDeps populated with test doubles and minimal settings.
    
    Returns:
        CreateConsumerDeps: Instance configured for tests with `settings.listmonk_list_ids` set to "1,2" and AsyncMock objects for users_repo, listmonk_repo, merge_repo, old_db_repo, listmonk_client, and teyca_client.
    """
    return CreateConsumerDeps(
        settings=SimpleNamespace(
            listmonk_list_ids="1,2",
        ),
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        old_db_repo=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_create_without_old_data_no_merge() -> None:
    deps = _deps()
    deps.merge_repo.exists.return_value = False
    deps.old_db_repo.get_user_data.return_value = None
    deps.listmonk_repo.get_by_user_id.return_value = None
    deps.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=500,
        status="enabled",
        list_ids=[1, 2],
    )

    await handle(_payload(), deps=deps)

    deps.users_repo.lock_user.assert_awaited_once_with(user_id=10)
    deps.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    deps.users_repo.upsert.assert_awaited_once()
    deps.listmonk_client.upsert_subscriber.assert_awaited_once()
    deps.merge_repo.create.assert_not_awaited()
    deps.teyca_client.accrue_bonuses.assert_not_awaited()
    deps.teyca_client.update_pass_fields.assert_not_awaited()
    deps.listmonk_repo.set_consent_pending.assert_awaited_once_with(user_id=10)


@pytest.mark.asyncio
async def test_create_with_old_data_and_existing_subscriber() -> None:
    deps = _deps()
    deps.merge_repo.exists.return_value = False
    deps.old_db_repo.get_user_data.return_value = OldUserData(
        bonus=55.0,
        summ=10,
        check_summ=5,
    )
    deps.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=777)
    deps.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=777,
        status="enabled",
        list_ids=[1, 2],
    )

    await handle(_payload(), deps=deps)

    deps.old_db_repo.get_user_data.assert_awaited_once_with(phone="79039859055")
    deps.listmonk_client.upsert_subscriber.assert_awaited_once()
    call_kwargs = deps.listmonk_client.upsert_subscriber.await_args.kwargs
    assert call_kwargs["subscriber_id"] == 777
    deps.teyca_client.accrue_bonuses.assert_awaited_once()
    deps.teyca_client.update_pass_fields.assert_awaited_once()
    deps.merge_repo.create.assert_awaited_once_with(user_id=10, source_event_type="CREATE")


@pytest.mark.asyncio
async def test_create_skips_merge_when_merge_already_exists() -> None:
    """
    Verify that when a merge record already exists for the user, the create handler skips merge-related work and external teyca interactions.
    
    Asserts that no old-database lookup is performed, no merge is created, and no teyca bonus accrual or pass-field updates are attempted.
    """
    deps = _deps()
    deps.merge_repo.exists.return_value = True
    deps.listmonk_repo.get_by_user_id.return_value = None
    deps.listmonk_client.upsert_subscriber.return_value = SimpleNamespace(
        subscriber_id=888,
        status="enabled",
        list_ids=[1, 2],
    )

    await handle(_payload(), deps=deps)

    deps.old_db_repo.get_user_data.assert_not_awaited()
    deps.merge_repo.create.assert_not_awaited()
    deps.teyca_client.accrue_bonuses.assert_not_awaited()
    deps.teyca_client.update_pass_fields.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_invalid_email_blocks_and_skips_listmonk() -> None:
    deps = _deps()
    deps.merge_repo.exists.return_value = False
    deps.old_db_repo.get_user_data.return_value = None
    deps.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(email="bad.mail@"), deps=deps)

    deps.listmonk_client.upsert_subscriber.assert_not_awaited()
    deps.listmonk_repo.upsert.assert_not_awaited()
    deps.listmonk_repo.set_consent_pending.assert_not_awaited()
    deps.listmonk_repo.mark_checked.assert_not_awaited()
    deps.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=10,
        fields={"key1": "blocked"},
    )


@pytest.mark.asyncio
async def test_create_invalid_email_blocks_and_marks_existing_subscriber() -> None:
    deps = _deps()
    deps.merge_repo.exists.return_value = True
    deps.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=500)

    await handle(_payload(email="bad"), deps=deps)

    deps.listmonk_client.upsert_subscriber.assert_not_awaited()
    deps.listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=10,
        pending=False,
        confirmed=False,
        status="blocked",
    )
