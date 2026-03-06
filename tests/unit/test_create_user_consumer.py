from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.consumers.create_user import CreateConsumerDeps, handle
from app.repositories.old_db import OldUserData


def _payload(user_id: int = 10) -> dict[str, object]:
    return {
        "type": "CREATE",
        "pass": {
            "user_id": user_id,
            "email": "user@example.com",
            "phone": "79039859055",
            "summ": 100,
            "summ_all": 1000,
            "visits": 2,
        },
    }


def _deps() -> CreateConsumerDeps:
    return CreateConsumerDeps(
        settings=SimpleNamespace(
            listmonk_list_ids="1,2",
            consent_bonus_ttl_days=30,
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
