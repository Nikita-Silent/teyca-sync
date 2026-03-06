from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.consumers.update_user import UpdateConsumerDeps, handle
from app.repositories.old_db import OldUserData


def _payload(user_id: int = 20) -> dict[str, object]:
    return {
        "type": "UPDATE",
        "pass": {
            "user_id": user_id,
            "email": "up@example.com",
            "phone": "79039859055",
            "summ": 90,
            "visits": 3,
        },
    }


def _deps() -> UpdateConsumerDeps:
    return UpdateConsumerDeps(
        settings=SimpleNamespace(
            listmonk_list_ids="3",
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
