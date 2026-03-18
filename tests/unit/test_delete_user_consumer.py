from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.clients.listmonk import ListmonkClientError
from app.consumers.delete_user import DeleteConsumerDeps, handle


def _payload(user_id: int = 30) -> dict[str, object]:
    return {
        "type": "DELETE",
        "pass": {
            "user_id": user_id,
        },
    }


@dataclass(slots=True)
class _Mocks:
    users_repo: AsyncMock
    listmonk_repo: AsyncMock
    merge_repo: AsyncMock
    bonus_accrual_repo: AsyncMock
    listmonk_client: AsyncMock
    session: AsyncMock


def _deps() -> tuple[DeleteConsumerDeps, _Mocks]:
    mocks = _Mocks(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        bonus_accrual_repo=AsyncMock(),
        listmonk_client=AsyncMock(),
        session=AsyncMock(),
    )
    deps = DeleteConsumerDeps(
        users_repo=mocks.users_repo,
        listmonk_repo=mocks.listmonk_repo,
        merge_repo=mocks.merge_repo,
        bonus_accrual_repo=mocks.bonus_accrual_repo,
        listmonk_client=mocks.listmonk_client,
        session=mocks.session,
    )
    return deps, mocks


@pytest.mark.asyncio
async def test_delete_with_subscriber_id() -> None:
    deps, mocks = _deps()
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=700)

    await handle(_payload(), deps=deps)

    mocks.users_repo.lock_user.assert_awaited_once_with(user_id=30, wait=False)
    mocks.listmonk_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    mocks.merge_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    mocks.bonus_accrual_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    mocks.users_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    mocks.session.commit.assert_awaited_once()
    mocks.listmonk_client.delete_subscriber.assert_awaited_once_with(subscriber_id=700)


@pytest.mark.asyncio
async def test_delete_without_subscriber_id() -> None:
    deps, mocks = _deps()
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    mocks.session.commit.assert_awaited_once()
    mocks.listmonk_client.delete_subscriber.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_ignores_listmonk_delete_error() -> None:
    deps, mocks = _deps()
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=701)
    mocks.listmonk_client.delete_subscriber.side_effect = ListmonkClientError("boom")

    await handle(_payload(), deps=deps)

    mocks.session.commit.assert_awaited_once()
    mocks.listmonk_client.delete_subscriber.assert_awaited_once_with(subscriber_id=701)
