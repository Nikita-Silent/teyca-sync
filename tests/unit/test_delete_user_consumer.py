from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.clients.listmonk import ListmonkClientError
from app.consumers.delete_user import DeleteConsumerDeps, handle


def _payload(user_id: int = 30) -> dict[str, object]:
    """
    Constructs a DELETE message payload containing the provided user_id.
    
    Parameters:
        user_id (int): ID of the user to delete. Defaults to 30.
    
    Returns:
        dict: Message payload with "type" set to "DELETE" and a "pass" mapping that includes the `user_id`.
    """
    return {
        "type": "DELETE",
        "pass": {
            "user_id": user_id,
        },
    }


def _deps() -> DeleteConsumerDeps:
    """
    Create a DeleteConsumerDeps populated with AsyncMock test doubles.
    
    Each field corresponds to a dependency of the delete-user consumer and is an AsyncMock instance:
    `users_repo`, `listmonk_repo`, `merge_repo`, `bonus_accrual_repo`, `listmonk_client`, and `session`.
    
    Returns:
        DeleteConsumerDeps: Instance whose attributes are AsyncMock objects for use in unit tests.
    """
    return DeleteConsumerDeps(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        bonus_accrual_repo=AsyncMock(),
        listmonk_client=AsyncMock(),
        session=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_delete_with_subscriber_id() -> None:
    deps = _deps()
    deps.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=700)

    await handle(_payload(), deps=deps)

    deps.users_repo.lock_user.assert_awaited_once_with(user_id=30)
    deps.listmonk_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    deps.merge_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    deps.bonus_accrual_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    deps.users_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    deps.session.commit.assert_awaited_once()
    deps.listmonk_client.delete_subscriber.assert_awaited_once_with(subscriber_id=700)


@pytest.mark.asyncio
async def test_delete_without_subscriber_id() -> None:
    deps = _deps()
    deps.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    deps.session.commit.assert_awaited_once()
    deps.listmonk_client.delete_subscriber.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_ignores_listmonk_delete_error() -> None:
    """
    Verifies that a Listmonk client deletion error does not prevent the consumer from committing and still attempts to delete the subscriber.
    
    Sets up a subscriber_id (701) and makes listmonk_client.delete_subscriber raise ListmonkClientError, then asserts the session was committed and delete_subscriber was awaited with the expected subscriber_id.
    """
    deps = _deps()
    deps.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=701)
    deps.listmonk_client.delete_subscriber.side_effect = ListmonkClientError("boom")

    await handle(_payload(), deps=deps)

    deps.session.commit.assert_awaited_once()
    deps.listmonk_client.delete_subscriber.assert_awaited_once_with(subscriber_id=701)
