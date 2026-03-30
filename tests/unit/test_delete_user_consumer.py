from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.consumers.delete_user import DeleteConsumerDeps, handle
from app.mq.queues import QUEUE_DELETE
from app.repositories.external_call_outbox import OUTBOX_OP_LISTMONK_DELETE


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
    outbox_repo: AsyncMock


def _deps() -> tuple[DeleteConsumerDeps, _Mocks]:
    mocks = _Mocks(
        users_repo=AsyncMock(),
        listmonk_repo=AsyncMock(),
        merge_repo=AsyncMock(),
        bonus_accrual_repo=AsyncMock(),
        outbox_repo=AsyncMock(),
    )
    deps = DeleteConsumerDeps(
        users_repo=mocks.users_repo,
        listmonk_repo=mocks.listmonk_repo,
        merge_repo=mocks.merge_repo,
        bonus_accrual_repo=mocks.bonus_accrual_repo,
        outbox_repo=mocks.outbox_repo,
    )
    return deps, mocks


@pytest.mark.asyncio
async def test_delete_with_subscriber_id_enqueues_outbox_delete() -> None:
    deps, mocks = _deps()
    mocks.listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=700)
    mocks.outbox_repo.enqueue_once.return_value = True

    await handle(_payload(), deps=deps)

    mocks.users_repo.lock_user.assert_awaited_once_with(user_id=30, wait=False)
    mocks.outbox_repo.enqueue_once.assert_awaited_once()
    enqueue_kwargs = mocks.outbox_repo.enqueue_once.await_args.kwargs
    assert enqueue_kwargs["operation"] == OUTBOX_OP_LISTMONK_DELETE
    assert enqueue_kwargs["queue_name"] == QUEUE_DELETE
    assert enqueue_kwargs["payload"] == {"subscriber_id": 700}
    mocks.listmonk_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    mocks.merge_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    mocks.bonus_accrual_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)
    mocks.users_repo.delete_by_user_id.assert_awaited_once_with(user_id=30)


@pytest.mark.asyncio
async def test_delete_without_subscriber_id_skips_outbox_enqueue() -> None:
    deps, mocks = _deps()
    mocks.listmonk_repo.get_by_user_id.return_value = None

    await handle(_payload(), deps=deps)

    mocks.outbox_repo.enqueue_once.assert_not_awaited()
