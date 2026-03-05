from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.clients.listmonk import SubscriberState
from app.clients.teyca import TeycaAPIError
from app.workers.consent_sync_worker import ConsentSyncWorker, parse_list_ids


@pytest.mark.asyncio
async def test_process_pending_user_subscriber_not_found() -> None:
    worker = ConsentSyncWorker(
        settings=SimpleNamespace(consent_bonus_value="100.0", consent_bonus_ttl_days=30),
        session_factory=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    pending = SimpleNamespace(user_id=1, subscriber_id=101)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = None

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    listmonk_repo.mark_checked.assert_awaited_once_with(user_id=1, pending=True, confirmed=False)
    accrual_repo.reserve.assert_not_awaited()
    worker.teyca_client.accrue_bonuses.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_pending_user_not_confirmed() -> None:
    worker = ConsentSyncWorker(
        settings=SimpleNamespace(consent_bonus_value="100.0", consent_bonus_ttl_days=30),
        session_factory=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    pending = SimpleNamespace(user_id=2, subscriber_id=102)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=102,
        status="disabled",
        list_ids=[1],
    )

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    listmonk_repo.mark_checked.assert_awaited_once_with(user_id=2, pending=True, confirmed=False)
    accrual_repo.reserve.assert_not_awaited()
    worker.teyca_client.accrue_bonuses.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_pending_user_already_accrued() -> None:
    worker = ConsentSyncWorker(
        settings=SimpleNamespace(consent_bonus_value="100.0", consent_bonus_ttl_days=30),
        session_factory=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    pending = SimpleNamespace(user_id=3, subscriber_id=103)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=103,
        status="confirmed",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = False

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    accrual_repo.reserve.assert_awaited_once()
    listmonk_repo.mark_checked.assert_awaited_once_with(user_id=3, pending=False, confirmed=True)
    worker.teyca_client.accrue_bonuses.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_pending_user_success_accrual() -> None:
    worker = ConsentSyncWorker(
        settings=SimpleNamespace(consent_bonus_value="100.0", consent_bonus_ttl_days=30),
        session_factory=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    pending = SimpleNamespace(user_id=4, subscriber_id=104)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=104,
        status="confirmed",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = True

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    worker.teyca_client.accrue_bonuses.assert_awaited_once()
    accrual_repo.mark_done.assert_awaited_once_with(idempotency_key="email_consent:4")
    listmonk_repo.mark_checked.assert_awaited_once_with(user_id=4, pending=False, confirmed=True)


@pytest.mark.asyncio
async def test_process_pending_user_failed_accrual() -> None:
    worker = ConsentSyncWorker(
        settings=SimpleNamespace(consent_bonus_value="100.0", consent_bonus_ttl_days=30),
        session_factory=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    pending = SimpleNamespace(user_id=5, subscriber_id=105)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=105,
        status="confirmed",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = True
    worker.teyca_client.accrue_bonuses.side_effect = TeycaAPIError("boom")

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    accrual_repo.mark_failed.assert_awaited_once()
    listmonk_repo.mark_checked.assert_awaited_once_with(user_id=5, pending=True, confirmed=False)


def test_parse_list_ids() -> None:
    assert parse_list_ids("1, 2, bad, ,3") == [1, 2, 3]
