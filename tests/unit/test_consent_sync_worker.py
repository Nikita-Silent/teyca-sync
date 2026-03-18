from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.clients.listmonk import SubscriberDelta, SubscriberState
from app.clients.teyca import TeycaAPIError
from app.repositories.listmonk_users import DuplicateListmonkSubscriberIdError
from app.workers.consent_sync_worker import (
    ConsentSyncMetrics,
    ConsentSyncWorker,
    _inc,
    _normalize_progress_payload,
    build_consent_sync_worker,
    parse_list_ids,
)


def _worker() -> ConsentSyncWorker:
    return ConsentSyncWorker(
        settings=SimpleNamespace(consent_bonus_amount="100.0", consent_bonus_ttl_days=30),
        session_factory=AsyncMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_process_pending_user_subscriber_not_found() -> None:
    worker = _worker()
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
    worker = _worker()
    pending = SimpleNamespace(user_id=2, subscriber_id=102)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=102,
        status="enabled",
        list_ids=[1],
        list_statuses={1: "unconfirmed"},
    )

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=2,
        pending=True,
        confirmed=False,
        status="unconfirmed",
    )
    accrual_repo.reserve.assert_not_awaited()
    worker.teyca_client.accrue_bonuses.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_pending_user_mixed_lists_is_not_confirmed() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=16, subscriber_id=116)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=116,
        status="enabled",
        list_ids=[1, 4],
        list_statuses={1: "confirmed", 4: "unconfirmed"},
    )

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1, 4],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=16,
        pending=True,
        confirmed=False,
        status="unconfirmed",
    )
    worker.teyca_client.accrue_bonuses.assert_not_awaited()
    worker.teyca_client.update_pass_fields.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_pending_user_blocked() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=12, subscriber_id=112)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=112,
        status="blocked",
        list_ids=[1],
    )

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    worker.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=12,
        fields={"key1": "blocked"},
    )
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=12,
        pending=False,
        confirmed=False,
        status="blocked",
    )
    accrual_repo.reserve.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_pending_user_blocked_has_priority_over_confirmed_in_other_list() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=15, subscriber_id=115)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=115,
        status="enabled",
        list_ids=[1, 2],
        list_statuses={1: "confirmed", 2: "blocklisted"},
    )

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1, 2],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    worker.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=15,
        fields={"key1": "blocked"},
    )
    accrual_repo.reserve.assert_not_awaited()
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=15,
        pending=False,
        confirmed=False,
        status="blocked",
    )


@pytest.mark.asyncio
async def test_process_pending_user_blocked_teyca_update_failed() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=13, subscriber_id=113)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=113,
        status="blocked",
        list_ids=[1],
    )
    worker.teyca_client.update_pass_fields.side_effect = TeycaAPIError("boom")

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=13,
        pending=True,
        confirmed=False,
        status="blocked",
    )


@pytest.mark.asyncio
async def test_process_pending_user_success_accrual_and_key1() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=4, subscriber_id=104)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=104,
        status="confirmed",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = True
    accrual_repo.get_by_key.return_value = SimpleNamespace(payload=None)

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    worker.teyca_client.accrue_bonuses.assert_awaited_once()
    worker.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=4,
        fields={"key1": "confirmed"},
    )
    assert accrual_repo.save_progress.await_count == 2
    accrual_repo.mark_done_with_payload.assert_awaited_once()
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=4,
        pending=False,
        confirmed=True,
        status="confirmed",
    )


@pytest.mark.asyncio
async def test_process_pending_user_enabled_status_is_saved_as_confirmed() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=14, subscriber_id=114)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=114,
        status="enabled",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = True
    accrual_repo.get_by_key.return_value = SimpleNamespace(payload=None)

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=14,
        pending=False,
        confirmed=True,
        status="confirmed",
    )


@pytest.mark.asyncio
async def test_process_pending_user_retry_only_key1_step() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=6, subscriber_id=106)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=106,
        status="confirmed",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = False
    accrual_repo.get_by_key.return_value = SimpleNamespace(
        payload={"subscriber_id": 106, "list_ids": [1], "bonus_done": True, "key1_done": False}
    )

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    worker.teyca_client.accrue_bonuses.assert_not_awaited()
    worker.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=6,
        fields={"key1": "confirmed"},
    )
    accrual_repo.mark_done_with_payload.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_pending_user_failed_confirmed_step() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=5, subscriber_id=105)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=105,
        status="confirmed",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = True
    accrual_repo.get_by_key.return_value = SimpleNamespace(payload=None)
    worker.teyca_client.accrue_bonuses.side_effect = TeycaAPIError("boom")

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    accrual_repo.save_progress.assert_awaited()
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=5,
        pending=True,
        confirmed=False,
        status="confirmed",
    )


@pytest.mark.asyncio
async def test_process_pending_user_when_all_steps_already_done() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=22, subscriber_id=122)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=122,
        status="confirmed",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = False
    accrual_repo.get_by_key.return_value = SimpleNamespace(
        payload={"subscriber_id": 122, "list_ids": [1], "bonus_done": True, "key1_done": True}
    )

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
    )

    worker.teyca_client.accrue_bonuses.assert_not_awaited()
    worker.teyca_client.update_pass_fields.assert_not_awaited()
    accrual_repo.mark_done_with_payload.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_pending_user_operation_missing() -> None:
    worker = _worker()
    pending = SimpleNamespace(user_id=21, subscriber_id=121)
    listmonk_repo = AsyncMock()
    accrual_repo = AsyncMock()
    worker.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=121,
        status="confirmed",
        list_ids=[1],
    )
    accrual_repo.reserve.return_value = False
    accrual_repo.get_by_key.return_value = None
    metrics = ConsentSyncMetrics(batch_size=10)

    await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
        accrual_repo=accrual_repo,
        metrics=metrics,
    )

    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=21,
        pending=True,
        confirmed=False,
        status="confirmed",
    )
    assert metrics.operation_missing == 1


def test_parse_list_ids() -> None:
    assert parse_list_ids("1, 2, bad, ,3") == [1, 2, 3]


def test_normalize_progress_payload_defaults() -> None:
    payload = _normalize_progress_payload(raw_payload=None, subscriber_id=7, list_ids=[2])
    assert payload["subscriber_id"] == 7
    assert payload["list_ids"] == [2]
    assert payload["bonus_done"] is False
    assert payload["key1_done"] is False


@pytest.mark.asyncio
async def test_run_once_uses_incremental_deltas_and_updates_watermark() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)

    worker = ConsentSyncWorker(
        settings=SimpleNamespace(
            consent_bonus_amount="100.0",
            consent_bonus_ttl_days=30,
            listmonk_list_ids="1",
            consent_sync_batch_size=500,
        ),
        session_factory=session_factory,
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    worker.listmonk_client.get_updated_subscribers.return_value = [
        SubscriberDelta(
            subscriber_id=1001,
            status="confirmed",
            list_ids=[1],
            updated_at=datetime(2026, 3, 6, 6, 0, tzinfo=UTC),
        )
    ]

    with (
        patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls,
        patch("app.workers.consent_sync_worker.BonusAccrualRepository") as accrual_cls,
        patch("app.workers.consent_sync_worker.SyncStateRepository") as sync_cls,
        patch.object(
            ConsentSyncWorker, "_process_pending_user", new_callable=AsyncMock
        ) as process_mock,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_subscriber_id.return_value = SimpleNamespace(
            user_id=77,
            subscriber_id=1001,
        )
        repo_cls.return_value = listmonk_repo
        accrual_cls.return_value = AsyncMock()

        sync_repo = AsyncMock()
        sync_repo.get_or_create.return_value = SimpleNamespace(
            watermark_updated_at=None,
            watermark_subscriber_id=None,
        )
        sync_cls.return_value = sync_repo

        processed = await worker.run_once()

    assert processed == 1
    worker.listmonk_client.get_updated_subscribers.assert_awaited_once()
    process_mock.assert_awaited_once()
    sync_repo.update_watermark.assert_awaited_once_with(
        source="listmonk_consent",
        list_id=1,
        updated_at=datetime(2026, 3, 6, 6, 0, tzinfo=UTC),
        subscriber_id=1001,
    )
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_once_skips_unmapped_subscribers_but_moves_watermark() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)

    worker = ConsentSyncWorker(
        settings=SimpleNamespace(
            consent_bonus_amount="100.0",
            consent_bonus_ttl_days=30,
            listmonk_list_ids="1",
            consent_sync_batch_size=500,
        ),
        session_factory=session_factory,
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    worker.listmonk_client.get_updated_subscribers.return_value = [
        SubscriberDelta(
            subscriber_id=2002,
            status="blocked",
            list_ids=[1],
            updated_at=datetime(2026, 3, 6, 6, 10, tzinfo=UTC),
        )
    ]

    with (
        patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls,
        patch("app.workers.consent_sync_worker.BonusAccrualRepository") as accrual_cls,
        patch("app.workers.consent_sync_worker.SyncStateRepository") as sync_cls,
        patch.object(
            ConsentSyncWorker, "_process_pending_user", new_callable=AsyncMock
        ) as process_mock,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_subscriber_id.return_value = None
        repo_cls.return_value = listmonk_repo
        accrual_cls.return_value = AsyncMock()

        sync_repo = AsyncMock()
        sync_repo.get_or_create.return_value = SimpleNamespace(
            watermark_updated_at=None,
            watermark_subscriber_id=None,
        )
        sync_cls.return_value = sync_repo

        processed = await worker.run_once()

    assert processed == 0
    process_mock.assert_not_awaited()
    sync_repo.update_watermark.assert_awaited_once_with(
        source="listmonk_consent",
        list_id=1,
        updated_at=datetime(2026, 3, 6, 6, 10, tzinfo=UTC),
        subscriber_id=2002,
    )


@pytest.mark.asyncio
async def test_run_once_returns_zero_without_target_lists() -> None:
    worker = ConsentSyncWorker(
        settings=SimpleNamespace(
            consent_bonus_amount="100.0",
            consent_bonus_ttl_days=30,
            listmonk_list_ids="",
            consent_sync_batch_size=500,
        ),
        session_factory=MagicMock(),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    assert await worker.run_once() == 0


@pytest.mark.asyncio
async def test_run_once_skips_empty_deltas() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)
    worker = ConsentSyncWorker(
        settings=SimpleNamespace(
            consent_bonus_amount="100.0",
            consent_bonus_ttl_days=30,
            listmonk_list_ids="1",
            consent_sync_batch_size=500,
        ),
        session_factory=session_factory,
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    worker.listmonk_client.get_updated_subscribers.return_value = []

    with (
        patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls,
        patch("app.workers.consent_sync_worker.BonusAccrualRepository") as accrual_cls,
        patch("app.workers.consent_sync_worker.SyncStateRepository") as sync_cls,
    ):
        repo_cls.return_value = AsyncMock()
        accrual_cls.return_value = AsyncMock()
        sync_repo = AsyncMock()
        sync_repo.get_or_create.return_value = SimpleNamespace(
            watermark_updated_at=None,
            watermark_subscriber_id=None,
        )
        sync_cls.return_value = sync_repo
        assert await worker.run_once() == 0
        sync_repo.update_watermark.assert_not_awaited()


def test_build_consent_sync_worker_and_inc_helper() -> None:
    with (
        patch(
            "app.workers.consent_sync_worker.get_settings",
            return_value=SimpleNamespace(),
        ),
        patch("app.workers.consent_sync_worker.ListmonkSDKClient"),
        patch("app.workers.consent_sync_worker.TeycaClient"),
    ):
        worker = build_consent_sync_worker()
    assert worker is not None

    metrics = ConsentSyncMetrics(batch_size=1)
    _inc(metrics, "teyca_errors")
    _inc(None, "teyca_errors")
    assert metrics.teyca_errors == 1


@pytest.mark.asyncio
async def test_run_once_skips_duplicate_subscriber_mapping_and_moves_watermark() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)

    worker = ConsentSyncWorker(
        settings=SimpleNamespace(
            consent_bonus_amount="100.0",
            consent_bonus_ttl_days=30,
            listmonk_list_ids="1",
            consent_sync_batch_size=500,
        ),
        session_factory=session_factory,
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )
    worker.listmonk_client.get_updated_subscribers.return_value = [
        SubscriberDelta(
            subscriber_id=3003,
            status="confirmed",
            list_ids=[1],
            updated_at=datetime(2026, 3, 6, 6, 20, tzinfo=UTC),
        )
    ]

    with (
        patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls,
        patch("app.workers.consent_sync_worker.BonusAccrualRepository") as accrual_cls,
        patch("app.workers.consent_sync_worker.SyncStateRepository") as sync_cls,
        patch.object(
            ConsentSyncWorker, "_process_pending_user", new_callable=AsyncMock
        ) as process_mock,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_subscriber_id.side_effect = DuplicateListmonkSubscriberIdError(
            subscriber_id=3003,
            rows=[],
        )
        repo_cls.return_value = listmonk_repo
        accrual_cls.return_value = AsyncMock()

        sync_repo = AsyncMock()
        sync_repo.get_or_create.return_value = SimpleNamespace(
            watermark_updated_at=None,
            watermark_subscriber_id=None,
        )
        sync_cls.return_value = sync_repo

        processed = await worker.run_once()

    assert processed == 0
    process_mock.assert_not_awaited()
    sync_repo.update_watermark.assert_awaited_once_with(
        source="listmonk_consent",
        list_id=1,
        updated_at=datetime(2026, 3, 6, 6, 20, tzinfo=UTC),
        subscriber_id=3003,
    )
