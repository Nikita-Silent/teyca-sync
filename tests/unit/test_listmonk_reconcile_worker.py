from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.clients.listmonk import ListmonkClientError, SubscriberDelta
from app.workers.listmonk_reconcile_worker import (
    ListmonkReconcileWorker,
    ReconcileMetrics,
    _extract_attr_user_id,
    _parse_list_ids_text,
    build_listmonk_reconcile_worker,
)


def _worker() -> ListmonkReconcileWorker:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)
    return ListmonkReconcileWorker(
        settings=SimpleNamespace(
            listmonk_list_ids="1",
            consent_sync_batch_size=500,
        ),
        session_factory=session_factory,
        listmonk_client=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_reconcile_restores_mapping_by_attribute_user_id() -> None:
    worker = _worker()
    worker.listmonk_client.get_updated_subscribers.return_value = [
        SubscriberDelta(
            subscriber_id=101,
            status="enabled",
            list_ids=[1],
            updated_at=datetime(2026, 3, 6, 7, 0, tzinfo=UTC),
            email="user@example.com",
            attributes={"user_id": "77"},
        )
    ]

    with (
        patch("app.workers.listmonk_reconcile_worker.ListmonkUsersRepository") as listmonk_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.UsersRepository") as users_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.SyncStateRepository") as sync_repo_cls,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_subscriber_id.return_value = None
        listmonk_repo_cls.return_value = listmonk_repo

        users_repo = AsyncMock()
        users_repo.get_by_user_id.return_value = SimpleNamespace(user_id=77)
        users_repo_cls.return_value = users_repo

        sync_repo = AsyncMock()
        sync_repo.get_or_create.side_effect = [
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=None),
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=0),
        ]
        sync_repo_cls.return_value = sync_repo
        listmonk_repo.get_batch_after_user_id.return_value = []

        restored = await worker.run_once()

    assert restored == 1
    users_repo.get_by_user_id.assert_awaited_once_with(user_id=77)
    listmonk_repo.upsert.assert_awaited_once_with(
        user_id=77,
        subscriber_id=101,
        email="user@example.com",
        status="enabled",
        list_ids=[1],
        attributes={"user_id": "77"},
    )
    listmonk_repo.set_consent_pending.assert_awaited_once_with(user_id=77)


@pytest.mark.asyncio
async def test_reconcile_restores_mapping_by_email_when_attribute_missing() -> None:
    worker = _worker()
    worker.listmonk_client.get_updated_subscribers.return_value = [
        SubscriberDelta(
            subscriber_id=202,
            status="confirmed",
            list_ids=[1],
            updated_at=datetime(2026, 3, 6, 7, 5, tzinfo=UTC),
            email="mapped@example.com",
            attributes=None,
        )
    ]

    with (
        patch("app.workers.listmonk_reconcile_worker.ListmonkUsersRepository") as listmonk_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.UsersRepository") as users_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.SyncStateRepository") as sync_repo_cls,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_subscriber_id.return_value = None
        listmonk_repo_cls.return_value = listmonk_repo

        users_repo = AsyncMock()
        users_repo.get_by_user_id.return_value = None
        users_repo.get_user_ids_by_email.return_value = [88]
        users_repo_cls.return_value = users_repo

        sync_repo = AsyncMock()
        sync_repo.get_or_create.side_effect = [
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=None),
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=0),
        ]
        sync_repo_cls.return_value = sync_repo
        listmonk_repo.get_batch_after_user_id.return_value = []

        restored = await worker.run_once()

    assert restored == 1
    users_repo.get_user_ids_by_email.assert_awaited_once_with(email="mapped@example.com", limit=2)
    listmonk_repo.upsert.assert_awaited_once_with(
        user_id=88,
        subscriber_id=202,
        email="mapped@example.com",
        status="confirmed",
        list_ids=[1],
        attributes=None,
    )
    listmonk_repo.set_consent_pending.assert_awaited_once_with(user_id=88)


@pytest.mark.asyncio
async def test_reconcile_skips_when_email_is_ambiguous() -> None:
    worker = _worker()
    worker.listmonk_client.get_updated_subscribers.return_value = [
        SubscriberDelta(
            subscriber_id=303,
            status="enabled",
            list_ids=[1],
            updated_at=datetime(2026, 3, 6, 7, 10, tzinfo=UTC),
            email="dup@example.com",
            attributes=None,
        )
    ]

    with (
        patch("app.workers.listmonk_reconcile_worker.ListmonkUsersRepository") as listmonk_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.UsersRepository") as users_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.SyncStateRepository") as sync_repo_cls,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_subscriber_id.return_value = None
        listmonk_repo_cls.return_value = listmonk_repo

        users_repo = AsyncMock()
        users_repo.get_user_ids_by_email.return_value = [11, 22]
        users_repo_cls.return_value = users_repo

        sync_repo = AsyncMock()
        sync_repo.get_or_create.side_effect = [
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=None),
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=0),
        ]
        sync_repo_cls.return_value = sync_repo
        listmonk_repo.get_batch_after_user_id.return_value = []

        restored = await worker.run_once()

    assert restored == 0
    listmonk_repo.upsert.assert_not_awaited()
    listmonk_repo.set_consent_pending.assert_not_awaited()
    sync_repo.update_watermark.assert_any_await(
        source="listmonk_reconcile",
        list_id=1,
        updated_at=datetime(2026, 3, 6, 7, 10, tzinfo=UTC),
        subscriber_id=303,
    )


@pytest.mark.asyncio
async def test_reconcile_restores_deleted_subscriber_from_local_mapping() -> None:
    worker = _worker()
    worker.listmonk_client.get_updated_subscribers.return_value = []
    worker.listmonk_client.get_subscriber_state.return_value = None
    worker.listmonk_client.restore_subscriber.return_value = SimpleNamespace(
        subscriber_id=9999,
        status="enabled",
        list_ids=[4],
    )

    with (
        patch("app.workers.listmonk_reconcile_worker.ListmonkUsersRepository") as listmonk_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.UsersRepository") as users_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.SyncStateRepository") as sync_repo_cls,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo_cls.return_value = listmonk_repo

        listmonk_repo.get_batch_after_user_id.return_value = [
            SimpleNamespace(
                user_id=77,
                subscriber_id=1234,
                email="user@example.com",
                status="confirmed",
                list_ids="4",
                attributes={"user_id": 77},
            )
        ]
        users_repo_cls.return_value = AsyncMock()

        sync_repo = AsyncMock()
        sync_repo.get_or_create.side_effect = [
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=None),
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=0),
        ]
        sync_repo_cls.return_value = sync_repo

        restored = await worker.run_once()

    assert restored == 1
    worker.listmonk_client.restore_subscriber.assert_awaited_once_with(
        email="user@example.com",
        list_ids=[4],
        attributes={"user_id": 77},
        desired_status="confirmed",
    )
    listmonk_repo.upsert.assert_awaited_once_with(
        user_id=77,
        subscriber_id=9999,
        email="user@example.com",
        status="enabled",
        list_ids=[4],
        attributes={"user_id": 77},
    )
    listmonk_repo.set_consent_pending.assert_awaited_once_with(user_id=77)


@pytest.mark.asyncio
async def test_reconcile_without_target_lists_returns_zero() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)
    worker = ListmonkReconcileWorker(
        settings=SimpleNamespace(listmonk_list_ids="", consent_sync_batch_size=500),
        session_factory=session_factory,
        listmonk_client=AsyncMock(),
    )
    assert await worker.run_once() == 0


@pytest.mark.asyncio
async def test_reconcile_continues_when_list_fetch_fails() -> None:
    worker = _worker()
    worker.settings.listmonk_list_ids = "1,2"
    worker.listmonk_client.get_updated_subscribers.side_effect = [
        ListmonkClientError("timeout"),
        [
            SubscriberDelta(
                subscriber_id=212,
                status="enabled",
                list_ids=[2],
                updated_at=datetime(2026, 3, 6, 7, 20, tzinfo=UTC),
                email="mapped@example.com",
                attributes=None,
            )
        ],
    ]

    with (
        patch("app.workers.listmonk_reconcile_worker.ListmonkUsersRepository") as listmonk_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.UsersRepository") as users_repo_cls,
        patch("app.workers.listmonk_reconcile_worker.SyncStateRepository") as sync_repo_cls,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_subscriber_id.return_value = None
        listmonk_repo.get_batch_after_user_id.return_value = []
        listmonk_repo_cls.return_value = listmonk_repo

        users_repo = AsyncMock()
        users_repo.get_user_ids_by_email.return_value = [88]
        users_repo_cls.return_value = users_repo

        sync_repo = AsyncMock()
        sync_repo.get_or_create.side_effect = [
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=None),
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=None),
            SimpleNamespace(watermark_updated_at=None, watermark_subscriber_id=0),
        ]
        sync_repo_cls.return_value = sync_repo

        restored = await worker.run_once()

    assert restored == 1
    listmonk_repo.upsert.assert_awaited_once()


@pytest.mark.asyncio
async def test_reconcile_delta_additional_branches() -> None:
    worker = _worker()
    metrics = ReconcileMetrics(batch_size=10)
    listmonk_repo = AsyncMock()
    users_repo = AsyncMock()
    delta = SubscriberDelta(
        subscriber_id=404,
        status="enabled",
        list_ids=[1],
        updated_at=datetime(2026, 3, 6, 7, 15, tzinfo=UTC),
        email=None,
        attributes={"user_id": "bad"},
    )

    listmonk_repo.get_by_subscriber_id.return_value = None
    await worker._reconcile_delta(
        delta=delta,
        list_id=1,
        listmonk_repo=listmonk_repo,
        users_repo=users_repo,
        metrics=metrics,
    )
    assert metrics.invalid_attribute_user_id == 1
    assert metrics.email_not_found == 1

    listmonk_repo.get_by_subscriber_id.return_value = SimpleNamespace(user_id=1)
    await worker._reconcile_delta(
        delta=delta,
        list_id=1,
        listmonk_repo=listmonk_repo,
        users_repo=users_repo,
        metrics=metrics,
    )
    assert metrics.already_mapped == 1

    listmonk_repo.get_by_subscriber_id.return_value = None
    users_repo.get_by_user_id.return_value = None
    delta_with_attr = SubscriberDelta(
        subscriber_id=405,
        status="enabled",
        list_ids=[1],
        updated_at=datetime(2026, 3, 6, 7, 16, tzinfo=UTC),
        email=None,
        attributes={"user_id": "55"},
    )
    await worker._reconcile_delta(
        delta=delta_with_attr,
        list_id=1,
        listmonk_repo=listmonk_repo,
        users_repo=users_repo,
        metrics=metrics,
    )
    assert metrics.attribute_user_not_found == 1
    assert metrics.email_not_found >= 2

    delta_with_email = SubscriberDelta(
        subscriber_id=406,
        status="enabled",
        list_ids=[1],
        updated_at=datetime(2026, 3, 6, 7, 17, tzinfo=UTC),
        email="nomatch@example.com",
        attributes=None,
    )
    users_repo.get_user_ids_by_email.return_value = []
    await worker._reconcile_delta(
        delta=delta_with_email,
        list_id=1,
        listmonk_repo=listmonk_repo,
        users_repo=users_repo,
        metrics=metrics,
    )
    assert metrics.email_not_found >= 3


@pytest.mark.asyncio
async def test_run_consistency_scan_handles_restore_error_and_live_subscriber() -> None:
    worker = _worker()
    listmonk_repo = AsyncMock()
    sync_repo = AsyncMock()
    sync_repo.get_or_create.return_value = SimpleNamespace(watermark_subscriber_id=0)
    listmonk_repo.get_batch_after_user_id.return_value = [
        SimpleNamespace(
            user_id=10,
            subscriber_id=100,
            email="x@y.z",
            status="enabled",
            list_ids="1",
            attributes={},
        ),
        SimpleNamespace(
            user_id=11,
            subscriber_id=101,
            email="y@z.x",
            status="enabled",
            list_ids="1",
            attributes={},
        ),
    ]
    worker.listmonk_client.get_subscriber_state.side_effect = [SimpleNamespace(), None]
    worker.listmonk_client.restore_subscriber.side_effect = ListmonkClientError("boom")

    metrics = ReconcileMetrics(batch_size=10)
    await worker._run_consistency_scan(
        listmonk_repo=listmonk_repo,
        sync_repo=sync_repo,
        metrics=metrics,
        limit=100,
    )

    assert metrics.consistency_scanned == 2
    assert metrics.consistency_missing == 1
    assert metrics.consistency_errors == 1
    sync_repo.update_watermark.assert_awaited_once_with(
        source="listmonk_consistency",
        list_id=0,
        updated_at=None,
        subscriber_id=10,
    )


@pytest.mark.asyncio
async def test_run_consistency_scan_reraises_unexpected_restore_error() -> None:
    worker = _worker()
    listmonk_repo = AsyncMock()
    sync_repo = AsyncMock()
    sync_repo.get_or_create.return_value = SimpleNamespace(watermark_subscriber_id=0)
    listmonk_repo.get_batch_after_user_id.return_value = [
        SimpleNamespace(
            user_id=11,
            subscriber_id=101,
            email="y@z.x",
            status="enabled",
            list_ids="1",
            attributes={},
        ),
    ]
    worker.listmonk_client.get_subscriber_state.return_value = None
    worker.listmonk_client.restore_subscriber.side_effect = RuntimeError("boom")

    metrics = ReconcileMetrics(batch_size=10)
    with pytest.raises(RuntimeError, match="boom"):
        await worker._run_consistency_scan(
            listmonk_repo=listmonk_repo,
            sync_repo=sync_repo,
            metrics=metrics,
            limit=100,
        )

    assert metrics.consistency_scanned == 1
    assert metrics.consistency_missing == 1
    assert metrics.consistency_errors == 0
    sync_repo.update_watermark.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_consistency_scan_keeps_failed_state_check_row_for_retry() -> None:
    worker = _worker()
    listmonk_repo = AsyncMock()
    sync_repo = AsyncMock()
    sync_repo.get_or_create.return_value = SimpleNamespace(watermark_subscriber_id=7)
    listmonk_repo.get_batch_after_user_id.return_value = [
        SimpleNamespace(
            user_id=10,
            subscriber_id=100,
            email="x@y.z",
            status="enabled",
            list_ids="1",
            attributes={},
        ),
        SimpleNamespace(
            user_id=11,
            subscriber_id=101,
            email="y@z.x",
            status="enabled",
            list_ids="1",
            attributes={},
        ),
    ]
    worker.listmonk_client.get_subscriber_state.side_effect = ListmonkClientError("timeout")

    metrics = ReconcileMetrics(batch_size=10)
    await worker._run_consistency_scan(
        listmonk_repo=listmonk_repo,
        sync_repo=sync_repo,
        metrics=metrics,
        limit=100,
    )

    assert metrics.consistency_scanned == 1
    assert metrics.consistency_errors == 1
    worker.listmonk_client.get_subscriber_state.assert_awaited_once_with(subscriber_id=100)
    sync_repo.update_watermark.assert_awaited_once_with(
        source="listmonk_consistency",
        list_id=0,
        updated_at=None,
        subscriber_id=7,
    )


def test_reconcile_build_and_helpers() -> None:
    with (
        patch("app.workers.listmonk_reconcile_worker.get_settings", return_value=SimpleNamespace()),
        patch("app.workers.listmonk_reconcile_worker.ListmonkSDKClient"),
    ):
        worker = build_listmonk_reconcile_worker()
    assert worker is not None

    assert _extract_attr_user_id(None) is None
    assert _extract_attr_user_id({"user_id": 10}) == 10
    assert _extract_attr_user_id({"user_id": " 20 "}) == 20
    assert _extract_attr_user_id({"user_id": "abc"}) is None

    assert _parse_list_ids_text(None) == []
    assert _parse_list_ids_text("1, x, 2, ,3") == [1, 2, 3]
