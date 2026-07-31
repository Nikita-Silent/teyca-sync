from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.clients.listmonk import SubscriberDelta, SubscriberState
from app.config import Settings
from app.repositories.listmonk_users import DuplicateListmonkSubscriberIdError
from app.workers.consent_sync_worker import (
    ConsentSyncMetrics,
    ConsentSyncWorker,
    _inc,
    build_consent_sync_worker,
    parse_list_ids,
)


@dataclass(slots=True)
class _WorkerMocks:
    listmonk_client: AsyncMock
    session_factory: AsyncMock | MagicMock


def _worker(**settings_overrides: object) -> tuple[ConsentSyncWorker, _WorkerMocks]:
    settings = {
        "listmonk_list_ids": "1",
        "consent_sync_batch_size": 500,
        **settings_overrides,
    }
    mocks = _WorkerMocks(
        listmonk_client=AsyncMock(),
        session_factory=AsyncMock(),
    )
    worker = ConsentSyncWorker(
        settings=cast(Settings, SimpleNamespace(**settings)),
        session_factory=mocks.session_factory,
        listmonk_client=mocks.listmonk_client,
    )
    return worker, mocks


@pytest.mark.asyncio
async def test_process_pending_user_subscriber_not_found() -> None:
    worker, mocks = _worker()
    pending = SimpleNamespace(user_id=1, subscriber_id=101)
    listmonk_repo = AsyncMock()
    mocks.listmonk_client.get_subscriber_state.return_value = None

    result = await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
    )

    assert result is True
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=1,
        pending=True,
        confirmed=False,
        status=None,
    )


@pytest.mark.asyncio
async def test_process_pending_user_not_blocked_clears_pending_without_bonus() -> None:
    worker, mocks = _worker()
    pending = SimpleNamespace(user_id=2, subscriber_id=102)
    listmonk_repo = AsyncMock()
    mocks.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=102,
        status="enabled",
        list_ids=[1],
        list_statuses={1: "unconfirmed"},
    )

    result = await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
    )

    assert result is True
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=2,
        pending=False,
        confirmed=False,
        status="enabled",
    )


@pytest.mark.asyncio
async def test_process_pending_user_blocked() -> None:
    worker, mocks = _worker()
    pending = SimpleNamespace(user_id=12, subscriber_id=112)
    listmonk_repo = AsyncMock()
    mocks.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=112,
        status="blocked",
        list_ids=[1],
    )

    result = await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
    )

    assert result is True
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=12,
        pending=False,
        confirmed=False,
        status="blocked",
    )


@pytest.mark.asyncio
async def test_process_pending_user_blocked_in_target_list_only() -> None:
    worker, mocks = _worker()
    pending = SimpleNamespace(user_id=15, subscriber_id=115)
    listmonk_repo = AsyncMock()
    mocks.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=115,
        status="enabled",
        list_ids=[1, 2],
        list_statuses={1: "confirmed", 2: "blocklisted"},
    )

    result = await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1, 2],
        listmonk_repo=listmonk_repo,
    )

    assert result is True
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=15,
        pending=False,
        confirmed=False,
        status="blocked",
    )


def test_worker_has_no_teyca_client_or_bonus_dependency() -> None:
    """Consent bonus moved to external_dispatcher_worker (teyca-sync-4ue) —
    this worker must not depend on Teyca or bonus_accrual_log at all."""
    import app.workers.consent_sync_worker as module

    worker, _mocks = _worker()
    assert not hasattr(worker, "teyca_client")
    assert not hasattr(module, "BonusAccrualRepository")
    assert not hasattr(module, "TeycaClient")
    assert not hasattr(module, "build_teyca_client")


@pytest.mark.asyncio
async def test_process_pending_user_confirmed_status_clears_pending_without_bonus() -> None:
    worker, mocks = _worker()
    pending = SimpleNamespace(user_id=4, subscriber_id=104)
    listmonk_repo = AsyncMock()
    mocks.listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=104,
        status="confirmed",
        list_ids=[1],
    )

    result = await worker._process_pending_user(
        pending=pending,
        target_list_ids=[1],
        listmonk_repo=listmonk_repo,
    )

    assert result is True
    listmonk_repo.mark_checked.assert_awaited_once_with(
        user_id=4,
        pending=False,
        confirmed=False,
        status="confirmed",
    )


def test_parse_list_ids() -> None:
    assert parse_list_ids("1, 2, bad, ,3") == [1, 2, 3]


@pytest.mark.asyncio
async def test_run_once_uses_incremental_deltas_and_updates_watermark() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)

    listmonk_client = AsyncMock()
    worker = ConsentSyncWorker(
        settings=cast(
            Settings,
            SimpleNamespace(
                listmonk_list_ids="1",
                consent_sync_batch_size=500,
            ),
        ),
        session_factory=session_factory,
        listmonk_client=listmonk_client,
    )

    async def get_updated_subscribers(**_: object) -> list[SubscriberDelta]:
        assert session.commit.await_count == 1
        return [
            SubscriberDelta(
                subscriber_id=1001,
                status="confirmed",
                list_ids=[1],
                updated_at=datetime(2026, 3, 6, 6, 0, tzinfo=UTC),
            )
        ]

    listmonk_client.get_updated_subscribers.side_effect = get_updated_subscribers

    with (
        patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls,
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

        sync_repo = AsyncMock()
        sync_repo.get_or_create.return_value = SimpleNamespace(
            watermark_updated_at=None,
            watermark_subscriber_id=None,
        )
        sync_cls.return_value = sync_repo

        processed = await worker.run_once()

    assert processed == 1
    listmonk_client.get_updated_subscribers.assert_awaited_once()
    process_mock.assert_awaited_once()
    sync_repo.update_watermark.assert_awaited_once_with(
        source="listmonk_consent",
        list_id=1,
        updated_at=datetime(2026, 3, 6, 6, 0, tzinfo=UTC),
        subscriber_id=1001,
    )
    assert session.commit.await_count == 3


@pytest.mark.asyncio
async def test_run_once_skips_unmapped_subscribers_but_moves_watermark() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)

    listmonk_client = AsyncMock()
    worker = ConsentSyncWorker(
        settings=cast(
            Settings,
            SimpleNamespace(
                listmonk_list_ids="1",
                consent_sync_batch_size=500,
            ),
        ),
        session_factory=session_factory,
        listmonk_client=listmonk_client,
    )

    async def get_updated_subscribers(**_: object) -> list[SubscriberDelta]:
        assert session.commit.await_count == 1
        return [
            SubscriberDelta(
                subscriber_id=2002,
                status="blocked",
                list_ids=[1],
                updated_at=datetime(2026, 3, 6, 6, 10, tzinfo=UTC),
            )
        ]

    listmonk_client.get_updated_subscribers.side_effect = get_updated_subscribers

    with (
        patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls,
        patch("app.workers.consent_sync_worker.SyncStateRepository") as sync_cls,
        patch.object(
            ConsentSyncWorker, "_process_pending_user", new_callable=AsyncMock
        ) as process_mock,
    ):
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_subscriber_id.return_value = None
        repo_cls.return_value = listmonk_repo

        sync_repo = AsyncMock()
        sync_repo.get_or_create.return_value = SimpleNamespace(
            watermark_updated_at=None,
            watermark_subscriber_id=None,
        )
        sync_cls.return_value = sync_repo

        processed = await worker.run_once()

    assert processed == 0
    process_mock.assert_not_awaited()
    assert session.commit.await_count == 3
    sync_repo.update_watermark.assert_awaited_once_with(
        source="listmonk_consent",
        list_id=1,
        updated_at=datetime(2026, 3, 6, 6, 10, tzinfo=UTC),
        subscriber_id=2002,
    )


@pytest.mark.asyncio
async def test_run_once_returns_zero_without_target_lists() -> None:
    worker = ConsentSyncWorker(
        settings=cast(
            Settings,
            SimpleNamespace(
                listmonk_list_ids="",
                consent_sync_batch_size=500,
            ),
        ),
        session_factory=MagicMock(),
        listmonk_client=AsyncMock(),
    )
    assert await worker.run_once() == 0


@pytest.mark.asyncio
async def test_run_once_skips_empty_deltas() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)
    listmonk_client = AsyncMock()
    worker = ConsentSyncWorker(
        settings=cast(
            Settings,
            SimpleNamespace(
                listmonk_list_ids="1",
                consent_sync_batch_size=500,
            ),
        ),
        session_factory=session_factory,
        listmonk_client=listmonk_client,
    )
    listmonk_client.get_updated_subscribers.return_value = []

    with (
        patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls,
        patch("app.workers.consent_sync_worker.SyncStateRepository") as sync_cls,
    ):
        repo_cls.return_value = AsyncMock()
        sync_repo = AsyncMock()
        sync_repo.get_or_create.return_value = SimpleNamespace(
            watermark_updated_at=None,
            watermark_subscriber_id=None,
        )
        sync_cls.return_value = sync_repo
        assert await worker.run_once() == 0
        session.commit.assert_awaited_once()
        sync_repo.update_watermark.assert_not_awaited()


def test_build_consent_sync_worker_and_inc_helper() -> None:
    with (
        patch(
            "app.workers.consent_sync_worker.get_settings",
            return_value=SimpleNamespace(),
        ),
        patch("app.workers.consent_sync_worker.ListmonkSDKClient"),
    ):
        worker = build_consent_sync_worker()
    assert worker is not None
    assert not hasattr(worker, "teyca_client")

    metrics = ConsentSyncMetrics(batch_size=1)
    _inc(metrics, "blocked_done")
    _inc(None, "blocked_done")
    assert metrics.blocked_done == 1


@pytest.mark.asyncio
async def test_run_once_skips_duplicate_subscriber_mapping_and_moves_watermark() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)

    listmonk_client = AsyncMock()
    worker = ConsentSyncWorker(
        settings=cast(
            Settings,
            SimpleNamespace(
                listmonk_list_ids="1",
                consent_sync_batch_size=500,
            ),
        ),
        session_factory=session_factory,
        listmonk_client=listmonk_client,
    )

    async def get_updated_subscribers(**_: object) -> list[SubscriberDelta]:
        assert session.commit.await_count == 1
        return [
            SubscriberDelta(
                subscriber_id=3003,
                status="confirmed",
                list_ids=[1],
                updated_at=datetime(2026, 3, 6, 6, 20, tzinfo=UTC),
            )
        ]

    listmonk_client.get_updated_subscribers.side_effect = get_updated_subscribers

    with (
        patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls,
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

        sync_repo = AsyncMock()
        sync_repo.get_or_create.return_value = SimpleNamespace(
            watermark_updated_at=None,
            watermark_subscriber_id=None,
        )
        sync_cls.return_value = sync_repo

        processed = await worker.run_once()

    assert processed == 0
    process_mock.assert_not_awaited()
    assert session.commit.await_count == 2
    sync_repo.update_watermark.assert_awaited_once_with(
        source="listmonk_consent",
        list_id=1,
        updated_at=datetime(2026, 3, 6, 6, 20, tzinfo=UTC),
        subscriber_id=3003,
    )


@pytest.mark.asyncio
async def test_process_pending_user_uses_short_transactions() -> None:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    session_factory = MagicMock(return_value=context_manager)
    listmonk_client = AsyncMock()
    worker = ConsentSyncWorker(
        settings=cast(Settings, SimpleNamespace()),
        session_factory=session_factory,
        listmonk_client=listmonk_client,
    )
    listmonk_client.get_subscriber_state.return_value = SubscriberState(
        subscriber_id=404,
        status="confirmed",
        list_ids=[1],
    )

    with patch("app.workers.consent_sync_worker.ListmonkUsersRepository") as repo_cls:
        listmonk_repo = AsyncMock()
        listmonk_repo.get_by_user_id.return_value = SimpleNamespace(subscriber_id=404)
        repo_cls.return_value = listmonk_repo

        result = await worker._process_pending_user(
            pending=SimpleNamespace(user_id=4, subscriber_id=404),
            target_list_ids=[1],
        )

    assert result is True
    assert session.commit.await_count == 1
