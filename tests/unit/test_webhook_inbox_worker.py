from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.teyca import TeycaAPIError
from app.config import Settings
from app.repositories.old_db import OldDBRepository
from app.repositories.users import UserLockNotAcquiredError
from app.repositories.webhook_inbox import InboxClaim
from app.workers.webhook_inbox_worker import InboxWorkerMetrics, WebhookInboxWorker


def _settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "webhook_inbox_batch_size": 10,
        "webhook_inbox_stale_claim_seconds": 300.0,
        "webhook_inbox_retry_base_delay_ms": 1_000,
        "webhook_inbox_retry_max_delay_ms": 60_000,
        "webhook_inbox_max_retries": 5,
        "webhook_inbox_lock_busy_retry_base_delay_ms": 1_000,
        "webhook_inbox_lock_busy_retry_max_delay_ms": 30_000,
        "webhook_inbox_lock_busy_retry_max_retries": 3,
        "webhook_inbox_teyca_rate_limit_retry_base_delay_ms": 60_000,
        "webhook_inbox_teyca_rate_limit_retry_max_delay_ms": 900_000,
        "webhook_inbox_teyca_rate_limit_retry_max_retries": 2,
    }
    defaults.update(overrides)
    return cast(Settings, SimpleNamespace(**defaults))


def _session_factory() -> async_sessionmaker[AsyncSession]:
    session = AsyncMock()
    cm = AsyncMock()
    cm.__aenter__.return_value = session
    cm.__aexit__.return_value = False

    def factory() -> AsyncMock:
        return cm

    return cast("async_sessionmaker[AsyncSession]", factory)


def _worker(*, settings: Settings | None = None) -> WebhookInboxWorker:
    return WebhookInboxWorker(
        settings=settings if settings is not None else _settings(),
        session_factory=_session_factory(),
        old_db_repo=cast(OldDBRepository, AsyncMock()),
        worker_id="webhook-inbox:worker-1",
    )


def _claim(*, attempts: int = 0, event_type: str = "CREATE") -> InboxClaim:
    return InboxClaim(
        id=1,
        source_event_id="evt-1",
        event_type=event_type,
        payload={"type": event_type, "pass": {"user_id": 42}},
        attempts=attempts,
        trace_id="trace-1",
    )


@pytest.mark.asyncio
async def test_run_once_returns_zero_when_no_claims() -> None:
    worker = _worker()
    repo = AsyncMock()
    repo.release_stale_processing_claims.return_value = 0
    repo.claim_batch.return_value = []
    with patch("app.workers.webhook_inbox_worker.WebhookInboxRepository", return_value=repo):
        processed = await worker.run_once()
    assert processed == 0


@pytest.mark.asyncio
async def test_process_claim_dispatches_and_marks_done() -> None:
    worker = _worker()
    metrics = InboxWorkerMetrics(batch_size=1)
    with (
        patch.object(WebhookInboxWorker, "_dispatch", new=AsyncMock()) as dispatch_mock,
        patch.object(WebhookInboxWorker, "_mark_done", new=AsyncMock()) as mark_done_mock,
    ):
        await worker._process_claim(claim=_claim(), metrics=metrics)
    dispatch_mock.assert_awaited_once()
    mark_done_mock.assert_awaited_once_with(inbox_id=1)
    assert metrics.done == 1
    assert metrics.retried == 0
    assert metrics.dead == 0


@pytest.mark.asyncio
async def test_process_claim_passes_wait_for_lock_when_retried() -> None:
    worker = _worker()
    metrics = InboxWorkerMetrics(batch_size=1)
    with (
        patch.object(WebhookInboxWorker, "_dispatch", new=AsyncMock()) as dispatch_mock,
        patch.object(WebhookInboxWorker, "_mark_done", new=AsyncMock()),
    ):
        await worker._process_claim(claim=_claim(attempts=2), metrics=metrics)
    assert dispatch_mock.call_args.kwargs["wait_for_lock"] is True


@pytest.mark.asyncio
async def test_process_claim_retries_on_user_lock_busy() -> None:
    worker = _worker(settings=_settings(webhook_inbox_lock_busy_retry_max_retries=3))
    metrics = InboxWorkerMetrics(batch_size=1)
    with (
        patch.object(
            WebhookInboxWorker,
            "_dispatch",
            new=AsyncMock(side_effect=UserLockNotAcquiredError(user_id=42)),
        ),
        patch.object(
            WebhookInboxWorker, "_mark_retry", new=AsyncMock(return_value="failed")
        ) as mark_retry,
    ):
        await worker._process_claim(claim=_claim(), metrics=metrics)
    mark_retry.assert_awaited_once()
    kwargs = mark_retry.call_args.kwargs
    assert kwargs["max_attempts"] == 3
    assert kwargs["base_delay_ms"] == 1_000
    assert "user_id=42" in kwargs["error_text"]
    assert metrics.retried == 1
    assert metrics.dead == 0


@pytest.mark.asyncio
async def test_process_claim_retries_on_teyca_rate_limit() -> None:
    worker = _worker()
    metrics = InboxWorkerMetrics(batch_size=1)
    error = TeycaAPIError("rate limited", status_code=429)
    with (
        patch.object(WebhookInboxWorker, "_dispatch", new=AsyncMock(side_effect=error)),
        patch.object(
            WebhookInboxWorker, "_mark_retry", new=AsyncMock(return_value="failed")
        ) as mark_retry,
    ):
        await worker._process_claim(claim=_claim(), metrics=metrics)
    kwargs = mark_retry.call_args.kwargs
    assert kwargs["max_attempts"] == 2
    assert kwargs["base_delay_ms"] == 60_000
    assert metrics.retried == 1


@pytest.mark.asyncio
async def test_process_claim_retries_on_non_rate_limit_teyca_error() -> None:
    worker = _worker()
    metrics = InboxWorkerMetrics(batch_size=1)
    error = TeycaAPIError("server error", status_code=500)
    with (
        patch.object(WebhookInboxWorker, "_dispatch", new=AsyncMock(side_effect=error)),
        patch.object(
            WebhookInboxWorker, "_mark_retry", new=AsyncMock(return_value="failed")
        ) as mark_retry,
    ):
        await worker._process_claim(claim=_claim(), metrics=metrics)
    kwargs = mark_retry.call_args.kwargs
    assert kwargs["max_attempts"] == 5
    assert kwargs["base_delay_ms"] == 1_000
    assert metrics.retried == 1


@pytest.mark.asyncio
async def test_process_claim_marks_dead_after_max_attempts() -> None:
    worker = _worker()
    metrics = InboxWorkerMetrics(batch_size=1)
    with (
        patch.object(
            WebhookInboxWorker, "_dispatch", new=AsyncMock(side_effect=RuntimeError("boom"))
        ),
        patch.object(
            WebhookInboxWorker, "_mark_retry", new=AsyncMock(return_value="dead")
        ) as mark_retry,
    ):
        await worker._process_claim(claim=_claim(attempts=5), metrics=metrics)
    mark_retry.assert_awaited_once()
    assert metrics.dead == 1
    assert metrics.retried == 0


@pytest.mark.asyncio
async def test_dispatch_routes_by_event_type() -> None:
    worker = _worker()
    with (
        patch("app.workers.webhook_inbox_worker.handle_create", new=AsyncMock()) as handle_create,
        patch("app.workers.webhook_inbox_worker.handle_update", new=AsyncMock()) as handle_update,
        patch("app.workers.webhook_inbox_worker.handle_delete", new=AsyncMock()) as handle_delete,
    ):
        await worker._dispatch(claim=_claim(event_type="CREATE"), wait_for_lock=False)
        await worker._dispatch(claim=_claim(event_type="UPDATE"), wait_for_lock=False)
        await worker._dispatch(claim=_claim(event_type="DELETE"), wait_for_lock=False)
    handle_create.assert_awaited_once()
    handle_update.assert_awaited_once()
    handle_delete.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_raises_on_unsupported_event_type() -> None:
    worker = _worker()
    with pytest.raises(ValueError, match="Unsupported event type"):
        await worker._dispatch(claim=_claim(event_type="OTHER"), wait_for_lock=False)


@pytest.mark.asyncio
async def test_run_once_releases_stale_claims_first() -> None:
    worker = _worker()
    repo = AsyncMock()
    repo.release_stale_processing_claims.return_value = 3
    repo.claim_batch.return_value = []
    with (
        patch("app.workers.webhook_inbox_worker.WebhookInboxRepository", return_value=repo),
        patch("app.workers.webhook_inbox_worker.logger") as logger,
    ):
        await worker.run_once()
    repo.release_stale_processing_claims.assert_awaited_once()
    logger.warning.assert_called_once_with("webhook_inbox_stale_claims_released", count=3)
