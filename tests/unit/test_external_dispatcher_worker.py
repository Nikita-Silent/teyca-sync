from __future__ import annotations

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import pytest

from app.clients.teyca import TeycaAPIError
from app.config import Settings
from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_MERGE_FINALIZE,
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
    OutboxClaim,
)
from app.workers import run_external_dispatcher
from app.workers.external_dispatcher_worker import (
    ExternalDispatcherMetrics,
    ExternalDispatcherWorker,
)


def _settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "external_dispatcher_batch_size": 10,
        "external_dispatcher_retry_base_delay_ms": 1_000,
        "external_dispatcher_retry_max_delay_ms": 60_000,
        "external_dispatcher_max_retries": 5,
    }
    defaults.update(overrides)
    return cast(Settings, SimpleNamespace(**defaults))


def _worker() -> ExternalDispatcherWorker:
    return ExternalDispatcherWorker(
        settings=_settings(),
        session_factory=cast(object, AsyncMock()),
        listmonk_client=cast(object, AsyncMock()),
        teyca_client=cast(object, AsyncMock()),
        worker_id="worker-1",
    )


@pytest.mark.asyncio
async def test_external_dispatcher_run_once_no_pending_jobs() -> None:
    worker = _worker()
    with (
        patch.object(ExternalDispatcherWorker, "_claim_batch", new=AsyncMock(return_value=[])),
        patch("app.workers.external_dispatcher_worker.logger") as logger,
    ):
        processed = await worker.run_once()

    assert processed == 0
    logger.info.assert_called_once_with("external_dispatcher_no_pending_jobs", batch_size=10)


@pytest.mark.asyncio
async def test_external_dispatcher_skips_listmonk_upsert_when_user_missing() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=1,
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key="listmonk-sync:10",
        user_id=10,
        payload={"email": "user@example.com", "list_ids": [1]},
        attempts=0,
        trace_id="trace-1",
        source_event_id="event-1",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=False)),
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_listmonk_upsert(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.listmonk_client.upsert_subscriber).assert_not_awaited()
    mark_done.assert_awaited_once_with(outbox_id=1, payload=claim.payload)
    assert metrics.skipped == 1


@pytest.mark.asyncio
async def test_external_dispatcher_invalid_email_block_success() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=2,
        operation=OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
        dedupe_key="invalid-email-block:20",
        user_id=20,
        payload={"status": "blocked"},
        attempts=1,
        trace_id="trace-2",
        source_event_id="event-2",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_invalid_email_block_success",
            new=AsyncMock(),
        ) as apply_ok,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_invalid_email_block(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=20,
        fields={"key1": "blocked"},
    )
    apply_ok.assert_awaited_once_with(user_id=20, status="blocked")
    mark_done.assert_awaited_once_with(outbox_id=2)
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_merge_finalize_tracks_step_progress() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=3,
        operation=OUTBOX_OP_MERGE_FINALIZE,
        dedupe_key="merge-finalize:30",
        user_id=30,
        payload={
            "bonus_done": False,
            "key2_done": False,
            "merge_logged": False,
            "old_bonus_value": 40.0,
            "merge_key2_value": "merge 30.03.2026 12:00",
            "source_event_type": "UPDATE",
        },
        attempts=0,
        trace_id="trace-3",
        source_event_id="event-3",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(
            ExternalDispatcherWorker,
            "_merge_already_logged",
            new=AsyncMock(return_value=False),
        ),
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(ExternalDispatcherWorker, "_save_progress", new=AsyncMock()) as save_progress,
        patch.object(
            ExternalDispatcherWorker, "_write_merge_log", new=AsyncMock()
        ) as write_merge_log,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_merge_finalize(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.accrue_bonuses).assert_awaited_once()
    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=30,
        fields={"key2": "merge 30.03.2026 12:00"},
    )
    assert save_progress.await_count == 2
    write_merge_log.assert_awaited_once_with(
        user_id=30,
        source_event_type="UPDATE",
        source_event_id="event-3",
        trace_id="trace-3",
    )
    done_payload = mark_done.await_args.kwargs["payload"]
    assert done_payload["bonus_done"] is True
    assert done_payload["key2_done"] is True
    assert done_payload["merge_logged"] is True
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_process_claim_schedules_retry_on_error() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=4,
        operation=OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
        dedupe_key="invalid-email-block:40",
        user_id=40,
        payload={"status": "blocked"},
        attempts=2,
        trace_id="trace-4",
        source_event_id="event-4",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(
            ExternalDispatcherWorker,
            "_process_invalid_email_block",
            new=AsyncMock(side_effect=TeycaAPIError("boom", status_code=429)),
        ),
        patch.object(
            ExternalDispatcherWorker, "_mark_retry", new=AsyncMock(return_value="failed")
        ) as mark_retry,
    ):
        await worker._process_claim(claim=claim, metrics=metrics)

    mark_retry.assert_awaited_once_with(outbox_id=4, attempts=3, error_text="boom")
    assert metrics.retried == 1


@pytest.mark.asyncio
async def test_run_external_dispatcher_single_iteration_logs_completion() -> None:
    with (
        patch(
            "app.workers.run_external_dispatcher.get_settings",
            return_value=SimpleNamespace(loki_url=None, log_component="external-dispatcher"),
        ),
        patch("app.workers.run_external_dispatcher.configure_logging"),
        patch("app.workers.run_external_dispatcher.shutdown_logging"),
        patch("app.workers.run_external_dispatcher.build_external_dispatcher_worker") as builder,
        patch("app.workers.run_external_dispatcher.logger") as logger,
        patch("app.workers.run_external_dispatcher.write_heartbeat", new=AsyncMock()) as heartbeat,
    ):
        worker = AsyncMock()
        worker.run_once.return_value = 3
        builder.return_value = worker
        await run_external_dispatcher._run()

    logger.info.assert_called_once_with("external_dispatcher_run_completed", processed=3)
    assert heartbeat.await_count == 3
