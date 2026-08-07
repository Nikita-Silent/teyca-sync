from __future__ import annotations

import asyncio
import os
import signal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.workers import run_worker
from app.workers.scheduled_task import ScheduledTask


def _settings(**overrides: object) -> SimpleNamespace:
    defaults: dict[str, object] = {
        "webhook_inbox_poll_interval_seconds": 1.0,
        "external_dispatcher_poll_interval_seconds": 5.0,
        "consent_sync_interval_seconds": 3600.0,
        "listmonk_reconcile_interval_seconds": 300.0,
        "worker_shutdown_drain_timeout_seconds": 5.0,
        "loki_url": None,
        "log_component": "worker",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _worker_stub(**extra: object) -> MagicMock:
    stub = MagicMock()
    stub.run_once = AsyncMock(return_value=0)
    for key, value in extra.items():
        setattr(stub, key, value)
    return stub


@pytest.mark.asyncio
async def test_build_tasks_names_and_intervals() -> None:
    settings = _settings()
    webhook_inbox = _worker_stub(old_db_repo=AsyncMock())
    dispatcher_stubs = [_worker_stub(teyca_client=AsyncMock()) for _ in range(5)]
    consent_sync = _worker_stub()
    reconcile = _worker_stub()

    with (
        patch("app.workers.run_worker.get_settings", return_value=settings),
        patch("app.workers.run_worker.build_webhook_inbox_worker", return_value=webhook_inbox),
        patch(
            "app.workers.run_worker.build_external_dispatcher_worker",
            side_effect=dispatcher_stubs,
        ),
        patch("app.workers.run_worker.build_consent_sync_worker", return_value=consent_sync),
        patch("app.workers.run_worker.build_listmonk_reconcile_worker", return_value=reconcile),
    ):
        tasks, closeable = run_worker._build_tasks()

    assert [t.name for t in tasks] == [
        "consumers",
        "external-dispatcher-listmonk",
        "external-dispatcher-merge",
        "external-dispatcher-invalid-email",
        "external-dispatcher-consent-block",
        "external-dispatcher-email-repair-sync",
        "consent-sync",
        "reconcile",
    ]
    intervals = {t.name: t.interval_seconds for t in tasks}
    assert intervals["consumers"] == 1.0
    assert intervals["external-dispatcher-listmonk"] == 5.0
    assert intervals["external-dispatcher-merge"] == 5.0
    assert intervals["consent-sync"] == 3600.0
    assert intervals["reconcile"] == 300.0
    assert all(isinstance(t, ScheduledTask) for t in tasks)
    # webhook inbox's OldDBRepository + one TeycaClient per dispatcher
    assert closeable == [webhook_inbox.old_db_repo, *(d.teyca_client for d in dispatcher_stubs)]


@pytest.mark.asyncio
async def test_run_drains_current_iteration_and_closes_resources_on_sigterm() -> None:
    settings = _settings(worker_shutdown_drain_timeout_seconds=5.0)
    old_db_repo = AsyncMock()
    teyca_client = AsyncMock()

    shutdown_requested = asyncio.Event()
    release_task = asyncio.Event()

    async def slow_run_once() -> int:
        shutdown_requested.set()
        await release_task.wait()
        return 0

    slow_task = ScheduledTask(name="slow", run_once=slow_run_once, interval_seconds=999.0)

    def fake_build_tasks() -> tuple[list[ScheduledTask], list[object]]:
        return [slow_task], [old_db_repo, teyca_client]

    with (
        patch("app.workers.run_worker.get_settings", return_value=settings),
        patch("app.workers.run_worker.configure_logging"),
        patch("app.workers.run_worker.shutdown_logging"),
        patch("app.workers.run_worker.wait_for_database", new=AsyncMock()),
        patch("app.workers.run_worker._build_tasks", side_effect=fake_build_tasks),
    ):
        run_task = asyncio.create_task(run_worker._run())

        await asyncio.wait_for(shutdown_requested.wait(), timeout=5)
        os.kill(os.getpid(), signal.SIGTERM)
        # Give the signal handler a beat to set shutdown_event before the
        # in-flight iteration is allowed to complete — this is what proves
        # the SIGTERM does not abort the current run_once() mid-flight.
        await asyncio.sleep(0.05)
        assert not run_task.done()
        release_task.set()

        await asyncio.wait_for(run_task, timeout=5)

    old_db_repo.close.assert_awaited_once()
    teyca_client.close.assert_awaited_once()
