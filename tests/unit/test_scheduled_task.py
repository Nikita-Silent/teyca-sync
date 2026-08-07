from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from app.workers.scheduled_task import ScheduledTask, run_scheduled_task


@pytest.mark.asyncio
async def test_busy_loops_while_work_is_processed() -> None:
    """No sleep between iterations as long as run_once() reports processed>0;
    the loop stops once shutdown_event is set."""
    shutdown_event = asyncio.Event()
    call_count = 0

    async def run_once() -> int:
        nonlocal call_count
        call_count += 1
        if call_count >= 3:
            shutdown_event.set()
        return 1

    task = ScheduledTask(name="test-task", run_once=run_once, interval_seconds=999.0)
    with patch("app.workers.scheduled_task.write_heartbeat", new=AsyncMock()):
        await run_scheduled_task(task, shutdown_event=shutdown_event)

    assert call_count == 3


@pytest.mark.asyncio
async def test_sleeps_interval_when_nothing_processed() -> None:
    """When run_once() reports nothing processed, the loop sleeps up to
    interval_seconds (bounded by shutdown_event) before the next iteration."""
    shutdown_event = asyncio.Event()
    call_count = 0

    async def run_once() -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            shutdown_event.set()
        return 0

    task = ScheduledTask(name="test-task", run_once=run_once, interval_seconds=0.01)
    with patch("app.workers.scheduled_task.write_heartbeat", new=AsyncMock()):
        await run_scheduled_task(task, shutdown_event=shutdown_event)

    assert call_count == 2


@pytest.mark.asyncio
async def test_exception_is_caught_and_loop_continues() -> None:
    shutdown_event = asyncio.Event()
    call_count = 0

    async def run_once() -> int:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("boom")
        shutdown_event.set()
        return 1

    task = ScheduledTask(name="test-task", run_once=run_once, interval_seconds=0.01)
    with (
        patch("app.workers.scheduled_task.write_heartbeat", new=AsyncMock()) as heartbeat_mock,
        patch("app.workers.scheduled_task.logger") as logger,
    ):
        await run_scheduled_task(task, shutdown_event=shutdown_event)

    assert call_count == 2
    logger.exception.assert_called_once()
    assert heartbeat_mock.await_count >= 3  # started, failed, started(again)/completed


@pytest.mark.asyncio
async def test_heartbeat_write_failure_is_logged_and_swallowed() -> None:
    shutdown_event = asyncio.Event()

    async def run_once() -> int:
        shutdown_event.set()
        return 1

    task = ScheduledTask(name="test-task", run_once=run_once, interval_seconds=999.0)
    with (
        patch(
            "app.workers.scheduled_task.write_heartbeat",
            new=AsyncMock(side_effect=RuntimeError("disk full")),
        ),
        patch("app.workers.scheduled_task.logger") as logger,
    ):
        await run_scheduled_task(task, shutdown_event=shutdown_event)

    logger.warning.assert_called()
