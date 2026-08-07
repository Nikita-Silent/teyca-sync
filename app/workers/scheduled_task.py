"""Generic periodic-task runner shared by every worker inside `run_worker.py`
(teyca-sync-2g7). Generalizes the busy-loop-while-work-pending / sleep-when-idle
loop already proven in the webhook inbox consumer (teyca-sync-8ib) to every
`run_once() -> int` worker, so a single container can run all of them instead
of one shell `while true; sleep N; done` loop per process.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import structlog

from app.service_health import write_heartbeat

logger = structlog.get_logger()


@dataclass(slots=True)
class ScheduledTask:
    """One periodic job. `name` doubles as the heartbeat/log identity and
    matches the pre-merge compose service_name so existing heartbeat
    thresholds (`WORKER_HEARTBEAT_CHECKS`) keep meaning the same thing."""

    name: str
    run_once: Callable[[], Awaitable[int]]
    interval_seconds: float


async def _safe_write_heartbeat(name: str, extra: dict[str, object]) -> None:
    try:
        await write_heartbeat(name, extra=extra)
    except Exception as exc:
        logger.warning(
            "scheduled_task_heartbeat_write_failed",
            task=name,
            error=str(exc),
            error_type=type(exc).__name__,
            stage=extra.get("stage"),
        )


async def run_scheduled_task(task: ScheduledTask, *, shutdown_event: asyncio.Event) -> None:
    """Run `task.run_once()` in a loop until `shutdown_event` is set.

    Busy-loops (no sleep) while a run reports work processed, so a backlog
    drains as fast as the DB allows; sleeps `interval_seconds` only once a
    run reports nothing to do. Exceptions are caught and logged rather than
    propagated — one task failing must not kill the other tasks sharing this
    process (teyca-sync-2g7; previously each task was its own OS process, so
    a crash only cost that one container until its shell loop restarted it).
    """
    while not shutdown_event.is_set():
        await _safe_write_heartbeat(task.name, {"stage": "started"})
        try:
            processed = await task.run_once()
            await _safe_write_heartbeat(task.name, {"stage": "completed", "processed": processed})
        except Exception as exc:
            processed = 0
            await _safe_write_heartbeat(task.name, {"stage": "failed"})
            logger.exception(
                "scheduled_task_run_failed",
                task=task.name,
                error=str(exc),
                error_type=type(exc).__name__,
            )
        if processed == 0:
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=task.interval_seconds)
            except TimeoutError:
                pass
