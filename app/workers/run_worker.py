"""Single long-running worker process replacing the six shell `while true;
sleep N; done` loops (teyca-sync-2g7): one asyncio task per job, scheduled by
`run_scheduled_task`, sharing one process/event loop and one graceful
shutdown instead of eight independent containers.
"""

from __future__ import annotations

import asyncio
import signal
from typing import Protocol

import structlog

from app.config import get_settings
from app.db.session import wait_for_database
from app.logging_config import configure_logging, shutdown_logging
from app.workers.consent_sync_worker import build_consent_sync_worker
from app.workers.external_dispatcher_worker import (
    CONSENT_BLOCK_OUTBOX_OPERATIONS,
    EMAIL_REPAIR_SYNC_OUTBOX_OPERATIONS,
    INVALID_EMAIL_OUTBOX_OPERATIONS,
    LISTMONK_OUTBOX_OPERATIONS,
    MERGE_OUTBOX_OPERATIONS,
    build_external_dispatcher_worker,
)
from app.workers.listmonk_reconcile_worker import build_listmonk_reconcile_worker
from app.workers.scheduled_task import ScheduledTask, run_scheduled_task
from app.workers.webhook_inbox_worker import build_webhook_inbox_worker

logger = structlog.get_logger()


class _Closeable(Protocol):
    async def close(self) -> None: ...


def _build_tasks() -> tuple[list[ScheduledTask], list[_Closeable]]:
    """Build every scheduled task plus the underlying worker objects that
    need closing on shutdown (webhook inbox's OldDBRepository, each
    dispatcher's TeycaClient)."""
    settings = get_settings()

    webhook_inbox = build_webhook_inbox_worker()
    dispatchers = [
        build_external_dispatcher_worker(
            operations=LISTMONK_OUTBOX_OPERATIONS,
            worker_id_prefix="external-dispatcher-listmonk",
        ),
        build_external_dispatcher_worker(
            operations=MERGE_OUTBOX_OPERATIONS,
            worker_id_prefix="external-dispatcher-merge",
        ),
        build_external_dispatcher_worker(
            operations=INVALID_EMAIL_OUTBOX_OPERATIONS,
            worker_id_prefix="external-dispatcher-invalid-email",
        ),
        build_external_dispatcher_worker(
            operations=CONSENT_BLOCK_OUTBOX_OPERATIONS,
            worker_id_prefix="external-dispatcher-consent-block",
        ),
        build_external_dispatcher_worker(
            operations=EMAIL_REPAIR_SYNC_OUTBOX_OPERATIONS,
            worker_id_prefix="external-dispatcher-email-repair-sync",
        ),
    ]
    consent_sync = build_consent_sync_worker()
    reconcile = build_listmonk_reconcile_worker()

    dispatcher_names = [
        "external-dispatcher-listmonk",
        "external-dispatcher-merge",
        "external-dispatcher-invalid-email",
        "external-dispatcher-consent-block",
        "external-dispatcher-email-repair-sync",
    ]
    tasks = [
        ScheduledTask(
            name="consumers",
            run_once=webhook_inbox.run_once,
            interval_seconds=settings.webhook_inbox_poll_interval_seconds,
        ),
        *(
            ScheduledTask(
                name=name,
                run_once=dispatcher.run_once,
                interval_seconds=settings.external_dispatcher_poll_interval_seconds,
            )
            for name, dispatcher in zip(dispatcher_names, dispatchers, strict=True)
        ),
        ScheduledTask(
            name="consent-sync",
            run_once=consent_sync.run_once,
            interval_seconds=settings.consent_sync_interval_seconds,
        ),
        ScheduledTask(
            name="reconcile",
            run_once=reconcile.run_once,
            interval_seconds=settings.listmonk_reconcile_interval_seconds,
        ),
    ]
    closeable = [webhook_inbox.old_db_repo, *(d.teyca_client for d in dispatchers)]
    return tasks, closeable


async def _run() -> None:
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component=getattr(settings, "log_component", "worker"),
    )
    await wait_for_database()
    tasks, closeable = _build_tasks()

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    try:
        logger.info("worker_started", tasks=[t.name for t in tasks])
        run = asyncio.gather(
            *(run_scheduled_task(t, shutdown_event=shutdown_event) for t in tasks),
            return_exceptions=True,
        )
        await shutdown_event.wait()
        logger.info("worker_shutdown_signal_received")
        drain_timeout = max(0.0, settings.worker_shutdown_drain_timeout_seconds)
        try:
            await asyncio.wait_for(run, timeout=drain_timeout)
        except TimeoutError:
            logger.warning("worker_shutdown_drain_timeout", drain_timeout_seconds=drain_timeout)
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
        for resource in closeable:
            try:
                await resource.close()
            except Exception as exc:
                logger.warning(
                    "worker_resource_close_failed",
                    resource=type(resource).__name__,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
        shutdown_logging()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
