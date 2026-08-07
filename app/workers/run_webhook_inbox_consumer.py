"""Run the webhook inbox consumer: poll webhook_inbox, dispatch, repeat (teyca-sync-8ib)."""

from __future__ import annotations

import asyncio
import signal

import structlog

from app.config import get_settings
from app.db.session import wait_for_database
from app.logging_config import configure_logging, shutdown_logging
from app.service_health import write_heartbeat
from app.workers.webhook_inbox_worker import build_webhook_inbox_worker

logger = structlog.get_logger()


async def _run() -> None:
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component=getattr(settings, "log_component", "consumers"),
    )
    await wait_for_database()
    worker = build_webhook_inbox_worker()

    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)

    heartbeat_task = asyncio.create_task(_heartbeat_loop("consumers", interval_seconds=15))
    poll_interval = max(0.1, settings.webhook_inbox_poll_interval_seconds)
    try:
        logger.info("webhook_inbox_consumer_started", poll_interval=poll_interval)
        while not shutdown_event.is_set():
            try:
                processed = await worker.run_once()
            except Exception as exc:
                logger.exception(
                    "webhook_inbox_run_once_failed",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                processed = 0
            if processed == 0:
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=poll_interval)
                except TimeoutError:
                    pass
        logger.info("webhook_inbox_consumer_shutdown_signal_received")
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        await worker.old_db_repo.close()
        shutdown_logging()


async def _heartbeat_loop(service_name: str, *, interval_seconds: int) -> None:
    while True:
        try:
            await write_heartbeat(service_name)
        except Exception as exc:
            logger.error(
                "service_heartbeat_write_failed",
                service_name=service_name,
                error=str(exc),
                error_type=type(exc).__name__,
            )
        await asyncio.sleep(interval_seconds)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
