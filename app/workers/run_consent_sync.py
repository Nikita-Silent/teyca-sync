"""Run one iteration of consent sync worker."""

import asyncio

import httpx
import structlog

from app.clients.listmonk import ListmonkClientError
from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.service_health import write_heartbeat
from app.workers.consent_sync_worker import build_consent_sync_worker

logger = structlog.get_logger()


async def _run() -> None:
    """
    Run a single iteration of the consent-sync worker and emit lifecycle heartbeats.
    
    Configures logging, emits a "started" heartbeat before running the worker, then runs the worker's single iteration. On success emits a "completed" heartbeat including the processed count and logs an info message; if a ListmonkClientError or httpx.HTTPError occurs, emits a "failed" heartbeat and logs an error with the exception details. Ensures logging is shut down before returning.
    """
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        component=getattr(settings, "log_component", "consent-sync"),
    )
    worker = build_consent_sync_worker()
    try:
        await write_heartbeat("consent-sync", extra={"stage": "started"})
        try:
            processed = await worker.run_once()
            await write_heartbeat(
                "consent-sync",
                extra={"stage": "completed", "processed": processed},
            )
            logger.info("consent_sync_run_completed", processed=processed)
        except (ListmonkClientError, httpx.HTTPError) as exc:
            await write_heartbeat("consent-sync", extra={"stage": "failed"})
            logger.error(
                "consent_sync_run_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
    finally:
        shutdown_logging()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
