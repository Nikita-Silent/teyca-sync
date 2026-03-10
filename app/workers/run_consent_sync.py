"""Run one iteration of consent sync worker."""

import asyncio

import structlog

from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.workers.consent_sync_worker import build_consent_sync_worker

logger = structlog.get_logger()


async def _run() -> None:
    """
    Run a single iteration of the consent-sync worker and manage the logging lifecycle.
    
    Retrieves runtime settings to configure structured logging (including optional Loki credentials), constructs a consent-sync worker, executes one run iteration, logs the processed count under the event name "consent_sync_run_completed", and ensures logging is shut down regardless of success or failure.
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
        processed = await worker.run_once()
        logger.info("consent_sync_run_completed", processed=processed)
    finally:
        shutdown_logging()


def main() -> None:
    """
    Execute a single iteration of the consent-sync worker.
    
    Blocks the current process until the worker iteration finishes.
    """
    asyncio.run(_run())


if __name__ == "__main__":
    main()
