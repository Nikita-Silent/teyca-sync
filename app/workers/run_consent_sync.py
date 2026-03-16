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
        task = asyncio.create_task(worker.run_once())
        try:
            while not task.done():
                await write_heartbeat("consent-sync", extra={"stage": "in_progress"})
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=30.0)
                except asyncio.TimeoutError:
                    continue
            processed = await task
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
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
    finally:
        shutdown_logging()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
