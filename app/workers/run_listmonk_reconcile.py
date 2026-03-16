"""Run one iteration of listmonk mapping reconcile worker."""

import asyncio

import httpx
import structlog

from app.config import get_settings
from app.clients.listmonk import ListmonkClientError
from app.logging_config import configure_logging, shutdown_logging
from app.service_health import write_heartbeat
from app.workers.listmonk_reconcile_worker import build_listmonk_reconcile_worker

logger = structlog.get_logger()


async def _run() -> None:
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        component=getattr(settings, "log_component", "reconcile"),
    )
    worker = build_listmonk_reconcile_worker()
    try:
        await write_heartbeat("reconcile", extra={"stage": "started"})
        try:
            restored = await worker.run_once()
            await write_heartbeat(
                "reconcile",
                extra={"stage": "completed", "restored": restored},
            )
            logger.info("listmonk_reconcile_run_completed", restored=restored)
        except (ListmonkClientError, httpx.HTTPError) as exc:
            await write_heartbeat("reconcile", extra={"stage": "failed"})
            logger.error(
                "listmonk_reconcile_run_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
    finally:
        shutdown_logging()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
