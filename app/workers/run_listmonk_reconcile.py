"""Run one iteration of listmonk mapping reconcile worker."""

import asyncio

import httpx
import structlog

from app.clients.listmonk import ListmonkClientError
from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.service_health import write_heartbeat
from app.workers.listmonk_reconcile_worker import build_listmonk_reconcile_worker

logger = structlog.get_logger()


async def _safe_write_heartbeat(extra: dict[str, object]) -> None:
    try:
        await write_heartbeat("reconcile", extra=extra)
    except Exception as exc:
        logger.error(
            "listmonk_reconcile_heartbeat_write_failed",
            error=str(exc),
            error_type=type(exc).__name__,
            stage=extra.get("stage"),
        )


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
        await _safe_write_heartbeat({"stage": "started"})
        task = asyncio.create_task(worker.run_once())
        try:
            while not task.done():
                await _safe_write_heartbeat({"stage": "in_progress"})
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=30.0)
                except TimeoutError:
                    continue
            restored = await task
            await _safe_write_heartbeat({"stage": "completed", "restored": restored})
            logger.info("listmonk_reconcile_run_completed", restored=restored)
        except (ListmonkClientError, httpx.HTTPError) as exc:
            await _safe_write_heartbeat({"stage": "failed"})
            logger.error(
                "listmonk_reconcile_run_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
            raise
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
