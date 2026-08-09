"""Run one iteration of external side effects dispatcher."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import httpx
import structlog

from app.clients.listmonk import ListmonkClientError, httpx2_network_exceptions
from app.clients.teyca import TeycaAPIError
from app.config import get_settings
from app.db.session import wait_for_database
from app.logging_config import configure_logging, shutdown_logging
from app.service_health import write_heartbeat
from app.workers.external_dispatcher_worker import build_external_dispatcher_worker

logger = structlog.get_logger()


async def _safe_write_heartbeat(service_name: str, extra: dict[str, object]) -> None:
    try:
        await write_heartbeat(service_name, extra=extra)
    except Exception as exc:
        logger.warning(
            "external_dispatcher_heartbeat_write_failed",
            error=str(exc),
            error_type=type(exc).__name__,
            service_name=service_name,
            stage=extra.get("stage"),
        )


async def _run(
    *,
    service_name: str = "external-dispatcher",
    operations: Sequence[str] | None = None,
) -> None:
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component=getattr(settings, "log_component", service_name),
    )
    await wait_for_database()
    worker = build_external_dispatcher_worker(
        operations=operations,
        worker_id_prefix=service_name,
    )
    try:
        await _safe_write_heartbeat(service_name, {"stage": "started"})
        task = asyncio.create_task(worker.run_once())
        try:
            while not task.done():
                await _safe_write_heartbeat(service_name, {"stage": "in_progress"})
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=30.0)
                except TimeoutError:
                    continue
            processed = await task
            await _safe_write_heartbeat(
                service_name,
                {"stage": "completed", "processed": processed},
            )
            logger.info(
                "external_dispatcher_run_completed",
                processed=processed,
                service_name=service_name,
            )
        except (
            ListmonkClientError,
            TeycaAPIError,
            httpx.HTTPError,
            *httpx2_network_exceptions(),
        ) as exc:
            await _safe_write_heartbeat(service_name, {"stage": "failed"})
            logger.error(
                "external_dispatcher_run_failed",
                service_name=service_name,
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


def run_main(*, service_name: str, operations: Sequence[str] | None = None) -> None:
    asyncio.run(_run(service_name=service_name, operations=operations))


def main() -> None:
    run_main(service_name="external-dispatcher")


if __name__ == "__main__":
    main()
