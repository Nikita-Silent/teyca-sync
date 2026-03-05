"""Run one iteration of consent sync worker."""

import asyncio

import structlog

from app.workers.consent_sync_worker import build_consent_sync_worker

logger = structlog.get_logger()


async def _run() -> None:
    worker = build_consent_sync_worker()
    processed = await worker.run_once()
    logger.info("consent_sync_run_completed", processed=processed)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
