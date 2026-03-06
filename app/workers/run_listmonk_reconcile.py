"""Run one iteration of listmonk mapping reconcile worker."""

import asyncio

import structlog

from app.workers.listmonk_reconcile_worker import build_listmonk_reconcile_worker

logger = structlog.get_logger()


async def _run() -> None:
    worker = build_listmonk_reconcile_worker()
    restored = await worker.run_once()
    logger.info("listmonk_reconcile_run_completed", restored=restored)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()

