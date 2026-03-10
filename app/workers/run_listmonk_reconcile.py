"""Run one iteration of listmonk mapping reconcile worker."""

import asyncio

import structlog

from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.workers.listmonk_reconcile_worker import build_listmonk_reconcile_worker

logger = structlog.get_logger()


async def _run() -> None:
    """
    Execute a single iteration of the listmonk reconcile worker using application settings to configure logging.
    
    Configures structured logging from application settings (Loki URL, username, password, and log component defaulting to "reconcile"), builds and runs the listmonk reconcile worker once, logs the completed run with the worker's restored result, and ensures logging is shut down on completion.
    """
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        component=getattr(settings, "log_component", "reconcile"),
    )
    worker = build_listmonk_reconcile_worker()
    try:
        restored = await worker.run_once()
        logger.info("listmonk_reconcile_run_completed", restored=restored)
    finally:
        shutdown_logging()


def main() -> None:
    """
    Run a single iteration of the listmonk reconciliation worker.
    
    Serves as the script entry point; executes the module's asynchronous reconcile task that performs one reconcile cycle and ensures logging is configured and torn down.
    """
    asyncio.run(_run())


if __name__ == "__main__":
    main()
