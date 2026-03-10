"""Application logging setup: Loki-only sink via python-logging-loki-v2."""

from __future__ import annotations

import logging
from queue import Queue

import logging_loki
import structlog

_loki_queue_handler: logging_loki.LokiQueueHandler | None = None


def configure_logging(
    loki_url: str | None,
    service_name: str = "teyca-sync",
    loki_username: str | None = None,
    loki_password: str | None = None,
    component: str = "app",
) -> None:
    """Configure structlog + stdlib logging with Loki as the only sink."""
    global _loki_queue_handler
    if _loki_queue_handler is not None:
        _loki_queue_handler.listener.stop()
        _loki_queue_handler.close()
        _loki_queue_handler = None

    if not loki_url:
        raise RuntimeError("LOKI_URL must be configured (logging sink is Loki-only)")

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    auth: tuple[str, str] | None = None
    if loki_username:
        auth = (loki_username, loki_password or "")

    loki_handler = logging_loki.LokiQueueHandler(
        Queue(-1),
        url=_normalize_loki_url(loki_url),
        tags={"service": service_name, "component": component},
        auth=auth,
        version="2",
    )
    loki_handler.setFormatter(logging.Formatter("%(message)s"))
    loki_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(loki_handler)
    _loki_queue_handler = loki_handler


def shutdown_logging() -> None:
    """Flush and stop background Loki queue listener."""
    global _loki_queue_handler
    if _loki_queue_handler is not None:
        _loki_queue_handler.listener.stop()
        _loki_queue_handler.close()
        _loki_queue_handler = None


def _normalize_loki_url(raw_url: str) -> str:
    stripped = raw_url.rstrip("/")
    if stripped.endswith("/loki/api/v1/push"):
        return stripped
    return f"{stripped}/loki/api/v1/push"
