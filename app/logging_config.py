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
    """
    Configure structlog and the standard library logger to send all application logs to a Loki sink.
    
    Parameters:
        loki_url (str | None): Base URL of the Loki instance or its full push endpoint; required.
        service_name (str): Value for the `service` Loki tag.
        loki_username (str | None): Username for basic auth; if provided, `loki_password` is used (empty string if None).
        loki_password (str | None): Password for basic auth.
        component (str): Value for the `component` Loki tag.
    
    Description:
        If a previous Loki queue handler is active, it is stopped and closed before configuring a new one.
        Validates that `loki_url` is provided, configures structlog processors and logger factories, creates
        a LokiQueueHandler (using a queue, tags, optional basic auth, and version "2"), sets its formatter
        and level to INFO, replaces root logger handlers with the Loki handler, and stores the handler for
        later shutdown.
    """
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
    """
    Shut down the module-level Loki queue handler and its background listener.
    
    If a Loki queue handler is configured, stops its background listener, closes the handler, and clears the module-level reference so the logging subsystem can be reconfigured.
    """
    global _loki_queue_handler
    if _loki_queue_handler is not None:
        _loki_queue_handler.listener.stop()
        _loki_queue_handler.close()
        _loki_queue_handler = None


def _normalize_loki_url(raw_url: str) -> str:
    """
    Normalize a Loki URL so it points to the push API endpoint.
    
    Strips any trailing slashes from `raw_url` and ensures the result ends with `/loki/api/v1/push`.
    
    Parameters:
        raw_url (str): The input Loki URL or base address (may include trailing slashes or a path).
    
    Returns:
        str: A URL that ends with `/loki/api/v1/push`.
    """
    stripped = raw_url.rstrip("/")
    if stripped.endswith("/loki/api/v1/push"):
        return stripped
    return f"{stripped}/loki/api/v1/push"
