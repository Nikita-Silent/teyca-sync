"""Application logging setup: Loki-only sink via python-logging-loki-v2."""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from queue import Empty, Queue
from typing import Any, cast

import logging_loki
import requests
import structlog
from structlog.typing import EventDict, WrappedLogger

_loki_queue_handler: logging_loki.LokiQueueHandler | None = None


def _add_static_fields(
    *, service_name: str, component: str
) -> Callable[[WrappedLogger, str, EventDict], EventDict]:
    def _processor(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
        event_dict.setdefault("service", service_name)
        event_dict.setdefault("component", component)
        return event_dict

    return _processor


def configure_logging(
    loki_url: str | None,
    service_name: str = "teyca-sync",
    loki_username: str | None = None,
    loki_password: str | None = None,
    component: str = "app",
    console: bool = False,
    loki_request_timeout_seconds: float = 5.0,
) -> None:
    """Configure structlog + stdlib logging with Loki as the only sink."""
    global _loki_queue_handler
    if _loki_queue_handler is not None:
        if _loki_queue_handler.listener is not None:
            _drain_logging_queue(_loki_queue_handler.queue)
            _loki_queue_handler.listener.stop()
        _loki_queue_handler.close()
        _loki_queue_handler = None

    if not loki_url:
        raise RuntimeError("LOKI_URL must be configured (logging sink is Loki-only)")

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _add_static_fields(service_name=service_name, component=component),
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
    _install_loki_request_timeout(
        loki_handler,
        timeout_seconds=max(0.1, float(loki_request_timeout_seconds)),
    )
    loki_handler.setFormatter(logging.Formatter("%(message)s"))
    loki_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(loki_handler)
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("%(message)s"))
        console_handler.setLevel(logging.INFO)
        root_logger.addHandler(console_handler)
    _loki_queue_handler = loki_handler


def shutdown_logging() -> None:
    """Flush and stop background Loki queue listener."""
    global _loki_queue_handler
    if _loki_queue_handler is not None:
        if _loki_queue_handler.listener is not None:
            _drain_logging_queue(_loki_queue_handler.queue)
            _loki_queue_handler.listener.stop()
        _loki_queue_handler.close()
        _loki_queue_handler = None


def _install_loki_request_timeout(
    loki_queue_handler: logging_loki.LokiQueueHandler,
    *,
    timeout_seconds: float,
) -> None:
    emitter = getattr(getattr(loki_queue_handler, "handler", None), "emitter", None)
    if emitter is None:
        return
    session = getattr(emitter, "session", None)
    if not isinstance(session, requests.Session):
        return
    original_request = cast(Callable[..., object], session.request)

    def _request(method: str, url: str, **kwargs: Any) -> object:
        kwargs.setdefault("timeout", timeout_seconds)
        return original_request(method, url, **kwargs)

    session.request = _request  # type: ignore[method-assign]


def _drain_logging_queue(queue: object) -> None:
    typed_queue = cast(Queue[object], queue)
    while True:
        try:
            typed_queue.get_nowait()
        except Empty:
            return


def _normalize_loki_url(raw_url: str) -> str:
    stripped = raw_url.rstrip("/")
    if stripped.endswith("/loki/api/v1/push"):
        return stripped
    return f"{stripped}/loki/api/v1/push"
