"""Application logging setup with optional Loki sink."""

from __future__ import annotations

import json
import logging
from logging.handlers import QueueHandler, QueueListener
from queue import SimpleQueue
from typing import Any
from urllib import request

import structlog

_listener: QueueListener | None = None


class LokiHandler(logging.Handler):
    """Send JSON log lines to Loki push API."""

    def __init__(self, loki_url: str, service_name: str, timeout: float = 2.0) -> None:
        super().__init__()
        self._push_url = self._normalize_url(loki_url)
        self._service_name = service_name
        self._timeout = timeout

    @staticmethod
    def _normalize_url(raw_url: str) -> str:
        stripped = raw_url.rstrip("/")
        if stripped.endswith("/loki/api/v1/push"):
            return stripped
        return f"{stripped}/loki/api/v1/push"

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record)
            timestamp_ns = str(int(record.created * 1_000_000_000))
            payload = {
                "streams": [
                    {
                        "stream": {
                            "service": self._service_name,
                            "logger": record.name,
                            "level": record.levelname.lower(),
                        },
                        "values": [[timestamp_ns, line]],
                    }
                ]
            }
            body = json.dumps(payload).encode("utf-8")
            req = request.Request(
                self._push_url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with request.urlopen(req, timeout=self._timeout):
                return
        except Exception:
            self.handleError(record)


def configure_logging(loki_url: str | None, service_name: str = "teyca-sync") -> None:
    """Configure structlog + stdlib logging, optionally with Loki."""
    global _listener
    if _listener is not None:
        _listener.stop()
        _listener = None

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

    sink_handlers: list[logging.Handler] = [logging.StreamHandler()]
    if loki_url:
        sink_handlers.append(LokiHandler(loki_url=loki_url, service_name=service_name))

    for handler in sink_handlers:
        handler.setFormatter(logging.Formatter("%(message)s"))
        handler.setLevel(logging.INFO)

    queue: SimpleQueue[logging.LogRecord] = SimpleQueue()
    queue_handler = QueueHandler(queue)
    queue_handler.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(queue_handler)

    _listener = QueueListener(queue, *sink_handlers, respect_handler_level=True)
    _listener.start()


def shutdown_logging() -> None:
    """Flush and stop background log listener."""
    global _listener
    if _listener is not None:
        _listener.stop()
        _listener = None
