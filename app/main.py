"""FastAPI app: webhook router, RabbitMQ lifecycle."""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika
import structlog
from fastapi import FastAPI

from app.api.webhook import health_router, router as webhook_router
from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.mq.publisher import MQPublisher
from app.service_health import write_heartbeat

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    heartbeat_task = None
    configure_logging(
        loki_url=settings.loki_url,
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        component=getattr(settings, "log_component", "app"),
    )
    try:
        if os.environ.get("TESTING"):
            from unittest.mock import AsyncMock

            app.state.mq_publisher = AsyncMock(spec=MQPublisher)
            await write_heartbeat("app")
            yield
        else:
            connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            app.state.mq_publisher = MQPublisher(connection)
            heartbeat_task = _start_heartbeat_task("app", interval_seconds=15)
            try:
                yield
            finally:
                if heartbeat_task is not None:
                    heartbeat_task.cancel()
                    try:
                        await heartbeat_task
                    except asyncio.CancelledError:
                        pass
                await connection.close()
    finally:
        shutdown_logging()


app = FastAPI(title="teyca-sync", lifespan=lifespan)
webhook_path = get_settings().webhook.strip() or "/webhook"
if not webhook_path.startswith("/"):
    webhook_path = f"/{webhook_path}"
app.include_router(health_router)
app.include_router(webhook_router, prefix=webhook_path)
app.include_router(health_router, prefix=webhook_path)


@app.get("/")
async def read_root() -> dict:
    return {"message": "Hello, World!"}


def _start_heartbeat_task(service_name: str, *, interval_seconds: int) -> asyncio.Task[None]:
    async def _run() -> None:
        while True:
            try:
                await write_heartbeat(service_name)
            except Exception as exc:
                logger.error(
                    "heartbeat_write_failed",
                    service_name=service_name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
            await asyncio.sleep(interval_seconds)

    return asyncio.create_task(_run())
