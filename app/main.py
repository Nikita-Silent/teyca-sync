"""FastAPI app: webhook router, RabbitMQ lifecycle."""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI

from app.api.webhook import router as webhook_router
from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.mq.publisher import MQPublisher
from app.service_health import write_heartbeat


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Manage application startup and shutdown, configuring logging, initializing the message publisher, starting a periodic heartbeat, and cleaning up resources.
    
    On startup, reads configuration via get_settings and configures logging. If the TESTING environment variable is set, sets app.state.mq_publisher to a mock and writes a single heartbeat. Otherwise, establishes a RabbitMQ-backed MQPublisher, stores it on app.state, and starts a background heartbeat task. On shutdown, cancels the heartbeat task (if started), closes the message broker connection, and shuts down logging.
    """
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
                await connection.close()
    finally:
        shutdown_logging()


app = FastAPI(title="teyca-sync", lifespan=lifespan)
webhook_path = get_settings().webhook.strip() or "/webhook"
if not webhook_path.startswith("/"):
    webhook_path = f"/{webhook_path}"
app.include_router(webhook_router, prefix=webhook_path)


@app.get("/")
async def read_root() -> dict:
    """
    Serve the application's root greeting message.
    
    Returns:
        dict: Dictionary with a 'message' key containing the greeting string "Hello, World!".
    """
    return {"message": "Hello, World!"}


def _start_heartbeat_task(service_name: str, *, interval_seconds: int) -> asyncio.Task[None]:
    """
    Start a background task that periodically writes a heartbeat for the given service.
    
    Parameters:
        service_name (str): Name of the service to include in each heartbeat.
        interval_seconds (int): Number of seconds to wait between consecutive heartbeats.
    
    Returns:
        asyncio.Task[None]: Background asyncio Task running the periodic heartbeat loop.
    """
    async def _run() -> None:
        while True:
            await write_heartbeat(service_name)
            await asyncio.sleep(interval_seconds)

    return asyncio.create_task(_run())
