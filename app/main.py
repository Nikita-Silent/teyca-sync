"""FastAPI app: webhook router, RabbitMQ lifecycle."""

import os
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI

from app.api.webhook import router as webhook_router
from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.mq.publisher import MQPublisher


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage the application's startup and shutdown lifecycle for a FastAPI app.
    
    On startup, configures structured logging from application settings and attaches an MQ publisher to app.state.mq_publisher. If the TESTING environment variable is set, a mocked AsyncMock MQPublisher is used; otherwise a robust RabbitMQ connection is established and an MQPublisher instantiated. On shutdown, any open RabbitMQ connection is closed and logging is shut down.
    
    Parameters:
        app (FastAPI): The FastAPI application whose lifespan is being managed.
    """
    settings = get_settings()
    configure_logging(
        loki_url=settings.loki_url,
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        component=getattr(settings, "log_component", "app"),
    )
    if os.environ.get("TESTING"):
        from unittest.mock import AsyncMock
        app.state.mq_publisher = AsyncMock(spec=MQPublisher)
        yield
    else:
        connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        app.state.mq_publisher = MQPublisher(connection)
        try:
            yield
        finally:
            await connection.close()
    shutdown_logging()


app = FastAPI(title="teyca-sync", lifespan=lifespan)
webhook_path = get_settings().webhook.strip() or "/webhook"
if not webhook_path.startswith("/"):
    webhook_path = f"/{webhook_path}"
app.include_router(webhook_router, prefix=webhook_path)


@app.get("/")
async def read_root() -> dict:
    """
    Provide a simple greeting JSON payload for the application's root endpoint.
    
    Returns:
        dict: A dictionary with a single key `"message"` set to `"Hello, World!"`.
    """
    return {"message": "Hello, World!"}
