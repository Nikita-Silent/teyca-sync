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
    settings = get_settings()
    configure_logging(loki_url=settings.loki_url)
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
app.include_router(webhook_router)


@app.get("/")
async def read_root() -> dict:
    return {"message": "Hello, World!"}
