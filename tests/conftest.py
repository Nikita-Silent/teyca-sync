"""Pytest fixtures. TESTING=1 so app lifespan uses mock publisher (no RabbitMQ)."""

import os

import pytest
from httpx import ASGITransport, AsyncClient

# Ensure app uses test lifespan (mock MQ)
os.environ["TESTING"] = "1"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def client() -> AsyncClient:
    from app.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac
