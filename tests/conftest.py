"""Pytest fixtures. TESTING=1 so app lifespan uses mock publisher (no RabbitMQ)."""

import os

import pytest
from httpx import ASGITransport, AsyncClient

# Ensure app uses test lifespan (mock MQ)
os.environ["TESTING"] = "1"


@pytest.fixture
def anyio_backend() -> str:
    """
    Select the anyio backend used by pytest for async tests.
    
    Returns:
        backend (str): The name of the anyio backend to use, 'asyncio'.
    """
    return "asyncio"


@pytest.fixture
async def client() -> AsyncClient:
    """
    Yield an AsyncClient configured to send HTTP requests to the application's ASGI app for tests.
    
    Returns:
        httpx.AsyncClient: An AsyncClient bound to the application's ASGI app and configured with base_url "http://test".
    """
    from app.main import app
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac
