"""Pytest fixtures."""

from collections.abc import AsyncGenerator, Generator
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture(autouse=True)
def _no_real_dns_mx_lookups() -> Generator[AsyncMock]:
    """Unit tests never hit real DNS; level-3 email checks default to valid.

    Tests that exercise MX behavior itself patch `has_valid_mx` again with
    their own return value/side effect, overriding this default.
    """
    with patch("app.consumers.common.has_valid_mx", new=AsyncMock(return_value=True)) as mock:
        yield mock


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient]:
    from app.main import app

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac
