"""Unit tests: webhook routing by type and static token auth."""

import os
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.api.webhook import get_mq_publisher
from app.main import app
from app.mq.publisher import MQPublisher

AUTH_TOKEN = "36545925e92437d467ec8bef30b07bb2"


@pytest.fixture
def mock_publisher() -> AsyncMock:
    return AsyncMock(spec=MQPublisher)


@pytest.fixture(autouse=True)
def _override_publisher(mock_publisher: AsyncMock) -> None:
    app.dependency_overrides[get_mq_publisher] = lambda: mock_publisher
    yield
    app.dependency_overrides.pop(get_mq_publisher, None)


@pytest.mark.asyncio
async def test_webhook_routes_create_to_queue_create(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    payload = {"type": "CREATE", "pass": {"user_id": 1}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={"Authorization": AUTH_TOKEN},
        )
    assert resp.status_code == 200
    mock_publisher.publish_webhook.assert_called_once()
    call_args = mock_publisher.publish_webhook.call_args
    assert call_args[0][0] == "CREATE"
    assert call_args[0][1]["type"] == "CREATE"
    assert call_args[0][1]["pass"]["user_id"] == 1


@pytest.mark.asyncio
async def test_webhook_accepts_bearer_prefix(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    payload = {"type": "UPDATE", "pass": {"user_id": 2}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
        )
    assert resp.status_code == 200
    mock_publisher.publish_webhook.assert_called_once()
    call_args = mock_publisher.publish_webhook.call_args
    assert call_args[0][0] == "UPDATE"
    assert call_args[0][1]["pass"]["user_id"] == 2


@pytest.mark.asyncio
async def test_webhook_routes_delete_to_queue_delete(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    payload = {"type": "DELETE", "pass": {"user_id": 3}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={"Authorization": AUTH_TOKEN},
        )
    assert resp.status_code == 200
    mock_publisher.publish_webhook.assert_called_once()
    call_args = mock_publisher.publish_webhook.call_args
    assert call_args[0][0] == "DELETE"
    assert call_args[0][1]["pass"]["user_id"] == 3


@pytest.mark.asyncio
async def test_webhook_rejects_missing_auth(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    payload = {"type": "CREATE", "pass": {"user_id": 1}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/webhook", json=payload)
    assert resp.status_code == 401
    mock_publisher.publish_webhook.assert_not_called()


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_token(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    payload = {"type": "CREATE", "pass": {"user_id": 1}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={"Authorization": "wrong-token"},
        )
    assert resp.status_code == 403
    mock_publisher.publish_webhook.assert_not_called()
