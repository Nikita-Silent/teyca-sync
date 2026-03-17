"""Unit tests: webhook routing by type and static token auth."""

import os
from unittest.mock import AsyncMock, patch

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
    async def override_publisher() -> AsyncMock:
        return mock_publisher

    app.dependency_overrides[get_mq_publisher] = override_publisher
    yield
    app.dependency_overrides.pop(get_mq_publisher, None)


@pytest.fixture(autouse=True)
def _restore_webhook_auth_env() -> None:
    original_token = os.environ.get("WEBHOOK_AUTH_TOKEN")
    original_enabled = os.environ.get("WEBHOOK_AUTH_ENABLED")
    yield
    if original_token is None:
        os.environ.pop("WEBHOOK_AUTH_TOKEN", None)
    else:
        os.environ["WEBHOOK_AUTH_TOKEN"] = original_token
    if original_enabled is None:
        os.environ.pop("WEBHOOK_AUTH_ENABLED", None)
    else:
        os.environ["WEBHOOK_AUTH_ENABLED"] = original_enabled


@pytest.mark.asyncio
async def test_webhook_routes_create_to_queue_create(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
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
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
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
async def test_webhook_propagates_trace_headers_to_payload(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    payload = {"type": "CREATE", "pass": {"user_id": 4}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={
                "Authorization": AUTH_TOKEN,
                "X-Trace-Id": "trace-123",
                "X-Event-Id": "event-123",
            },
        )
    assert resp.status_code == 200
    call_args = mock_publisher.publish_webhook.call_args
    assert call_args[0][1]["trace_id"] == "trace-123"
    assert call_args[0][1]["source_event_id"] == "event-123"


@pytest.mark.asyncio
async def test_webhook_routes_delete_to_queue_delete(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
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
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    payload = {"type": "CREATE", "pass": {"user_id": 1}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/webhook", json=payload)
    assert resp.status_code == 401
    mock_publisher.publish_webhook.assert_not_called()


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_token(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    payload = {"type": "CREATE", "pass": {"user_id": 1}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={"Authorization": "wrong-token"},
        )
    assert resp.status_code == 403
    mock_publisher.publish_webhook.assert_not_called()


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_json_body(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            content=b'{"type":"CREATE","pass":{"user_id":1}, bad}',
            headers={
                "Authorization": AUTH_TOKEN,
                "Content-Type": "application/json",
            },
        )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "Invalid JSON body"
    mock_publisher.publish_webhook.assert_not_called()


@pytest.mark.asyncio
async def test_webhook_allows_request_without_auth_when_disabled(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_ENABLED"] = "false"
    os.environ.pop("WEBHOOK_AUTH_TOKEN", None)
    payload = {"type": "CREATE", "pass": {"user_id": 7}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/webhook", json=payload)

    assert resp.status_code == 200
    mock_publisher.publish_webhook.assert_called_once()
    call_args = mock_publisher.publish_webhook.call_args
    assert call_args[0][0] == "CREATE"
    assert call_args[0][1]["pass"]["user_id"] == 7


@pytest.mark.asyncio
async def test_webhook_logs_validation_failure_without_publishing(
    mock_publisher: AsyncMock,
) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    payload = {"type": "UPDATE", "pass": {"user_id": 5757993, "tags": "1,2,3"}}
    with patch("app.api.webhook.logger") as logger:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.post(
                "/webhook",
                json=payload,
                headers={"Authorization": AUTH_TOKEN},
            )

    assert resp.status_code == 422
    assert resp.json()["detail"] == "Invalid webhook payload"
    mock_publisher.publish_webhook.assert_not_called()
    logger.error.assert_called_once()
    call = logger.error.call_args
    assert call.args[0] == "webhook_validation_failed"
    assert call.kwargs["user_id"] == 5757993
    assert "pass.tags" in call.kwargs["invalid_fields"]
