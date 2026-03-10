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
    """
    Create an AsyncMock configured to mimic the MQPublisher interface for use in tests.
    
    Returns:
        AsyncMock: An AsyncMock instance with spec set to MQPublisher.
    """
    return AsyncMock(spec=MQPublisher)


@pytest.fixture(autouse=True)
def _override_publisher(mock_publisher: AsyncMock) -> None:
    """
    Temporarily override the app's `get_mq_publisher` dependency to return the provided mock and restore the original override afterward.
    
    Parameters:
        mock_publisher (AsyncMock): AsyncMock instance to be returned by the `get_mq_publisher` dependency while the override is active.
    """
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
    """
    Verifies the webhook endpoint accepts an Authorization header prefixed with "Bearer" and routes the payload as an UPDATE webhook.
    
    Asserts the response status is 200 and that MQPublisher.publish_webhook is called once with the first positional argument equal to "UPDATE" and a payload whose `pass.user_id` equals 2.
    """
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
async def test_webhook_propagates_trace_headers_to_payload(mock_publisher: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
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
    """
    Verify that POST /webhook without an Authorization header is rejected and no message is published.
    
    Sends a CREATE webhook payload without any Authorization header and asserts the response status is 401 (Unauthorized) and that the MQ publisher's publish_webhook was not called.
    
    Parameters:
        mock_publisher (AsyncMock): AsyncMock fixture that replaces the MQ publisher used by the app.
    """
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    payload = {"type": "CREATE", "pass": {"user_id": 1}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/webhook", json=payload)
    assert resp.status_code == 401
    mock_publisher.publish_webhook.assert_not_called()


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_token(mock_publisher: AsyncMock) -> None:
    """
    Verifies that a POST /webhook request with an invalid authorization token is rejected and no message is published.
    
    Sends a valid webhook payload with an incorrect Authorization header, asserts the response status is 403, and asserts that the MQ publisher's publish_webhook was not called.
    """
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


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_json_body(mock_publisher: AsyncMock) -> None:
    """
    Verifies that sending a malformed JSON body to POST /webhook results in a 400 response and no publish attempt.
    
    Sets the WEBHOOK_AUTH_TOKEN environment variable, posts an invalid JSON payload with the proper Content-Type and Authorization header, and asserts the response status is 400 with detail "Invalid JSON body". Confirms that the MQ publisher's publish_webhook was not called.
    """
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
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
