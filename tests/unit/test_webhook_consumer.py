"""Unit tests: webhook routing by type, static token auth, inbox persistence."""

import os
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app

AUTH_TOKEN = "36545925e92437d467ec8bef30b07bb2"


@pytest.fixture
def mock_inbox_repo() -> Generator[AsyncMock]:
    """Patch WebhookInboxRepository so webhook() never touches a real DB."""
    repo_instance = AsyncMock()
    repo_instance.enqueue.return_value = True
    repo_cls = MagicMock(return_value=repo_instance)
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    session_cm.__aexit__.return_value = False
    with (
        patch("app.api.webhook.WebhookInboxRepository", repo_cls),
        patch("app.api.webhook.SessionLocal", return_value=session_cm),
    ):
        yield repo_instance


@pytest.fixture(autouse=True)
def _restore_webhook_auth_env() -> Generator[None]:
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
async def test_webhook_persists_create_event_to_inbox(mock_inbox_repo: AsyncMock) -> None:
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
    mock_inbox_repo.enqueue.assert_awaited_once()
    call_kwargs = mock_inbox_repo.enqueue.call_args.kwargs
    assert call_kwargs["event_type"] == "CREATE"
    assert call_kwargs["payload"]["type"] == "CREATE"
    assert call_kwargs["payload"]["pass"]["user_id"] == 1


@pytest.mark.asyncio
async def test_webhook_accepts_bearer_prefix(mock_inbox_repo: AsyncMock) -> None:
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
    mock_inbox_repo.enqueue.assert_awaited_once()
    call_kwargs = mock_inbox_repo.enqueue.call_args.kwargs
    assert call_kwargs["event_type"] == "UPDATE"
    assert call_kwargs["payload"]["pass"]["user_id"] == 2


@pytest.mark.asyncio
async def test_webhook_propagates_trace_headers_to_payload(mock_inbox_repo: AsyncMock) -> None:
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
    call_kwargs = mock_inbox_repo.enqueue.call_args.kwargs
    assert call_kwargs["trace_id"] == "trace-123"
    assert call_kwargs["source_event_id"] == "event-123"
    assert call_kwargs["payload"]["trace_id"] == "trace-123"
    assert call_kwargs["payload"]["source_event_id"] == "event-123"


@pytest.mark.asyncio
async def test_webhook_routes_delete_event(mock_inbox_repo: AsyncMock) -> None:
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
    mock_inbox_repo.enqueue.assert_awaited_once()
    call_kwargs = mock_inbox_repo.enqueue.call_args.kwargs
    assert call_kwargs["event_type"] == "DELETE"
    assert call_kwargs["payload"]["pass"]["user_id"] == 3


@pytest.mark.asyncio
async def test_webhook_rejects_missing_auth(mock_inbox_repo: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    payload = {"type": "CREATE", "pass": {"user_id": 1}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/webhook", json=payload)
    assert resp.status_code == 401
    mock_inbox_repo.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_token(mock_inbox_repo: AsyncMock) -> None:
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
    mock_inbox_repo.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_rejects_invalid_json_body(mock_inbox_repo: AsyncMock) -> None:
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
    mock_inbox_repo.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_allows_request_without_auth_when_disabled(
    mock_inbox_repo: AsyncMock,
) -> None:
    os.environ["WEBHOOK_AUTH_ENABLED"] = "false"
    os.environ.pop("WEBHOOK_AUTH_TOKEN", None)
    payload = {"type": "CREATE", "pass": {"user_id": 7}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post("/webhook", json=payload)

    assert resp.status_code == 200
    mock_inbox_repo.enqueue.assert_awaited_once()
    call_kwargs = mock_inbox_repo.enqueue.call_args.kwargs
    assert call_kwargs["event_type"] == "CREATE"
    assert call_kwargs["payload"]["pass"]["user_id"] == 7


@pytest.mark.asyncio
async def test_webhook_logs_validation_failure_without_persisting(
    mock_inbox_repo: AsyncMock,
) -> None:
    """Ingress only gates on type + pass.user_id (teyca-sync-iil.4) — an
    unroutable event (missing user_id here) is the only kind of `pass` problem
    that still 422s, because Teyca doesn't retry a 422 and we'd have nowhere
    to route the event anyway without a user_id."""
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    payload = {"type": "UPDATE", "pass": {"email": "a@b.c"}}
    with patch("app.api.webhook.logger") as logger:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.post(
                "/webhook",
                json=payload,
                headers={"Authorization": AUTH_TOKEN},
            )

    assert resp.status_code == 422
    assert resp.json()["detail"] == "Invalid webhook payload"
    mock_inbox_repo.enqueue.assert_not_awaited()
    logger.error.assert_called_once()
    call = logger.error.call_args
    assert call.args[0] == "webhook_validation_failed"
    assert "pass.user_id" in call.kwargs["invalid_fields"]


@pytest.mark.asyncio
async def test_webhook_accepts_malformed_pass_field_and_stores_it_raw(
    mock_inbox_repo: AsyncMock,
) -> None:
    """A malformed `pass` field (here: tags as a string instead of a list, the
    kind of thing WebhookPayload/PassData would reject) is no longer an
    ingress concern — it's stored raw in the inbox and validated downstream by
    the consumer (teyca-sync-iil.4), where a failure is a retryable/inspectable
    `dead` row instead of a silently dropped event."""
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    payload = {"type": "UPDATE", "pass": {"user_id": 5757993, "tags": "1,2,3"}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={"Authorization": AUTH_TOKEN},
        )

    assert resp.status_code == 200
    mock_inbox_repo.enqueue.assert_awaited_once()
    call_kwargs = mock_inbox_repo.enqueue.call_args.kwargs
    assert call_kwargs["payload"]["pass"]["tags"] == "1,2,3"


@pytest.mark.asyncio
async def test_webhook_rejects_unsupported_type(mock_inbox_repo: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    payload = {"type": "PATCH", "pass": {"user_id": 1}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={"Authorization": AUTH_TOKEN},
        )
    assert resp.status_code == 422
    mock_inbox_repo.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_rejects_body_over_size_limit(mock_inbox_repo: AsyncMock) -> None:
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    from app.config import get_settings

    limit = get_settings().webhook_max_body_bytes
    oversized = {"type": "UPDATE", "pass": {"user_id": 1, "fio": "x" * (limit + 1)}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=oversized,
            headers={"Authorization": AUTH_TOKEN},
        )
    assert resp.status_code == 413
    mock_inbox_repo.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_webhook_redelivery_with_same_event_id_is_idempotent(
    mock_inbox_repo: AsyncMock,
) -> None:
    """A redelivered webhook (same x-event-id) hits enqueue() again but the
    repository's ON CONFLICT DO NOTHING makes it a no-op; the endpoint still
    replies 200 either way."""
    os.environ["WEBHOOK_AUTH_TOKEN"] = AUTH_TOKEN
    os.environ["WEBHOOK_AUTH_ENABLED"] = "true"
    mock_inbox_repo.enqueue.return_value = False
    payload = {"type": "CREATE", "pass": {"user_id": 9}}
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.post(
            "/webhook",
            json=payload,
            headers={"Authorization": AUTH_TOKEN, "X-Event-Id": "dup-event"},
        )
    assert resp.status_code == 200
    mock_inbox_repo.enqueue.assert_awaited_once()
