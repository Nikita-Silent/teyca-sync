"""Health and root endpoint tests."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.service_health import heartbeat_status


@pytest.mark.asyncio
async def test_root_returns_hello() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"message": "Hello, World!"}


@pytest.mark.asyncio
async def test_health_returns_ok_when_dependencies_are_available() -> None:
    with patch(
        "app.api.webhook._check_database_health",
        new=AsyncMock(return_value=None),
    ), patch(
        "app.api.webhook._check_rabbitmq_health",
        new=AsyncMock(return_value=None),
    ), patch(
        "app.api.webhook.get_settings",
        return_value=type("Settings", (), {"rabbitmq_url": "amqp://guest:guest@rabbitmq:5672/"})(),
    ), patch(
        "app.api.webhook.heartbeat_status",
        return_value={"status": "ok", "fresh": True, "service": "app"},
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.get("/webhook/health")

    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["checks"]["app"] == {"status": "ok", "fresh": True, "service": "app"}
    assert resp.json()["checks"]["database"] == {"status": "ok"}
    assert resp.json()["checks"]["rabbitmq"] == {"status": "ok"}


@pytest.mark.asyncio
async def test_health_returns_503_when_dependency_fails() -> None:
    with patch(
        "app.api.webhook._check_database_health",
        new=AsyncMock(return_value="db is down"),
    ), patch(
        "app.api.webhook._check_rabbitmq_health",
        new=AsyncMock(return_value=None),
    ), patch(
        "app.api.webhook.get_settings",
        return_value=type("Settings", (), {"rabbitmq_url": "amqp://guest:guest@rabbitmq:5672/"})(),
    ), patch(
        "app.api.webhook.heartbeat_status",
        return_value={"status": "ok", "fresh": True, "service": "app"},
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.get("/webhook/health")

    assert resp.status_code == 503
    assert resp.json()["status"] == "error"
    assert resp.json()["checks"]["database"] == {"status": "error", "error": "db is down"}
    assert resp.json()["checks"]["rabbitmq"] == {"status": "ok"}


@pytest.mark.asyncio
async def test_live_and_ready_routes_are_split() -> None:
    with patch(
        "app.api.webhook._check_database_health",
        new=AsyncMock(return_value=None),
    ), patch(
        "app.api.webhook._check_rabbitmq_health",
        new=AsyncMock(return_value="rabbit down"),
    ), patch(
        "app.api.webhook.get_settings",
        return_value=type("Settings", (), {"rabbitmq_url": "amqp://guest:guest@rabbitmq:5672/"})(),
    ), patch(
        "app.api.webhook.heartbeat_status",
        return_value={"status": "ok", "fresh": True, "service": "app"},
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            live_resp = await ac.get("/webhook/live")
            ready_resp = await ac.get("/webhook/ready")

    assert live_resp.status_code == 200
    assert live_resp.json()["checks"] == {
        "app": {"status": "ok", "fresh": True, "service": "app"}
    }
    assert ready_resp.status_code == 503
    assert ready_resp.json()["checks"] == {
        "database": {"status": "ok"},
        "rabbitmq": {"status": "error", "error": "rabbit down"},
    }


@pytest.mark.asyncio
async def test_live_returns_503_when_app_heartbeat_is_stale() -> None:
    with patch(
        "app.api.webhook.heartbeat_status",
        return_value={"status": "error", "fresh": False, "service": "app", "error": "stale"},
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.get("/webhook/live")

    assert resp.status_code == 503
    assert resp.json()["status"] == "error"
    assert resp.json()["checks"] == {
        "app": {"status": "error", "fresh": False, "service": "app", "error": "stale"}
    }


def test_heartbeat_status_detects_stale_file(tmp_path: Path) -> None:
    heartbeat_dir = tmp_path / "heartbeats"
    heartbeat_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "service": "consumers",
        "timestamp": (datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
    }
    (heartbeat_dir / "consumers.json").write_text(json.dumps(payload), encoding="utf-8")

    with patch("app.service_health.HEARTBEAT_DIR", heartbeat_dir):
        result = heartbeat_status("consumers", max_age_seconds=60)

    assert result["status"] == "error"
    assert result["fresh"] is False
