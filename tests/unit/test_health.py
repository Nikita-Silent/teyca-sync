"""Health and root endpoint tests."""

import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app
from app.service_health import heartbeat_status, write_heartbeat


@pytest.mark.asyncio
async def test_root_returns_hello() -> None:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        resp = await ac.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"message": "Hello, World!"}


@pytest.mark.asyncio
async def test_health_returns_ok_when_dependencies_are_available() -> None:
    with (
        patch(
            "app.api.webhook._check_database_health",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "app.api.webhook.heartbeat_status",
            new=AsyncMock(return_value={"status": "ok", "fresh": True, "service": "app"}),
        ),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.get("/health")

    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    assert resp.json()["checks"]["app"] == {"status": "ok", "fresh": True, "service": "app"}
    assert resp.json()["checks"]["database"] == {"status": "ok"}


@pytest.mark.asyncio
async def test_health_returns_503_when_dependency_fails() -> None:
    with (
        patch(
            "app.api.webhook._check_database_health",
            new=AsyncMock(return_value="db is down"),
        ),
        patch(
            "app.api.webhook.heartbeat_status",
            new=AsyncMock(return_value={"status": "ok", "fresh": True, "service": "app"}),
        ),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.get("/health")

    assert resp.status_code == 503
    assert resp.json()["status"] == "error"
    assert resp.json()["checks"]["database"] == {"status": "error", "error": "internal error"}


@pytest.mark.asyncio
async def test_live_and_ready_routes_are_split() -> None:
    with (
        patch(
            "app.api.webhook._check_database_health",
            new=AsyncMock(return_value=None),
        ),
        patch(
            "app.api.webhook.heartbeat_status",
            new=AsyncMock(return_value={"status": "ok", "fresh": True, "service": "app"}),
        ),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            live_resp = await ac.get("/live")
            ready_resp = await ac.get("/ready")

    assert live_resp.status_code == 200
    assert live_resp.json()["checks"] == {"app": {"status": "ok", "fresh": True, "service": "app"}}
    assert ready_resp.status_code == 200
    assert ready_resp.json()["checks"] == {"database": {"status": "ok"}}


@pytest.mark.asyncio
async def test_live_returns_503_when_app_heartbeat_is_stale() -> None:
    with patch(
        "app.api.webhook.heartbeat_status",
        new=AsyncMock(
            return_value={
                "status": "error",
                "fresh": False,
                "service": "app",
                "error": "stale",
            }
        ),
    ):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            resp = await ac.get("/live")

    assert resp.status_code == 503
    assert resp.json()["status"] == "error"
    assert resp.json()["checks"] == {
        "app": {"status": "error", "fresh": False, "service": "app", "error": "internal error"}
    }


@pytest.mark.asyncio
async def test_heartbeat_status_detects_stale_file(tmp_path: Path) -> None:
    heartbeat_dir = tmp_path / "heartbeats"
    heartbeat_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "service": "consumers",
        "timestamp": (datetime.now(UTC) - timedelta(seconds=120)).isoformat(),
    }
    (heartbeat_dir / "consumers.json").write_text(json.dumps(payload), encoding="utf-8")

    with patch("app.service_health.HEARTBEAT_DIR", heartbeat_dir):
        result = await heartbeat_status("consumers", max_age_seconds=60)

    assert result["status"] == "error"
    assert result["fresh"] is False


@pytest.mark.asyncio
async def test_heartbeat_status_rejects_non_object_payload(tmp_path: Path) -> None:
    heartbeat_dir = tmp_path / "heartbeats"
    heartbeat_dir.mkdir(parents=True, exist_ok=True)
    (heartbeat_dir / "consumers.json").write_text(json.dumps(["bad"]), encoding="utf-8")

    with patch("app.service_health.HEARTBEAT_DIR", heartbeat_dir):
        result = await heartbeat_status("consumers", max_age_seconds=60)

    assert result == {
        "status": "error",
        "error": "heartbeat payload is not an object",
        "fresh": False,
    }


@pytest.mark.asyncio
async def test_write_heartbeat_gives_up_when_the_write_stalls(tmp_path: Path) -> None:
    """A heartbeat write that blocks on a stalled disk must not block its
    caller: every task loop awaits one before and after each run, so a write
    that waits out a multi-minute I/O stall stalls the work itself (the app's
    15s heartbeat drifted to 50-180s and the worker's loops nearly stopped)."""
    heartbeat_dir = tmp_path / "heartbeats"
    heartbeat_dir.mkdir(parents=True, exist_ok=True)

    def stalled_write(path: Path, payload: dict[str, object]) -> None:
        time.sleep(0.5)

    with (
        patch("app.service_health.HEARTBEAT_DIR", heartbeat_dir),
        patch("app.service_health.HEARTBEAT_WRITE_TIMEOUT_SECONDS", 0.05),
        patch("app.service_health._write_json", new=stalled_write),
    ):
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            await write_heartbeat("app")
        elapsed = time.monotonic() - started

    assert elapsed < 1.0


@pytest.mark.asyncio
async def test_write_heartbeat_is_atomic_without_fsync(tmp_path: Path) -> None:
    """A heartbeat is liveness state, not durable data — a timestamp lost to a
    crash is worthless, since the crash is what the reader needs to notice. The
    fsync only bought that worthless durability, and cost the latency that made
    the writes above stall, so it is gone; os.replace still keeps readers from
    seeing a half-written file."""
    heartbeat_dir = tmp_path / "heartbeats"
    heartbeat_dir.mkdir(parents=True, exist_ok=True)

    with (
        patch("app.service_health.HEARTBEAT_DIR", heartbeat_dir),
        patch("app.service_health.os.fsync") as fsync,
    ):
        await write_heartbeat("app", extra={"stage": "started"})
        result = await heartbeat_status("app", max_age_seconds=60)

    fsync.assert_not_called()
    assert result["status"] == "ok"
    assert result["fresh"] is True
    written = json.loads((heartbeat_dir / "app.json").read_text(encoding="utf-8"))
    assert written["service"] == "app"
    assert written["stage"] == "started"
    assert list(heartbeat_dir.glob(".*.tmp")) == []
