"""Service heartbeat helpers for container-level healthchecks."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HEARTBEAT_DIR = Path(os.environ.get("HEARTBEAT_DIR", "/tmp/teyca-sync"))


def heartbeat_path(service_name: str) -> Path:
    return HEARTBEAT_DIR / f"{service_name}.json"


async def write_heartbeat(service_name: str, *, extra: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {
        "service": service_name,
        "timestamp": datetime.now(UTC).isoformat(),
    }
    if extra:
        payload.update(extra)
    await asyncio.to_thread(_write_json, heartbeat_path(service_name), payload)


def heartbeat_status(service_name: str, *, max_age_seconds: int) -> dict[str, Any]:
    path = heartbeat_path(service_name)
    if not path.exists():
        return {
            "status": "error",
            "error": "heartbeat file is missing",
            "fresh": False,
        }

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "status": "error",
            "error": str(exc),
            "fresh": False,
        }

    raw_timestamp = payload.get("timestamp")
    if not isinstance(raw_timestamp, str):
        return {
            "status": "error",
            "error": "heartbeat timestamp is missing",
            "fresh": False,
        }

    try:
        heartbeat_dt = datetime.fromisoformat(raw_timestamp)
    except ValueError as exc:
        return {
            "status": "error",
            "error": str(exc),
            "fresh": False,
        }

    now = datetime.now(UTC)
    if heartbeat_dt.tzinfo is None:
        heartbeat_dt = heartbeat_dt.replace(tzinfo=UTC)

    age_seconds = max(0.0, (now - heartbeat_dt).total_seconds())
    is_fresh = age_seconds <= max_age_seconds
    return {
        "status": "ok" if is_fresh else "error",
        "fresh": is_fresh,
        "age_seconds": round(age_seconds, 3),
        "timestamp": raw_timestamp,
        "service": payload.get("service", service_name),
    }


def is_heartbeat_fresh(service_name: str, *, max_age_seconds: int) -> bool:
    return bool(heartbeat_status(service_name, max_age_seconds=max_age_seconds)["fresh"])


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
