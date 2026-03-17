"""Service heartbeat helpers for container-level healthchecks."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4


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


async def heartbeat_status(service_name: str, *, max_age_seconds: int) -> dict[str, Any]:
    path = heartbeat_path(service_name)
    if not await _path_exists(path):
        return {
            "status": "error",
            "error": "heartbeat file is missing",
            "fresh": False,
        }

    try:
        payload = json.loads(await _read_text(path))
    except (OSError, json.JSONDecodeError) as exc:
        return {
            "status": "error",
            "error": str(exc),
            "fresh": False,
        }
    if not isinstance(payload, dict):
        return {
            "status": "error",
            "error": "heartbeat payload is not an object",
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


async def is_heartbeat_fresh(service_name: str, *, max_age_seconds: int) -> bool:
    return bool((await heartbeat_status(service_name, max_age_seconds=max_age_seconds))["fresh"])


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    _ensure_directory(path.parent)
    temp_path = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    try:
        with temp_path.open("w", encoding="utf-8") as file_obj:
            json.dump(payload, file_obj)
            file_obj.flush()
            os.fsync(file_obj.fileno())
        os.replace(temp_path, path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise


def _resolve_heartbeat_dir() -> Path:
    raw_path = os.environ.get("HEARTBEAT_DIR")
    if raw_path is not None:
        normalized = raw_path.strip()
        if not normalized:
            raise ValueError("HEARTBEAT_DIR must not be empty when set")
        path = Path(normalized)
    else:
        path = Path(os.environ.get("XDG_RUNTIME_DIR", "/var/run")) / "teyca-sync"
    _ensure_directory(path)
    return path


def _ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    path.chmod(0o700)


async def _path_exists(path: Path) -> bool:
    return path.exists()


async def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


HEARTBEAT_DIR = _resolve_heartbeat_dir()
