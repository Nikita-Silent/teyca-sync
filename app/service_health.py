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
    """
    Compute the filesystem path for a service's heartbeat JSON file.
    
    Parameters:
        service_name (str): Service identifier used to form the heartbeat filename.
    
    Returns:
        Path: Path to the heartbeat file at HEARTBEAT_DIR/{service_name}.json
    """
    return HEARTBEAT_DIR / f"{service_name}.json"


async def write_heartbeat(service_name: str, *, extra: dict[str, Any] | None = None) -> None:
    """
    Write a heartbeat JSON file for the given service.
    
    The file payload contains a UTC ISO 8601 `timestamp` and the `service` name; if `extra` is provided its keys are merged into the payload before writing. The heartbeat file is placed under the configured HEARTBEAT_DIR using the service name.
    
    Parameters:
    	service_name (str): Name of the service to record in the heartbeat file.
    	extra (dict[str, Any] | None): Additional keys to merge into the heartbeat payload; omitted if None.
    """
    payload: dict[str, Any] = {
        "service": service_name,
        "timestamp": datetime.now(UTC).isoformat(),
    }
    if extra:
        payload.update(extra)
    await asyncio.to_thread(_write_json, heartbeat_path(service_name), payload)


def heartbeat_status(service_name: str, *, max_age_seconds: int) -> dict[str, Any]:
    """
    Assess the freshness and status of a service heartbeat file.
    
    Parameters:
        service_name (str): Name of the service whose heartbeat file is checked.
        max_age_seconds (int): Maximum allowed age in seconds for a heartbeat to be considered fresh.
    
    Returns:
        dict[str, Any]: Status report with keys:
            - `status` (str): "ok" if the heartbeat is within `max_age_seconds`, "error" otherwise.
            - `fresh` (bool): `true` if the heartbeat age is <= `max_age_seconds`, `false` otherwise.
            - `age_seconds` (float): Heartbeat age in seconds, rounded to 3 decimal places.
            - `timestamp` (str): Original timestamp string read from the heartbeat file.
            - `service` (str): Service name from the heartbeat payload or the provided `service_name`.
            - `error` (str, optional): Error message when `status` is "error" (present for missing file, read/parse errors, or invalid/missing timestamp).
    """
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
    """
    Check whether a service's heartbeat is within the allowed age.
    
    Parameters:
        service_name (str): Name of the service whose heartbeat to check.
        max_age_seconds (int): Maximum allowed age in seconds for the heartbeat to be considered fresh.
    
    Returns:
        `true` if the heartbeat age is less than or equal to max_age_seconds, `false` otherwise.
    """
    return bool(heartbeat_status(service_name, max_age_seconds=max_age_seconds)["fresh"])


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    """
    Write a mapping as UTF-8 JSON to the specified file, creating parent directories if necessary.
    
    Parameters:
        path (Path): Destination file path for the JSON payload.
        payload (dict[str, Any]): JSON-serializable mapping to write to the file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
