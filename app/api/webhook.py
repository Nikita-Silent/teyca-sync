"""Webhook endpoint: static token auth, parse body, publish to RabbitMQ by type."""

import json
from datetime import UTC, datetime
from json import JSONDecodeError
from typing import Any
from uuid import uuid4

import aio_pika
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from aio_pika.exceptions import CONNECTION_EXCEPTIONS

from app.api.auth import verify_webhook_token
from app.config import get_settings
from app.db.session import SessionLocal
from app.mq.publisher import MQPublisher
from app.schemas.webhook import WebhookPayload
from app.service_health import heartbeat_status

logger = structlog.get_logger()

router = APIRouter(prefix="", tags=["webhook"])
health_router = APIRouter(prefix="", tags=["health"])


def get_mq_publisher(request: Request) -> MQPublisher:
    return request.app.state.mq_publisher


@health_router.get("/live")
async def live() -> JSONResponse:
    live_check = {
        "app": await heartbeat_status("app", max_age_seconds=60),
    }
    checks = {
        "app": _build_check_payload("app", live_check["app"]),
    }
    is_healthy = checks["app"]["status"] == "ok"
    return JSONResponse(
        status_code=200 if is_healthy else 503,
        content={
            "status": "ok" if is_healthy else "error",
            "timestamp": datetime.now(UTC).isoformat(),
            "checks": checks,
        },
    )


@health_router.get("/ready")
async def ready() -> JSONResponse:
    settings = get_settings()
    database_error = await _check_database_health()
    rabbitmq_error = await _check_rabbitmq_health(settings.rabbitmq_url)

    checks: dict[str, dict[str, Any]] = {
        "database": _build_check_payload("database", database_error),
        "rabbitmq": _build_check_payload("rabbitmq", rabbitmq_error),
    }
    is_healthy = database_error is None and rabbitmq_error is None

    return JSONResponse(
        status_code=200 if is_healthy else 503,
        content={
            "status": "ok" if is_healthy else "error",
            "timestamp": datetime.now(UTC).isoformat(),
            "checks": checks,
        },
    )


@health_router.get("/health")
async def health() -> JSONResponse:
    live_response = await live()
    ready_response = await ready()
    live_payload = _decode_json_response(live_response)
    ready_payload = _decode_json_response(ready_response)
    checks: dict[str, dict[str, Any]] = {
        **live_payload["checks"],
        **ready_payload["checks"],
    }
    is_healthy = (
        live_response.status_code == 200
        and ready_response.status_code == 200
    )
    return JSONResponse(
        status_code=200 if is_healthy else 503,
        content={
            "status": "ok" if is_healthy else "error",
            "timestamp": datetime.now(UTC).isoformat(),
            "checks": checks,
        },
    )


@router.post("")
async def webhook(
    request: Request,
    publisher: MQPublisher = Depends(get_mq_publisher),
    _auth: None = Depends(verify_webhook_token),
) -> dict:
    """Accept webhook from Teyca: check Authorization token, parse body, publish to queue by type."""
    trace_id = _extract_trace_id(request)
    source_event_id = _extract_source_event_id(request)
    try:
        body = await request.json()
    except JSONDecodeError as exc:
        logger.warning(
            "webhook_invalid_json",
            trace_id=trace_id,
            source_event_id=source_event_id,
            error=str(exc),
        )
        raise HTTPException(status_code=400, detail="Invalid JSON body") from exc

    payload = WebhookPayload.model_validate(body)
    message = payload.model_dump(by_alias=True)
    message["trace_id"] = trace_id
    message["source_event_id"] = source_event_id
    message["received_at"] = datetime.now(UTC).isoformat()
    logger.info(
        "webhook_received",
        trace_id=trace_id,
        source_event_id=source_event_id,
        type=payload.type,
        user_id=payload.pass_data.user_id,
    )
    await publisher.publish_webhook(payload.type, message)
    return {"ok": True}


def _extract_trace_id(request: Request) -> str:
    raw = request.headers.get("x-trace-id", "").strip()
    return raw or str(uuid4())


def _extract_source_event_id(request: Request) -> str:
    raw = request.headers.get("x-event-id", "").strip()
    return raw or str(uuid4())


async def _check_database_health() -> str | None:
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        return str(exc)
    return None


async def _check_rabbitmq_health(rabbitmq_url: str) -> str | None:
    try:
        connection = await aio_pika.connect_robust(rabbitmq_url, timeout=5.0)
    except CONNECTION_EXCEPTIONS as exc:
        return str(exc)

    await connection.close()
    return None


def _build_check_payload(check_name: str, result: str | dict[str, Any] | None) -> dict[str, Any]:
    if result is None:
        return {"status": "ok"}
    if isinstance(result, dict):
        if result.get("status") == "ok":
            return result
        logger.error(
            "health_check_failed",
            check_name=check_name,
            error=result.get("error"),
            payload=result,
        )
        sanitized = dict(result)
        sanitized["error"] = "internal error"
        return sanitized
    logger.error(
        "health_check_failed",
        check_name=check_name,
        error=result,
    )
    return {"status": "error", "error": "internal error"}


def _decode_json_response(response: JSONResponse) -> dict[str, Any]:
    return json.loads(response.body.decode("utf-8"))
