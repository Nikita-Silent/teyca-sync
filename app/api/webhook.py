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

from app.api.auth import verify_webhook_token
from app.config import get_settings
from app.db.session import SessionLocal
from app.mq.publisher import MQPublisher
from app.schemas.webhook import WebhookPayload
from app.service_health import heartbeat_status

logger = structlog.get_logger()

router = APIRouter(prefix="", tags=["webhook"])


def get_mq_publisher(request: Request) -> MQPublisher:
    """
    Retrieve the application's MQPublisher instance from the FastAPI request state.
    
    Returns:
        MQPublisher: The MQPublisher stored on request.app.state.mq_publisher.
    """
    return request.app.state.mq_publisher


@router.get("/live")
async def live() -> JSONResponse:
    """
    Perform a liveness check for the application.
    
    Builds a JSON response containing an overall status, ISO8601 timestamp, and a "checks" object with the app heartbeat check (using a 60‑second freshness window). 
    
    Returns:
        JSONResponse: Response body with keys:
            - "status": "ok" if the app check is healthy, "error" otherwise.
            - "timestamp": ISO8601 timestamp when the check ran.
            - "checks": dictionary of individual check results (includes "app").
        Uses HTTP 200 when healthy and HTTP 503 when unhealthy.
    """
    live_check = {
        "app": heartbeat_status("app", max_age_seconds=60),
    }
    is_healthy = live_check["app"]["status"] == "ok"
    return JSONResponse(
        status_code=200 if is_healthy else 503,
        content={
            "status": "ok" if is_healthy else "error",
            "timestamp": datetime.now(UTC).isoformat(),
            "checks": live_check,
        },
    )


@router.get("/ready")
async def ready() -> JSONResponse:
    """
    Perform readiness checks for the database and RabbitMQ and return a JSONResponse summarizing their status.
    
    The response body contains:
    - "status": "ok" when all checks pass, otherwise "error".
    - "timestamp": ISO 8601 timestamp of the check.
    - "checks": a mapping of service names ("database", "rabbitmq") to their individual check payloads; each payload is {"status": "ok"} or {"status": "error", "error": "<message>"}.
    
    Returns:
        JSONResponse: HTTP 200 if both checks are healthy, HTTP 503 otherwise; body as described above.
    """
    settings = get_settings()
    database_error = await _check_database_health()
    rabbitmq_error = await _check_rabbitmq_health(settings.rabbitmq_url)

    checks: dict[str, dict[str, Any]] = {
        "database": _build_check_payload(database_error),
        "rabbitmq": _build_check_payload(rabbitmq_error),
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


@router.get("/health")
async def health() -> JSONResponse:
    """
    Compose overall service health by aggregating liveness and readiness checks.
    
    Calls `live()` and `ready()`, merges their `"checks"` objects, and returns a JSON response containing:
    - `"status"`: `"ok"` if both checks are healthy, `"error"` otherwise.
    - `"timestamp"`: current UTC time in ISO 8601 format.
    - `"checks"`: combined checks from liveness and readiness.
    
    Returns:
        JSONResponse: HTTP 200 when both liveness and readiness are healthy, HTTP 503 otherwise; the response body contains `status`, `timestamp`, and `checks`.
    """
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
    """
    Handle an incoming webhook: validate the request body, enrich it with tracing metadata, and publish it to the message queue by message type.
    
    Parses the request JSON and validates it against the WebhookPayload schema. Adds `trace_id`, `source_event_id`, and `received_at` to the published message, logs receipt, and publishes the message using the provided MQPublisher.
    
    Raises:
        HTTPException: 400 if the request body is not valid JSON.
    
    Returns:
        dict: `{"ok": True}` on successful validation and publishing.
    """
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
    """
    Obtain the source event identifier from the request headers or generate a new one.
    
    Reads the `x-event-id` header and returns its trimmed value if present and not empty; otherwise returns a newly generated UUID4 string.
    
    Parameters:
        request (Request): The incoming request whose headers are inspected.
    
    Returns:
        str: The `x-event-id` header value when available and non-empty, or a new UUID4 string.
    """
    raw = request.headers.get("x-event-id", "").strip()
    return raw or str(uuid4())


async def _check_database_health() -> str | None:
    """
    Check database connectivity by executing a simple query.
    
    Attempts to open a database session and run `SELECT 1`. 
    
    Returns:
        str: Error message if the check fails.
        None: If the database is reachable and the query succeeds.
    """
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        return str(exc)
    return None


async def _check_rabbitmq_health(rabbitmq_url: str) -> str | None:
    """
    Check connectivity to a RabbitMQ server.
    
    Parameters:
        rabbitmq_url (str): AMQP connection URL for the RabbitMQ server.
    
    Returns:
        None if a connection could be established and closed successfully, otherwise an error string describing the failure.
    """
    try:
        connection = await aio_pika.connect_robust(rabbitmq_url, timeout=5.0)
    except Exception as exc:
        return str(exc)

    await connection.close()
    return None


def _build_check_payload(error: str | None) -> dict[str, str]:
    """
    Builds a health check payload from an optional error message.
    
    Parameters:
        error (str | None): Error message produced by a health check, or None if the check succeeded.
    
    Returns:
        dict[str, str]: A payload with `{"status": "ok"}` when `error` is None, otherwise `{"status": "error", "error": <message>}`.
    """
    if error is None:
        return {"status": "ok"}
    return {"status": "error", "error": error}


def _decode_json_response(response: JSONResponse) -> dict[str, Any]:
    """
    Decode the JSON body from a FastAPI JSONResponse into a Python dict.
    
    Parameters:
        response (JSONResponse): The response whose body contains JSON bytes.
    
    Returns:
        dict[str, Any]: The decoded JSON object from the response body.
    """
    return json.loads(response.body.decode("utf-8"))
