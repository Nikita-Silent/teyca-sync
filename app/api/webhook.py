"""Webhook endpoint: static token auth, parse body, publish to RabbitMQ by type."""

from datetime import UTC, datetime
from json import JSONDecodeError
from uuid import uuid4

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.auth import verify_webhook_token
from app.mq.publisher import MQPublisher
from app.schemas.webhook import WebhookPayload

logger = structlog.get_logger()

router = APIRouter(prefix="", tags=["webhook"])


def get_mq_publisher(request: Request) -> MQPublisher:
    """
    Retrieve the application's MQPublisher instance from the FastAPI request state.
    
    Returns:
        MQPublisher: The publisher stored at request.app.state.mq_publisher.
    """
    return request.app.state.mq_publisher


@router.post("")
async def webhook(
    request: Request,
    publisher: MQPublisher = Depends(get_mq_publisher),
    _auth: None = Depends(verify_webhook_token),
) -> dict:
    """
    Handle an incoming webhook: validate and enrich the payload, then publish it to the message queue by payload type.
    
    Parses the request JSON into a WebhookPayload, adds `trace_id`, `source_event_id`, and `received_at` metadata, and publishes the resulting message to RabbitMQ using the payload's `type`.
    
    Raises:
        HTTPException: If the request body is not valid JSON (returns HTTP 400).
    
    Returns:
        dict: A simple acknowledgment object `{"ok": True}` when the message has been published.
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
    """
    Obtain a trace identifier from the incoming HTTP request.
    
    Reads the "x-trace-id" header and returns its trimmed value if present and non-empty; if the header is missing or empty, returns a newly generated UUID string.
    
    Parameters:
        request (Request): The incoming HTTP request whose headers are inspected.
    
    Returns:
        str: The trace identifier from the "x-trace-id" header, or a generated UUID string when absent or empty.
    """
    raw = request.headers.get("x-trace-id", "").strip()
    return raw or str(uuid4())


def _extract_source_event_id(request: Request) -> str:
    """
    Retrieve the source event identifier from the request headers, or generate a new UUID if missing.
    
    Parameters:
        request (Request): Incoming HTTP request; the function reads the `x-event-id` header.
    
    Returns:
        source_event_id (str): The `x-event-id` header value trimmed of whitespace if present, otherwise a newly generated UUID string.
    """
    raw = request.headers.get("x-event-id", "").strip()
    return raw or str(uuid4())
