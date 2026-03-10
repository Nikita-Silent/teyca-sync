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
    return request.app.state.mq_publisher


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
