"""Webhook endpoint: static token auth, parse body, publish to RabbitMQ by type."""

import structlog
from fastapi import APIRouter, Depends, Request

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
    body = await request.json()
    payload = WebhookPayload.model_validate(body)
    message = payload.model_dump(by_alias=True)
    await publisher.publish_webhook(payload.type, message)
    logger.info(
        "webhook_published",
        type=payload.type,
        user_id=payload.pass_data.user_id,
    )
    return {"ok": True}
