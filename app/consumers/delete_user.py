"""Consumer for DELETE webhook events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from app.mq.queues import QUEUE_DELETE
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_DELETE,
    ExternalCallOutboxRepository,
    dedupe_key_for_listmonk_delete,
)
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.merge_log import MergeLogRepository
from app.repositories.users import UsersRepository
from app.schemas.webhook import WebhookPayload
from app.utils import to_optional_str

logger = structlog.get_logger()


@dataclass(slots=True)
class DeleteConsumerDeps:
    """Dependencies for delete consumer business logic."""

    users_repo: UsersRepository
    listmonk_repo: ListmonkUsersRepository
    merge_repo: MergeLogRepository
    bonus_accrual_repo: BonusAccrualRepository
    outbox_repo: ExternalCallOutboxRepository


async def handle(
    payload: dict[str, Any], *, deps: DeleteConsumerDeps, wait_for_lock: bool = False
) -> None:
    """Handle DELETE payload."""
    trace_id = to_optional_str(payload.get("trace_id"))
    source_event_id = to_optional_str(payload.get("source_event_id"))
    event = WebhookPayload.model_validate(payload)
    user_id = event.pass_data.user_id
    await deps.users_repo.lock_user(user_id=user_id, wait=wait_for_lock)

    existing = await deps.listmonk_repo.get_by_user_id(user_id=user_id)
    subscriber_id = existing.subscriber_id if existing is not None else None
    delete_enqueued = False
    if subscriber_id is not None:
        delete_enqueued = await deps.outbox_repo.enqueue_once(
            operation=OUTBOX_OP_LISTMONK_DELETE,
            dedupe_key=dedupe_key_for_listmonk_delete(user_id=user_id),
            user_id=user_id,
            payload={"subscriber_id": int(subscriber_id)},
            trace_id=trace_id,
            source_event_id=source_event_id,
            queue_name=QUEUE_DELETE,
        )

    await deps.listmonk_repo.delete_by_user_id(user_id=user_id)
    await deps.merge_repo.delete_by_user_id(user_id=user_id)
    await deps.bonus_accrual_repo.delete_by_user_id(user_id=user_id)
    await deps.users_repo.delete_by_user_id(user_id=user_id)

    logger.info(
        "delete_consumer_processed",
        user_id=user_id,
        trace_id=trace_id,
        source_event_id=source_event_id,
        subscriber_id=subscriber_id,
        delete_enqueued=delete_enqueued,
    )
