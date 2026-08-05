"""Consumer for CREATE webhook events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.consumers.common import (
    build_listmonk_attributes,
    build_merge_key2_value,
    build_profile_from_pass,
    is_email_deliverable,
    merge_profile_with_old_data,
)
from app.consumers.email_conflict import is_email_unique_violation, resolve_users_email_conflict
from app.mq.queues import QUEUE_CREATE
from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_MERGE_FINALIZE,
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
    ExternalCallOutboxRepository,
    dedupe_key_for_invalid_email_block,
    dedupe_key_for_listmonk_sync,
    dedupe_key_for_merge_finalize,
)
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.merge_log import MergeLogRepository
from app.repositories.old_db import OldDBRepository
from app.repositories.users import UsersRepository
from app.schemas.webhook import WebhookPayload
from app.utils import to_optional_str
from app.workers.consent_sync_worker import parse_list_ids

logger = structlog.get_logger()

BONUS_REASON_MERGE_OLD_DB = "merge_old_db"
TEYCA_KEY1_BLOCKED = "blocked"


@dataclass(slots=True)
class CreateConsumerDeps:
    """Dependencies for create consumer business logic."""

    settings: Settings
    session: AsyncSession
    users_repo: UsersRepository
    listmonk_repo: ListmonkUsersRepository
    outbox_repo: ExternalCallOutboxRepository
    merge_repo: MergeLogRepository
    old_db_repo: OldDBRepository


async def handle(
    payload: dict[str, Any], *, deps: CreateConsumerDeps, wait_for_lock: bool = False
) -> None:
    """Handle CREATE payload."""
    trace_id = to_optional_str(payload.get("trace_id"))
    source_event_id = to_optional_str(payload.get("source_event_id"))
    event = WebhookPayload.model_validate(payload)
    user_id = event.pass_data.user_id
    logger.info(
        "create_consumer_start",
        user_id=user_id,
        trace_id=trace_id,
        source_event_id=source_event_id,
    )
    old_data = None
    merge_exists_hint = await deps.merge_repo.exists(user_id=user_id)
    if not merge_exists_hint:
        old_data = await deps.old_db_repo.get_user_data(phone=event.pass_data.phone)

    await deps.users_repo.lock_user(user_id=user_id, wait=wait_for_lock)

    merged_already = await deps.merge_repo.exists(user_id=user_id)
    if merged_already:
        logger.info(
            "create_merge_skipped_already_done",
            user_id=user_id,
            trace_id=trace_id,
            source_event_id=source_event_id,
        )
        old_data = None

    profile = build_profile_from_pass(event.pass_data)
    merge_result = merge_profile_with_old_data(profile, old_data)
    try:
        await deps.users_repo.upsert(user_id=user_id, profile=merge_result.profile)
    except IntegrityError as exc:
        if not is_email_unique_violation(exc):
            raise
        won = await resolve_users_email_conflict(
            session=deps.session,
            user_id=user_id,
            profile=merge_result.profile,
            source_event_type=event.type,
            source_event_id=source_event_id,
            trace_id=trace_id,
        )
        if not won:
            logger.info(
                "create_consumer_email_conflict_lost",
                user_id=user_id,
                trace_id=trace_id,
                source_event_id=source_event_id,
            )
            return
        logger.info(
            "create_consumer_email_conflict_won",
            user_id=user_id,
            trace_id=trace_id,
            source_event_id=source_event_id,
        )

    target_list_ids = parse_list_ids(deps.settings.listmonk_list_ids)
    existing = await deps.listmonk_repo.get_by_user_id(user_id=user_id)
    if not await is_email_deliverable(event.pass_data.email):
        await deps.outbox_repo.enqueue_latest(
            operation=OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
            dedupe_key=dedupe_key_for_invalid_email_block(user_id=user_id),
            user_id=user_id,
            payload={"status": TEYCA_KEY1_BLOCKED},
            trace_id=trace_id,
            source_event_id=source_event_id,
            queue_name=QUEUE_CREATE,
        )
        logger.info(
            "create_consumer_email_invalid_block_enqueued",
            user_id=user_id,
            trace_id=trace_id,
            source_event_id=source_event_id,
            email=event.pass_data.email,
            had_existing_mapping=existing is not None,
        )
        return

    logger.info(
        "create_consumer_listmonk_enqueue_start",
        user_id=user_id,
        trace_id=trace_id,
        source_event_id=source_event_id,
        subscriber_id=existing.subscriber_id if existing is not None else None,
    )
    attributes = build_listmonk_attributes(event.pass_data)
    await deps.outbox_repo.enqueue_latest(
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key=dedupe_key_for_listmonk_sync(user_id=user_id),
        user_id=user_id,
        payload={
            "email": event.pass_data.email,
            "list_ids": target_list_ids,
            "attributes": attributes,
            "subscriber_id": None if existing is None else int(existing.subscriber_id),
            "event_type": event.type,
        },
        trace_id=trace_id,
        source_event_id=source_event_id,
        queue_name=QUEUE_CREATE,
    )
    logger.info(
        "create_consumer_listmonk_enqueue_done",
        user_id=user_id,
        trace_id=trace_id,
        source_event_id=source_event_id,
        list_ids=target_list_ids,
    )

    merge_needs_write = merge_result.merged and (old_data is not None) and old_data.has_merge_data()
    merge_enqueued = False
    if merge_needs_write and not merged_already:
        old_bonus_value = old_data.bonus if old_data is not None else None
        merge_enqueued = await deps.outbox_repo.enqueue_once(
            operation=OUTBOX_OP_MERGE_FINALIZE,
            dedupe_key=dedupe_key_for_merge_finalize(user_id=user_id),
            user_id=user_id,
            payload={
                "bonus_done": False,
                "key2_done": False,
                "merge_logged": False,
                "old_bonus_value": old_bonus_value,
                "merge_key2_value": build_merge_key2_value(),
                "source_event_type": event.type,
            },
            trace_id=trace_id,
            source_event_id=source_event_id,
            queue_name=QUEUE_CREATE,
        )
        logger.info(
            "create_merge_enqueued",
            user_id=user_id,
            trace_id=trace_id,
            source_event_id=source_event_id,
            old_bonus_value=old_bonus_value,
            enqueue_created=merge_enqueued,
        )
    else:
        logger.info(
            "create_merge_not_applied",
            user_id=user_id,
            trace_id=trace_id,
            source_event_id=source_event_id,
            merge_log_exists_before=merged_already,
            old_data_found=old_data is not None,
        )

    logger.info(
        "create_consumer_processed",
        user_id=user_id,
        trace_id=trace_id,
        source_event_id=source_event_id,
        merge_applied_this_event=False,
        merge_log_exists_before=merged_already,
        listmonk_sync_enqueued=True,
        merge_enqueued=merge_enqueued,
        merge_reason=(BONUS_REASON_MERGE_OLD_DB if merge_enqueued else "none"),
    )
