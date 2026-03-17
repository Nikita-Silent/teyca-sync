"""Consumer for CREATE webhook events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from app.clients.listmonk import ListmonkSDKClient
from app.clients.teyca import BonusOperation, TeycaClient
from app.config import Settings
from app.consumers.common import (
    build_listmonk_attributes,
    build_merge_key2_value,
    build_profile_from_pass,
    is_valid_email,
    merge_profile_with_old_data,
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
    users_repo: UsersRepository
    listmonk_repo: ListmonkUsersRepository
    merge_repo: MergeLogRepository
    old_db_repo: OldDBRepository
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient


async def handle(payload: dict[str, Any], *, deps: CreateConsumerDeps) -> None:
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
    await deps.users_repo.lock_user(user_id=user_id)

    merged_already = await deps.merge_repo.exists(user_id=user_id)
    if merged_already:
        logger.info(
            "create_merge_skipped_already_done",
            user_id=user_id,
            trace_id=trace_id,
            source_event_id=source_event_id,
        )
    old_data = None
    if not merged_already:
        old_data = await deps.old_db_repo.get_user_data(phone=event.pass_data.phone)

    profile = build_profile_from_pass(event.pass_data)
    merge_result = merge_profile_with_old_data(profile, old_data)
    await deps.users_repo.upsert(user_id=user_id, profile=merge_result.profile)

    target_list_ids = parse_list_ids(deps.settings.listmonk_list_ids)
    existing = await deps.listmonk_repo.get_by_user_id(user_id=user_id)
    if not is_valid_email(event.pass_data.email):
        await deps.teyca_client.update_pass_fields(
            user_id=user_id,
            fields={"key1": TEYCA_KEY1_BLOCKED},
        )
        if existing is not None:
            await deps.listmonk_repo.mark_checked(
                user_id=user_id,
                pending=False,
                confirmed=False,
                status=TEYCA_KEY1_BLOCKED,
            )
        logger.info(
            "create_consumer_email_invalid_blocked",
            user_id=user_id,
            trace_id=trace_id,
            source_event_id=source_event_id,
            email=event.pass_data.email,
        )
        return

    logger.info(
        "create_consumer_listmonk_upsert_start",
        user_id=user_id,
        trace_id=trace_id,
        source_event_id=source_event_id,
        subscriber_id=existing.subscriber_id if existing is not None else None,
    )
    subscriber_state = await deps.listmonk_client.upsert_subscriber(
        email=event.pass_data.email,
        list_ids=target_list_ids,
        attributes=build_listmonk_attributes(event.pass_data),
        subscriber_id=existing.subscriber_id if existing is not None else None,
    )
    logger.info(
        "create_consumer_listmonk_upsert_done",
        user_id=user_id,
        trace_id=trace_id,
        source_event_id=source_event_id,
        subscriber_id=subscriber_state.subscriber_id,
        subscriber_status=subscriber_state.status,
        list_ids=subscriber_state.list_ids,
    )
    await deps.listmonk_repo.upsert(
        user_id=user_id,
        subscriber_id=subscriber_state.subscriber_id,
        email=event.pass_data.email,
        status=subscriber_state.status,
        list_ids=subscriber_state.list_ids,
        attributes=build_listmonk_attributes(event.pass_data),
    )

    merge_needs_write = merge_result.merged and (old_data is not None) and old_data.has_merge_data()
    if merge_needs_write and not merged_already:
        old_bonus_value = old_data.bonus if old_data is not None else None
        if old_bonus_value is not None and old_bonus_value > 0:
            bonus = BonusOperation.one_shot(value=str(old_bonus_value))
            await deps.teyca_client.accrue_bonuses(user_id=user_id, bonuses=[bonus])
        await deps.teyca_client.update_pass_fields(
            user_id=user_id,
            fields={"key2": build_merge_key2_value()},
        )
        await deps.merge_repo.create(
            user_id=user_id,
            source_event_type=event.type,
            source_event_id=source_event_id,
            trace_id=trace_id,
        )
        logger.info(
            "create_merge_applied",
            user_id=user_id,
            trace_id=trace_id,
            source_event_id=source_event_id,
            old_bonus_value=old_bonus_value,
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

    await deps.listmonk_repo.set_consent_pending(user_id=user_id)
    logger.info(
        "create_consumer_processed",
        user_id=user_id,
        trace_id=trace_id,
        source_event_id=source_event_id,
        merge_applied_this_event=merge_needs_write and not merged_already,
        merge_log_exists_before=merged_already,
        merge_reason=(
            BONUS_REASON_MERGE_OLD_DB if merge_needs_write and not merged_already else "none"
        ),
    )
