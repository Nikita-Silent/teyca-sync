"""Consumer for CREATE webhook events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog

from app.clients.listmonk import ListmonkSDKClient
from app.clients.teyca import BonusOperation, TeycaClient
from app.config import Settings
from app.consumers.common import (
    build_merge_key2_value,
    build_listmonk_attributes,
    build_profile_from_pass,
    is_valid_email,
    merge_profile_with_old_data,
)
from app.repositories.merge_log import MergeLogRepository
from app.repositories.old_db import OldDBRepository
from app.repositories.users import UsersRepository
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.schemas.webhook import WebhookPayload
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
    """
    Process a CREATE webhook payload and synchronize user state across repositories and external services.
    
    Validates and parses the incoming webhook payload, merges the provided profile with any available old-database data, upserts the resulting user profile, and ensures Listmonk and TEYCA are updated accordingly. If the payload contains an invalid email the function blocks the user in TEYCA and marks Listmonk as blocked (if present) and returns early. When a merge with old data is applied (and not previously recorded), the function may accrue a one-shot bonus via TEYCA, update merge-related pass fields, and create a merge log entry. Finally, it sets the user's Listmonk consent state to pending and emits structured logs throughout processing.
    
    Parameters:
        payload (dict[str, Any]): Raw webhook payload for a CREATE event; must be a structure compatible with WebhookPayload.model_validate.
        deps (CreateConsumerDeps): Dependency container providing repositories and clients used to perform upserts, merges, external calls, and logging.
    """
    trace_id = _to_optional_str(payload.get("trace_id"))
    source_event_id = _to_optional_str(payload.get("source_event_id"))
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
        merge_create_kwargs: dict[str, object] = {
            "user_id": user_id,
            "source_event_type": event.type,
        }
        if source_event_id is not None:
            merge_create_kwargs["source_event_id"] = source_event_id
        if trace_id is not None:
            merge_create_kwargs["trace_id"] = trace_id
        await deps.merge_repo.create(**merge_create_kwargs)
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
        merge_reason=BONUS_REASON_MERGE_OLD_DB if merge_needs_write and not merged_already else "none",
    )


def _to_optional_str(raw: object) -> str | None:
    """
    Convert a value to a trimmed string or return None when not applicable.
    
    Parameters:
        raw (object): Value to convert; only string inputs are considered.
    
    Returns:
        str | None: The input string with surrounding whitespace removed if it is a non-empty string, `None` otherwise.
    """
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    return value or None
