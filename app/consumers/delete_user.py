"""Consumer for DELETE webhook events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from app.clients.listmonk import ListmonkClientError, ListmonkSDKClient
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.merge_log import MergeLogRepository
from app.repositories.users import UsersRepository
from app.schemas.webhook import WebhookPayload

logger = structlog.get_logger()


@dataclass(slots=True)
class DeleteConsumerDeps:
    """Dependencies for delete consumer business logic."""

    users_repo: UsersRepository
    listmonk_repo: ListmonkUsersRepository
    merge_repo: MergeLogRepository
    bonus_accrual_repo: BonusAccrualRepository
    listmonk_client: ListmonkSDKClient
    session: AsyncSession


async def handle(
    payload: dict[str, Any], *, deps: DeleteConsumerDeps, wait_for_lock: bool = False
) -> None:
    """Handle DELETE payload."""
    event = WebhookPayload.model_validate(payload)
    user_id = event.pass_data.user_id
    await deps.users_repo.lock_user(user_id=user_id, wait=wait_for_lock)

    existing = await deps.listmonk_repo.get_by_user_id(user_id=user_id)
    subscriber_id = existing.subscriber_id if existing is not None else None

    await deps.listmonk_repo.delete_by_user_id(user_id=user_id)
    await deps.merge_repo.delete_by_user_id(user_id=user_id)
    await deps.bonus_accrual_repo.delete_by_user_id(user_id=user_id)
    await deps.users_repo.delete_by_user_id(user_id=user_id)
    await deps.session.commit()

    if subscriber_id is not None:
        try:
            await deps.listmonk_client.delete_subscriber(subscriber_id=subscriber_id)
        except ListmonkClientError as exc:
            logger.error(
                "delete_consumer_listmonk_delete_failed",
                user_id=user_id,
                subscriber_id=subscriber_id,
                error=str(exc),
            )

    logger.info(
        "delete_consumer_processed",
        user_id=user_id,
    )
