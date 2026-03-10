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


async def handle(payload: dict[str, Any], *, deps: DeleteConsumerDeps) -> None:
    """
    Process a DELETE webhook payload and remove the associated user from the database and Listmonk.
    
    Validates the incoming payload as a WebhookPayload, locks the targeted user, deletes Listmonk mapping, merge logs, bonus accruals, and the user record in the database, commits the transaction, then attempts to delete the corresponding Listmonk subscriber if one exists; logs an error if the Listmonk deletion fails and logs completion on success.
    
    Parameters:
        payload (dict[str, Any]): Incoming webhook payload conforming to the WebhookPayload schema.
        deps (DeleteConsumerDeps): Dependency container providing repositories, the Listmonk client, and an AsyncSession used to perform deletions and commit the transaction.
    """
    event = WebhookPayload.model_validate(payload)
    user_id = event.pass_data.user_id
    await deps.users_repo.lock_user(user_id=user_id)

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
