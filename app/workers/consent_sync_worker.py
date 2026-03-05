"""Periodic worker: sync Listmonk consent and accrue consent bonuses."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.listmonk import ListmonkSDKClient
from app.clients.teyca import BonusOperation, TeycaAPIError, TeycaClient
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.bonus_accrual import BonusAccrualRepository
from app.repositories.listmonk_users import ListmonkUsersRepository

logger = structlog.get_logger()

BONUS_REASON_EMAIL_CONSENT = "email_consent"


def parse_list_ids(raw_list_ids: str) -> list[int]:
    """Parse comma-separated LISTMONK_LIST_IDS."""
    result: list[int] = []
    for chunk in raw_list_ids.split(","):
        stripped = chunk.strip()
        if not stripped:
            continue
        try:
            result.append(int(stripped))
        except ValueError:
            continue
    return result


@dataclass(slots=True)
class ConsentSyncWorker:
    """Runs consent sync loop for pending users."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient
    teyca_client: TeycaClient

    async def _process_pending_user(
        self,
        *,
        pending: Any,
        target_list_ids: list[int],
        listmonk_repo: ListmonkUsersRepository,
        accrual_repo: BonusAccrualRepository,
    ) -> None:
        user_id = int(pending.user_id)
        subscriber_id = int(pending.subscriber_id)
        idempotency_key = f"{BONUS_REASON_EMAIL_CONSENT}:{user_id}"

        subscriber = await self.listmonk_client.get_subscriber_state(subscriber_id=subscriber_id)
        if subscriber is None:
            await listmonk_repo.mark_checked(user_id=user_id, pending=True, confirmed=False)
            logger.info(
                "consent_sync_subscriber_not_found",
                user_id=user_id,
                subscriber_id=subscriber_id,
            )
            return

        confirmed = subscriber.is_confirmed_for_any(target_list_ids=target_list_ids)
        if not confirmed:
            await listmonk_repo.mark_checked(user_id=user_id, pending=True, confirmed=False)
            logger.info(
                "consent_sync_not_confirmed",
                user_id=user_id,
                subscriber_id=subscriber_id,
                status=subscriber.status,
            )
            return

        reserved = await accrual_repo.reserve(
            user_id=user_id,
            reason=BONUS_REASON_EMAIL_CONSENT,
            idempotency_key=idempotency_key,
            payload={"subscriber_id": subscriber_id, "list_ids": subscriber.list_ids},
        )
        if not reserved:
            await listmonk_repo.mark_checked(user_id=user_id, pending=False, confirmed=True)
            logger.info(
                "consent_sync_already_accrued",
                user_id=user_id,
                subscriber_id=subscriber_id,
            )
            return

        bonus_operation = BonusOperation.one_shot(
            value=self.settings.consent_bonus_value,
            ttl_days=self.settings.consent_bonus_ttl_days,
        )
        try:
            await self.teyca_client.accrue_bonuses(
                user_id=user_id,
                bonuses=[bonus_operation],
            )
            await accrual_repo.mark_done(idempotency_key=idempotency_key)
            await listmonk_repo.mark_checked(user_id=user_id, pending=False, confirmed=True)
            logger.info(
                "consent_sync_accrual_done",
                user_id=user_id,
                subscriber_id=subscriber_id,
            )
        except TeycaAPIError as exc:
            await accrual_repo.mark_failed(idempotency_key=idempotency_key, error_text=str(exc))
            await listmonk_repo.mark_checked(user_id=user_id, pending=True, confirmed=False)
            logger.error(
                "consent_sync_accrual_failed",
                user_id=user_id,
                subscriber_id=subscriber_id,
                error=str(exc),
            )

    async def run_once(self) -> int:
        """Process one batch of pending users. Returns processed count."""
        target_list_ids = parse_list_ids(self.settings.listmonk_list_ids)
        batch_size = max(1, self.settings.consent_sync_batch_size)

        processed = 0
        async with self.session_factory() as session:
            listmonk_repo = ListmonkUsersRepository(session)
            accrual_repo = BonusAccrualRepository(session)
            pending_users = await listmonk_repo.get_pending_batch(limit=batch_size)

            for pending in pending_users:
                processed += 1
                await self._process_pending_user(
                    pending=pending,
                    target_list_ids=target_list_ids,
                    listmonk_repo=listmonk_repo,
                    accrual_repo=accrual_repo,
                )

            await session.commit()
        return processed


def build_consent_sync_worker() -> ConsentSyncWorker:
    """Build worker instance from application settings."""
    settings = get_settings()
    return ConsentSyncWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
        teyca_client=TeycaClient(settings),
    )
