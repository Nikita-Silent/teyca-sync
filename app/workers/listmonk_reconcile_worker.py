"""Periodic worker: reconcile missing subscriber->user links."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.listmonk import ListmonkSDKClient, SubscriberDelta
from app.config import Settings, get_settings
from app.db.session import SessionLocal
from app.repositories.listmonk_users import ListmonkUsersRepository
from app.repositories.sync_state import SyncStateRepository
from app.repositories.users import UsersRepository
from app.workers.consent_sync_worker import parse_list_ids

logger = structlog.get_logger()

RECONCILE_SOURCE = "listmonk_reconcile"
CONSISTENCY_SOURCE = "listmonk_consistency"
CONSISTENCY_LIST_ID = 0


@dataclass(slots=True)
class ListmonkReconcileWorker:
    """Restores missing mappings in listmonk_users."""

    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient

    async def run_once(self) -> int:
        """
        Process one incremental reconcile batch across configured ListMonk list IDs, update watermarks, run a consistency scan, and persist any restored subscriber-to-user mappings.
        
        Processes updated subscribers for each configured list since the last stored watermark (up to the configured batch size), attempts to restore missing subscriber-to-user mappings, runs a follow-up consistency scan to find and restore additional missing subscribers, and commits all changes.
        
        Returns:
            int: Total number of mappings restored during this run (reconcile + consistency).
        """
        target_list_ids = parse_list_ids(self.settings.listmonk_list_ids)
        batch_size = max(1, self.settings.consent_sync_batch_size)
        metrics = ReconcileMetrics(batch_size=batch_size)

        if not target_list_ids:
            logger.info("listmonk_reconcile_no_target_lists")
            return 0

        async with self.session_factory() as session:
            listmonk_repo = ListmonkUsersRepository(session)
            users_repo = UsersRepository(session)
            sync_repo = SyncStateRepository(session)

            for list_id in target_list_ids:
                state = await sync_repo.get_or_create(source=RECONCILE_SOURCE, list_id=list_id)
                deltas = await self.listmonk_client.get_updated_subscribers(
                    list_id=list_id,
                    watermark_updated_at=state.watermark_updated_at,
                    watermark_subscriber_id=state.watermark_subscriber_id,
                    limit=batch_size,
                )
                metrics.deltas_fetched += len(deltas)
                if not deltas:
                    continue

                last_updated_at: datetime | None = None
                last_subscriber_id: int | None = None
                for delta in deltas:
                    metrics.scanned += 1
                    last_updated_at = delta.updated_at
                    last_subscriber_id = delta.subscriber_id
                    await self._reconcile_delta(
                        delta=delta,
                        list_id=list_id,
                        listmonk_repo=listmonk_repo,
                        users_repo=users_repo,
                        metrics=metrics,
                    )

                await sync_repo.update_watermark(
                    source=RECONCILE_SOURCE,
                    list_id=list_id,
                    updated_at=last_updated_at,
                    subscriber_id=last_subscriber_id,
                )
                logger.info(
                    "listmonk_reconcile_list_processed",
                    list_id=list_id,
                    deltas=len(deltas),
                    watermark_updated_at=last_updated_at.isoformat() if last_updated_at else None,
                    watermark_subscriber_id=last_subscriber_id,
                )

            await self._run_consistency_scan(
                listmonk_repo=listmonk_repo,
                sync_repo=sync_repo,
                metrics=metrics,
                limit=batch_size,
            )
            await session.commit()
        logger.info(
            "listmonk_reconcile_metrics",
            batch_size=metrics.batch_size,
            scanned=metrics.scanned,
            deltas_fetched=metrics.deltas_fetched,
            already_mapped=metrics.already_mapped,
            restored=metrics.restored,
            mapped_by_attribute=metrics.mapped_by_attribute,
            mapped_by_email=metrics.mapped_by_email,
            attribute_user_not_found=metrics.attribute_user_not_found,
            invalid_attribute_user_id=metrics.invalid_attribute_user_id,
            email_not_found=metrics.email_not_found,
            email_ambiguous=metrics.email_ambiguous,
            consistency_scanned=metrics.consistency_scanned,
            consistency_missing=metrics.consistency_missing,
            consistency_restored=metrics.consistency_restored,
            consistency_errors=metrics.consistency_errors,
        )
        return metrics.restored + metrics.consistency_restored

    async def _run_consistency_scan(
        self,
        *,
        listmonk_repo: ListmonkUsersRepository,
        sync_repo: SyncStateRepository,
        metrics: "ReconcileMetrics",
        limit: int,
    ) -> None:
        """
        Perform a forward consistency scan of stored Listmonk user records and restore any missing subscribers.
        
        This updates the CONSISTENCY_SOURCE watermark to continue scanning across runs. For each row after the last scanned user_id it verifies the live subscriber state; when a subscriber is missing it attempts to restore the subscriber via the Listmonk client, upserts the restored mapping, marks consent pending, and updates the provided metrics counters. Errors during restoration increment the consistency_errors metric and are logged; the watermark is advanced to the last processed user_id on completion.
        
        Parameters:
            listmonk_repo: Repository for reading and upserting Listmonk user rows.
            sync_repo: Repository used to read and update the consistency scan watermark.
            metrics: ReconcileMetrics instance used to accumulate scan counters.
            limit: Maximum number of rows to process in this scan invocation.
        """
        state = await sync_repo.get_or_create(source=CONSISTENCY_SOURCE, list_id=CONSISTENCY_LIST_ID)
        last_user_id = int(state.watermark_subscriber_id or 0)
        rows = await listmonk_repo.get_batch_after_user_id(last_user_id=last_user_id, limit=limit)
        if not rows:
            # Restart round-robin scan from the beginning.
            await sync_repo.update_watermark(
                source=CONSISTENCY_SOURCE,
                list_id=CONSISTENCY_LIST_ID,
                updated_at=None,
                subscriber_id=0,
            )
            return

        current_last_user_id = last_user_id
        for row in rows:
            current_last_user_id = int(row.user_id)
            metrics.consistency_scanned += 1
            state_live = await self.listmonk_client.get_subscriber_state(subscriber_id=int(row.subscriber_id))
            if state_live is not None:
                continue

            metrics.consistency_missing += 1
            parsed_list_ids = _parse_list_ids_text(row.list_ids)
            try:
                restored = await self.listmonk_client.restore_subscriber(
                    email=row.email,
                    list_ids=parsed_list_ids,
                    attributes=row.attributes,
                    desired_status=row.status,
                )
            except Exception as exc:
                metrics.consistency_errors += 1
                logger.error(
                    "listmonk_reconcile_restore_failed",
                    user_id=int(row.user_id),
                    old_subscriber_id=int(row.subscriber_id),
                    error=str(exc),
                )
                continue

            await listmonk_repo.upsert(
                user_id=int(row.user_id),
                subscriber_id=int(restored.subscriber_id),
                email=row.email,
                status=restored.status,
                list_ids=restored.list_ids,
                attributes=row.attributes,
            )
            await listmonk_repo.set_consent_pending(user_id=int(row.user_id))
            metrics.consistency_restored += 1
            logger.info(
                "listmonk_reconcile_subscriber_restored",
                user_id=int(row.user_id),
                old_subscriber_id=int(row.subscriber_id),
                new_subscriber_id=int(restored.subscriber_id),
                status=restored.status,
            )

        await sync_repo.update_watermark(
            source=CONSISTENCY_SOURCE,
            list_id=CONSISTENCY_LIST_ID,
            updated_at=None,
            subscriber_id=current_last_user_id,
        )

    async def _reconcile_delta(
        self,
        *,
        delta: SubscriberDelta,
        list_id: int,
        listmonk_repo: ListmonkUsersRepository,
        users_repo: UsersRepository,
        metrics: "ReconcileMetrics",
    ) -> None:
        """
        Attempt to restore a missing mapping between a Listmonk subscriber and a local user using subscriber attributes or email, update repositories, and increment reconciliation metrics.
        
        Parameters:
            delta (SubscriberDelta): Subscriber delta containing subscriber_id, email, status, list_ids, and attributes used to resolve a user.
            list_id (int): The Listmonk list identifier associated with the delta (used for logging/metrics context).
            listmonk_repo (ListmonkUsersRepository): Repository used to read/insert subscriber↔user mappings and set consent pending.
            users_repo (UsersRepository): Repository used to look up users by user_id or email.
            metrics (ReconcileMetrics): Metrics counters updated to reflect mapping outcomes (e.g., already_mapped, mapped_by_attribute, mapped_by_email, restored, various not-found/ambiguous counters).
        """
        existing = await listmonk_repo.get_by_subscriber_id(subscriber_id=delta.subscriber_id)
        if existing is not None:
            metrics.already_mapped += 1
            return

        user_id: int | None = None
        attr_user_id = _extract_attr_user_id(delta.attributes)
        if attr_user_id is not None:
            user = await users_repo.get_by_user_id(user_id=attr_user_id)
            if user is not None:
                user_id = attr_user_id
                metrics.mapped_by_attribute += 1
            else:
                metrics.attribute_user_not_found += 1
                logger.info(
                    "listmonk_reconcile_attribute_user_not_found",
                    subscriber_id=delta.subscriber_id,
                    list_id=list_id,
                    attribute_user_id=attr_user_id,
                )
        elif delta.attributes and "user_id" in delta.attributes:
            metrics.invalid_attribute_user_id += 1
            logger.info(
                "listmonk_reconcile_invalid_attribute_user_id",
                subscriber_id=delta.subscriber_id,
                list_id=list_id,
                attribute_user_id=delta.attributes.get("user_id"),
            )

        if user_id is None and delta.email:
            matched_user_ids = await users_repo.get_user_ids_by_email(email=delta.email, limit=2)
            if len(matched_user_ids) == 1:
                user_id = matched_user_ids[0]
                metrics.mapped_by_email += 1
            elif len(matched_user_ids) > 1:
                metrics.email_ambiguous += 1
                logger.info(
                    "listmonk_reconcile_email_ambiguous",
                    subscriber_id=delta.subscriber_id,
                    list_id=list_id,
                    email=delta.email,
                    user_ids=matched_user_ids,
                )
            else:
                metrics.email_not_found += 1
                logger.info(
                    "listmonk_reconcile_email_not_found",
                    subscriber_id=delta.subscriber_id,
                    list_id=list_id,
                    email=delta.email,
                )
        elif user_id is None:
            metrics.email_not_found += 1

        if user_id is None:
            logger.info(
                "listmonk_reconcile_unmapped",
                subscriber_id=delta.subscriber_id,
                list_id=list_id,
                email=delta.email,
            )
            return

        await listmonk_repo.upsert(
            user_id=user_id,
            subscriber_id=delta.subscriber_id,
            email=delta.email,
            status=delta.status,
            list_ids=delta.list_ids,
            attributes=delta.attributes,
        )
        await listmonk_repo.set_consent_pending(user_id=user_id)
        metrics.restored += 1
        logger.info(
            "listmonk_reconcile_mapping_restored",
            user_id=user_id,
            subscriber_id=delta.subscriber_id,
            list_id=list_id,
            source="attribute" if attr_user_id == user_id else "email",
        )


def build_listmonk_reconcile_worker() -> ListmonkReconcileWorker:
    """Build worker instance from application settings."""
    settings = get_settings()
    return ListmonkReconcileWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
    )


@dataclass(slots=True)
class ReconcileMetrics:
    """Aggregated counters for one reconcile run."""

    batch_size: int
    scanned: int = 0
    deltas_fetched: int = 0
    already_mapped: int = 0
    restored: int = 0
    mapped_by_attribute: int = 0
    mapped_by_email: int = 0
    attribute_user_not_found: int = 0
    invalid_attribute_user_id: int = 0
    email_not_found: int = 0
    email_ambiguous: int = 0
    consistency_scanned: int = 0
    consistency_missing: int = 0
    consistency_restored: int = 0
    consistency_errors: int = 0


def _extract_attr_user_id(attributes: dict[str, Any] | None) -> int | None:
    """
    Extracts a numeric user ID from an attributes mapping if present.
    
    Parameters:
        attributes (dict[str, Any] | None): Mapping of subscriber attributes; may contain a "user_id" key whose value can be an integer or a numeric string.
    
    Returns:
        int | None: The `user_id` as an integer if attributes contains a numeric `user_id`, otherwise `None`.
    """
    if not attributes:
        return None
    raw = attributes.get("user_id")
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.strip().isdigit():
        return int(raw.strip())
    return None


def _parse_list_ids_text(raw: str | None) -> list[int]:
    """
    Parse a comma-separated string of list IDs into a list of integers.
    
    Parameters:
    	raw (str | None): Comma-separated text containing integer IDs (e.g. "1, 2,3"), or None.
    
    Returns:
    	list[int]: Parsed integer IDs. Non-numeric or empty tokens are ignored; returns an empty list if `raw` is None or no valid integers are found.
    """
    if raw is None:
        return []
    result: list[int] = []
    for chunk in raw.split(","):
        stripped = chunk.strip()
        if not stripped:
            continue
        try:
            result.append(int(stripped))
        except ValueError:
            continue
    return result
