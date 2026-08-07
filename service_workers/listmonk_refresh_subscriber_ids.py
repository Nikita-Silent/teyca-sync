"""One-off refresh of local Listmonk subscriber ids by email."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, cast

import httpx
import structlog
from sqlalchemy import Select, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.listmonk import ListmonkClientError, ListmonkSDKClient, SubscriberProfile
from app.config import Settings, get_settings
from app.db.models import ListmonkUser
from app.db.session import SessionLocal

logger = structlog.get_logger()


@dataclass(slots=True)
class RefreshRow:
    user_id: int
    subscriber_id: int
    email: str | None


@dataclass(slots=True)
class ResolvedSubscriber:
    user_id: int
    email: str
    profile: SubscriberProfile


@dataclass(slots=True)
class LookupResult:
    row: RefreshRow
    email: str | None
    profile: SubscriberProfile | None
    error: ListmonkClientError | httpx.HTTPError | None = None


@dataclass(slots=True)
class RefreshMetrics:
    scanned: int = 0
    matched: int = 0
    updated: int = 0
    unchanged: int = 0
    no_email: int = 0
    not_found: int = 0
    lookup_errors: int = 0
    duplicate_target_ids: int = 0
    staged_conflicts: int = 0
    not_found_emails: list[str] | None = None
    lookup_error_details: list[str] | None = None
    duplicate_target_details: list[str] | None = None


@dataclass(slots=True)
class ListmonkSubscriberIdRefreshWorker:
    settings: Settings
    session_factory: async_sessionmaker[AsyncSession]
    listmonk_client: ListmonkSDKClient

    async def run(
        self,
        *,
        batch_size: int,
        apply: bool,
        limit: int | None = None,
        concurrency: int = 10,
    ) -> RefreshMetrics:
        """Refresh all local Listmonk rows that can be matched by email."""
        metrics = RefreshMetrics()
        last_user_id = 0
        normalized_batch_size = max(1, batch_size)
        normalized_concurrency = max(1, concurrency)

        while True:
            remaining = None if limit is None else max(0, limit - metrics.scanned)
            if remaining == 0:
                break
            current_batch_size = normalized_batch_size
            if remaining is not None:
                current_batch_size = min(current_batch_size, remaining)
            rows = await self._load_batch(last_user_id=last_user_id, limit=current_batch_size)
            if not rows:
                break

            last_user_id = rows[-1].user_id
            resolved = await self._resolve_batch(
                rows=rows,
                metrics=metrics,
                concurrency=normalized_concurrency,
            )
            if apply and resolved:
                updated, staged_conflicts = await self._apply_resolved(resolved=resolved)
                metrics.updated += updated
                metrics.staged_conflicts += staged_conflicts

            logger.info(
                "listmonk_subscriber_id_refresh_batch_processed",
                apply=apply,
                scanned=metrics.scanned,
                matched=metrics.matched,
                updated=metrics.updated,
                unchanged=metrics.unchanged,
                not_found=metrics.not_found,
                lookup_errors=metrics.lookup_errors,
                concurrency=normalized_concurrency,
                last_user_id=last_user_id,
            )

        logger.info(
            "listmonk_subscriber_id_refresh_completed",
            apply=apply,
            scanned=metrics.scanned,
            matched=metrics.matched,
            updated=metrics.updated,
            unchanged=metrics.unchanged,
            no_email=metrics.no_email,
            not_found=metrics.not_found,
            lookup_errors=metrics.lookup_errors,
            duplicate_target_ids=metrics.duplicate_target_ids,
            staged_conflicts=metrics.staged_conflicts,
            concurrency=normalized_concurrency,
        )
        return metrics

    async def _load_batch(self, *, last_user_id: int, limit: int) -> list[RefreshRow]:
        async with self.session_factory() as session:
            stmt: Select[tuple[int, int, str | None]] = (
                select(ListmonkUser.user_id, ListmonkUser.subscriber_id, ListmonkUser.email)
                .where(ListmonkUser.user_id > last_user_id)
                .order_by(ListmonkUser.user_id.asc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            return [
                RefreshRow(
                    user_id=int(user_id),
                    subscriber_id=int(subscriber_id),
                    email=email,
                )
                for user_id, subscriber_id, email in result.all()
            ]

    async def _resolve_batch(
        self, *, rows: list[RefreshRow], metrics: RefreshMetrics, concurrency: int = 10
    ) -> list[ResolvedSubscriber]:
        resolved: list[ResolvedSubscriber] = []
        target_user_by_subscriber_id: dict[int, int] = {}
        duplicate_subscriber_ids: set[int] = set()
        semaphore = asyncio.Semaphore(max(1, concurrency))

        lookup_results = await asyncio.gather(
            *(self._lookup_row(row=row, semaphore=semaphore) for row in rows)
        )

        for lookup_result in lookup_results:
            row = lookup_result.row
            metrics.scanned += 1
            normalized_email = lookup_result.email
            if normalized_email is None:
                metrics.no_email += 1
                continue
            if lookup_result.error is not None:
                metrics.lookup_errors += 1
                _append_metric_detail(
                    metrics,
                    "lookup_error_details",
                    f"{normalized_email}: {type(lookup_result.error).__name__}: "
                    f"{lookup_result.error}",
                )
                logger.error(
                    "listmonk_subscriber_id_refresh_lookup_failed",
                    user_id=row.user_id,
                    email=normalized_email,
                    error=str(lookup_result.error),
                    error_type=type(lookup_result.error).__name__,
                )
                continue
            profile = lookup_result.profile
            if profile is None:
                metrics.not_found += 1
                _append_metric_detail(metrics, "not_found_emails", normalized_email)
                logger.warning(
                    "listmonk_subscriber_id_refresh_email_not_found",
                    user_id=row.user_id,
                    email=normalized_email,
                    old_subscriber_id=row.subscriber_id,
                )
                continue
            existing_user_id = target_user_by_subscriber_id.get(profile.subscriber_id)
            if existing_user_id is not None:
                duplicate_subscriber_ids.add(profile.subscriber_id)
                metrics.duplicate_target_ids += 1
                _append_metric_detail(
                    metrics,
                    "duplicate_target_details",
                    f"subscriber_id={profile.subscriber_id}, user_id={row.user_id}, "
                    f"existing_user_id={existing_user_id}, email={normalized_email}",
                )
                logger.error(
                    "listmonk_subscriber_id_refresh_duplicate_target_id",
                    subscriber_id=profile.subscriber_id,
                    user_id=row.user_id,
                    existing_user_id=existing_user_id,
                    email=normalized_email,
                )
                continue
            target_user_by_subscriber_id[profile.subscriber_id] = row.user_id
            if profile.subscriber_id in duplicate_subscriber_ids:
                metrics.duplicate_target_ids += 1
                continue
            metrics.matched += 1
            if profile.subscriber_id == row.subscriber_id:
                metrics.unchanged += 1
            resolved.append(
                ResolvedSubscriber(
                    user_id=row.user_id,
                    email=normalized_email,
                    profile=profile,
                )
            )
        if duplicate_subscriber_ids:
            resolved = [
                item
                for item in resolved
                if item.profile.subscriber_id not in duplicate_subscriber_ids
            ]
        return resolved

    async def _lookup_row(
        self, *, row: RefreshRow, semaphore: asyncio.Semaphore
    ) -> LookupResult:
        normalized_email = _normalize_email(row.email)
        if normalized_email is None:
            return LookupResult(row=row, email=None, profile=None)
        async with semaphore:
            try:
                profile = await self.listmonk_client.get_subscriber_profile_by_email(
                    email=normalized_email
                )
            except (ListmonkClientError, httpx.HTTPError) as exc:
                return LookupResult(
                    row=row,
                    email=normalized_email,
                    profile=None,
                    error=exc,
                )
        return LookupResult(row=row, email=normalized_email, profile=profile)

    async def _apply_resolved(self, *, resolved: list[ResolvedSubscriber]) -> tuple[int, int]:
        matched_user_ids = [item.user_id for item in resolved]
        target_subscriber_ids = [item.profile.subscriber_id for item in resolved]
        async with self.session_factory() as session:
            try:
                conflict_rows = await self._load_conflicting_rows(
                    session=session,
                    matched_user_ids=matched_user_ids,
                    target_subscriber_ids=target_subscriber_ids,
                )
                for user_id in [*matched_user_ids, *conflict_rows]:
                    await session.execute(
                        update(ListmonkUser)
                        .where(ListmonkUser.user_id == user_id)
                        .values(subscriber_id=_temporary_subscriber_id(user_id))
                    )
                for item in resolved:
                    await session.execute(
                        update(ListmonkUser)
                        .where(ListmonkUser.user_id == item.user_id)
                        .values(
                            subscriber_id=item.profile.subscriber_id,
                            email=_normalize_email(item.profile.email) or item.email,
                            status=item.profile.status,
                            list_ids=",".join(str(list_id) for list_id in item.profile.list_ids),
                            attributes=_normalize_attributes(item.profile.attributes),
                            consent_pending=True,
                        )
                    )
                await session.commit()
            except Exception:
                await session.rollback()
                raise
        return len(resolved), len(conflict_rows)

    async def _load_conflicting_rows(
        self,
        *,
        session: AsyncSession,
        matched_user_ids: list[int],
        target_subscriber_ids: list[int],
    ) -> list[int]:
        if not target_subscriber_ids:
            return []
        stmt: Select[tuple[int]] = select(ListmonkUser.user_id).where(
            ListmonkUser.subscriber_id.in_(target_subscriber_ids),
            ListmonkUser.user_id.not_in(matched_user_ids),
        )
        result = await session.execute(stmt)
        user_ids = [int(user_id) for user_id in result.scalars().all()]
        if user_ids:
            logger.warning(
                "listmonk_subscriber_id_refresh_staging_conflicts",
                conflict_rows=len(user_ids),
                conflict_user_ids=user_ids,
            )
        return user_ids


def build_listmonk_subscriber_id_refresh_worker() -> ListmonkSubscriberIdRefreshWorker:
    settings = get_settings()
    return ListmonkSubscriberIdRefreshWorker(
        settings=settings,
        session_factory=SessionLocal,
        listmonk_client=ListmonkSDKClient(settings),
    )


def _normalize_email(email: str | None) -> str | None:
    if email is None:
        return None
    normalized = email.strip().lower()
    return normalized or None


def _normalize_attributes(attributes: dict[str, Any] | None) -> dict[str, object] | None:
    if attributes is None:
        return None
    return dict(attributes)


def _temporary_subscriber_id(user_id: int) -> int:
    return -(abs(user_id) + 1)


def _append_metric_detail(metrics: RefreshMetrics, field_name: str, value: str) -> None:
    current = cast(list[str] | None, getattr(metrics, field_name))
    if current is None:
        current = []
        setattr(metrics, field_name, current)
    current.append(value)
