import asyncio
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from app.clients.listmonk import ListmonkClientError, SubscriberProfile
from app.config import Settings
from service_workers.listmonk_refresh_subscriber_ids import (
    ListmonkSubscriberIdRefreshWorker,
    RefreshMetrics,
    RefreshRow,
    ResolvedSubscriber,
    _temporary_subscriber_id,
)


def _worker() -> ListmonkSubscriberIdRefreshWorker:
    return ListmonkSubscriberIdRefreshWorker(
        settings=cast(Settings, SimpleNamespace()),
        session_factory=MagicMock(),
        listmonk_client=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_resolve_batch_matches_by_email_and_counts_skips() -> None:
    worker = _worker()
    metrics = RefreshMetrics()
    profile = SubscriberProfile(
        subscriber_id=501,
        email="User@Example.com",
        status="enabled",
        list_ids=[7],
        attributes={"user_id": 10},
    )
    cast(AsyncMock, worker.listmonk_client).get_subscriber_profile_by_email.side_effect = [
        profile,
        None,
        ListmonkClientError("boom"),
    ]

    resolved = await worker._resolve_batch(
        rows=[
            RefreshRow(user_id=10, subscriber_id=100, email=" user@example.com "),
            RefreshRow(user_id=11, subscriber_id=101, email="missing@example.com"),
            RefreshRow(user_id=12, subscriber_id=102, email="broken@example.com"),
            RefreshRow(user_id=13, subscriber_id=103, email=None),
        ],
        metrics=metrics,
    )

    assert resolved == [ResolvedSubscriber(user_id=10, email="user@example.com", profile=profile)]
    assert metrics.scanned == 4
    assert metrics.matched == 1
    assert metrics.no_email == 1
    assert metrics.not_found == 1
    assert metrics.lookup_errors == 1


@pytest.mark.asyncio
async def test_resolve_batch_counts_http_lookup_errors_without_failing() -> None:
    worker = _worker()
    metrics = RefreshMetrics()
    request = httpx.Request("GET", "https://listmonk.example/api/subscribers")
    response = httpx.Response(400, request=request)
    cast(AsyncMock, worker.listmonk_client).get_subscriber_profile_by_email.side_effect = (
        httpx.HTTPStatusError("bad query", request=request, response=response)
    )

    resolved = await worker._resolve_batch(
        rows=[RefreshRow(user_id=10, subscriber_id=100, email="it's@example.com")],
        metrics=metrics,
    )

    assert resolved == []
    assert metrics.scanned == 1
    assert metrics.lookup_errors == 1


@pytest.mark.asyncio
async def test_resolve_batch_drops_duplicate_target_subscriber_ids() -> None:
    worker = _worker()
    metrics = RefreshMetrics()
    cast(AsyncMock, worker.listmonk_client).get_subscriber_profile_by_email.side_effect = [
        SubscriberProfile(
            subscriber_id=700,
            email="one@example.com",
            status="enabled",
            list_ids=[1],
        ),
        SubscriberProfile(
            subscriber_id=700,
            email="two@example.com",
            status="enabled",
            list_ids=[1],
        ),
    ]

    resolved = await worker._resolve_batch(
        rows=[
            RefreshRow(user_id=1, subscriber_id=10, email="one@example.com"),
            RefreshRow(user_id=2, subscriber_id=11, email="two@example.com"),
        ],
        metrics=metrics,
    )

    assert resolved == []
    assert metrics.duplicate_target_ids == 1


@pytest.mark.asyncio
async def test_run_dry_run_does_not_apply_resolved_rows() -> None:
    worker = _worker()
    with (
        patch.object(
            ListmonkSubscriberIdRefreshWorker,
            "_load_batch",
            side_effect=[
                [RefreshRow(user_id=5, subscriber_id=50, email="user@example.com")],
                [],
            ],
        ),
        patch.object(
            ListmonkSubscriberIdRefreshWorker,
            "_resolve_batch",
            return_value=[
                ResolvedSubscriber(
                    user_id=5,
                    email="user@example.com",
                    profile=SubscriberProfile(
                        subscriber_id=55,
                        email="user@example.com",
                        status="enabled",
                        list_ids=[1],
                    ),
                )
            ],
        ),
        patch.object(
            ListmonkSubscriberIdRefreshWorker, "_apply_resolved", new_callable=AsyncMock
        ) as apply_resolved,
    ):
        metrics = await worker.run(batch_size=100, apply=False, concurrency=7)

    assert metrics.updated == 0
    apply_resolved.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolve_batch_limits_concurrent_listmonk_lookups() -> None:
    worker = _worker()
    metrics = RefreshMetrics()
    active = 0
    max_active = 0

    async def lookup(email: str) -> SubscriberProfile:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0)
        active -= 1
        return SubscriberProfile(
            subscriber_id=int(email.split("@", maxsplit=1)[0].replace("user", "")),
            email=email,
            status="enabled",
            list_ids=[1],
        )

    cast(AsyncMock, worker.listmonk_client).get_subscriber_profile_by_email.side_effect = lookup

    resolved = await worker._resolve_batch(
        rows=[
            RefreshRow(user_id=index, subscriber_id=index + 100, email=f"user{index}@example.com")
            for index in range(1, 8)
        ],
        metrics=metrics,
        concurrency=2,
    )

    assert len(resolved) == 7
    assert max_active == 2


@pytest.mark.asyncio
async def test_apply_resolved_stages_conflicting_rows_before_final_update() -> None:
    worker = _worker()
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    worker.session_factory = MagicMock(return_value=context_manager)

    result = MagicMock()
    result.scalars.return_value.all.return_value = [99]
    session.execute.side_effect = [result, MagicMock(), MagicMock(), MagicMock()]

    updated, staged_conflicts = await worker._apply_resolved(
        resolved=[
            ResolvedSubscriber(
                user_id=10,
                email="user@example.com",
                profile=SubscriberProfile(
                    subscriber_id=99,
                    email="user@example.com",
                    status="confirmed",
                    list_ids=[3],
                    attributes={"user_id": 10},
                ),
            )
        ]
    )

    assert updated == 1
    assert staged_conflicts == 1
    assert session.execute.await_count == 4
    session.commit.assert_awaited_once()
    session.rollback.assert_not_awaited()


def test_temporary_subscriber_id_is_negative_and_stable() -> None:
    assert _temporary_subscriber_id(10) == -11
    assert _temporary_subscriber_id(10) == _temporary_subscriber_id(10)
