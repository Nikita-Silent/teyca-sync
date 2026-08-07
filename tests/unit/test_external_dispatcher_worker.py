from __future__ import annotations

from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.clients.listmonk import ListmonkSDKClient
from app.clients.teyca import TeycaAPIError, TeycaClient, TeycaRateLimitBusyError
from app.config import Settings
from app.repositories.external_call_outbox import (
    OUTBOX_OP_LISTMONK_UPSERT,
    OUTBOX_OP_MERGE_FINALIZE,
    OUTBOX_OP_TEYCA_BLOCK_CONSENT,
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
    OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC,
    OutboxClaim,
)
from app.repositories.listmonk_users import (
    DuplicateListmonkSubscriberIdError,
    DuplicateListmonkUserEmailError,
)
from app.workers import run_external_dispatcher
from app.workers.external_dispatcher_worker import (
    DEFAULT_OUTBOX_OPERATIONS,
    EMAIL_REPAIR_SYNC_OUTBOX_OPERATIONS,
    MERGE_OUTBOX_OPERATIONS,
    ExternalDispatcherMetrics,
    ExternalDispatcherWorker,
    ListmonkUpsertOutcome,
)


def _settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "external_dispatcher_batch_size": 10,
        "external_dispatcher_retry_base_delay_ms": 1_000,
        "external_dispatcher_retry_max_delay_ms": 60_000,
        "external_dispatcher_max_retries": 5,
        "external_dispatcher_teyca_rate_limit_max_wait_seconds": 0.0,
        "consent_bonus_amount": "100.0",
    }
    defaults.update(overrides)
    return cast(Settings, SimpleNamespace(**defaults))


async def _run_operation_directly(operation: Callable[[AsyncSession], Awaitable[object]]) -> object:
    return await operation(cast(AsyncSession, AsyncMock()))


def _worker(
    *,
    operations: tuple[str, ...] = DEFAULT_OUTBOX_OPERATIONS,
    settings: Settings | None = None,
) -> ExternalDispatcherWorker:
    return ExternalDispatcherWorker(
        settings=settings if settings is not None else _settings(),
        session_factory=cast(async_sessionmaker[AsyncSession], AsyncMock()),
        listmonk_client=cast(ListmonkSDKClient, AsyncMock()),
        teyca_client=cast(TeycaClient, AsyncMock()),
        worker_id="worker-1",
        operations=operations,
    )


@pytest.mark.asyncio
async def test_release_stale_claims_uses_configured_threshold() -> None:
    """teyca-sync-4nr: the reaper threshold must come from settings, not the
    repository's own default, so ops can tune it without a code change."""
    worker = _worker(
        settings=_settings(external_dispatcher_stale_claim_seconds=42.0),
    )
    repo = AsyncMock(release_stale_processing_claims=AsyncMock(return_value=2))

    with (
        patch(
            "app.workers.external_dispatcher_worker.ExternalCallOutboxRepository",
            return_value=repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        count = await worker._release_stale_claims()

    assert count == 2
    repo.release_stale_processing_claims.assert_awaited_once_with(stale_after_seconds=42.0)


@pytest.mark.asyncio
async def test_run_once_releases_stale_claims_before_claiming_new_work() -> None:
    """teyca-sync-4nr: the reaper must run every cycle, ahead of claim_batch, so a
    row stuck in PROCESSING by a crashed worker is freed before the next attempt."""
    worker = _worker()
    with (
        patch.object(
            ExternalDispatcherWorker,
            "_release_stale_claims",
            new=AsyncMock(return_value=5),
        ) as release_stale,
        patch.object(
            ExternalDispatcherWorker, "_teyca_budget_remaining", new=AsyncMock(return_value=100)
        ),
        patch.object(ExternalDispatcherWorker, "_claim_batch", new=AsyncMock(return_value=[])),
        patch("app.workers.external_dispatcher_worker.logger") as logger,
    ):
        await worker.run_once()

    release_stale.assert_awaited_once()
    logger.warning.assert_called_once_with("external_dispatcher_stale_claims_released", count=5)


@pytest.mark.asyncio
async def test_external_dispatcher_run_once_no_pending_jobs() -> None:
    worker = _worker()
    with (
        patch.object(
            ExternalDispatcherWorker,
            "_release_stale_claims",
            new=AsyncMock(return_value=0),
        ),
        patch.object(
            ExternalDispatcherWorker, "_teyca_budget_remaining", new=AsyncMock(return_value=100)
        ),
        patch.object(ExternalDispatcherWorker, "_claim_batch", new=AsyncMock(return_value=[])),
        patch("app.workers.external_dispatcher_worker.logger") as logger,
    ):
        processed = await worker.run_once()

    assert processed == 0
    logger.info.assert_called_once_with("external_dispatcher_no_pending_jobs", batch_size=10)


@pytest.mark.asyncio
async def test_external_dispatcher_run_once_skips_claim_when_budget_exhausted() -> None:
    """teyca-sync-3al: an exhausted budget must not even attempt to claim rows."""
    worker = _worker()
    with (
        patch.object(
            ExternalDispatcherWorker,
            "_release_stale_claims",
            new=AsyncMock(return_value=0),
        ),
        patch.object(
            ExternalDispatcherWorker, "_teyca_budget_remaining", new=AsyncMock(return_value=0)
        ),
        patch.object(ExternalDispatcherWorker, "_claim_batch", new=AsyncMock()) as claim_batch,
        patch("app.workers.external_dispatcher_worker.logger") as logger,
    ):
        processed = await worker.run_once()

    assert processed == 0
    claim_batch.assert_not_awaited()
    logger.info.assert_called_once_with(
        "external_dispatcher_budget_exhausted", batch_size=10, remaining_budget=0
    )


@pytest.mark.asyncio
async def test_external_dispatcher_claim_batch_uses_worker_operations() -> None:
    worker = _worker(operations=MERGE_OUTBOX_OPERATIONS)
    repo = AsyncMock()
    repo.claim_batch.return_value = []

    async def run_in_session(
        operation: Callable[[AsyncSession], Awaitable[list[OutboxClaim]]],
    ) -> list[OutboxClaim]:
        session = AsyncMock()
        claims = await operation(cast(AsyncSession, session))
        repo.claim_batch.assert_awaited_once_with(
            operations=list(MERGE_OUTBOX_OPERATIONS),
            limit=3,
            worker_id="worker-1",
        )
        return cast(list[OutboxClaim], claims)

    with patch(
        "app.workers.external_dispatcher_worker.ExternalCallOutboxRepository",
        return_value=repo,
    ):
        with patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=run_in_session),
        ):
            claims = await worker._claim_batch(limit=3)

    assert claims == []


@pytest.mark.asyncio
async def test_external_dispatcher_skips_listmonk_upsert_when_user_missing() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=1,
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key="listmonk-sync:10",
        user_id=10,
        payload={"email": "user@example.com", "list_ids": [1]},
        attempts=0,
        trace_id="trace-1",
        source_event_id="event-1",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=False)),
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_listmonk_upsert(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.listmonk_client.upsert_subscriber).assert_not_awaited()
    mark_done.assert_awaited_once_with(outbox_id=1, payload=claim.payload)
    assert metrics.skipped == 1


@pytest.mark.asyncio
async def test_external_dispatcher_listmonk_upsert_success_accrues_consent_bonus() -> None:
    """Р4а/teyca-sync-4ue: a successful CRM-driven listmonk_upsert is the only
    trigger for the consent bonus."""
    from app.clients.listmonk import SubscriberState
    from app.workers.external_dispatcher_worker import ListmonkUpsertOutcome

    worker = _worker()
    claim = OutboxClaim(
        id=4,
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key="listmonk-sync:40",
        user_id=40,
        payload={"email": "user@example.com", "list_ids": [1], "event_type": "CREATE"},
        attempts=0,
        trace_id="trace-4",
        source_event_id="event-4",
        queue_name="queue-create",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    state = SubscriberState(subscriber_id=999, status="unconfirmed", list_ids=[1])
    cast(AsyncMock, worker.listmonk_client.upsert_subscriber).return_value = state

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_listmonk_upsert_success",
            new=AsyncMock(
                return_value=ListmonkUpsertOutcome(mapped=True, duplicate_reason=None)
            ),
        ) as apply_success,
        patch.object(
            ExternalDispatcherWorker,
            "_accrue_consent_bonus_if_needed",
            new=AsyncMock(),
        ) as accrue_bonus,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_listmonk_upsert(claim=claim, metrics=metrics)

    apply_success.assert_awaited_once()
    accrue_bonus.assert_awaited_once_with(user_id=40)
    mark_done.assert_awaited_once_with(outbox_id=4)
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_listmonk_upsert_conflict_skips_consent_bonus() -> None:
    """Duplicate-email/subscriber conflicts must never accrue a bonus (Р5)."""
    from app.clients.listmonk import SubscriberState
    from app.workers.external_dispatcher_worker import ListmonkUpsertOutcome

    worker = _worker()
    claim = OutboxClaim(
        id=5,
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key="listmonk-sync:50",
        user_id=50,
        payload={"email": "dup@example.com", "list_ids": [1], "event_type": "CREATE"},
        attempts=0,
        trace_id="trace-5",
        source_event_id="event-5",
        queue_name="queue-create",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    cast(AsyncMock, worker.listmonk_client.upsert_subscriber).return_value = SubscriberState(
        subscriber_id=888, status="unconfirmed", list_ids=[1]
    )

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_listmonk_upsert_success",
            new=AsyncMock(
                return_value=ListmonkUpsertOutcome(mapped=False, duplicate_reason="dup")
            ),
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_accrue_consent_bonus_if_needed",
            new=AsyncMock(),
        ) as accrue_bonus,
        patch.object(
            ExternalDispatcherWorker, "_mark_retry", new=AsyncMock(return_value="retry")
        ),
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_listmonk_upsert(claim=claim, metrics=metrics)

    accrue_bonus.assert_not_awaited()
    mark_done.assert_not_awaited()


@pytest.mark.asyncio
async def test_external_dispatcher_listmonk_upsert_duplicate_subscriber_schedules_retry() -> None:
    """A duplicate subscriber_id must retry/dead the claim, never mark_done (teyca-sync-kia)."""
    from app.clients.listmonk import SubscriberState

    worker = _worker()
    claim = OutboxClaim(
        id=7,
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key="listmonk-sync:70",
        user_id=70,
        payload={"email": "user@example.com", "list_ids": [1], "event_type": "CREATE"},
        attempts=1,
        trace_id="trace-7",
        source_event_id="event-7",
        queue_name="queue-create",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    cast(AsyncMock, worker.listmonk_client.upsert_subscriber).return_value = SubscriberState(
        subscriber_id=999, status="unconfirmed", list_ids=[1]
    )

    async def run_in_session(operation: Callable[[AsyncSession], Awaitable[object]]) -> object:
        session = AsyncMock()
        with (
            patch(
                "app.workers.external_dispatcher_worker.UsersRepository",
                return_value=AsyncMock(
                    get_by_user_id=AsyncMock(return_value=SimpleNamespace(user_id=70))
                ),
            ),
            patch(
                "app.workers.external_dispatcher_worker.ListmonkUsersRepository",
                return_value=AsyncMock(
                    upsert=AsyncMock(
                        side_effect=DuplicateListmonkSubscriberIdError(
                            subscriber_id=999,
                            rows=[],
                        )
                    )
                ),
            ),
            patch(
                "app.workers.external_dispatcher_worker.EmailRepairLogRepository",
                return_value=AsyncMock(),
            ),
        ):
            return await operation(cast(AsyncSession, session))

    with (
        patch.object(
            ExternalDispatcherWorker, "_run_in_session", new=AsyncMock(side_effect=run_in_session)
        ),
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker, "_mark_retry", new=AsyncMock(return_value="retry")
        ) as mark_retry,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_listmonk_upsert(claim=claim, metrics=metrics)

    mark_done.assert_not_awaited()
    mark_retry.assert_awaited_once()
    mark_retry_await_args = mark_retry.await_args
    assert mark_retry_await_args is not None
    assert mark_retry_await_args.kwargs["outbox_id"] == 7
    assert mark_retry_await_args.kwargs["attempts"] == 2
    assert "999" in mark_retry_await_args.kwargs["error_text"]
    assert metrics.retried == 1


@pytest.mark.asyncio
async def test_external_dispatcher_listmonk_upsert_duplicate_email_schedules_retry() -> None:
    """A duplicate email must retry/dead the claim, never mark_done (teyca-sync-kia)."""
    from app.clients.listmonk import SubscriberState

    worker = _worker()
    claim = OutboxClaim(
        id=8,
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key="listmonk-sync:80",
        user_id=80,
        payload={"email": "dup@example.com", "list_ids": [1], "event_type": "CREATE"},
        attempts=4,
        trace_id="trace-8",
        source_event_id="event-8",
        queue_name="queue-create",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    cast(AsyncMock, worker.listmonk_client.upsert_subscriber).return_value = SubscriberState(
        subscriber_id=888, status="unconfirmed", list_ids=[1]
    )
    email_repair_repo = AsyncMock()

    async def run_in_session(operation: Callable[[AsyncSession], Awaitable[object]]) -> object:
        session = AsyncMock()
        with (
            patch(
                "app.workers.external_dispatcher_worker.UsersRepository",
                return_value=AsyncMock(
                    get_by_user_id=AsyncMock(return_value=SimpleNamespace(user_id=80))
                ),
            ),
            patch(
                "app.workers.external_dispatcher_worker.ListmonkUsersRepository",
                return_value=AsyncMock(
                    upsert=AsyncMock(
                        side_effect=DuplicateListmonkUserEmailError(
                            normalized_email="dup@example.com",
                            user_id=80,
                            existing_user_ids=[81],
                        )
                    )
                ),
            ),
            patch(
                "app.workers.external_dispatcher_worker.EmailRepairLogRepository",
                return_value=email_repair_repo,
            ),
        ):
            return await operation(cast(AsyncSession, session))

    with (
        patch.object(
            ExternalDispatcherWorker, "_run_in_session", new=AsyncMock(side_effect=run_in_session)
        ),
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker, "_mark_retry", new=AsyncMock(return_value="dead")
        ) as mark_retry,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_listmonk_upsert(claim=claim, metrics=metrics)

    mark_done.assert_not_awaited()
    mark_retry.assert_awaited_once()
    mark_retry_await_args = mark_retry.await_args
    assert mark_retry_await_args is not None
    assert mark_retry_await_args.kwargs["outbox_id"] == 8
    assert mark_retry_await_args.kwargs["attempts"] == 5
    assert "dup@example.com" in mark_retry_await_args.kwargs["error_text"]
    assert metrics.dead == 1
    email_repair_repo.create_pending.assert_awaited_once()


@pytest.mark.asyncio
async def test_apply_listmonk_upsert_success_returns_duplicate_reason_on_duplicate_email() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=6,
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key="listmonk-sync:60",
        user_id=60,
        payload={"email": "dup@example.com", "list_ids": [1]},
        attempts=0,
        trace_id="trace-6",
        source_event_id="event-6",
        queue_name="queue-create",
    )
    from app.clients.listmonk import SubscriberState

    state = SubscriberState(subscriber_id=777, status="unconfirmed", list_ids=[1])

    async def run_in_session(operation: Callable[[AsyncSession], Awaitable[object]]) -> object:
        session = AsyncMock()
        with (
            patch(
                "app.workers.external_dispatcher_worker.UsersRepository",
                return_value=AsyncMock(
                    get_by_user_id=AsyncMock(return_value=SimpleNamespace(user_id=60))
                ),
            ),
            patch(
                "app.workers.external_dispatcher_worker.ListmonkUsersRepository",
                return_value=AsyncMock(
                    upsert=AsyncMock(
                        side_effect=DuplicateListmonkUserEmailError(
                            normalized_email="dup@example.com",
                            user_id=60,
                            existing_user_ids=[61],
                        )
                    )
                ),
            ),
            patch(
                "app.workers.external_dispatcher_worker.EmailRepairLogRepository",
                return_value=AsyncMock(),
            ),
        ):
            return await operation(cast(AsyncSession, session))

    with patch.object(
        ExternalDispatcherWorker, "_run_in_session", new=AsyncMock(side_effect=run_in_session)
    ):
        result = await worker._apply_listmonk_upsert_success(
            claim=claim, state=state, event_type="CREATE"
        )

    assert result.mapped is False
    assert result.duplicate_reason is not None
    assert "dup@example.com" in result.duplicate_reason


@pytest.mark.asyncio
async def test_accrue_consent_bonus_resumes_only_remaining_step() -> None:
    worker = _worker()
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value=None),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch(
            "app.workers.external_dispatcher_worker.BonusAccrualRepository",
            return_value=AsyncMock(
                reserve=AsyncMock(return_value=False),
                get_by_key=AsyncMock(
                    return_value=SimpleNamespace(payload={"bonus_done": True, "key1_done": False})
                ),
                save_progress=AsyncMock(),
                mark_done_with_payload=AsyncMock(),
            ),
        ),
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._accrue_consent_bonus_if_needed(user_id=70)

    cast(AsyncMock, worker.teyca_client.accrue_bonuses).assert_not_awaited()
    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=70,
        fields={"key1": "confirmed"},
        rate_limit_max_wait_seconds=0.0,
    )


@pytest.mark.asyncio
async def test_accrue_consent_bonus_runs_both_steps_and_marks_done() -> None:
    worker = _worker()
    accrual_repo = AsyncMock(
        reserve=AsyncMock(return_value=True),
        get_by_key=AsyncMock(
            return_value=SimpleNamespace(payload={"bonus_done": False, "key1_done": False})
        ),
        save_progress=AsyncMock(),
        mark_done_with_payload=AsyncMock(),
    )
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value=None),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch(
            "app.workers.external_dispatcher_worker.BonusAccrualRepository",
            return_value=accrual_repo,
        ),
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._accrue_consent_bonus_if_needed(user_id=72)

    cast(AsyncMock, worker.teyca_client.accrue_bonuses).assert_awaited_once()
    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=72,
        fields={"key1": "confirmed"},
        rate_limit_max_wait_seconds=0.0,
    )
    assert accrual_repo.save_progress.await_count == 2
    done_payload = accrual_repo.mark_done_with_payload.await_args.kwargs["payload"]
    assert done_payload == {"bonus_done": True, "key1_done": True}


@pytest.mark.asyncio
async def test_accrue_consent_bonus_skips_teyca_call_when_key1_unchanged() -> None:
    """teyca-sync-agd: resending the same key1 wastes rate limit budget."""
    worker = _worker()
    accrual_repo = AsyncMock(
        reserve=AsyncMock(return_value=True),
        get_by_key=AsyncMock(
            return_value=SimpleNamespace(payload={"bonus_done": True, "key1_done": False})
        ),
        save_progress=AsyncMock(),
        mark_done_with_payload=AsyncMock(),
    )
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value="confirmed"),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch(
            "app.workers.external_dispatcher_worker.BonusAccrualRepository",
            return_value=accrual_repo,
        ),
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._accrue_consent_bonus_if_needed(user_id=74)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_not_awaited()
    users_repo.set_teyca_key_value.assert_not_awaited()
    done_payload = accrual_repo.mark_done_with_payload.await_args.kwargs["payload"]
    assert done_payload["key1_done"] is True


@pytest.mark.asyncio
async def test_accrue_consent_bonus_logs_and_returns_when_operation_missing() -> None:
    worker = _worker()

    with (
        patch(
            "app.workers.external_dispatcher_worker.BonusAccrualRepository",
            return_value=AsyncMock(
                reserve=AsyncMock(return_value=True),
                get_by_key=AsyncMock(return_value=None),
            ),
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
        patch("app.workers.external_dispatcher_worker.logger") as logger,
    ):
        await worker._accrue_consent_bonus_if_needed(user_id=73)

    cast(AsyncMock, worker.teyca_client.accrue_bonuses).assert_not_awaited()
    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_not_awaited()
    logger.error.assert_called_once_with(
        "external_dispatcher_consent_bonus_operation_missing",
        user_id=73,
        idempotency_key="email_consent:73",
    )


@pytest.mark.asyncio
async def test_accrue_consent_bonus_skips_when_already_done() -> None:
    worker = _worker()

    with (
        patch(
            "app.workers.external_dispatcher_worker.BonusAccrualRepository",
            return_value=AsyncMock(
                reserve=AsyncMock(return_value=False),
                get_by_key=AsyncMock(
                    return_value=SimpleNamespace(payload={"bonus_done": True, "key1_done": True})
                ),
                save_progress=AsyncMock(),
                mark_done_with_payload=AsyncMock(),
            ),
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._accrue_consent_bonus_if_needed(user_id=71)

    cast(AsyncMock, worker.teyca_client.accrue_bonuses).assert_not_awaited()
    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_not_awaited()


@pytest.mark.asyncio
async def test_external_dispatcher_invalid_email_block_success() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=2,
        operation=OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
        dedupe_key="invalid-email-block:20",
        user_id=20,
        payload={"status": "blocked"},
        attempts=1,
        trace_id="trace-2",
        source_event_id="event-2",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value=None),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_invalid_email_block_success",
            new=AsyncMock(),
        ) as apply_ok,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._process_invalid_email_block(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=20,
        fields={"key1": "blocked"},
        rate_limit_max_wait_seconds=0.0,
    )
    users_repo.set_teyca_key_value.assert_awaited_once_with(
        user_id=20, key="key1", value="blocked"
    )
    apply_ok.assert_awaited_once_with(user_id=20, status="blocked")
    mark_done.assert_awaited_once_with(outbox_id=2)
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_invalid_email_block_skips_teyca_call_when_unchanged() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=2,
        operation=OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
        dedupe_key="invalid-email-block:20",
        user_id=20,
        payload={"status": "blocked"},
        attempts=1,
        trace_id="trace-2",
        source_event_id="event-2",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value="blocked"),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_invalid_email_block_success",
            new=AsyncMock(),
        ) as apply_ok,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._process_invalid_email_block(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_not_awaited()
    users_repo.set_teyca_key_value.assert_not_awaited()
    apply_ok.assert_awaited_once_with(user_id=20, status="blocked")
    mark_done.assert_awaited_once_with(outbox_id=2)
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_consent_block_success() -> None:
    """teyca-sync-dd2.1: consent-sync unsubscribes deliver key1=blocked via
    this low-priority outbox operation, not synchronously from consent_sync."""
    worker = _worker()
    claim = OutboxClaim(
        id=8,
        operation=OUTBOX_OP_TEYCA_BLOCK_CONSENT,
        dedupe_key="consent-block:30",
        user_id=30,
        payload={"status": "blocked"},
        attempts=0,
        trace_id="trace-8",
        source_event_id="event-8",
        queue_name="queue-consent-sync",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value=None),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_invalid_email_block_success",
            new=AsyncMock(),
        ) as apply_ok,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._process_consent_block(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=30,
        fields={"key1": "blocked"},
        rate_limit_max_wait_seconds=0.0,
    )
    apply_ok.assert_awaited_once_with(user_id=30, status="blocked")
    mark_done.assert_awaited_once_with(outbox_id=8)
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_consent_block_skips_teyca_call_when_unchanged() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=8,
        operation=OUTBOX_OP_TEYCA_BLOCK_CONSENT,
        dedupe_key="consent-block:30",
        user_id=30,
        payload={"status": "blocked"},
        attempts=0,
        trace_id="trace-8",
        source_event_id="event-8",
        queue_name="queue-consent-sync",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value="blocked"),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_invalid_email_block_success",
            new=AsyncMock(),
        ) as apply_ok,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._process_consent_block(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_not_awaited()
    users_repo.set_teyca_key_value.assert_not_awaited()
    apply_ok.assert_awaited_once_with(user_id=30, status="blocked")
    mark_done.assert_awaited_once_with(outbox_id=8)
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_consent_block_skips_when_user_missing() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=9,
        operation=OUTBOX_OP_TEYCA_BLOCK_CONSENT,
        dedupe_key="consent-block:31",
        user_id=31,
        payload={"status": "blocked"},
        attempts=0,
        trace_id="trace-9",
        source_event_id="event-9",
        queue_name="queue-consent-sync",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=False)),
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_consent_block(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_not_awaited()
    mark_done.assert_awaited_once_with(outbox_id=9, payload=claim.payload)
    assert metrics.skipped == 1


def test_default_outbox_operations_include_consent_block() -> None:
    assert OUTBOX_OP_TEYCA_BLOCK_CONSENT in DEFAULT_OUTBOX_OPERATIONS


@pytest.mark.asyncio
async def test_external_dispatcher_process_claim_routes_consent_block() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=10,
        operation=OUTBOX_OP_TEYCA_BLOCK_CONSENT,
        dedupe_key="consent-block:32",
        user_id=32,
        payload={"status": "blocked"},
        attempts=0,
        trace_id="trace-10",
        source_event_id="event-10",
        queue_name="queue-consent-sync",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with patch.object(
        ExternalDispatcherWorker, "_process_consent_block", new=AsyncMock()
    ) as process_consent_block:
        await worker._process_claim(claim=claim, metrics=metrics)

    process_consent_block.assert_awaited_once_with(claim=claim, metrics=metrics)


@pytest.mark.asyncio
async def test_external_dispatcher_merge_finalize_tracks_step_progress() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=3,
        operation=OUTBOX_OP_MERGE_FINALIZE,
        dedupe_key="merge-finalize:30",
        user_id=30,
        payload={
            "bonus_done": False,
            "key2_done": False,
            "merge_logged": False,
            "old_bonus_value": 40.0,
            "merge_key2_value": "merge 30.03.2026 12:00",
            "source_event_type": "UPDATE",
        },
        attempts=0,
        trace_id="trace-3",
        source_event_id="event-3",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value=None),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch.object(
            ExternalDispatcherWorker,
            "_merge_already_logged",
            new=AsyncMock(return_value=False),
        ),
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(ExternalDispatcherWorker, "_save_progress", new=AsyncMock()) as save_progress,
        patch.object(
            ExternalDispatcherWorker, "_write_merge_log", new=AsyncMock()
        ) as write_merge_log,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._process_merge_finalize(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.accrue_bonuses).assert_awaited_once()
    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=30,
        fields={"key2": "merge 30.03.2026 12:00"},
        rate_limit_max_wait_seconds=0.0,
    )
    users_repo.set_teyca_key_value.assert_awaited_once_with(
        user_id=30, key="key2", value="merge 30.03.2026 12:00"
    )
    accrue_await_args = cast(AsyncMock, worker.teyca_client.accrue_bonuses).await_args
    assert accrue_await_args is not None
    accrue_kwargs = accrue_await_args.kwargs
    assert accrue_kwargs["rate_limit_max_wait_seconds"] == 0.0
    assert save_progress.await_count == 2
    write_merge_log.assert_awaited_once_with(
        user_id=30,
        source_event_type="UPDATE",
        source_event_id="event-3",
        trace_id="trace-3",
    )
    mark_done_await_args = mark_done.await_args
    assert mark_done_await_args is not None
    done_payload = mark_done_await_args.kwargs["payload"]
    assert done_payload["bonus_done"] is True
    assert done_payload["key2_done"] is True
    assert done_payload["merge_logged"] is True
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_merge_finalize_skips_teyca_call_when_key2_unchanged() -> None:
    """teyca-sync-agd: resending the same key2 wastes rate limit budget."""
    worker = _worker()
    claim = OutboxClaim(
        id=3,
        operation=OUTBOX_OP_MERGE_FINALIZE,
        dedupe_key="merge-finalize:30",
        user_id=30,
        payload={
            "bonus_done": True,
            "key2_done": False,
            "merge_logged": False,
            "old_bonus_value": 40.0,
            "merge_key2_value": "merge 30.03.2026 12:00",
            "source_event_type": "UPDATE",
        },
        attempts=0,
        trace_id="trace-3",
        source_event_id="event-3",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value="merge 30.03.2026 12:00"),
        set_teyca_key_value=AsyncMock(),
    )

    with (
        patch.object(
            ExternalDispatcherWorker,
            "_merge_already_logged",
            new=AsyncMock(return_value=False),
        ),
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(ExternalDispatcherWorker, "_save_progress", new=AsyncMock()),
        patch.object(ExternalDispatcherWorker, "_write_merge_log", new=AsyncMock()),
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        await worker._process_merge_finalize(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_not_awaited()
    users_repo.set_teyca_key_value.assert_not_awaited()
    done_payload = mark_done.await_args.kwargs["payload"]
    assert done_payload["key2_done"] is True
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_run_once_combines_new_client_merge_and_consent_into_two_teyca_calls() -> None:
    """teyca-sync-axq: POST /bonuses takes an array, PUT takes arbitrary fields —
    a new client (merge_finalize + consent bonus claimed together) should cost
    exactly one accrue_bonuses call and one update_pass_fields call, not four."""
    worker = _worker()
    listmonk_claim = OutboxClaim(
        id=1,
        operation=OUTBOX_OP_LISTMONK_UPSERT,
        dedupe_key="listmonk-sync:40",
        user_id=40,
        payload={"email": "user@example.com", "list_ids": [1], "event_type": "CREATE"},
        attempts=0,
        trace_id="trace-1",
        source_event_id="event-1",
        queue_name="queue-create",
    )
    merge_claim = OutboxClaim(
        id=2,
        operation=OUTBOX_OP_MERGE_FINALIZE,
        dedupe_key="merge-finalize:40",
        user_id=40,
        payload={
            "bonus_done": False,
            "key2_done": False,
            "merge_logged": False,
            "old_bonus_value": 40.0,
            "merge_key2_value": "merge 30.03.2026 12:00",
            "source_event_type": "CREATE",
        },
        attempts=0,
        trace_id="trace-2",
        source_event_id="event-2",
        queue_name="queue-create",
    )
    accrual_repo = AsyncMock(
        reserve=AsyncMock(return_value=True),
        get_by_key=AsyncMock(
            return_value=SimpleNamespace(payload={"bonus_done": False, "key1_done": False})
        ),
        save_progress=AsyncMock(),
        mark_done_with_payload=AsyncMock(),
    )
    users_repo = AsyncMock(
        get_teyca_key_value=AsyncMock(return_value=None),
        set_teyca_key_value=AsyncMock(),
    )
    outbox_repo = AsyncMock(mark_done=AsyncMock(), save_progress=AsyncMock())
    state = SimpleNamespace(subscriber_id=99, status="enabled", list_ids=[1])
    worker.listmonk_client.upsert_subscriber = AsyncMock(return_value=state)

    with (
        patch.object(
            ExternalDispatcherWorker,
            "_claim_batch",
            new=AsyncMock(return_value=[listmonk_claim, merge_claim]),
        ),
        patch.object(
            ExternalDispatcherWorker, "_release_stale_claims", new=AsyncMock(return_value=0)
        ),
        patch.object(
            ExternalDispatcherWorker, "_teyca_budget_remaining", new=AsyncMock(return_value=100)
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_listmonk_upsert_success",
            new=AsyncMock(return_value=ListmonkUpsertOutcome(mapped=True)),
        ),
        patch.object(ExternalDispatcherWorker, "_write_merge_log", new=AsyncMock()),
        patch(
            "app.workers.external_dispatcher_worker.BonusAccrualRepository",
            return_value=accrual_repo,
        ),
        patch(
            "app.workers.external_dispatcher_worker.UsersRepository",
            return_value=users_repo,
        ),
        patch(
            "app.workers.external_dispatcher_worker.ExternalCallOutboxRepository",
            return_value=outbox_repo,
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_run_in_session",
            new=AsyncMock(side_effect=_run_operation_directly),
        ),
    ):
        processed = await worker.run_once()

    assert processed == 2
    cast(AsyncMock, worker.teyca_client.accrue_bonuses).assert_awaited_once()
    accrue_kwargs = cast(AsyncMock, worker.teyca_client.accrue_bonuses).await_args.kwargs
    assert len(accrue_kwargs["bonuses"]) == 2
    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=40,
        fields={"key1": "confirmed", "key2": "merge 30.03.2026 12:00"},
        rate_limit_max_wait_seconds=0.0,
    )
    outbox_repo.mark_done.assert_any_await(outbox_id=1, payload=None)
    merge_done_call = next(
        call for call in outbox_repo.mark_done.await_args_list if call.kwargs["outbox_id"] == 2
    )
    assert merge_done_call.kwargs["payload"]["key2_done"] is True
    assert merge_done_call.kwargs["payload"]["bonus_done"] is True
    accrual_repo.mark_done_with_payload.assert_awaited_once()


@pytest.mark.asyncio
async def test_external_dispatcher_process_claim_schedules_retry_on_error() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=4,
        operation=OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
        dedupe_key="invalid-email-block:40",
        user_id=40,
        payload={"status": "blocked"},
        attempts=2,
        trace_id="trace-4",
        source_event_id="event-4",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(
            ExternalDispatcherWorker,
            "_process_invalid_email_block",
            new=AsyncMock(side_effect=TeycaAPIError("boom", status_code=429)),
        ),
        patch.object(
            ExternalDispatcherWorker, "_mark_retry", new=AsyncMock(return_value="failed")
        ) as mark_retry,
    ):
        await worker._process_claim(claim=claim, metrics=metrics)

    mark_retry.assert_awaited_once_with(outbox_id=4, attempts=3, error_text="boom")
    assert metrics.retried == 1


@pytest.mark.asyncio
async def test_external_dispatcher_process_claim_defers_when_teyca_limiter_is_busy() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=5,
        operation=OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
        dedupe_key="invalid-email-block:50",
        user_id=50,
        payload={"status": "blocked"},
        attempts=0,
        trace_id="trace-5",
        source_event_id="event-5",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(
            ExternalDispatcherWorker,
            "_process_invalid_email_block",
            new=AsyncMock(
                side_effect=TeycaRateLimitBusyError(
                    wait_seconds=12.0,
                    max_wait_seconds=0.0,
                    backend="redis",
                )
            ),
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_defer_rate_limit_busy",
            new=AsyncMock(return_value="pending"),
        ) as defer_mock,
        patch.object(ExternalDispatcherWorker, "_mark_retry", new=AsyncMock()) as mark_retry,
    ):
        await worker._process_claim(claim=claim, metrics=metrics)

    defer_mock.assert_awaited_once_with(
        outbox_id=5,
        attempts=1,
        wait_seconds=12.0,
        error_text=(
            "Teyca rate limiter is busy: backend=redis, wait_seconds=12.000, max_wait_seconds=0.000"
        ),
    )
    mark_retry.assert_not_awaited()
    assert metrics.retried == 1


@pytest.mark.asyncio
async def test_external_dispatcher_process_claim_dead_letters_after_defer_cap() -> None:
    """teyca-sync-3al: rate-limit defers must consume attempts and cap out,
    otherwise a persistently-busy budget window defers a job forever."""
    worker = _worker()
    claim = OutboxClaim(
        id=6,
        operation=OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
        dedupe_key="invalid-email-block:60",
        user_id=60,
        payload={"status": "blocked"},
        attempts=4,
        trace_id="trace-6",
        source_event_id="event-6",
        queue_name="queue-update",
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(
            ExternalDispatcherWorker,
            "_process_invalid_email_block",
            new=AsyncMock(
                side_effect=TeycaRateLimitBusyError(
                    wait_seconds=12.0,
                    max_wait_seconds=0.0,
                    backend="postgres",
                )
            ),
        ),
        patch.object(
            ExternalDispatcherWorker,
            "_defer_rate_limit_busy",
            new=AsyncMock(return_value="dead"),
        ) as defer_mock,
    ):
        await worker._process_claim(claim=claim, metrics=metrics)

    defer_mock.assert_awaited_once_with(
        outbox_id=6,
        attempts=5,
        wait_seconds=12.0,
        error_text=(
            "Teyca rate limiter is busy: "
            "backend=postgres, wait_seconds=12.000, max_wait_seconds=0.000"
        ),
    )
    assert metrics.dead == 1
    assert metrics.retried == 0


@pytest.mark.asyncio
async def test_run_external_dispatcher_single_iteration_logs_completion() -> None:
    with (
        patch(
            "app.workers.run_external_dispatcher.get_settings",
            return_value=SimpleNamespace(loki_url=None, log_component="external-dispatcher-merge"),
        ),
        patch("app.workers.run_external_dispatcher.configure_logging"),
        patch("app.workers.run_external_dispatcher.shutdown_logging"),
        patch("app.workers.run_external_dispatcher.wait_for_database", new=AsyncMock()),
        patch("app.workers.run_external_dispatcher.build_external_dispatcher_worker") as builder,
        patch("app.workers.run_external_dispatcher.logger") as logger,
        patch("app.workers.run_external_dispatcher.write_heartbeat", new=AsyncMock()) as heartbeat,
    ):
        worker = AsyncMock()
        worker.run_once.return_value = 3
        builder.return_value = worker
        await run_external_dispatcher._run(
            service_name="external-dispatcher-merge",
            operations=MERGE_OUTBOX_OPERATIONS,
        )

    builder.assert_called_once_with(
        operations=MERGE_OUTBOX_OPERATIONS,
        worker_id_prefix="external-dispatcher-merge",
    )
    logger.info.assert_called_once_with(
        "external_dispatcher_run_completed",
        processed=3,
        service_name="external-dispatcher-merge",
    )
    assert heartbeat.await_count == 3
    heartbeat.assert_any_await(
        "external-dispatcher-merge",
        extra={"stage": "started"},
    )


def _email_repair_sync_claim(
    *,
    mark_bad_email: bool = True,
    winner_subscriber_id: int | None = 777,
) -> OutboxClaim:
    return OutboxClaim(
        id=40,
        operation=OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC,
        dedupe_key="email-repair-sync:5",
        user_id=10,
        payload={
            "repair_id": 5,
            "winner_user_id": 20,
            "winner_subscriber_id": winner_subscriber_id,
            "mark_bad_email": mark_bad_email,
        },
        attempts=0,
        trace_id="trace-40",
        source_event_id="event-40",
        queue_name=None,
    )


@pytest.mark.asyncio
async def test_external_dispatcher_email_repair_sync_delivers_winner_and_loser() -> None:
    """teyca-sync-y1c: no longer called directly by the backfill script — this
    is the paced replacement that never blows through the shared hourly
    Teyca budget in one burst."""
    worker = _worker()
    claim = _email_repair_sync_claim()
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker, "_mark_email_repair_synced", new=AsyncMock()
        ) as mark_synced,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_email_repair_sync(claim=claim, metrics=metrics)

    update_pass_fields = cast(AsyncMock, worker.teyca_client.update_pass_fields)
    assert update_pass_fields.await_count == 2
    update_pass_fields.assert_any_await(
        user_id=20,
        fields={"key6": "bugs"},
        rate_limit_max_wait_seconds=0.0,
    )
    update_pass_fields.assert_any_await(
        user_id=10,
        fields={"email": None, "key6": "bugs", "key1": "bad email"},
        rate_limit_max_wait_seconds=0.0,
    )
    mark_synced.assert_awaited_once_with(repair_id=5, winner_user_id=20, winner_subscriber_id=777)
    mark_done.assert_awaited_once_with(outbox_id=40)
    assert metrics.done == 1


@pytest.mark.asyncio
async def test_external_dispatcher_email_repair_sync_same_person_skips_bad_email_mark() -> None:
    """Р5/Р6 (teyca-sync-37z): same-phone losers are cleared without key1=bad email."""
    worker = _worker()
    claim = _email_repair_sync_claim(mark_bad_email=False, winner_subscriber_id=None)
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(ExternalDispatcherWorker, "_mark_email_repair_synced", new=AsyncMock()),
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()),
    ):
        await worker._process_email_repair_sync(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_any_await(
        user_id=10,
        fields={"email": None, "key6": "bugs"},
        rate_limit_max_wait_seconds=0.0,
    )


@pytest.mark.asyncio
async def test_external_dispatcher_email_repair_sync_skips_when_loser_missing() -> None:
    worker = _worker()
    claim = _email_repair_sync_claim()
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=False)),
        patch.object(
            ExternalDispatcherWorker, "_mark_email_repair_synced", new=AsyncMock()
        ) as mark_synced,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_email_repair_sync(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_not_awaited()
    mark_synced.assert_awaited_once_with(repair_id=5, winner_user_id=20, winner_subscriber_id=777)
    mark_done.assert_awaited_once_with(outbox_id=40)
    assert metrics.skipped == 1


@pytest.mark.asyncio
async def test_external_dispatcher_email_repair_sync_rejects_incomplete_payload() -> None:
    worker = _worker()
    claim = OutboxClaim(
        id=41,
        operation=OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC,
        dedupe_key="email-repair-sync:6",
        user_id=10,
        payload={"repair_id": 6},
        attempts=0,
        trace_id=None,
        source_event_id=None,
        queue_name=None,
    )
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with pytest.raises(RuntimeError):
        await worker._process_email_repair_sync(claim=claim, metrics=metrics)


@pytest.mark.asyncio
async def test_external_dispatcher_process_claim_routes_email_repair_sync() -> None:
    worker = _worker()
    claim = _email_repair_sync_claim()
    metrics = ExternalDispatcherMetrics(batch_size=10)

    with patch.object(
        ExternalDispatcherWorker, "_process_email_repair_sync", new=AsyncMock()
    ) as process_email_repair_sync:
        await worker._process_claim(claim=claim, metrics=metrics)

    process_email_repair_sync.assert_awaited_once_with(claim=claim, metrics=metrics)


def test_default_outbox_operations_include_email_repair_sync() -> None:
    assert OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC in DEFAULT_OUTBOX_OPERATIONS
    assert EMAIL_REPAIR_SYNC_OUTBOX_OPERATIONS == (OUTBOX_OP_TEYCA_EMAIL_REPAIR_SYNC,)
