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
    OUTBOX_OP_TEYCA_BLOCK_INVALID_EMAIL,
    OutboxClaim,
)
from app.repositories.listmonk_users import (
    DuplicateListmonkSubscriberIdError,
    DuplicateListmonkUserEmailError,
)
from app.workers import run_external_dispatcher
from app.workers.external_dispatcher_worker import (
    DEFAULT_OUTBOX_OPERATIONS,
    MERGE_OUTBOX_OPERATIONS,
    ExternalDispatcherMetrics,
    ExternalDispatcherWorker,
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


def _worker(*, operations: tuple[str, ...] = DEFAULT_OUTBOX_OPERATIONS) -> ExternalDispatcherWorker:
    return ExternalDispatcherWorker(
        settings=_settings(),
        session_factory=cast(async_sessionmaker[AsyncSession], AsyncMock()),
        listmonk_client=cast(ListmonkSDKClient, AsyncMock()),
        teyca_client=cast(TeycaClient, AsyncMock()),
        worker_id="worker-1",
        operations=operations,
    )


@pytest.mark.asyncio
async def test_external_dispatcher_run_once_no_pending_jobs() -> None:
    worker = _worker()
    with (
        patch.object(
            ExternalDispatcherWorker,
            "_release_stale_claims",
            new=AsyncMock(return_value=0),
        ),
        patch.object(ExternalDispatcherWorker, "_claim_batch", new=AsyncMock(return_value=[])),
        patch("app.workers.external_dispatcher_worker.logger") as logger,
    ):
        processed = await worker.run_once()

    assert processed == 0
    logger.info.assert_called_once_with("external_dispatcher_no_pending_jobs", batch_size=10)


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

    with (
        patch(
            "app.workers.external_dispatcher_worker.BonusAccrualRepository",
            return_value=accrual_repo,
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

    with (
        patch.object(ExternalDispatcherWorker, "_user_exists", new=AsyncMock(return_value=True)),
        patch.object(
            ExternalDispatcherWorker,
            "_apply_invalid_email_block_success",
            new=AsyncMock(),
        ) as apply_ok,
        patch.object(ExternalDispatcherWorker, "_mark_done", new=AsyncMock()) as mark_done,
    ):
        await worker._process_invalid_email_block(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=20,
        fields={"key1": "blocked"},
        rate_limit_max_wait_seconds=0.0,
    )
    apply_ok.assert_awaited_once_with(user_id=20, status="blocked")
    mark_done.assert_awaited_once_with(outbox_id=2)
    assert metrics.done == 1


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
    ):
        await worker._process_merge_finalize(claim=claim, metrics=metrics)

    cast(AsyncMock, worker.teyca_client.accrue_bonuses).assert_awaited_once()
    cast(AsyncMock, worker.teyca_client.update_pass_fields).assert_awaited_once_with(
        user_id=30,
        fields={"key2": "merge 30.03.2026 12:00"},
        rate_limit_max_wait_seconds=0.0,
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
            new=AsyncMock(),
        ) as defer_mock,
        patch.object(ExternalDispatcherWorker, "_mark_retry", new=AsyncMock()) as mark_retry,
    ):
        await worker._process_claim(claim=claim, metrics=metrics)

    defer_mock.assert_awaited_once_with(
        outbox_id=5,
        wait_seconds=12.0,
        error_text=(
            "Teyca rate limiter is busy: backend=redis, wait_seconds=12.000, max_wait_seconds=0.000"
        ),
    )
    mark_retry.assert_not_awaited()
    assert metrics.retried == 1


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
