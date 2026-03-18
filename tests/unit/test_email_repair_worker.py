from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.clients.listmonk import ListmonkClientError
from app.clients.teyca import TeycaAPIError
from app.workers.email_repair_worker import (
    EMAIL_REPAIR_MAX_ATTEMPTS,
    TEYCA_KEY1_BAD_EMAIL,
    EmailRepairMetrics,
    EmailRepairWorker,
)


def _worker() -> EmailRepairWorker:
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    return EmailRepairWorker(
        settings=SimpleNamespace(consent_sync_batch_size=100),
        session_factory=MagicMock(return_value=context_manager),
        listmonk_client=AsyncMock(),
        teyca_client=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_process_row_resolves_by_authoritative_subscriber() -> None:
    worker = _worker()
    repair_repo = AsyncMock()
    listmonk_repo = AsyncMock()
    users_repo = AsyncMock()
    row = SimpleNamespace(
        id=1,
        normalized_email="duplicate@example.com",
        incoming_user_id=10,
        existing_user_id=20,
        attempts=0,
    )
    worker.listmonk_client.get_subscriber_by_email.return_value = SimpleNamespace(subscriber_id=777)
    listmonk_repo.get_by_subscriber_id.return_value = SimpleNamespace(user_id=20)

    await worker._process_row(
        row=row,
        repair_repo=repair_repo,
        listmonk_repo=listmonk_repo,
        users_repo=users_repo,
        metrics=EmailRepairMetrics(batch_size=10),
    )

    users_repo.clear_email.assert_awaited_once_with(user_id=10)
    listmonk_repo.clear_email.assert_awaited_once_with(user_id=10)
    worker.teyca_client.update_pass_fields.assert_awaited_once_with(
        user_id=10,
        fields={"email": None, "key1": TEYCA_KEY1_BAD_EMAIL},
    )
    repair_repo.mark_teyca_synced.assert_awaited_once_with(
        repair_id=1,
        winner_user_id=20,
        winner_subscriber_id=777,
    )
    repair_repo.mark_retry.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_row_marks_manual_review_after_attempt_limit() -> None:
    worker = _worker()
    repair_repo = AsyncMock()
    repair_repo.mark_retry.return_value = "manual_review"
    listmonk_repo = AsyncMock()
    users_repo = AsyncMock()
    row = SimpleNamespace(
        id=2,
        normalized_email="duplicate@example.com",
        incoming_user_id=10,
        existing_user_id=20,
        attempts=EMAIL_REPAIR_MAX_ATTEMPTS - 1,
    )
    worker.listmonk_client.get_subscriber_by_email.side_effect = ListmonkClientError("boom")

    await worker._process_row(
        row=row,
        repair_repo=repair_repo,
        listmonk_repo=listmonk_repo,
        users_repo=users_repo,
        metrics=EmailRepairMetrics(batch_size=10),
    )

    repair_repo.mark_retry.assert_awaited_once_with(
        repair_id=2,
        attempts=EMAIL_REPAIR_MAX_ATTEMPTS,
        error_text="boom",
        max_attempts=EMAIL_REPAIR_MAX_ATTEMPTS,
    )
    repair_repo.mark_teyca_synced.assert_not_awaited()
    users_repo.clear_email.assert_not_awaited()
    listmonk_repo.clear_email.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_row_marks_retry_when_winner_not_in_pair() -> None:
    worker = _worker()
    repair_repo = AsyncMock()
    repair_repo.mark_retry.return_value = "failed"
    listmonk_repo = AsyncMock()
    users_repo = AsyncMock()
    row = SimpleNamespace(
        id=3,
        normalized_email="duplicate@example.com",
        incoming_user_id=10,
        existing_user_id=20,
        attempts=0,
    )
    worker.listmonk_client.get_subscriber_by_email.return_value = SimpleNamespace(subscriber_id=777)
    listmonk_repo.get_by_subscriber_id.return_value = SimpleNamespace(user_id=99)

    await worker._process_row(
        row=row,
        repair_repo=repair_repo,
        listmonk_repo=listmonk_repo,
        users_repo=users_repo,
        metrics=EmailRepairMetrics(batch_size=10),
    )

    retry_error = repair_repo.mark_retry.await_args.kwargs["error_text"]
    assert "winner_user_id=99" in retry_error
    users_repo.clear_email.assert_not_awaited()
    listmonk_repo.clear_email.assert_not_awaited()
    repair_repo.mark_teyca_synced.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_row_marks_retry_when_teyca_cleanup_fails() -> None:
    worker = _worker()
    repair_repo = AsyncMock()
    repair_repo.mark_retry.return_value = "failed"
    listmonk_repo = AsyncMock()
    users_repo = AsyncMock()
    row = SimpleNamespace(
        id=4,
        normalized_email="duplicate@example.com",
        incoming_user_id=10,
        existing_user_id=20,
        attempts=0,
    )
    worker.listmonk_client.get_subscriber_by_email.return_value = SimpleNamespace(subscriber_id=777)
    listmonk_repo.get_by_subscriber_id.return_value = SimpleNamespace(user_id=20)
    worker.teyca_client.update_pass_fields.side_effect = TeycaAPIError("teyca boom")

    await worker._process_row(
        row=row,
        repair_repo=repair_repo,
        listmonk_repo=listmonk_repo,
        users_repo=users_repo,
        metrics=EmailRepairMetrics(batch_size=10),
    )

    repair_repo.mark_retry.assert_awaited_once()
    repair_repo.mark_teyca_synced.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_once_processes_rows_and_commits() -> None:
    worker = _worker()
    session = AsyncMock()
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = session
    context_manager.__aexit__.return_value = False
    worker.session_factory = MagicMock(return_value=context_manager)
    row = SimpleNamespace(
        id=1,
        normalized_email="duplicate@example.com",
        incoming_user_id=10,
        existing_user_id=20,
        trace_id="trace-1",
        source_event_id="event-1",
        attempts=0,
    )
    process_row = AsyncMock()

    with (
        patch.object(EmailRepairWorker, "_load_pending_rows", new=AsyncMock(return_value=[row])),
        patch.object(EmailRepairWorker, "_mark_processing", new=AsyncMock()) as mark_processing,
        patch.object(EmailRepairWorker, "_process_row", new=process_row),
    ):
        processed = await worker.run_once()

    assert processed == 1
    mark_processing.assert_awaited_once_with(repair_id=1)
    process_row.assert_awaited_once()
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_once_logs_no_pending_rows() -> None:
    worker = _worker()
    with (
        patch.object(EmailRepairWorker, "_load_pending_rows", new=AsyncMock(return_value=[])),
        patch("app.workers.email_repair_worker.logger.info") as logger_info,
    ):
        processed = await worker.run_once()

    assert processed == 0
    logger_info.assert_any_call("email_repair_no_pending_rows", batch_size=100)
