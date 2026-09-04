"""teyca-sync-iil.6: replay `dead` webhook_inbox rows once the schema that
killed them is fixed. Mocked-session unit tests; the real-Postgres round trip
(claim -> die -> replay -> reprocess -> done) is covered in
tests/integration/test_webhook_inbox_full_path.py.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.repositories.webhook_inbox import DeadInboxRow
from service_workers.replay_dead_webhook_inbox import (
    DeadWebhookInboxReplay,
    build_dead_webhook_inbox_replay,
)


def _replay() -> tuple[DeadWebhookInboxReplay, MagicMock]:
    session_factory = MagicMock()
    replay = DeadWebhookInboxReplay(session_factory=session_factory)
    return replay, session_factory


@pytest.mark.asyncio
async def test_collect_delegates_filters_to_repository() -> None:
    replay, session_factory = _replay()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    session_factory.return_value = session_cm

    inbox_repo = AsyncMock()
    since = datetime(2026, 8, 28, tzinfo=UTC)
    inbox_repo.get_dead_batch.return_value = [
        DeadInboxRow(
            id=1,
            source_event_id="evt-1",
            event_type="UPDATE",
            attempts=1,
            last_error="validation error",
            created_at=since,
        ),
    ]

    with patch(
        "service_workers.replay_dead_webhook_inbox.WebhookInboxRepository",
        return_value=inbox_repo,
    ):
        rows = await replay.collect(batch_size=50, since=since, event_type="UPDATE")

    inbox_repo.get_dead_batch.assert_awaited_once_with(
        limit=50, since=since, event_type="UPDATE"
    )
    assert rows == [
        DeadInboxRow(
            id=1,
            source_event_id="evt-1",
            event_type="UPDATE",
            attempts=1,
            last_error="validation error",
            created_at=since,
        )
    ]


@pytest.mark.asyncio
async def test_apply_replays_every_row_and_commits() -> None:
    replay, session_factory = _replay()
    session = AsyncMock()
    session_cm = AsyncMock()
    session_cm.__aenter__.return_value = session
    session_factory.return_value = session_cm

    inbox_repo = AsyncMock()
    inbox_repo.replay_dead.return_value = 2
    rows = [
        DeadInboxRow(
            id=1,
            source_event_id="evt-1",
            event_type="UPDATE",
            attempts=1,
            last_error="err",
            created_at=datetime.now(UTC),
        ),
        DeadInboxRow(
            id=2,
            source_event_id="evt-2",
            event_type="CREATE",
            attempts=1,
            last_error="err",
            created_at=datetime.now(UTC),
        ),
    ]

    with patch(
        "service_workers.replay_dead_webhook_inbox.WebhookInboxRepository",
        return_value=inbox_repo,
    ):
        summary = await replay.apply(rows=rows)

    inbox_repo.replay_dead.assert_awaited_once_with(inbox_ids=[1, 2])
    session.commit.assert_awaited_once()
    assert summary.candidates == 2
    assert summary.replayed == 2


@pytest.mark.asyncio
async def test_apply_with_no_rows_is_a_noop_and_does_not_open_a_session() -> None:
    replay, session_factory = _replay()

    summary = await replay.apply(rows=[])

    session_factory.assert_not_called()
    assert summary.candidates == 0
    assert summary.replayed == 0


def test_build_dead_webhook_inbox_replay() -> None:
    replay = build_dead_webhook_inbox_replay()
    assert replay is not None
