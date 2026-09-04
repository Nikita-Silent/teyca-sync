"""CLI for replaying `dead` webhook_inbox rows (teyca-sync-iil.6).

Dry-run by default: lists the dead rows matching the filters and their
last_error, without touching anything. Pass --apply to reset them to
`pending` — the normal worker loop then reprocesses them on its own schedule.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime

import structlog

from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from service_workers.replay_dead_webhook_inbox import build_dead_webhook_inbox_replay

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Reset `dead` webhook_inbox rows back to `pending` for reprocessing"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually reset matched rows to pending. Without this flag, only reports the plan.",
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only rows created at/after this ISO 8601 timestamp (e.g. 2026-08-28T00:00:00+00:00)",
    )
    parser.add_argument(
        "--event-type",
        type=str,
        default=None,
        choices=["CREATE", "UPDATE", "DELETE"],
        help="Only rows of this event type",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Max rows to inspect/replay per run (default: 200)",
    )
    return parser


def _parse_since(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    parsed = datetime.fromisoformat(raw)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


async def _run(
    *, apply: bool, since: datetime | None, event_type: str | None, batch_size: int
) -> None:
    replay = build_dead_webhook_inbox_replay()
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component="replay-dead-webhook-inbox",
        console=True,
    )
    try:
        rows = await replay.collect(batch_size=batch_size, since=since, event_type=event_type)
        logger.info("replay_dead_webhook_inbox_plan", candidates=len(rows))
        for row in rows:
            logger.info(
                "replay_dead_webhook_inbox_candidate",
                inbox_id=row.id,
                source_event_id=row.source_event_id,
                event_type=row.event_type,
                attempts=row.attempts,
                last_error=row.last_error,
                created_at=row.created_at.isoformat(),
            )

        if not apply:
            logger.info("replay_dead_webhook_inbox_dry_run_complete", candidates=len(rows))
            return

        summary = await replay.apply(rows=rows)
        logger.info(
            "replay_dead_webhook_inbox_apply_complete",
            candidates=summary.candidates,
            replayed=summary.replayed,
        )
    finally:
        shutdown_logging()


def main() -> None:
    args = _build_parser().parse_args()
    asyncio.run(
        _run(
            apply=args.apply,
            since=_parse_since(args.since),
            event_type=args.event_type,
            batch_size=args.batch_size,
        )
    )


if __name__ == "__main__":
    main()
