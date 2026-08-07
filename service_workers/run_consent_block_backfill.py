"""CLI for the one-time consent-block outbox backfill (teyca-sync-dd2.1).

Dry-run by default: reports how many blocked/blocklisted/blacklisted users
would get a task queued, without writing anything. Pass --apply to actually
enqueue — this never calls Teyca directly, only seeds `external_call_outbox`;
the existing low-priority dispatcher delivers under budget afterwards.
"""

from __future__ import annotations

import argparse
import asyncio

import structlog

from app.logging_config import configure_logging, shutdown_logging
from service_workers.consent_block_backfill import build_consent_block_backfill

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed teyca_block_consent outbox tasks for the historical unsubscribe backlog"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually enqueue tasks. Without this flag, only reports the plan.",
    )
    return parser


async def _run(*, apply: bool) -> None:
    backfill = build_consent_block_backfill()
    settings = backfill.settings
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component="consent-block-backfill",
        console=True,
    )
    try:
        candidates = await backfill.collect_candidates()
        logger.info("consent_block_backfill_plan", candidates=len(candidates))

        if not apply:
            logger.info(
                "consent_block_backfill_dry_run_complete",
                candidates=len(candidates),
            )
            return

        summary = await backfill.enqueue(candidates=candidates)
        logger.info(
            "consent_block_backfill_apply_complete",
            candidates=summary.candidates,
            enqueued=summary.enqueued,
            already_queued=summary.already_queued,
        )
    finally:
        shutdown_logging()


def main() -> None:
    args = _build_parser().parse_args()
    asyncio.run(_run(apply=args.apply))


if __name__ == "__main__":
    main()
