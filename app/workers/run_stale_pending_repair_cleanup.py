"""CLI for the one-time stale-pending email_repair_log cleanup (teyca-sync-y1c).

Dry-run by default: reports which pending rows no longer describe a real
conflict. Pass --apply to mark them `stale`. Never calls Teyca or Listmonk.
"""

from __future__ import annotations

import argparse
import asyncio

import structlog

from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.workers.stale_pending_repair_cleanup import build_stale_pending_repair_cleanup

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mark email_repair_log pending rows stale once their conflict no longer exists"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Mark matched rows stale. Without this flag, only reports the plan.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Max rows to inspect/mark per run (default: 200)",
    )
    return parser


async def _run(*, apply: bool, batch_size: int) -> None:
    cleanup = build_stale_pending_repair_cleanup()
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component="stale-pending-repair-cleanup",
        console=True,
    )
    try:
        candidates = await cleanup.collect(batch_size=batch_size)
        logger.info("stale_pending_repair_cleanup_plan", candidates=len(candidates))
        for candidate in candidates:
            logger.info(
                "stale_pending_repair_cleanup_candidate",
                repair_id=candidate.repair_id,
                normalized_email=candidate.normalized_email,
                incoming_user_id=candidate.incoming_user_id,
                existing_user_id=candidate.existing_user_id,
            )

        if apply:
            await cleanup.apply(candidates=candidates)
    finally:
        shutdown_logging()


def main() -> None:
    args = _build_parser().parse_args()
    asyncio.run(_run(apply=args.apply, batch_size=args.batch_size))


if __name__ == "__main__":
    main()
