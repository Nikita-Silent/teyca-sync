"""CLI for one-time duplicate-email backfill and Teyca sync."""

from __future__ import annotations

import argparse
import asyncio

import structlog

from app.logging_config import configure_logging, shutdown_logging
from app.workers.email_repair_backfill import (
    DuplicateEmailBackfillError,
    build_duplicate_email_backfill,
)

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize duplicate emails via Listmonk truth")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply atomic local DB cleanup for all resolvable duplicate emails",
    )
    parser.add_argument(
        "--sync-teyca",
        action="store_true",
        help="Sync already-applied loser cleanup rows to Teyca",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of duplicate emails inspected during planning",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for --sync-teyca rows (default: 100)",
    )
    return parser


async def _run(*, apply: bool, sync_teyca: bool, limit: int | None, batch_size: int) -> None:
    backfill = build_duplicate_email_backfill()
    settings = backfill.settings
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component="email-repair-backfill",
        console=True,
    )
    try:
        plans, issues = await backfill.collect_plans(limit=limit)
        logger.info(
            "email_repair_backfill_plan",
            duplicate_emails=len(plans) + len(issues),
            resolved_emails=len(plans),
            unresolved_emails=len(issues),
            loser_rows=sum(len(plan.loser_user_ids) for plan in plans),
        )
        for issue in issues:
            logger.warning(
                "email_repair_backfill_unresolved",
                normalized_email=issue.normalized_email,
                candidate_user_ids=issue.candidate_user_ids,
                error=issue.error,
            )
        for plan in plans:
            logger.info(
                "email_repair_backfill_resolved",
                normalized_email=plan.normalized_email,
                winner_user_id=plan.winner_user_id,
                winner_subscriber_id=plan.winner_subscriber_id,
                loser_user_ids=plan.loser_user_ids,
            )

        if apply:
            await backfill.apply(plans=plans, issues=issues)
        if sync_teyca:
            await backfill.sync_teyca(batch_size=batch_size)
    finally:
        shutdown_logging()


def main() -> None:
    args = _build_parser().parse_args()
    try:
        asyncio.run(
            _run(
                apply=args.apply,
                sync_teyca=args.sync_teyca,
                limit=args.limit,
                batch_size=args.batch_size,
            )
        )
    except DuplicateEmailBackfillError as exc:
        logger.error("email_repair_backfill_failed", error=str(exc))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
