"""CLI for the one-time Р5/Р6 policy-based duplicate-email cleanup (teyca-sync-y1c).

Unlike run_email_repair_backfill.py (Listmonk truth), this resolves
users.email duplicate groups via the deterministic phone/activity
policy (teyca-sync-37z): no external calls during planning, no manual
review path. Always dry-run by default — review the plan before
passing --apply.
"""

from __future__ import annotations

import argparse
import asyncio

import structlog

from app.logging_config import configure_logging, shutdown_logging
from service_workers.email_repair_backfill import (
    DuplicateEmailBackfillError,
    build_duplicate_email_backfill,
)

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Normalize duplicate emails via the Р5/Р6 phone/activity policy"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply atomic local DB cleanup for all resolved duplicate-email groups",
    )
    parser.add_argument(
        "--sync-teyca",
        action="store_true",
        help=(
            "Enqueue already-applied loser cleanup rows for Teyca sync "
            "(external_call_outbox; drained by "
            "run_external_dispatcher_email_repair_sync, not sent directly)"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for --sync-teyca enqueue (default: 100)",
    )
    return parser


async def _run(*, apply: bool, sync_teyca: bool, batch_size: int) -> None:
    backfill = build_duplicate_email_backfill()
    settings = backfill.settings
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component="email-duplicate-policy-backfill",
        console=True,
    )
    try:
        plans, issues = await backfill.collect_plans_via_policy()
        logger.info(
            "email_duplicate_policy_backfill_plan",
            duplicate_emails=len(plans) + len(issues),
            resolved_emails=len(plans),
            unresolved_emails=len(issues),
            loser_rows=sum(len(plan.loser_user_ids) for plan in plans),
        )
        for plan in plans:
            logger.info(
                "email_duplicate_policy_backfill_resolved",
                normalized_email=plan.normalized_email,
                winner_user_id=plan.winner_user_id,
                loser_user_ids=plan.loser_user_ids,
                mark_bad_email=plan.mark_bad_email,
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
                batch_size=args.batch_size,
            )
        )
    except DuplicateEmailBackfillError as exc:
        logger.error("email_duplicate_policy_backfill_failed", error=str(exc))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
