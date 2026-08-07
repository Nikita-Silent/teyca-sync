"""CLI for the one-time consent bonus backfill (teyca-sync-io3).

Dry-run by default: reports candidates and Teyca reconciliation results
without moving any money. Pass --apply to actually accrue.
"""

from __future__ import annotations

import argparse
import asyncio

import structlog

from app.logging_config import configure_logging, shutdown_logging
from service_workers.consent_bonus_backfill import (
    ConsentBonusBackfillError,
    build_consent_bonus_backfill,
)

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backdate the consent bonus for clients missed by the confirmed-status bug"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually accrue the bonus in Teyca. Without this flag, only reports the plan.",
    )
    return parser


async def _run(*, apply: bool) -> None:
    backfill = build_consent_bonus_backfill()
    settings = backfill.settings
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component="consent-bonus-backfill",
        console=True,
    )
    try:
        candidates = await backfill.collect_candidates()
        logger.info("consent_bonus_backfill_plan", candidates=len(candidates))
        for candidate in candidates:
            logger.info(
                "consent_bonus_backfill_candidate",
                user_id=candidate.user_id,
                subscriber_id=candidate.subscriber_id,
                status=candidate.status,
            )

        already_paid = await backfill.reconcile_with_teyca(candidates=candidates)
        if already_paid:
            logger.warning(
                "consent_bonus_backfill_operations_matches",
                user_ids=sorted(already_paid),
            )

        if not apply:
            logger.info(
                "consent_bonus_backfill_dry_run_complete",
                candidates=len(candidates),
                would_accrue=len(candidates) - len(already_paid),
                skipped_operations_match=len(already_paid),
            )
            return

        summary = await backfill.accrue(candidates=candidates, already_paid=already_paid)
        logger.info(
            "consent_bonus_backfill_apply_complete",
            candidates=summary.candidates,
            accrued=summary.accrued,
            skipped_operations_match=summary.skipped_operations_match,
            failed=summary.failed,
        )
    finally:
        shutdown_logging()


def main() -> None:
    args = _build_parser().parse_args()
    try:
        asyncio.run(_run(apply=args.apply))
    except ConsentBonusBackfillError as exc:
        logger.error("consent_bonus_backfill_failed", error=str(exc))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
