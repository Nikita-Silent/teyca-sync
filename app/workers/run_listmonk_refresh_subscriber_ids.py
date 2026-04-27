"""Run one-off Listmonk subscriber id refresh by email."""

from __future__ import annotations

import argparse
import asyncio

import structlog

from app.config import get_settings
from app.logging_config import configure_logging, shutdown_logging
from app.workers.listmonk_refresh_subscriber_ids import (
    RefreshMetrics,
    build_listmonk_subscriber_id_refresh_worker,
)

logger = structlog.get_logger()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh listmonk_users subscriber_id/status/list_ids/attributes by email."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes to the database. Without this flag only dry-run metrics are logged.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of local rows to process per batch.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of local rows to scan.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Maximum concurrent Listmonk lookup requests per batch.",
    )
    return parser.parse_args()


async def _run(
    *, apply: bool, batch_size: int, limit: int | None, concurrency: int
) -> RefreshMetrics:
    settings = get_settings()
    configure_logging(
        loki_url=getattr(settings, "loki_url", None),
        loki_username=getattr(settings, "loki_username", None),
        loki_password=getattr(settings, "loki_password", None),
        loki_request_timeout_seconds=getattr(settings, "loki_request_timeout_seconds", 5.0),
        component=getattr(settings, "log_component", "listmonk-refresh-subscriber-ids"),
    )
    worker = build_listmonk_subscriber_id_refresh_worker()
    try:
        metrics = await worker.run(
            batch_size=batch_size,
            apply=apply,
            limit=limit,
            concurrency=concurrency,
        )
        logger.info(
            "listmonk_subscriber_id_refresh_run_completed",
            apply=apply,
            scanned=metrics.scanned,
            matched=metrics.matched,
            updated=metrics.updated,
            unchanged=metrics.unchanged,
            no_email=metrics.no_email,
            not_found=metrics.not_found,
            lookup_errors=metrics.lookup_errors,
            duplicate_target_ids=metrics.duplicate_target_ids,
            staged_conflicts=metrics.staged_conflicts,
            concurrency=max(1, concurrency),
        )
        _print_summary(metrics=metrics, apply=apply)
        return metrics
    finally:
        shutdown_logging()


def main() -> None:
    args = _parse_args()
    asyncio.run(
        _run(
            apply=args.apply,
            batch_size=args.batch_size,
            limit=args.limit,
            concurrency=args.concurrency,
        )
    )


def _print_summary(*, metrics: RefreshMetrics, apply: bool) -> None:
    mode = "apply" if apply else "dry-run"
    error_count = metrics.lookup_errors + metrics.not_found + metrics.duplicate_target_ids
    print(  # noqa: T201
        "\n".join(
            [
                "Listmonk subscriber id refresh completed",
                f"mode: {mode}",
                f"scanned: {metrics.scanned}",
                f"matched: {metrics.matched}",
                f"unchanged: {metrics.unchanged}",
                f"updated: {metrics.updated}",
                f"no_email: {metrics.no_email}",
                f"errors_total: {error_count}",
                f"not_found: {metrics.not_found}",
                f"lookup_errors: {metrics.lookup_errors}",
                f"duplicate_target_ids: {metrics.duplicate_target_ids}",
                f"staged_conflicts: {metrics.staged_conflicts}",
            ]
        )
    )
    _print_details(title="Not found emails", details=metrics.not_found_emails)
    _print_details(title="Lookup errors", details=metrics.lookup_error_details)
    _print_details(
        title="Duplicate target subscriber ids",
        details=metrics.duplicate_target_details,
    )


def _print_details(*, title: str, details: list[str] | None) -> None:
    if not details:
        return
    print(f"\n{title}:")  # noqa: T201
    for item in details:
        print(f"- {item}")  # noqa: T201


if __name__ == "__main__":
    main()
