"""CLI entrypoint for one-off legacy snapshot import."""

from __future__ import annotations

import argparse
import asyncio

import structlog

from app.db.session import SessionLocal
from app.workers.legacy_snapshot_importer import LegacySnapshotImporter, LegacySnapshotImportError

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import legacy users/listmonk/merge snapshot into current DB"
    )
    parser.add_argument(
        "--source-db-url",
        required=True,
        help="SQLAlchemy async URL for legacy PostgreSQL DB",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read and map all rows, but rollback target transaction at the end",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Rows per single INSERT statement (default: 500)",
    )
    return parser


async def _run(source_db_url: str, *, dry_run: bool, batch_size: int) -> None:
    async with SessionLocal() as session:
        importer = LegacySnapshotImporter(
            source_db_url=source_db_url,
            session=session,
            batch_size=batch_size,
        )
        stats = await importer.run(dry_run=dry_run)
    logger.info(
        "legacy_snapshot_import_finished",
        dry_run=dry_run,
        users_imported=stats.users_imported,
        users_skipped=stats.users_skipped,
        listmonk_users_imported=stats.listmonk_users_imported,
        listmonk_users_skipped=stats.listmonk_users_skipped,
        merge_rows_imported=stats.merge_rows_imported,
        merge_rows_skipped=stats.merge_rows_skipped,
        consent_accrual_rows_imported=stats.consent_accrual_rows_imported,
        consent_accrual_rows_skipped=stats.consent_accrual_rows_skipped,
        field_stats=stats.field_stats,
    )


def main() -> None:
    args = _build_parser().parse_args()
    try:
        asyncio.run(
            _run(
                source_db_url=args.source_db_url,
                dry_run=args.dry_run,
                batch_size=args.batch_size,
            )
        )
    except LegacySnapshotImportError as exc:
        logger.error("legacy_snapshot_import_failed", error=str(exc))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
