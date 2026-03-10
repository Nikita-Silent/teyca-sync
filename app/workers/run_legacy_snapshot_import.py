"""CLI entrypoint for one-off legacy snapshot import."""

from __future__ import annotations

import argparse
import asyncio

import structlog

from app.db.session import SessionLocal
from app.workers.legacy_snapshot_importer import LegacySnapshotImportError, LegacySnapshotImporter

logger = structlog.get_logger()


def _build_parser() -> argparse.ArgumentParser:
    """
    Create and configure the command-line ArgumentParser for the legacy snapshot importer.
    
    The parser defines the required --source-db-url option (SQLAlchemy async URL for the legacy PostgreSQL database),
    an optional --dry-run flag to simulate the import without committing, and an optional --batch-size integer
    (default 500) to control rows per INSERT.
    
    Returns:
        argparse.ArgumentParser: Configured parser with the `--source-db-url`, `--dry-run`, and `--batch-size` options.
    """
    parser = argparse.ArgumentParser(description="Import legacy users/listmonk/merge snapshot into current DB")
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
    """
    Run a legacy snapshot import from the specified legacy database and log summary statistics.
    
    Parameters:
        source_db_url (str): SQLAlchemy async URL for the legacy PostgreSQL database to import from.
        dry_run (bool): If True, simulate the import without committing changes to the destination database.
        batch_size (int): Number of rows to batch when inserting into the destination database; higher values reduce transaction overhead.
    """
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
    """
    Entry point for the legacy snapshot import CLI.
    
    Parses command-line arguments, runs the asynchronous import task with the provided options, and exits with status code 2 if the import fails due to a LegacySnapshotImportError (logging the failure).
    """
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
