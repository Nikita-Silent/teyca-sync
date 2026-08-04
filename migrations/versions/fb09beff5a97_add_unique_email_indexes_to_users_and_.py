"""add unique email indexes to users and listmonk_users

Revision ID: fb09beff5a97
Revises: 7a2c9e4f1d3b
Create Date: 2026-08-04 16:00:02.008838

teyca-sync-4wh / teyca-sync-ogi: closes the email race at the schema
level (Р5, Р6, Р13 — decided 2026-07-31). `get_other_user_ids_by_email`
cannot catch this on its own: it locks by user_id, but a race is between
two *different* user_id values, so both readers pass the check at once.
A unique index is the only reliable guard.

Verified on a prod copy (2026-07-30):
- listmonk_users accepts the index immediately (0 duplicates today).
- users fails with "Key (lower(TRIM(BOTH FROM email)))=(123@mail.ru) is
  duplicated" — so this migration must run strictly AFTER the one-time
  cleanup (teyca-sync-y1c). Applying it before that leaves 82 duplicate
  groups and the migration will fail on the users index.

CREATE/DROP INDEX CONCURRENTLY cannot run inside a transaction, hence
the autocommit_block() — this is the pitfall ogi calls out explicitly
for this migration.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "fb09beff5a97"
down_revision: str | Sequence[str] | None = "8e87a8097854"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

UNIQUE_PREDICATE_SQL = "email IS NOT NULL AND trim(email) <> ''"
UNIQUE_EXPRESSION_SQL = "lower(trim(email))"


def upgrade() -> None:
    """Add CONCURRENTLY-built unique indexes on lower(trim(email)).

    Raw SQL with IF NOT EXISTS, not op.create_index: CONCURRENTLY runs
    outside the migration's transaction, so a failure partway through
    (e.g. users still has duplicates) must leave a rerun safe — it would
    otherwise hit "relation already exists" on the listmonk_users index
    that already committed before the users index failed.
    """
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
                "uq_listmonk_users_email_lower_trim ON listmonk_users "
                f"({UNIQUE_EXPRESSION_SQL}) WHERE {UNIQUE_PREDICATE_SQL}"
            )
        )
    with op.get_context().autocommit_block():
        op.execute(
            sa.text(
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
                "uq_users_email_lower_trim ON users "
                f"({UNIQUE_EXPRESSION_SQL}) WHERE {UNIQUE_PREDICATE_SQL}"
            )
        )


def downgrade() -> None:
    """Drop both unique indexes, also outside a transaction."""
    with op.get_context().autocommit_block():
        op.execute(
            sa.text("DROP INDEX CONCURRENTLY IF EXISTS uq_users_email_lower_trim")
        )
    with op.get_context().autocommit_block():
        op.execute(
            sa.text("DROP INDEX CONCURRENTLY IF EXISTS uq_listmonk_users_email_lower_trim")
        )
