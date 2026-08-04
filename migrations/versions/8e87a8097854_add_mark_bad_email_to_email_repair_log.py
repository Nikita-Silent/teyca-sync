"""add mark_bad_email to email_repair_log

Revision ID: 8e87a8097854
Revises: fb09beff5a97
Create Date: 2026-08-04 16:09:46.066235

teyca-sync-y1c: the Р5/Р6 policy (teyca-sync-37z) distinguishes a
"same person" loser (cleared without a bad-email mark) from a
"different person" loser (cleared and marked bad email). The existing
Listmonk-truth backfill always marked bad email, so default true keeps
its behavior unchanged; the new policy-based backfill sets this
explicitly per group.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8e87a8097854"
down_revision: str | Sequence[str] | None = "fb09beff5a97"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add mark_bad_email, backfilling existing rows to true."""
    op.add_column(
        "email_repair_log",
        sa.Column(
            "mark_bad_email",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
    )


def downgrade() -> None:
    """Drop mark_bad_email."""
    op.drop_column("email_repair_log", "mark_bad_email")
