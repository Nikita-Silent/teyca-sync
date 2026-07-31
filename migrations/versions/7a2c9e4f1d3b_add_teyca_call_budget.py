"""add teyca_call_budget

Revision ID: 7a2c9e4f1d3b
Revises: 4eb6e114bc0a
Create Date: 2026-07-31 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7a2c9e4f1d3b"
down_revision: str | Sequence[str] | None = "4eb6e114bc0a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "teyca_call_budget",
        sa.Column("window_kind", sa.String(length=16), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_count", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("window_kind"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("teyca_call_budget")
