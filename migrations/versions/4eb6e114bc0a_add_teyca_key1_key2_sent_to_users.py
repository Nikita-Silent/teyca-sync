"""add teyca_key1_sent/teyca_key2_sent to users

Revision ID: 4eb6e114bc0a
Revises: 9bac67d00026
Create Date: 2026-07-31 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4eb6e114bc0a"
down_revision: str | Sequence[str] | None = "9bac67d00026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("users", sa.Column("teyca_key1_sent", sa.String(length=64), nullable=True))
    op.add_column("users", sa.Column("teyca_key2_sent", sa.String(length=64), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("users", "teyca_key2_sent")
    op.drop_column("users", "teyca_key1_sent")
