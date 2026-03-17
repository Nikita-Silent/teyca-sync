"""normalize_emails_to_lowercase

Revision ID: e2c76c887215
Revises: 0e44aefbf8b3
Create Date: 2026-03-10 20:56:30.671697

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'e2c76c887215'
down_revision: str | Sequence[str] | None = '0e44aefbf8b3'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Normalize stored email values to lowercase for deterministic matching."""
    op.execute(
        sa.text(
            """
            UPDATE users
            SET email = lower(trim(email))
            WHERE email IS NOT NULL AND email <> lower(trim(email))
            """
        )
    )
    op.execute(
        sa.text(
            """
            UPDATE listmonk_users
            SET email = lower(trim(email))
            WHERE email IS NOT NULL AND email <> lower(trim(email))
            """
        )
    )


def downgrade() -> None:
    """No-op: data normalization is irreversible."""
