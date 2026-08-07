"""add webhook inbox

Revision ID: c1a2f7e9b4d6
Revises: fb09beff5a97
Create Date: 2026-08-07 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "c1a2f7e9b4d6"
down_revision: str | Sequence[str] | None = "fb09beff5a97"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "webhook_inbox",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source_event_id", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=16), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_by", sa.String(length=128), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("trace_id", sa.String(length=64), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_event_id", name="uq_webhook_inbox_source_event_id"),
    )
    op.create_index(
        op.f("ix_webhook_inbox_status"),
        "webhook_inbox",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_webhook_inbox_status"), table_name="webhook_inbox")
    op.drop_table("webhook_inbox")
