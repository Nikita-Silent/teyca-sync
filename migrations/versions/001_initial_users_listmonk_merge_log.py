"""Initial: users, listmonk_users, merge_log

Revision ID: 001
Revises:
Create Date: 2026-03-05

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("user_id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("phone", sa.String(length=64), nullable=True),
        sa.Column("first_name", sa.String(length=255), nullable=True),
        sa.Column("last_name", sa.String(length=255), nullable=True),
        sa.Column("pat_name", sa.String(length=255), nullable=True),
        sa.Column("birthday", sa.String(length=32), nullable=True),
        sa.Column("gender", sa.String(length=32), nullable=True),
        sa.Column("barcode", sa.String(length=64), nullable=True),
        sa.Column("discount", sa.String(length=32), nullable=True),
        sa.Column("bonus", sa.Float(), nullable=True),
        sa.Column("loyalty_level", sa.String(length=64), nullable=True),
        sa.Column("summ", sa.Float(), nullable=True),
        sa.Column("summ_all", sa.Float(), nullable=True),
        sa.Column("summ_last", sa.Float(), nullable=True),
        sa.Column("check_summ", sa.Float(), nullable=True),
        sa.Column("visits", sa.Integer(), nullable=True),
        sa.Column("visits_all", sa.Integer(), nullable=True),
        sa.Column("date_last", sa.String(length=32), nullable=True),
        sa.Column("city", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_event_id", sa.String(length=64), nullable=True),
        sa.Column("last_event_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("user_id"),
    )
    op.create_table(
        "listmonk_users",
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("subscriber_id", sa.BigInteger(), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=True),
        sa.Column("list_ids", sa.String(length=255), nullable=True),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("user_id"),
    )
    op.create_table(
        "merge_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("merged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("source_event_type", sa.String(length=32), nullable=True),
        sa.Column("source_event_id", sa.String(length=64), nullable=True),
        sa.Column("trace_id", sa.String(length=64), nullable=True),
        sa.Column("merge_version", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("merge_log")
    op.drop_table("listmonk_users")
    op.drop_table("users")
