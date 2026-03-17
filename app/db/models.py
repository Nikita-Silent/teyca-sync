"""ORM models. No CASCADE deletes (roadmap)."""

from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, DateTime, ForeignKey, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    type_annotation_map = {
        dict[str, Any]: JSONB,
        list[int]: JSONB,
    }


class User(Base):
    """Master CRM profile and merged aggregates. PK = CRM user_id."""

    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=False)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    phone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    pat_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    birthday: Mapped[str | None] = mapped_column(String(32), nullable=True)
    gender: Mapped[str | None] = mapped_column(String(32), nullable=True)
    barcode: Mapped[str | None] = mapped_column(String(64), nullable=True)
    discount: Mapped[str | None] = mapped_column(String(32), nullable=True)
    bonus: Mapped[float | None] = mapped_column(nullable=True)
    loyalty_level: Mapped[str | None] = mapped_column(String(64), nullable=True)
    summ: Mapped[float | None] = mapped_column(nullable=True)
    summ_all: Mapped[float | None] = mapped_column(nullable=True)
    summ_last: Mapped[float | None] = mapped_column(nullable=True)
    check_summ: Mapped[float | None] = mapped_column(nullable=True)
    visits: Mapped[int | None] = mapped_column(nullable=True)
    visits_all: Mapped[int | None] = mapped_column(nullable=True)
    date_last: Mapped[str | None] = mapped_column(String(32), nullable=True)
    city: Mapped[str | None] = mapped_column(String(255), nullable=True)
    referal: Mapped[str | None] = mapped_column(String(255), nullable=True)
    tags: Mapped[list[int] | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )
    last_event_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_event_ts: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    version: Mapped[int] = mapped_column(default=1, nullable=True)


class ListmonkUser(Base):
    """Listmonk sync state per user."""

    __tablename__ = "listmonk_users"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.user_id", ondelete="RESTRICT"), primary_key=True
    )
    subscriber_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    list_ids: Mapped[str | None] = mapped_column(String(255), nullable=True)  # comma-separated
    attributes: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    consent_pending: Mapped[bool] = mapped_column(default=False)
    consent_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    consent_confirmed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=True
    )


class MergeLog(Base):
    """Merge fact; prevents duplicate merge."""

    __tablename__ = "merge_log"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    merged_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, nullable=True
    )
    source_event_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_event_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    merge_version: Mapped[int] = mapped_column(default=1, nullable=True)


class BonusAccrualLog(Base):
    """Idempotency and status of bonus accrual operations."""

    __tablename__ = "bonus_accrual_log"
    __table_args__ = (UniqueConstraint("idempotency_key", name="uq_bonus_accrual_idempotency_key"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reason: Mapped[str] = mapped_column(String(32), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="pending")
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    error_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class SyncState(Base):
    """Watermark state for incremental sync jobs."""

    __tablename__ = "sync_state"
    __table_args__ = (UniqueConstraint("source", "list_id", name="uq_sync_state_source_list"),)

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    list_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    watermark_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    watermark_subscriber_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow
    )
