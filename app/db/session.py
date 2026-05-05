"""Async SQLAlchemy engine/session factory."""

import asyncio
from collections.abc import AsyncGenerator

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

logger = structlog.get_logger()

settings = get_settings()
engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=max(1, settings.database_pool_size),
    max_overflow=max(0, settings.database_pool_max_overflow),
    pool_timeout=max(0.1, settings.database_pool_timeout_seconds),
    pool_recycle=3600,
)
SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession]:
    """Yield async DB session."""
    async with SessionLocal() as session:
        yield session


async def wait_for_database(
    *,
    max_attempts: int = 10,
    initial_delay: float = 2.0,
    max_delay: float = 30.0,
) -> None:
    """Wait for the database with exponential backoff. Raises on last failed attempt."""
    delay = initial_delay
    for attempt in range(1, max_attempts + 1):
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return
        except Exception as exc:
            if attempt == max_attempts:
                raise
            logger.warning(
                "database_not_ready",
                attempt=attempt,
                max_attempts=max_attempts,
                retry_in=delay,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)
