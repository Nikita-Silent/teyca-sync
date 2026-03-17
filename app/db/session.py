"""Async SQLAlchemy engine/session factory."""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

settings = get_settings()
engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=max(1, settings.database_pool_size),
    max_overflow=max(0, settings.database_pool_max_overflow),
    pool_timeout=max(0.1, settings.database_pool_timeout_seconds),
)
SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession]:
    """Yield async DB session."""
    async with SessionLocal() as session:
        yield session
