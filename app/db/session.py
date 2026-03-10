"""Async SQLAlchemy engine/session factory."""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

settings = get_settings()
engine = create_async_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Provide an AsyncSession for use in asynchronous database operations.
    
    Yields a session instance and guarantees the session is closed when the generator exits.
    
    Returns:
        AsyncSession: An asynchronous SQLAlchemy session for performing database operations.
    """
    async with SessionLocal() as session:
        yield session
