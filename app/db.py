"""Async SQLAlchemy engine/session and schema creation."""

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from .config import get_settings

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


settings = get_settings()
engine = create_async_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def init_db(retries: int = 15, delay: float = 2.0) -> None:
    """Create tables on startup (no migrations for this scope)."""
    for attempt in range(1, retries + 1):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("database ready")
            return
        except Exception as exc:
            if attempt == retries:
                raise
            logger.warning("database not reachable yet (%s); retrying...", exc)
            await asyncio.sleep(delay)
