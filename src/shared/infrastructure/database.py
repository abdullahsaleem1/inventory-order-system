"""
Shared database session infrastructure.
Both bounded contexts (Inventory, Orders) depend on this, but do NOT share
tables directly — each context owns its own tables/schema to keep the
bounded contexts decoupled, even though they currently sit in one physical DB.
"""
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from src.core.config import get_settings
from src.core.telemetry import instrument_engine

settings = get_settings()

engine = create_async_engine(settings.DATABASE_URL, echo=settings.DEBUG, future=True)

# Week 10: every statement this engine issues becomes a `db.*` CLIENT span,
# child of whatever request or event handler triggered it.
instrument_engine(engine, name="write-model")

AsyncSessionLocal = async_sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)


class Base(DeclarativeBase):
    """Shared declarative base. Each context's ORM models inherit from this."""
    pass


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields a request-scoped DB session."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
