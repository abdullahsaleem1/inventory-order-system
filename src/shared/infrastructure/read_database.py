"""
Read-side database infrastructure (CQRS Week 7).

When `READ_DATABASE_URL` is configured the read side gets its own engine and
session factory, pointed at the read-optimized store (a separate physical or
logical database that hosts the denormalized read-model tables). Query
handlers and read repositories resolve their sessions through
`get_read_db_session()`.

When `READ_DATABASE_URL` is left empty — the default, and the only mode the
test-suite exercises — the system runs in **single-database CQRS**: the write
model and the read model share one physical database, but remain completely
separate *logically* (different tables, different repositories, different
handlers). The read side still never touches the normalized write tables for
API queries; it reads the projections.

Tests override `get_read_db_session` with the same in-memory SQLite session
they use for the write side, so both sides share one throwaway database.
"""
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.config import get_settings
from src.core.telemetry import instrument_engine
from src.shared.infrastructure.database import AsyncSessionLocal

settings = get_settings()

_read_engine = None
_read_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_read_session_factory() -> async_sessionmaker[AsyncSession] | None:
    """Return a session factory bound to the dedicated read store, or None
    when READ_DATABASE_URL is unset (single-database CQRS mode)."""
    global _read_engine, _read_session_factory
    if _read_session_factory is None:
        read_url = (settings.READ_DATABASE_URL or "").strip()
        if read_url:
            _read_engine = create_async_engine(read_url, echo=settings.DEBUG, future=True)
            instrument_engine(_read_engine, name="read-model")
            _read_session_factory = async_sessionmaker(
                bind=_read_engine, expire_on_commit=False, class_=AsyncSession
            )
        else:
            return None
    return _read_session_factory


async def get_read_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for the read side.

    Yields a request-scoped session to the read-optimized store when one is
    configured; otherwise falls back to a session on the primary database
    (single-database CQRS). Read-only by design — query handlers never write.
    """
    factory = get_read_session_factory()
    if factory is not None:
        async with factory() as session:
            yield session
    else:
        async with AsyncSessionLocal() as session:
            yield session