"""
Shared test fixtures.

Integration tests run against a throwaway in-memory SQLite database (via the
async aiosqlite driver) wired through FastAPI's dependency override for
`get_db_session`. The full HTTP stack is exercised end-to-end with httpx's
ASGITransport — real routing, middleware, exception handlers, and DB access —
so these are genuine integration tests that need no external PostgreSQL.
"""
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.main import app
from src.shared.infrastructure.database import Base, get_db_session

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture
async def client():
    engine = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async def override_get_db_session():
        async with session_factory() as session:
            try:
                yield session
            finally:
                if session.is_active:
                    await session.commit()

    app.dependency_overrides[get_db_session] = override_get_db_session

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()
    await engine.dispose()
