"""
Shared test fixtures.

Integration tests run against a throwaway in-memory SQLite database (via the
async aiosqlite driver) wired through FastAPI's dependency override for
`get_db_session`. The full HTTP stack is exercised end-to-end with httpx's
ASGITransport — real routing, middleware, exception handlers, and DB access —
so these are genuine integration tests that need no external PostgreSQL.

Since Week 5 (event-driven order creation), POST /orders publishes an event
instead of writing to the DB synchronously. Tests therefore also override the
`get_event_publisher` dependency with an InMemoryEventPublisher that:
  1. records every published event (assertable), and
  2. forwards it inline to the REAL persistence handler, so the asynchronous
     side of order creation behaves deterministically without a broker.
"""
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.contexts.orders.services.order_event_handler import PersistOrderCreatedHandler
from src.main import app
from src.shared.infrastructure.database import Base, get_db_session
from src.shared.infrastructure.read_database import get_read_db_session
from src.shared.messaging.provider import get_event_publisher
from src.shared.messaging.publisher import InMemoryEventPublisher

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

    async def override_get_read_db_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db_session
    app.dependency_overrides[get_read_db_session] = override_get_read_db_session

    # Event pipeline test double: record every event and deliver it inline to
    # the real persistence handler (simulates instant broker round-trip).
    publisher = InMemoryEventPublisher()
    publisher.subscribers.append(PersistOrderCreatedHandler(session_factory).handle)
    app.dependency_overrides[get_event_publisher] = lambda: publisher

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()
    await engine.dispose()


@pytest.fixture
async def evented_client():
    """Like `client`, but yields the client together with the InMemoryEventPublisher
    so tests can assert on exactly which events were published."""
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

    async def override_get_read_db_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db_session
    app.dependency_overrides[get_read_db_session] = override_get_read_db_session

    publisher = InMemoryEventPublisher()
    app.dependency_overrides[get_event_publisher] = lambda: publisher

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client, publisher

    app.dependency_overrides.clear()
    await engine.dispose()


@pytest.fixture
async def no_broker_client():
    """Client WITHOUT a publisher override: BROKER_URL is unset in the test
    environment, so POST /orders must fail closed with 503 EVENT_BROKER_UNAVAILABLE."""
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

    async def override_get_read_db_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db_session
    app.dependency_overrides[get_read_db_session] = override_get_read_db_session
    app.dependency_overrides.pop(get_event_publisher, None)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client

    app.dependency_overrides.clear()
    await engine.dispose()
