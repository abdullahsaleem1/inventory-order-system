"""
Shared test fixtures.

Integration tests run against a throwaway in-memory SQLite database (via the
async aiosqlite driver) wired through FastAPI's dependency override for
`get_db_session`. The full HTTP stack is exercised end-to-end with httpx's
ASGITransport — real routing, middleware, exception handlers, and DB access —
so these are genuine integration tests that need no external PostgreSQL.

Since Week 5 (event-driven order creation), POST /orders publishes an event
instead of writing to the DB synchronously. Tests therefore also override the
`get_event_publisher` dependency with an InMemoryEventPublisher that records
every published event and forwards it inline to its subscribers.

Since Week 8 (CQRS read phase), order query handlers read EXCLUSIVELY from the
dedicated read store. Fixtures override `get_read_store` with a fresh
InMemoryOrderReadStore and register the read-model projector as an event
subscriber, so the event -> read-store projection happens deterministically the
same way the real sync worker would — with no Elasticsearch container needed.
"""
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.contexts.orders.services.order_event_handler import PersistOrderCreatedHandler
from src.contexts.orders.services.read_model_projector import ProjectOrderToReadStoreHandler
from src.main import app
from src.shared.infrastructure.database import Base, get_db_session
from src.shared.infrastructure.read_database import get_read_db_session
from src.shared.messaging.events import EventTypes
from src.shared.messaging.provider import get_event_publisher
from src.shared.messaging.publisher import InMemoryEventPublisher
from src.shared.readstore import InMemoryOrderReadStore
from src.shared.readstore.factory import get_read_store

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


def _for_event_types(event_types: set[str]):
    """Wrap a consumer-group handler so the in-memory broker only delivers the
    event types its real queue is bound to (mirrors queue routing-key binds)."""

    def decorator(subscriber):
        async def filtered(event) -> None:
            if event.event_type in event_types:
                await subscriber(event)

        return filtered

    return decorator


def _make_read_store_wiring() -> InMemoryOrderReadStore:
    """Override `get_read_store` with a fresh in-memory read store — the store
    order query handlers read from (Week 8 dedicated read store)."""
    read_store = InMemoryOrderReadStore()

    async def override_get_read_store():
        return read_store

    app.dependency_overrides[get_read_store] = override_get_read_store
    return read_store


def _subscribe_read_pipeline(session_factory, read_store) -> InMemoryEventPublisher:
    """Record every published event and deliver it inline to the REAL
    persistence and read-projector handlers — simulating a broker + sync-worker
    round trip deterministically, with no container required."""
    publisher = InMemoryEventPublisher()
    publisher.subscribers.append(
        _for_event_types({EventTypes.ORDER_CREATED})(PersistOrderCreatedHandler(session_factory).handle)
    )
    publisher.subscribers.append(
        _for_event_types({EventTypes.ORDER_CREATED, EventTypes.ORDER_STATUS_CHANGED})(
            ProjectOrderToReadStoreHandler(read_store).handle
        )
    )
    app.dependency_overrides[get_event_publisher] = lambda: publisher
    return publisher


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
    # the real persistence AND read-projector handlers (simulates a broker +
    # sync worker round-trip; queries read from the projected read store).
    read_store = _make_read_store_wiring()
    _subscribe_read_pipeline(session_factory, read_store)

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

    # Persist the write model inline (so confirm/cancel commands find the order)
    # but do NOT project into the read store — the test drives the projector
    # itself to exercise eventual-consistency explicitly. The fresh read store
    # is exposed via the publisher for that purpose.
    read_store = _make_read_store_wiring()
    publisher = InMemoryEventPublisher()
    publisher.subscribers.append(
        _for_event_types({EventTypes.ORDER_CREATED})(PersistOrderCreatedHandler(session_factory).handle)
    )
    publisher.read_store = read_store
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
    _make_read_store_wiring()
    app.dependency_overrides.pop(get_event_publisher, None)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client

    app.dependency_overrides.clear()
    await engine.dispose()
