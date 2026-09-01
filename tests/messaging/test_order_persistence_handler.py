"""
Integration tests — the `orders.order-created.persistence` consumer group
handler: persistence correctness, idempotency under at-least-once delivery,
and dead-letter classification of poison payloads.
"""
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.contexts.orders.events import order_from_created_event
from src.contexts.orders.repositories.order_write_repository import OrderWriteRepository
from src.contexts.orders.services.order_event_handler import PersistOrderCreatedHandler
from src.shared.infrastructure.database import Base
from src.shared.messaging.consumer import PermanentMessageError
from src.shared.messaging.events import DomainEvent


def _order_created_event(order_id=None) -> DomainEvent:
    return DomainEvent(
        event_type="order.created",
        correlation_id="corr-test",
        payload={
            "order_id": str(order_id or uuid4()),
            "customer_id": "00000000-0000-0000-0000-000000000001",
            "status": "PENDING",
            "total_cents": 3000,
            "lines": [
                {"product_id": "00000000-0000-0000-0000-000000000010", "quantity": 2, "unit_price_cents": 1000},
                {"product_id": "00000000-0000-0000-0000-000000000011", "quantity": 1, "unit_price_cents": 1000},
            ],
        },
    )


@pytest.fixture
async def handler_with_db():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield PersistOrderCreatedHandler(session_factory), session_factory
    await engine.dispose()


async def _get_order(session_factory, order_id):
    async with session_factory() as session:
        return await OrderWriteRepository(session).get_by_id(UUID(str(order_id)))

async def _get_read_model(session_factory, order_id):
    from src.contexts.orders.repositories.order_read_repository import OrderReadRepository

    async with session_factory() as session:
        return await OrderReadRepository(session).get_by_id(UUID(str(order_id)))


async def test_handler_persists_order_with_lines(handler_with_db) -> None:
    handler, session_factory = handler_with_db
    event = _order_created_event()

    await handler.handle(event)

    order = await _get_order(session_factory, event.payload["order_id"])
    assert order is not None
    assert str(order.id) == event.payload["order_id"]
    assert len(order.lines) == 2
    assert order.total_cents == 3000
    assert order.status.value == "PENDING"

    # CQRS read model is maintained transactionally with the write model.
    record = await _get_read_model(session_factory, event.payload["order_id"])
    assert record is not None
    assert record.status == "PENDING"
    assert record.total_cents == 3000
    assert record.line_count == 2
    assert len(record.lines) == 2


async def test_duplicate_delivery_is_idempotent(handler_with_db) -> None:
    """At-least-once delivery means the same event can arrive twice; the
    second occurrence must be a no-op (ack), never a crash or duplicate row."""
    handler, session_factory = handler_with_db
    event = _order_created_event()

    await handler.handle(event)
    await handler.handle(event)  # redelivery after ack-loss / restart

    order = await _get_order(session_factory, event.payload["order_id"])
    assert order is not None
    assert len(order.lines) == 2  # still exactly one persisted aggregate

    # Read model also stays idempotent (no duplicate projection rows).
    record = await _get_read_model(session_factory, event.payload["order_id"])
    assert record is not None
    assert record.line_count == 2


@pytest.mark.parametrize(
    "mutator",
    [
        lambda p: p.update(order_id="not-a-uuid"),
        lambda p: p.update(lines=[]),
        lambda p: p["lines"].append(
            {"product_id": "00000000-0000-0000-0000-000000000010", "quantity": -1, "unit_price_cents": 100}
        ),
        lambda p: p.update(customer_id="also-not-a-uuid"),
        lambda p: p.pop("order_id"),
    ],
)
async def test_poison_payloads_raise_permanent_error(handler_with_db, mutator) -> None:
    """PermanentMessageError => the consumer rejects without requeue => DLQ."""
    handler, session_factory = handler_with_db
    event = _order_created_event()
    mutator(event.payload)

    with pytest.raises(PermanentMessageError):
        await handler.handle(event)


async def test_unsupported_event_type_is_rejected_as_permanent(handler_with_db) -> None:
    handler, _ = handler_with_db
    event = DomainEvent(event_type="inventory.restocked", payload={"product_id": "x"})

    with pytest.raises(PermanentMessageError):
        await handler.handle(event)


def test_round_trip_through_event_bridge_preserves_aggregate() -> None:
    """build -> decode contract: the payload produced for publishing must be
    reconstructable into an equivalent aggregate by the consumer side."""
    from src.contexts.orders.domain.order import Order, OrderLine

    original = Order(
        customer_id=uuid4(),
        lines=[
            OrderLine(product_id=uuid4(), quantity=3, unit_price_cents=250),
            OrderLine(product_id=uuid4(), quantity=1, unit_price_cents=500),
        ],
    )

    from src.contexts.orders.events import build_order_created_event

    event = build_order_created_event(original, correlation_id="abc")
    assert event.to_json()  # serializable
    rebuilt = order_from_created_event(DomainEvent.from_json(event.to_json()))

    assert rebuilt.id == original.id
    assert rebuilt.customer_id == original.customer_id
    assert rebuilt.total_cents == original.total_cents
    assert [ln.product_id for ln in rebuilt.lines] == [ln.product_id for ln in original.lines]
