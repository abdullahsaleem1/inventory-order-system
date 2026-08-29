"""
Integration + unit tests — Week 6 async worker.

Covers the `orders.order-created.inventory` consumer group (the separate
worker service) that asynchronously deducts stock from `order.created` events:

  * correct stock deduction + idempotency-log write,
  * idempotency under at-least-once redelivery (never double-deduct),
  * graceful handling of malformed messages -> PermanentMessageError -> DLQ,
  * transient failures (insufficient stock) are retryable, not permanent,
  * the consumer's exponential-backoff schedule.
"""
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.events import (
    StockDeductionIntent,
    deduction_intent_from_created_event,
)
from src.contexts.inventory.repositories.inventory_reservation_repository import (
    InventoryReservationRepository,
)
from src.contexts.inventory.repositories.product_repository import ProductRepository
from src.contexts.inventory.services.order_event_handler import DeductInventoryHandler
from src.shared.infrastructure.database import Base
from src.shared.messaging.consumer import PermanentMessageError, RabbitMQEventConsumer
from src.shared.messaging.events import DomainEvent

PRODUCT_A = str(uuid4())
PRODUCT_B = str(uuid4())


async def _seed_products(session_factory, quantities: dict[str, int]) -> None:
    async with session_factory() as session:
        repo = ProductRepository(session)
        for pid, qty in quantities.items():
            await repo.add(
                Product(
                    id=UUID(pid),
                    sku=f"SKU-{pid[:8]}",
                    name="Widget",
                    price_cents=1000,
                    quantity_on_hand=qty,
                )
            )
        await session.commit()


def _order_created_event(product_ids, quantities, order_id=None) -> DomainEvent:
    return DomainEvent(
        event_type="order.created",
        correlation_id="corr-worker",
        payload={
            "order_id": str(order_id or uuid4()),
            "customer_id": str(uuid4()),
            "status": "PENDING",
            "total_cents": sum(1000 * q for q in quantities),
            "lines": [
                {"product_id": pid, "quantity": qty, "unit_price_cents": 1000}
                for pid, qty in zip(product_ids, quantities)
            ],
        },
    )


@pytest.fixture
async def worker_with_db():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield DeductInventoryHandler(session_factory), session_factory
    await engine.dispose()


async def _set_product_qty(session_factory, product_id: str, qty: int) -> None:
    async with session_factory() as session:
        repo = ProductRepository(session)
        product = await repo.get_by_id(UUID(product_id))
        product.quantity_on_hand = qty
        await repo.update(product)
        await session.commit()


async def _product_qty(session_factory, product_id) -> int:
    async with session_factory() as session:
        product = await ProductRepository(session).get_by_id(UUID(product_id))
        return product.quantity_on_hand


async def _reservation_exists(session_factory, order_id) -> bool:
    async with session_factory() as session:
        return await InventoryReservationRepository(session).exists(UUID(order_id))


# --- happy path --------------------------------------------------------------


async def test_worker_deducts_stock_for_each_line(worker_with_db) -> None:
    handler, session_factory = worker_with_db
    await _seed_products(session_factory, {PRODUCT_A: 10, PRODUCT_B: 5})
    event = _order_created_event([PRODUCT_A, PRODUCT_B], [3, 2])

    await handler.handle(event)

    assert await _product_qty(session_factory, PRODUCT_A) == 7
    assert await _product_qty(session_factory, PRODUCT_B) == 3
    assert await _reservation_exists(session_factory, event.payload["order_id"])


async def test_duplicate_delivery_is_idempotent(worker_with_db) -> None:
    """At-least-once delivery -> a redelivered event must not double-deduct."""
    handler, session_factory = worker_with_db
    await _seed_products(session_factory, {PRODUCT_A: 10})
    event = _order_created_event([PRODUCT_A], [4])

    await handler.handle(event)
    await handler.handle(event)  # redelivery after ack-loss / restart

    assert await _product_qty(session_factory, PRODUCT_A) == 6  # deducted exactly once


# --- transient failures -> retryable -----------------------------------------


async def test_insufficient_stock_is_retryable(worker_with_db) -> None:
    """Not enough stock is a transient condition (a restock may fix it), so it
    must raise a NON-permanent error -> the consumer retries with backoff -
    and must leave no partial deductions behind."""
    handler, session_factory = worker_with_db
    await _seed_products(session_factory, {PRODUCT_A: 2})
    event = _order_created_event([PRODUCT_A], [5])

    with pytest.raises(Exception) as exc_info:
        await handler.handle(event)

    assert not isinstance(exc_info.value, PermanentMessageError)
    # Atomic rollback: the reservation log row must NOT have been written.
    assert not await _reservation_exists(session_factory, event.payload["order_id"])


async def test_transient_deduction_after_restock_succeeds(worker_with_db) -> None:
    """E2E retry story: first attempt fails (insufficient stock), the product is
    restocked, and a second attempt then succeeds — the retry mechanism works."""
    handler, session_factory = worker_with_db
    await _seed_products(session_factory, {PRODUCT_A: 2})
    event = _order_created_event([PRODUCT_A], [5])

    with pytest.raises(Exception):
        await handler.handle(event)

    # Restock then redeliver the same event.
    await _set_product_qty(session_factory, PRODUCT_A, 10)
    await handler.handle(event)

    assert await _product_qty(session_factory, PRODUCT_A) == 5


# --- malformed messages -> gracefully dead-lettered ---------------------------


@pytest.mark.parametrize(
    "mutator",
    [
        lambda p: p.update(order_id="not-a-uuid"),
        lambda p: p.pop("order_id"),
        lambda p: p["lines"].pop(),  # empty lines
        lambda p: p["lines"][0].update(product_id="not-a-uuid"),
        lambda p: p["lines"][0].update(quantity=-1),
        lambda p: p["lines"][0].update(quantity="many"),
        lambda p: p.update(lines="not-a-list"),
    ],
)
async def test_malformed_payload_raises_permanent_error(worker_with_db, mutator) -> None:
    """Malformed `order.created` payloads are rejected as permanent => the
    consumer nacks without requeue => straight to the DLQ, no retry burn."""
    handler, session_factory = worker_with_db
    await _seed_products(session_factory, {PRODUCT_A: 10})
    event = _order_created_event([PRODUCT_A], [1])
    mutator(event.payload)

    with pytest.raises(PermanentMessageError):
        await handler.handle(event)


async def test_unknown_product_is_permanent_not_retryable(worker_with_db) -> None:
    """A missing product can never be fixed by retrying, so it must be permanent."""
    handler, session_factory = worker_with_db
    event = _order_created_event([str(uuid4())], [1])  # no product seeded

    with pytest.raises(PermanentMessageError):
        await handler.handle(event)


async def test_unsupported_event_type_is_rejected_as_permanent(worker_with_db) -> None:
    handler, _ = worker_with_db
    event = DomainEvent(event_type="inventory.restocked", payload={"lines": []})

    with pytest.raises(PermanentMessageError):
        await handler.handle(event)


# --- bridge / intent reconstruction ------------------------------------------


def test_deduction_intent_round_trip_preserves_lines() -> None:
    event = _order_created_event([PRODUCT_A, PRODUCT_B], [2, 5])
    intent: StockDeductionIntent = deduction_intent_from_created_event(event)

    assert str(intent.order_id) == event.payload["order_id"]
    assert intent.event_id == event.event_id
    assert [(str(l.product_id), l.quantity) for l in intent.lines] == [(PRODUCT_A, 2), (PRODUCT_B, 5)]


# --- exponential backoff schedule --------------------------------------------


def test_backoff_is_exponential_and_bounded() -> None:
    from src.core.config import get_settings

    consumer = RabbitMQEventConsumer(
        "amqp://x",
        [],
        max_retries=4,
        backoff_base_seconds=1.0,
        backoff_max_seconds=60.0,
    )
    assert consumer._backoff_seconds(1) == 1.0
    assert consumer._backoff_seconds(2) == 2.0
    assert consumer._backoff_seconds(3) == 4.0
    # Exponential growth is capped by the max bound.
    assert consumer._backoff_seconds(20) == 60.0
