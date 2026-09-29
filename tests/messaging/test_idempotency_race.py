"""Idempotency-under-concurrency tests for the two persistence consumers.

The `exists()` guard each handler uses to make at-least-once delivery idempotent
is a check-then-act, so two concurrent redeliveries of the same event can both
observe "missing" and race on the unique constraint. The constraint is what
actually makes the guard safe: the loser of the race gets an `IntegrityError`
and its whole transaction rolls back.

Neither handler treated that `IntegrityError` as the duplicate it is, so the
consumer classified it as transient, burned all three retries, and then
dead-lettered an order that had in fact been persisted correctly — a silently
lost order plus a spurious DLQ entry to explain it.

The race is reproduced faithfully here: the order/reservation row is really
persisted first (standing in for the worker that won the race), then the
existence guard is made to miss it once — the exact window a concurrent
duplicate falls into. The INSERT then genuinely violates the unique constraint,
so the `IntegrityError` under test comes from the real database, not a mock.
"""
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.repositories.inventory_reservation_repository import (
    InventoryReservationRepository,
)
from src.contexts.inventory.repositories.product_write_repository import ProductWriteRepository
from src.contexts.inventory.services.order_event_handler import DeductInventoryHandler
from src.contexts.orders.repositories.order_write_repository import OrderWriteRepository
from src.contexts.orders.services.order_event_handler import PersistOrderCreatedHandler
from src.shared.infrastructure.database import Base
from src.shared.messaging.events import DomainEvent

PRODUCT_A = str(uuid4())

_ORDER_GUARD = (
    "src.contexts.orders.services.order_event_handler.OrderWriteRepository.get_by_id"
)
_RESERVATION_GUARD = (
    "src.contexts.inventory.services.order_event_handler."
    "InventoryReservationRepository.exists"
)


def _order_created_event(order_id=None) -> DomainEvent:
    return DomainEvent(
        event_type="order.created",
        correlation_id="corr-race",
        payload={
            "order_id": str(order_id or uuid4()),
            "customer_id": str(uuid4()),
            "status": "PENDING",
            "total_cents": 2000,
            "lines": [{"product_id": PRODUCT_A, "quantity": 2, "unit_price_cents": 1000}],
        },
    )


def _blind_the_guard(monkeypatch, cls, method: str) -> None:
    """Make the existence guard report "absent" exactly once.

    That is the check-then-act window: the guard cannot see the row the winning
    transaction inserted moments ago, so this transaction's INSERT races and the
    database's unique constraint rejects it.
    """
    original = getattr(cls, method)
    calls = {"n": 0}

    async def _racy(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return None
        return await original(*args, **kwargs)

    monkeypatch.setattr(cls, method, _racy, raising=True)


@pytest.fixture
async def session_factory():
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield factory
    await engine.dispose()


@pytest.fixture
async def seeded(session_factory):
    async with session_factory() as session:
        repo = ProductWriteRepository(session)
        await repo.add(
            Product(
                id=UUID(PRODUCT_A),
                sku=f"SKU-{PRODUCT_A[:8]}",
                name="Widget",
                price_cents=1000,
                quantity_on_hand=100,
            )
        )
        await session.commit()
    return session_factory


async def _quantity(session_factory) -> int:
    async with session_factory() as session:
        product = await ProductWriteRepository(session).get_by_id(UUID(PRODUCT_A))
        return product.quantity_on_hand


class TestOrderPersistenceDuplicateRace:
    async def test_integrity_error_is_treated_as_a_duplicate_not_a_failure(
        self, session_factory, monkeypatch
    ):
        """The order is already persisted (the other worker won the race), so the
        handler must return successfully and let the consumer ack."""
        from src.contexts.orders.services import order_event_handler as mod

        event = _order_created_event()
        await PersistOrderCreatedHandler(session_factory).handle(event)

        _blind_the_guard(monkeypatch, mod.OrderWriteRepository, "get_by_id")

        # Must NOT raise: the consumer counts any raise as a failed delivery,
        # which is what caused the retry burn and the false-positive DLQ.
        await PersistOrderCreatedHandler(session_factory).handle(event)

    async def test_row_is_unchanged_by_the_losing_race(
        self, session_factory, monkeypatch
    ):
        """The winning transaction's row must be untouched by the loser."""
        from src.contexts.orders.services import order_event_handler as mod

        event = _order_created_event()
        await PersistOrderCreatedHandler(session_factory).handle(event)

        _blind_the_guard(monkeypatch, mod.OrderWriteRepository, "get_by_id")
        await PersistOrderCreatedHandler(session_factory).handle(event)

        async with session_factory() as session:
            order = await OrderWriteRepository(session).get_by_id(
                UUID(event.payload["order_id"])
            )
        assert order is not None
        assert order.total_cents == 2000
        assert len(order.lines) == 1, "the loser must not append duplicate lines"

    async def test_integrity_error_is_not_blanket_swallowed(
        self, session_factory, monkeypatch
    ):
        """The duplicate rescue must be conditional on the row actually existing.

        If the INSERT violated a constraint for some other reason and no order
        row can be found afterwards, the error is a genuine fault and must
        propagate so the consumer retries (and eventually dead-letters) it
        rather than acking data that was never written.
        """
        from src.contexts.orders.services import order_event_handler as mod

        event = _order_created_event()
        await PersistOrderCreatedHandler(session_factory).handle(event)

        # Guard blind on BOTH calls: the INSERT still violates the unique
        # constraint, but the post-rollback re-check cannot confirm the row.
        async def _always_absent(*args, **kwargs):
            return None

        monkeypatch.setattr(
            mod.OrderWriteRepository, "get_by_id", _always_absent, raising=True
        )

        with pytest.raises(IntegrityError):
            await PersistOrderCreatedHandler(session_factory).handle(event)


class TestInventoryDeductionDuplicateRace:
    async def test_integrity_error_is_treated_as_a_duplicate(
        self, seeded, monkeypatch
    ):
        """Same defect in the inventory worker: a lost insert race is a
        *successful* duplicate, so the event must be acked, not retried."""
        from src.contexts.inventory.services import order_event_handler as mod

        event = _order_created_event()
        await DeductInventoryHandler(seeded).handle(event)

        _blind_the_guard(monkeypatch, mod.InventoryReservationRepository, "exists")

        await DeductInventoryHandler(seeded).handle(event)  # must not raise

    async def test_stock_is_not_double_deducted(self, seeded, monkeypatch):
        """The losing transaction must roll back completely; a second deduction
        would silently corrupt inventory."""
        from src.contexts.inventory.services import order_event_handler as mod

        event = _order_created_event()
        await DeductInventoryHandler(seeded).handle(event)
        before = await _quantity(seeded)
        assert before == 98, "one deduction of 2 units"

        _blind_the_guard(monkeypatch, mod.InventoryReservationRepository, "exists")
        await DeductInventoryHandler(seeded).handle(event)

        assert await _quantity(seeded) == before, (
            "a duplicate must never deduct stock twice"
        )

    async def test_reservation_still_recorded_after_the_losing_race(
        self, seeded, monkeypatch
    ):
        from src.contexts.inventory.services import order_event_handler as mod

        event = _order_created_event()
        await DeductInventoryHandler(seeded).handle(event)

        _blind_the_guard(monkeypatch, mod.InventoryReservationRepository, "exists")
        await DeductInventoryHandler(seeded).handle(event)

        async with seeded() as session:
            assert await InventoryReservationRepository(session).exists(
                UUID(event.payload["order_id"])
            )
