"""
CQRS read phase (Week 8) — read-projector sync-worker handler unit tests.

The projector is the ONLY writer of the dedicated read store. These tests
verify it handles both event types, classifies anything else as a permanent
failure (=> DLQ), stays idempotent, and that documents round-trip through the
store-to-record mapping the query handlers rely on.
"""
from uuid import uuid4

import pytest

from src.contexts.orders.events import build_order_created_event, build_order_status_changed_event
from src.contexts.orders.domain.order import Order, OrderLine
from src.contexts.orders.repositories.order_read_store_repository import (
    document_to_record,
    record_to_document,
)
from src.contexts.orders.services.read_model_projector import ProjectOrderToReadStoreHandler
from src.shared.messaging.consumer import PermanentMessageError
from src.shared.messaging.events import DomainEvent
from src.shared.readstore import InMemoryOrderReadStore


def _sample_order() -> Order:
    return Order(
        customer_id=uuid4(),
        lines=[
            OrderLine(product_id=uuid4(), quantity=2, unit_price_cents=1500),
            OrderLine(product_id=uuid4(), quantity=1, unit_price_cents=500),
        ],
    )


async def test_created_event_projects_full_document() -> None:
    store = InMemoryOrderReadStore()
    projector = ProjectOrderToReadStoreHandler(store)
    order = _sample_order()

    await projector.handle(build_order_created_event(order, correlation_id="c1"))

    doc = await store.get_order(str(order.id))
    assert doc is not None
    assert doc["customer_id"] == str(order.customer_id)
    assert doc["status"] == "PENDING"
    assert doc["total_cents"] == 2 * 1500 + 500
    assert doc["line_count"] == 2
    assert len(doc["items"]) == 2
    assert doc["items"][0]["subtotal_cents"] == 3000


async def test_status_changed_event_updates_status_only() -> None:
    store = InMemoryOrderReadStore()
    projector = ProjectOrderToReadStoreHandler(store)
    order = _sample_order()

    await projector.handle(build_order_created_event(order))
    order.confirm()
    await projector.handle(build_order_status_changed_event(order))

    doc = await store.get_order(str(order.id))
    assert doc is not None
    assert doc["status"] == "CONFIRMED"
    assert doc["total_cents"] == 3500  # preserved, not clobbered
    assert doc["line_count"] == 2


async def test_duplicate_delivery_is_idempotent() -> None:
    store = InMemoryOrderReadStore()
    projector = ProjectOrderToReadStoreHandler(store)
    order = _sample_order()
    event = build_order_created_event(order)

    await projector.handle(event)
    await projector.handle(event)
    await projector.handle(event)

    assert await store.count() == 1
    assert await store.get_order(str(order.id)) is not None


async def test_unsupported_event_type_is_permanent() -> None:
    projector = ProjectOrderToReadStoreHandler(InMemoryOrderReadStore())
    event = DomainEvent(event_type="inventory.restocked", payload={"product_id": "x"})

    with pytest.raises(PermanentMessageError):
        await projector.handle(event)


async def test_malformed_status_payload_is_permanent() -> None:
    projector = ProjectOrderToReadStoreHandler(InMemoryOrderReadStore())
    event = DomainEvent(event_type="order.status.changed", payload={"order_id": ""})

    with pytest.raises(PermanentMessageError):
        await projector.handle(event)


def test_record_document_round_trip_preserves_order() -> None:
    """The document stored by the projector deserializes to the same read
    record the query handlers return."""
    order = _sample_order()
    from src.contexts.orders.repositories.order_read_repository import record_from_order

    doc = record_to_document(record_from_order(order))
    record = document_to_record(doc)

    assert record.id == order.id
    assert record.customer_id == order.customer_id
    assert record.status == order.status.value
    assert record.total_cents == order.total_cents
    assert record.line_count == 2
    assert record.lines[0].subtotal_cents == 3000


async def test_status_update_without_prior_document_is_safe() -> None:
    """A status event can beat the create event through the pipeline; the store
    must tolerate updating a not-yet-projected order without crashing."""
    store = InMemoryOrderReadStore()
    await store.update_status(str(uuid4()), "CONFIRMED")
    assert await store.count() == 0