"""
CQRS read phase (Week 8) — eventual-consistency integration tests.

These verify the behaviour that separates the dedicated read store from the
write side:

  * ORDER QUERY HANDLERS READ EXCLUSIVELY FROM THE READ STORE — an order that
    has been persisted to the write model but not yet projected returns 404.
  * The read store is updated ONLY by the sync worker (the read projector)
    consuming broker events — in these tests the InMemoryEventPublisher is
    deliberately wired WITHOUT the projector subscriber, so the test itself
    plays the role of the sync worker and drives the catch-up explicitly.
  * Status transitions (confirm/cancel) flow through `order.status.changed`
    events; until the projector applies them the read store serves the stale
    (pre-transition) status, then converges.
  * Projection is idempotent under at-least-once delivery.
"""
from uuid import uuid4

from httpx import AsyncClient

from src.contexts.orders.services.read_model_projector import ProjectOrderToReadStoreHandler
from src.shared.messaging.events import EventTypes

_CUSTOMER_ID = str(uuid4())


async def _register(client: AsyncClient, email: str, role: str = "CUSTOMER") -> dict:
    resp = await client.post(
        "/auth/register",
        json={"email": email, "full_name": "Consistency Test", "password": "S3curePass!", "role": role},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _order_body() -> dict:
    return {
        "customer_id": _CUSTOMER_ID,
        "lines": [{"product_id": str(uuid4()), "quantity": 2, "unit_price_cents": 1500}],
    }


async def test_get_before_read_store_catches_up_returns_404(evented_client) -> None:
    """The write model is persisted, but the read store is empty — a GET must
    404 because query handlers read exclusively from the (not yet projected)
    read store, never the write database."""
    client, publisher = evented_client
    user = await _register(client, "consistency-gap@example.com")

    resp = await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))
    assert resp.status_code == 202, resp.text
    order_id = resp.json()["id"]
    assert len(publisher.published) == 1  # order.created published

    before = await client.get(f"/orders/{order_id}", headers=_auth(user["access_token"]))
    assert before.status_code == 404


async def test_read_store_catches_up_when_projector_processes_event(evented_client) -> None:
    """Driving the sync worker (projecting the buffered event) makes the order
    readable — the eventual-consistency convergence."""
    client, publisher = evented_client
    read_store = publisher.read_store
    user = await _register(client, "consistency-converge@example.com")
    projector = ProjectOrderToReadStoreHandler(read_store)

    resp = await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))
    order_id = resp.json()["id"]
    assert (await client.get(f"/orders/{order_id}", headers=_auth(user["access_token"]))).status_code == 404

    event = publisher.published[0]
    await projector.handle(event)  # the sync worker consumes the event

    after = await client.get(f"/orders/{order_id}", headers=_auth(user["access_token"]))
    assert after.status_code == 200
    body = after.json()
    assert body["id"] == order_id
    assert body["status"] == "PENDING"
    assert body["total_cents"] == 3000
    assert body["lines"][0]["subtotal_cents"] == 3000


async def test_status_change_is_eventually_consistent(evented_client) -> None:
    """Confirm transitions the WRITE model and publishes order.status.changed;
    until the projector applies it the read store still serves PENDING, then
    converges to CONFIRMED."""
    client, publisher = evented_client
    read_store = publisher.read_store
    user = await _register(client, "consistency-status@example.com", role="ADMIN")
    projector = ProjectOrderToReadStoreHandler(read_store)

    resp = await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))
    order_id = resp.json()["id"]
    await projector.handle(publisher.published[0])
    assert (await client.get(f"/orders/{order_id}", headers=_auth(user["access_token"]))).json()["status"] == "PENDING"

    confirm = await client.post(f"/orders/{order_id}/confirm", headers=_auth(user["access_token"]))
    assert confirm.status_code == 200
    assert confirm.json()["status"] == "CONFIRMED"

    status_events = [e for e in publisher.published if e.event_type == EventTypes.ORDER_STATUS_CHANGED]
    assert len(status_events) == 1
    assert status_events[0].payload == {"order_id": order_id, "status": "CONFIRMED"}

    # Not yet projected: the read store still serves the stale status.
    stale = (await client.get(f"/orders/{order_id}", headers=_auth(user["access_token"]))).json()
    assert stale["status"] == "PENDING"

    # Sync worker applies the status event -> read store converges.
    await projector.handle(status_events[0])
    converged = (await client.get(f"/orders/{order_id}", headers=_auth(user["access_token"]))).json()
    assert converged["status"] == "CONFIRMED"


async def test_cancel_status_change_update_works_even_if_no_previous_status(evented_client) -> None:
    """update_status is safe when the document is not projected yet (a status
    event can beat the create event through the pipeline); reprocessing the
    status event afterwards converges the read store."""
    client, publisher = evented_client
    read_store = publisher.read_store
    user = await _register(client, "consistency-cancel@example.com", role="ADMIN")
    projector = ProjectOrderToReadStoreHandler(read_store)

    resp = await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))
    order_id = resp.json()["id"]
    created = publisher.published[0]

    # Cancel (publishes status.changed) BEFORE the created event is projected.
    cancel = await client.post(f"/orders/{order_id}/cancel", headers=_auth(user["access_token"]))
    assert cancel.status_code == 200

    status_event = next(e for e in publisher.published if e.event_type == EventTypes.ORDER_STATUS_CHANGED)

    # Status event arrives first: no document yet -> safe no-op, no partial doc.
    await projector.handle(status_event)
    assert await read_store.count() == 0

    # The create event lands the full snapshot (status as-of publish time).
    await projector.handle(created)
    body = (await client.get(f"/orders/{order_id}", headers=_auth(user["access_token"]))).json()
    assert body["status"] == "PENDING"
    assert body["total_cents"] == 3000

    # Worker reprocesses the status event -> read store converges to CANCELLED
    # without clobbering the rest of the document.
    await projector.handle(status_event)
    body = (await client.get(f"/orders/{order_id}", headers=_auth(user["access_token"]))).json()
    assert body["status"] == "CANCELLED"
    assert body["total_cents"] == 3000


async def test_duplicate_delivery_is_idempotent(evented_client) -> None:
    """At-least-once delivery: replaying the same event must not error or
    duplicate the document."""
    client, publisher = evented_client
    read_store = publisher.read_store
    user = await _register(client, "consistency-idempotent@example.com")
    projector = ProjectOrderToReadStoreHandler(read_store)

    await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))
    await projector.handle(publisher.published[0])
    await projector.handle(publisher.published[0])  # redelivery

    assert await read_store.count() == 1
    order_id = publisher.published[0].payload["order_id"]
    doc = await read_store.get_order(order_id)
    assert doc is not None
    assert doc["line_count"] == 1


async def test_converged_read_store_matches_write_model(evented_client) -> None:
    """After the projector has caught up on several orders, the dedicated read
    store matches the write model for by-customer listing."""
    client, publisher = evented_client
    read_store = publisher.read_store
    user = await _register(client, "consistency-match@example.com")
    projector = ProjectOrderToReadStoreHandler(read_store)

    for _ in range(3):
        resp = await client.post("/orders", json=_order_body(), headers=_auth(user["access_token"]))
        assert resp.status_code == 202

    for event in publisher.published:
        await projector.handle(event)

    docs = await read_store.list_by_customer(_CUSTOMER_ID)
    assert len(docs) == 3
    for doc in docs:
        assert doc["customer_id"] == _CUSTOMER_ID
        assert doc["status"] == "PENDING"
        assert doc["total_cents"] == 3000