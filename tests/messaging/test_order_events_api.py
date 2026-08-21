"""
Integration tests — Week 5: verify the event publishing logic of the
refactored POST /orders flow.

These tests exercise the full HTTP stack (routing, auth, RBAC, middleware,
validation) with an InMemoryEventPublisher substituted for the RabbitMQ
publisher, asserting:

  * a successful order creation publishes EXACTLY ONE `order.created` event
  * the event envelope + payload faithfully represent the request
  * correlation_id propagates from the HTTP X-Request-ID middleware value
  * validation/auth failures publish NOTHING (no side effects on the broker)
  * with no broker configured, creation fails closed (503) instead of
    silently dropping the event

The publisher is overridden at the dependency level, so no RabbitMQ container
is needed to run this suite.
"""
from datetime import datetime, timezone

from httpx import AsyncClient

from src.shared.messaging.events import EventTypes


async def _register(client: AsyncClient, email: str) -> dict:
    resp = await client.post(
        "/auth/register",
        json={"email": email, "full_name": "Events Test", "password": "S3curePass!"},
    )
    assert resp.status_code == 201
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


_ORDER_BODY = {
    "customer_id": "00000000-0000-0000-0000-000000000001",
    "lines": [
        {"product_id": "00000000-0000-0000-0000-000000000010", "quantity": 2, "unit_price_cents": 1500},
        {"product_id": "00000000-0000-0000-0000-000000000011", "quantity": 1, "unit_price_cents": 500},
    ],
}


# --- happy path: one event per accepted order --------------------------------


async def test_create_order_publishes_exactly_one_order_created_event(evented_client) -> None:
    client, publisher = evented_client
    user = await _register(client, "evt-happy@example.com")

    resp = await client.post("/orders", json=_ORDER_BODY, headers=_auth(user["access_token"]))

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["status"] == "PENDING"
    assert body["total_cents"] == 2 * 1500 + 1 * 500

    assert len(publisher.published) == 1
    event = publisher.published[0]
    assert event.event_type == EventTypes.ORDER_CREATED
    # The published event id is echoed back to the client for traceability.
    assert str(event.event_id) == body["event_id"]
    assert body["id"] == event.payload["order_id"]


async def test_published_event_payload_matches_request(evented_client) -> None:
    client, publisher = evented_client
    user = await _register(client, "evt-payload@example.com")

    await client.post("/orders", json=_ORDER_BODY, headers=_auth(user["access_token"]))

    event = publisher.published[0]
    payload = event.payload
    assert payload["customer_id"] == _ORDER_BODY["customer_id"]
    assert payload["status"] == "PENDING"
    assert payload["total_cents"] == 3500
    assert len(payload["lines"]) == 2
    assert payload["lines"][0] == {
        "product_id": _ORDER_BODY["lines"][0]["product_id"],
        "quantity": 2,
        "unit_price_cents": 1500,
    }


async def test_correlation_id_is_http_request_id(evented_client) -> None:
    client, publisher = evented_client
    user = await _register(client, "evt-corr@example.com")

    resp = await client.post(
        "/orders",
        json={
            "customer_id": "00000000-0000-0000-0000-000000000002",
            "lines": [{"product_id": "00000000-0000-0000-0000-000000000010", "quantity": 1, "unit_price_cents": 100}],
        },
        headers={**_auth(user["access_token"]), "X-Request-ID": "test-correlation-42"},
    )

    assert resp.status_code == 202
    assert publisher.published[0].correlation_id == "test-correlation-42"


async def test_event_envelope_is_serializable_with_utc_timestamp(evented_client) -> None:
    import json

    client, publisher = evented_client
    user = await _register(client, "evt-json@example.com")
    await client.post("/orders", json=_ORDER_BODY, headers=_auth(user["access_token"]))

    event = publisher.published[0]
    doc = json.loads(event.to_json())
    assert set(doc.keys()) == {"event_id", "event_type", "occurred_at", "correlation_id", "payload"}
    assert doc["event_type"] == "order.created"
    parsed_ts = datetime.fromisoformat(doc["occurred_at"])
    assert parsed_ts.tzinfo is not None and parsed_ts.utcoffset() == timezone.utc.utcoffset(parsed_ts)


# --- failure paths: nothing may reach the broker ------------------------------


async def test_validation_failure_publishes_nothing(evented_client) -> None:
    client, publisher = evented_client
    user = await _register(client, "evt-invalid@example.com")

    invalid = {"customer_id": "00000000-0000-0000-0000-000000000001", "lines": []}
    resp = await client.post("/orders", json=invalid, headers=_auth(user["access_token"]))

    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "VALIDATION_ERROR"
    assert publisher.published == []  # no event for invalid orders


async def test_unauthenticated_request_publishes_nothing(evented_client) -> None:
    client, publisher = evented_client

    resp = await client.post("/orders", json=_ORDER_BODY)

    assert resp.status_code == 401
    assert publisher.published == []


async def test_broker_not_configured_fails_closed_with_503(no_broker_client) -> None:
    client = no_broker_client
    user = await _register(client, "evt-nobroker@example.com")

    resp = await client.post("/orders", json=_ORDER_BODY, headers=_auth(user["access_token"]))

    assert resp.status_code == 503
    envelope = resp.json()
    assert envelope["error"]["code"] == "EVENT_BROKER_UNAVAILABLE"
    assert envelope["status"] == 503
    assert envelope["path"] == "/orders"
    assert envelope["request_id"]  # standardized envelope preserved
