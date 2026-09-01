"""
CQRS (Week 7) read-side integration tests for the Orders bounded context.

These verify the CQRS behavioural guarantees on the order read path:

* `GET /orders/{id}` resolves through the **query handler** off the read
  model (`orders_read_orders`).
* the projection is maintained transactionally by the persistence consumer,
  so an accepted order is immediately readable.
* confirm/cancel **commands** keep the projection's status in sync.
* by-customer listing is served from the customer_id-indexed read model via
  `OrderReadRepository.list_by_customer` (the write table no longer carries
  that index).
"""
from uuid import uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.contexts.orders.domain.order import Order, OrderLine
from src.contexts.orders.repositories.order_read_repository import OrderReadRepository
from src.contexts.orders.services.order_event_handler import PersistOrderCreatedHandler
from src.contexts.orders.events import build_order_created_event
from src.shared.infrastructure.database import Base

_CUSTOMER_ID = uuid4()


async def _register(client: AsyncClient, email: str, role: str = "CUSTOMER") -> dict:
    resp = await client.post(
        "/auth/register",
        json={"email": email, "full_name": "CQRS Read Test", "password": "S3curePass!", "role": role},
    )
    assert resp.status_code == 201
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _create_order(client: AsyncClient, token: str) -> dict:
    body = {
        "customer_id": str(_CUSTOMER_ID),
        "lines": [
            {"product_id": str(uuid4()), "quantity": 2, "unit_price_cents": 1500},
            {"product_id": str(uuid4()), "quantity": 1, "unit_price_cents": 500},
        ],
    }
    resp = await client.post("/orders", json=body, headers=_auth(token))
    assert resp.status_code == 202, resp.text
    return resp.json()


async def test_get_order_reads_from_read_model(client) -> None:
    """A GET after accept resolves via the query handler's read-model lookup."""
    user = await _register(client, "cqrs-read@example.com")
    order = await _create_order(client, user["access_token"])

    resp = await client.get(f"/orders/{order['id']}", headers=_auth(user["access_token"]))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == order["id"]
    assert body["status"] == "PENDING"
    assert body["total_cents"] == 2 * 1500 + 1 * 500
    assert len(body["lines"]) == 2
    assert body["lines"][0]["subtotal_cents"] == 3000  # materialized on read model


async def test_confirm_command_syncs_read_model(client) -> None:
    """Confirm transitions BOTH the write aggregate and the projection status."""
    user = await _register(client, "cqrs-confirm@example.com", role="ADMIN")
    order = await _create_order(client, user["access_token"])

    resp = await client.post(f"/orders/{order['id']}/confirm", headers=_auth(user["access_token"]))
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "CONFIRMED"

    after = await client.get(f"/orders/{order['id']}", headers=_auth(user["access_token"]))
    assert after.json()["status"] == "CONFIRMED"


async def test_cancel_command_syncs_read_model(client) -> None:
    user = await _register(client, "cqrs-cancel@example.com", role="ADMIN")
    order = await _create_order(client, user["access_token"])

    resp = await client.post(f"/orders/{order['id']}/cancel", headers=_auth(user["access_token"]))
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "CANCELLED"

    after = await client.get(f"/orders/{order['id']}", headers=_auth(user["access_token"]))
    assert after.json()["status"] == "CANCELLED"


@pytest.fixture
async def seeded_read_model_db():
    """A dedicated in-memory DB seeded through the persistence handler, used to
    exercise the query handler / read repository directly."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    handler = PersistOrderCreatedHandler(session_factory)
    for _ in range(3):
        order = Order(customer_id=_CUSTOMER_ID, lines=[OrderLine(product_id=uuid4(), quantity=1, unit_price_cents=1000)])
        await handler.handle(build_order_created_event(order))

    yield session_factory
    await engine.dispose()


async def test_list_by_customer_read_repository(seeded_read_model_db) -> None:
    """By-customer listing reads ONLY the customer_id-indexed projection."""
    async with seeded_read_model_db() as session:
        records = await OrderReadRepository(session).list_by_customer(_CUSTOMER_ID)

    assert len(records) == 3
    for record in records:
        assert record.customer_id == _CUSTOMER_ID
        assert record.status == "PENDING"
        assert record.total_cents == 1000
        assert record.line_count == 1


async def test_list_by_customer_is_isolated_per_customer(seeded_read_model_db) -> None:
    async with seeded_read_model_db() as session:
        other = await OrderReadRepository(session).list_by_customer(uuid4())
    assert other == []