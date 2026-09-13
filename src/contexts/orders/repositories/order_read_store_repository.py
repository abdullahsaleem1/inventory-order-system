"""
Orders bounded context — read-side repository for the dedicated read store
(CQRS Week 8).

This repository is the ONLY path the order query handlers use. It reads
denormalized documents from the `OrderReadStore` (Elasticsearch in production,
in-memory in tests) — the write database is never consulted for API reads.

Mapping helpers convert between two interchangeable representations:
  * `OrderReadRecord` — the read-side DTO returned to query handlers (kept so
    controllers/schemas are unchanged from Week 7), and
  * plain document dicts — the JSON shape stored in the read store.

The Week 7 `OrderReadRepository` (backed by the `orders_read_orders` Postgres
projection) still exists, but it now serves ONLY projection maintenance on the
write side (persistence consumer, confirm/cancel transaction) — API queries no
longer touch it.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from src.shared.readstore.base import OrderReadStore

from src.contexts.orders.domain.order import Order
from src.contexts.orders.repositories.order_read_repository import (
    OrderLineRecord,
    OrderReadRecord,
    record_from_order,
)


def record_to_document(record: OrderReadRecord, *, occurred_at: datetime | None = None) -> dict:
    """Serialize a read record into the denormalized read-store document."""
    return {
        "order_id": str(record.id),
        "customer_id": str(record.customer_id),
        "status": record.status,
        "total_cents": record.total_cents,
        "line_count": record.line_count,
        "items": [
            {
                "product_id": str(line.product_id),
                "quantity": line.quantity,
                "unit_price_cents": line.unit_price_cents,
                "subtotal_cents": line.subtotal_cents,
            }
            for line in record.lines
        ],
        "created_at": (occurred_at or record.created_at).isoformat(),
        "updated_at": record.updated_at.isoformat(),
    }


def document_from_order(order: Order, *, occurred_at: datetime | None = None) -> dict:
    """Map an Order aggregate straight to a read-store document."""
    return record_to_document(record_from_order(order), occurred_at=occurred_at)


def _parse_dt(value: str | datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def document_to_record(document: dict) -> OrderReadRecord:
    """Deserialize a read-store document back into an `OrderReadRecord`."""
    return OrderReadRecord(
        id=UUID(str(document["order_id"])),
        customer_id=UUID(str(document["customer_id"])),
        status=str(document["status"]),
        total_cents=int(document["total_cents"]),
        lines=[
            OrderLineRecord(
                product_id=UUID(str(item["product_id"])),
                quantity=int(item["quantity"]),
                unit_price_cents=int(item["unit_price_cents"]),
                subtotal_cents=int(item.get("subtotal_cents", int(item["quantity"]) * int(item["unit_price_cents"]))),
            )
            for item in document.get("items", [])
        ],
        created_at=_parse_dt(document.get("created_at")),
        updated_at=_parse_dt(document.get("updated_at")),
    )


class OrderReadStoreRepository:
    """Read-side query repository backed by the dedicated read store."""

    def __init__(self, store: OrderReadStore) -> None:
        self._store = store

    @property
    def store(self) -> OrderReadStore:
        return self._store

    async def get_by_id(self, order_id: UUID) -> OrderReadRecord | None:
        document = await self._store.get_order(str(order_id))
        return document_to_record(document) if document else None

    async def list_by_customer(
        self, customer_id: UUID, limit: int = 20, offset: int = 0
    ) -> list[OrderReadRecord]:
        documents = await self._store.list_by_customer(
            str(customer_id), limit=limit, offset=offset
        )
        return [document_to_record(document) for document in documents]