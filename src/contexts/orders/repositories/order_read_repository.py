"""
Orders bounded context — read-side repository (CQRS Week 7).

Queries the denormalized read model (`orders_read_orders`) exclusively. The
record dataclasses below are the read-side DTOs — never the `Order` aggregate.

`upsert_from` / `update_status` are write-ish helpers, but they are reserved
for *projection maintenance* (persistence consumer + confirm/cancel command
handlers keep the read model in sync); API queries never write through this
repository.
"""
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.orders.domain.order import Order
from src.contexts.orders.infrastructure.read_models import OrderReadModel


@dataclass(frozen=True)
class OrderLineRecord:
    product_id: UUID
    quantity: int
    unit_price_cents: int
    subtotal_cents: int


@dataclass(frozen=True)
class OrderReadRecord:
    id: UUID
    customer_id: UUID
    status: str
    total_cents: int
    lines: list[OrderLineRecord]
    created_at: datetime
    updated_at: datetime

    @property
    def line_count(self) -> int:
        return len(self.lines)


def _items_from_order(order: Order) -> list[dict]:
    return [
        {
            "product_id": str(line.product_id),
            "quantity": line.quantity,
            "unit_price_cents": line.unit_price_cents,
            "subtotal_cents": line.subtotal_cents,
        }
        for line in order.lines
    ]


def record_from_order(order: Order) -> OrderReadRecord:
    """Map an aggregate straight to a read record (used for fallback reads and
    command-side projection synchronization)."""
    return OrderReadRecord(
        id=order.id,
        customer_id=order.customer_id,
        status=order.status.value,
        total_cents=order.total_cents,
        lines=[
            OrderLineRecord(
                product_id=line.product_id,
                quantity=line.quantity,
                unit_price_cents=line.unit_price_cents,
                subtotal_cents=line.subtotal_cents,
            )
            for line in order.lines
        ],
        created_at=order.created_at,
        updated_at=order.updated_at,
    )


def _record_from_row(row: OrderReadModel) -> OrderReadRecord:
    return OrderReadRecord(
        id=row.id,
        customer_id=row.customer_id,
        status=row.status,
        total_cents=row.total_cents,
        lines=[
            OrderLineRecord(
                product_id=UUID(item["product_id"]),
                quantity=int(item["quantity"]),
                unit_price_cents=int(item["unit_price_cents"]),
                subtotal_cents=int(item["subtotal_cents"]),
            )
            for item in (row.items or [])
        ],
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


class OrderReadRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, order_id: UUID) -> OrderReadRecord | None:
        row = await self._session.get(OrderReadModel, order_id)
        return _record_from_row(row) if row else None

    async def list_by_customer(
        self, customer_id: UUID, limit: int = 20, offset: int = 0
    ) -> list[OrderReadRecord]:
        result = await self._session.execute(
            select(OrderReadModel)
            .where(OrderReadModel.customer_id == customer_id)
            .order_by(OrderReadModel.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        return [_record_from_row(row) for row in result.scalars().all()]

    # --- projection maintenance ---------------------------------------------
    # (only called by the persistence consumer and the confirm/cancel command
    #  handlers — never by API queries)

    async def upsert_from(self, order: Order, *, created_at: datetime | None = None) -> None:
        """Insert or update the read-model row from an `Order` aggregate."""
        existing = await self._session.get(OrderReadModel, order.id)
        if existing is None:
            projected = record_from_order(order)
            self._session.add(
                OrderReadModel(
                    id=projected.id,
                    customer_id=projected.customer_id,
                    status=projected.status,
                    total_cents=projected.total_cents,
                    line_count=projected.line_count,
                    items=_items_from_order(order),
                    created_at=created_at or order.created_at,
                    updated_at=datetime.now(timezone.utc),
                )
            )
        else:
            existing.status = order.status.value
            existing.total_cents = order.total_cents
            existing.line_count = len(order.lines)
            existing.items = _items_from_order(order)
            existing.updated_at = datetime.now(timezone.utc)
        await self._session.flush()

    async def update_status(self, order: Order) -> None:
        """Status-only projection update (confirm/cancel — lines don't change)."""
        updated = await self._session.execute(
            update(OrderReadModel)
            .where(OrderReadModel.id == order.id)
            .values(status=order.status.value, updated_at=datetime.now(timezone.utc))
        )
        if updated.rowcount == 0:
            raise OrderNotFoundError(f"Read model row for order {order.id} not found")
        await self._session.flush()