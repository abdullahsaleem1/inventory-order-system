"""
Orders bounded context — write-side repository (CQRS Week 7).

The write repository is the ONLY path to the normalized write tables
(`orders_orders` / `orders_order_lines`). It is deliberately the only
repository allowed to add, update, or delete write-model rows. Reads here exist
only so command handlers can load an aggregate to apply domain rules on — they
are write-side reads of the *aggregate*, not API query reads.
"""
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.contexts.orders.domain.order import Order, OrderLine, OrderStatus
from src.contexts.orders.infrastructure.models import OrderLineModel, OrderModel
from src.contexts.orders.errors import OrderNotFoundError


class OrderWriteRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def _to_domain(row: OrderModel) -> Order:
        return Order(
            id=row.id,
            customer_id=row.customer_id,
            status=OrderStatus(row.status),
            lines=[
                OrderLine(product_id=ln.product_id, quantity=ln.quantity, unit_price_cents=ln.unit_price_cents)
                for ln in row.lines
            ],
        )

    async def get_by_id(self, order_id: UUID) -> Order | None:
        """Load the aggregate (with its lines) for a command handler."""
        result = await self._session.execute(
            select(OrderModel).where(OrderModel.id == order_id).options(selectinload(OrderModel.lines))
        )
        row = result.scalar_one_or_none()
        return self._to_domain(row) if row else None

    async def add(self, entity: Order) -> Order:
        row = OrderModel(id=entity.id, customer_id=entity.customer_id, status=entity.status.value)
        row.lines = [
            OrderLineModel(product_id=ln.product_id, quantity=ln.quantity, unit_price_cents=ln.unit_price_cents)
            for ln in entity.lines
        ]
        self._session.add(row)
        await self._session.flush()
        return self._to_domain(row)

    async def update_status(self, entity: Order) -> Order:
        row = await self._session.get(OrderModel, entity.id)
        if row is None:
            raise OrderNotFoundError(f"Order {entity.id} not found")
        row.status = entity.status.value
        await self._session.flush()
        return entity