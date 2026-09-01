"""
Orders bounded context — CQRS query handlers (Week 7).

The read side. Every data-retrieval use case is a `Query` (queries.py) answered
by exactly one of these. Handlers read from the read-optimized projection via
`OrderReadRepository` — never from the normalized write tables.

`GetOrderQueryHandler` performs a bounded fallback to the write store when the
projection has not caught up yet (eventual consistency): the projection is
populated by the async persistence consumer, so a GET racing the consumer
returns the aggregate and still answers correctly.
"""
from uuid import UUID

from src.contexts.orders.errors import OrderNotFoundError
from src.contexts.orders.queries import GetOrderQuery, ListOrdersByCustomerQuery
from src.contexts.orders.repositories.order_read_repository import (
    OrderReadRecord,
    OrderReadRepository,
    record_from_order,
)
from src.contexts.orders.repositories.order_write_repository import OrderWriteRepository


class GetOrderQueryHandler:
    def __init__(self, read_repo: OrderReadRepository, write_repo: OrderWriteRepository) -> None:
        self._read_repo = read_repo
        self._write_repo = write_repo

    async def handle(self, query: GetOrderQuery) -> OrderReadRecord:
        record = await self._read_repo.get_by_id(query.order_id)
        if record is not None:
            return record
        # Projection not yet written (async consumer is still catching up) —
        # fall back to the write aggregate so the read still succeeds.
        order = await self._write_repo.get_by_id(query.order_id)
        if order is None:
            raise OrderNotFoundError(f"Order {query.order_id} not found")
        return record_from_order(order)


class ListOrdersByCustomerQueryHandler:
    """Answers off the customer_id index on the read model only."""

    def __init__(self, read_repo: OrderReadRepository) -> None:
        self._read_repo = read_repo

    async def handle(self, query: ListOrdersByCustomerQuery) -> list[OrderReadRecord]:
        return await self._read_repo.list_by_customer(
            query.customer_id,
            limit=query.limit,
            offset=query.offset,
        )