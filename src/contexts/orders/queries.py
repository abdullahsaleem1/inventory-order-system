"""
Orders bounded context — CQRS read side (queries, Week 7).

Each query is a frozen dataclass describing a question about data. Query
handlers (in `query_handlers.py`) answer them from the read-optimized
projection (`orders_read_orders`), never from the normalized write tables.
"""
from dataclasses import dataclass, field
from uuid import UUID

from src.shared.cqrs import Query


@dataclass(frozen=True)
class GetOrderQuery(Query):
    """Fetch a single order by id from the read model."""

    order_id: UUID


@dataclass(frozen=True)
class ListOrdersByCustomerQuery(Query):
    """List an order summary per customer — the read model's bread and butter.

    `orders_read_orders` is indexed on customer_id precisely because this
    query is hot on the read side; the write table deliberately dropped that
    index (see migration 0004).
    """

    customer_id: UUID
    limit: int = 20
    offset: int = 0