"""
Orders bounded context — CQRS query handlers (Week 7, read phase Week 8).

The read side. Every data-retrieval use case is a `Query` (queries.py) answered
by exactly one of these.

Since Week 8 the query handlers read **exclusively from the dedicated read
store** (`OrderReadStoreRepository` over the Elasticsearch/in-memory read
store). There is deliberately no fall-back to the write database: a GET that
races the async read projector returns 404 until the read store catches up —
that is the expected eventual-consistency behaviour of a CQRS read phase, and
it keeps the write path free of read traffic.
"""
from src.contexts.orders.errors import OrderNotFoundError
from src.contexts.orders.queries import GetOrderQuery, ListOrdersByCustomerQuery
from src.contexts.orders.repositories.order_read_store_repository import (
    OrderReadStoreRepository,
)
from src.shared.exceptions import ServiceUnavailableError
from src.shared.readstore.errors import ReadStoreUnavailableError


class GetOrderQueryHandler:
    def __init__(self, read_repo: OrderReadStoreRepository) -> None:
        self._read_repo = read_repo

    async def handle(self, query: GetOrderQuery) -> "object":
        try:
            record = await self._read_repo.get_by_id(query.order_id)
        except ReadStoreUnavailableError as exc:
            # The read store could not answer. This must NOT be reported as a
            # 404: during an Elasticsearch outage "not found" is a lie that
            # sends an on-call engineer hunting for a missing order.
            raise ServiceUnavailableError(
                "Order lookup is temporarily unavailable: the read store is down",
                code="READ_STORE_UNAVAILABLE",
            ) from exc
        if record is None:
            raise OrderNotFoundError(f"Order {query.order_id} not found")
        return record


class ListOrdersByCustomerQueryHandler:
    """Answers off the dedicated read store's customer_id index only."""

    def __init__(self, read_repo: OrderReadStoreRepository) -> None:
        self._read_repo = read_repo

    async def handle(self, query: ListOrdersByCustomerQuery) -> list["object"]:
        try:
            return await self._read_repo.list_by_customer(
                query.customer_id,
                limit=query.limit,
                offset=query.offset,
            )
        except ReadStoreUnavailableError as exc:
            raise ServiceUnavailableError(
                "Order listing is temporarily unavailable: the read store is down",
                code="READ_STORE_UNAVAILABLE",
            ) from exc
