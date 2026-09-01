"""
Inventory bounded context — CQRS read side (queries, Week 7).

Each query is a frozen dataclass describing a question about inventory. Query
handlers (query_handlers.py) answer them through the read repository. The
product table is currently shared by both sides (single-table domain), but the
handler/repository split keeps the read path independent of write intent —
ready to point at a read-optimized copy when the read store is configured.
"""
from dataclasses import dataclass
from uuid import UUID

from src.shared.cqrs import Query


@dataclass(frozen=True)
class GetProductQuery(Query):
    product_id: UUID


@dataclass(frozen=True)
class ListProductsQuery(Query):
    limit: int = 100
    offset: int = 0