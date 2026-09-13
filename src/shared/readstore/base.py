"""
Order read store contract (CQRS Week 8).

`OrderReadStore` is the read-side storage abstraction. Implementations store
**denormalized order documents** (plain JSON-serializable dicts):

    {
      "order_id":    "uuid str",
      "customer_id": "uuid str",
      "status":      "PENDING",
      "total_cents": 3500,
      "line_count":  2,
      "items":       [{"product_id", "quantity", "unit_price_cents", "subtotal_cents"}],
      "created_at":  "2026-09-05T...Z",
      "updated_at":  "2026-09-05T...Z"
    }

Query handlers never touch it directly — they go through the shared command/query
split, but the store itself is storage-agnostic (Elasticsearch vs. in-memory)
so the read path can be swapped without touching handlers or the API.
"""
from abc import ABC, abstractmethod


class OrderReadStore(ABC):
    """Storage for denormalized order documents (read side only)."""

    @abstractmethod
    async def upsert_order(self, document: dict) -> None:
        """Insert or replace the document for `document["order_id"]` (idempotent)."""

    @abstractmethod
    async def update_status(self, order_id: str, status: str) -> None:
        """Update only the `status` (and `updated_at`) of an existing document.

        Must be safe to call even if the document has not been projected yet —
        the projection is eventually consistent, so a `status.changed` event may
        beat the `order.created` event through the pipeline.
        """

    @abstractmethod
    async def get_order(self, order_id: str) -> dict | None:
        """Return the document for an order id, or None if not yet projected."""

    @abstractmethod
    async def list_by_customer(
        self, customer_id: str, limit: int = 20, offset: int = 0
    ) -> list[dict]:
        """Return documents for one customer, newest `created_at` first."""

    @abstractmethod
    async def count(self) -> int:
        """Total number of documents (used for tests / benchmarking)."""

    @abstractmethod
    async def ping(self) -> bool:
        """Cheap connectivity check (used by GET /ready and probes)."""

    @abstractmethod
    async def refresh(self) -> None:
        """Make previously written documents immediately searchable.

        No-op for synchronous backends; Elasticsearch needs an explicit
        refresh for reads that follow writes in the same instant.
        """

    @abstractmethod
    async def close(self) -> None:
        """Release any connections held by the store."""