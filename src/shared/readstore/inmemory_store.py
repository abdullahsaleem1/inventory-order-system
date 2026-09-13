"""
In-memory order read store — deterministic test double + local fallback.

Stores documents in a plain dict keyed by `order_id`. Exposes the exact same
interface as the Elasticsearch store, so the fixture suite, local dev (without
a container), and the benchmark script all exercise the query handlers against
a real read side that is separate from the write database.
"""
import asyncio
from typing import Any

from src.shared.readstore.base import OrderReadStore


class InMemoryOrderReadStore(OrderReadStore):
    def __init__(self) -> None:
        self._documents: dict[str, dict] = {}
        self._lock = asyncio.Lock()

    async def upsert_order(self, document: dict) -> None:
        async with self._lock:
            doc = dict(document)
            doc["_seq"] = len(self._documents)  # preserves insertion order for sort
            self._documents[str(doc["order_id"])] = doc

    async def update_status(self, order_id: str, status: str) -> None:
        async with self._lock:
            key = str(order_id)
            if key not in self._documents:
                # Status event beat the create event through the pipeline — the
                # document does not exist yet, so there is nothing to update.
                return
            self._documents[key]["status"] = status

    async def get_order(self, order_id: str) -> dict | None:
        async with self._lock:
            doc = self._documents.get(str(order_id))
            return dict(doc) if doc else None

    async def list_by_customer(
        self, customer_id: str, limit: int = 20, offset: int = 0
    ) -> list[dict]:
        async with self._lock:
            matches = [
                doc
                for doc in self._documents.values()
                if str(doc["customer_id"]) == str(customer_id)
            ]
            matches.sort(key=lambda d: d.get("created_at", ""), reverse=True)
            return [dict(d) for d in matches[offset : offset + limit]]

    async def count(self) -> int:
        async with self._lock:
            return len(self._documents)

    async def ping(self) -> bool:
        return True

    async def refresh(self) -> None:
        return None

    async def close(self) -> None:
        self._documents.clear()

    # --- introspection helper used by tests ---------------------------------

    def all_documents(self) -> dict[str, dict[Any, Any]]:
        return {k: dict(v) for k, v in self._documents.items()}