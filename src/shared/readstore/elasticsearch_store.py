"""
Elasticsearch-backed order read store (CQRS Week 8).

Each order is a single indexed document in `READ_STORE_INDEX` (`orders`),
identified by `order_id`. The mapping stores the materialized fields needed by
the API responses (totals, line_count, items) plus an indexed `customer_id`
keyword so "all orders for customer X" is a cheap filtered search — the
equivalent, in the read store, of the customer_id index the write table
deliberately does not carry.

Client wiring notes:
  * the Elasticsearch Py client is the only implementation-specific dependency
    and is imported lazily so tests and non-ES deployments don't need it
    installed;
  * writes use `refresh="wait_for"` so a read immediately after the sync worker
    upserts is already observable (tight eventual consistency on a single
    node);
  * the client is created lazily on first use and reused for the process
    lifetime (`close()` on shutdown).
"""
from elasticsearch import AsyncElasticsearch

from src.shared.readstore.base import OrderReadStore

INDEX_SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0}

# Minimal mapping — everything the API reads is stored as-is; customer_id is
# a keyword so listing per customer is an indexed term query, not a scan.
INDEX_MAPPINGS = {
    "properties": {
        "order_id": {"type": "keyword"},
        "customer_id": {"type": "keyword"},
        "status": {"type": "keyword"},
        "total_cents": {"type": "long"},
        "line_count": {"type": "long"},
        "items": {"type": "object", "enabled": True},
        "created_at": {"type": "date"},
        "updated_at": {"type": "date"},
    }
}


class ElasticsearchOrderReadStore(OrderReadStore):
    def __init__(self, url: str, index: str = "orders") -> None:
        self._url = url
        self._index = index
        self._client: AsyncElasticsearch | None = None

    @property
    def client(self) -> AsyncElasticsearch:
        if self._client is None:
            self._client = AsyncElasticsearch(self._url)
        return self._client

    async def _ensure_index(self) -> None:
        client = self.client
        if await client.indices.exists(index=self._index):
            return
        await client.indices.create(
            index=self._index,
            settings=INDEX_SETTINGS,
            mappings=INDEX_MAPPINGS,
        )

    async def upsert_order(self, document: dict) -> None:
        await self._ensure_index()
        order_id = str(document["order_id"])
        await self.client.index(
            index=self._index,
            id=order_id,
            document=document,
            refresh="wait_for",
        )

    async def update_status(self, order_id: str, status: str) -> None:
        await self._ensure_index()
        # upsert: even if the status event beats the create event through the
        # pipeline, the eventual full order.created projection will overwrite
        # the whole document (upsert_order replaces by id).
        await self.client.update(
            index=self._index,
            id=str(order_id),
            doc={"status": status},
            upsert={"order_id": str(order_id), "status": status, "items": []},
            refresh="wait_for",
        )

    async def get_order(self, order_id: str) -> dict | None:
        await self._ensure_index()
        try:
            resp = await self.client.get(index=self._index, id=str(order_id))
        except Exception:
            return None
        source = resp.get("_source")
        return dict(source) if source else None

    async def list_by_customer(
        self, customer_id: str, limit: int = 20, offset: int = 0
    ) -> list[dict]:
        await self._ensure_index()
        resp = await self.client.search(
            index=self._index,
            query={"term": {"customer_id": str(customer_id)}},
            sort=[{"created_at": {"order": "desc"}}],
            from_=offset,
            size=limit,
        )
        return [dict(hit["_source"]) for hit in resp["hits"]["hits"]]

    async def count(self) -> int:
        await self._ensure_index()
        return int((await self.client.count(index=self._index))["count"])

    async def ping(self) -> bool:
        try:
            return bool(await self.client.ping())
        except Exception:
            return False

    async def refresh(self) -> None:
        await self._ensure_index()
        await self.client.indices.refresh(index=self._index)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None