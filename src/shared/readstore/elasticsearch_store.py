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

Week 10 — tracing. The official `opentelemetry-instrumentation-elasticsearch`
package only patches the *synchronous* client, so `CLIENT` spans are written by
hand here, following the OpenTelemetry conventions for database clients
(`db.system=elasticsearch`, `db.operation.name`, `db.namespace`). The result is
that the read-projector trace shows which index operation dominated, the same
way the write side shows `db.statement` spans from SQLAlchemy.
"""
from typing import Any, Awaitable, Callable, TypeVar

from elasticsearch import AsyncElasticsearch, NotFoundError

from src.core.telemetry import CLIENT, StatusCode, get_tracer, set_span_attributes
from src.shared.readstore.base import OrderReadStore
from src.shared.readstore.errors import ReadStoreUnavailableError

T = TypeVar("T")

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
        self._tracer = get_tracer()

    @property
    def client(self) -> AsyncElasticsearch:
        if self._client is None:
            self._client = AsyncElasticsearch(self._url)
        return self._client

    def _span(self, operation: str, **attributes: Any):
        """CLIENT span around one Elasticsearch call.

        Synchronous on purpose: it returns the context manager for a `with`
        block, and the awaitable work happens inside. `db.system` is
        `elasticsearch` per the semantic conventions, and `db.namespace` carries
        the index so a trace can be filtered to just the `orders` index in
        Jaeger.
        """
        return self._tracer.start_as_current_span(
            f"elasticsearch {operation}",
            kind=CLIENT,
            attributes={
                "db.system": "elasticsearch",
                "db.operation.name": operation,
                "db.namespace": self._index,
                "server.address": self._url,
                **attributes,
            },
        )

    async def _call(
        self, operation: str, func: Callable[[], Awaitable[T]], **attributes: Any
    ) -> T:
        """Await `func()` inside a CLIENT span, recording success or failure."""
        with self._span(operation, **attributes) as span:
            try:
                result = await func()
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                span.record_exception(exc)
                set_span_attributes(span, error_type=type(exc).__name__)
                raise
            span.set_status(StatusCode.OK)
            return result

    async def _ensure_index(self) -> None:
        client = self.client
        if await self._call("indices.exists", lambda: client.indices.exists(index=self._index)):
            return
        await self._call(
            "indices.create",
            lambda: client.indices.create(
                index=self._index,
                settings=INDEX_SETTINGS,
                mappings=INDEX_MAPPINGS,
            ),
        )

    def _unavailable(self, operation: str, exc: BaseException) -> ReadStoreUnavailableError:
        """Build the outage error for a failed read-store operation (Week 11).

        The read store cannot distinguish its own bugs from its driver's
        transport failures, and the distinction the API needs is "did not
        answer" (503) vs "answered: absent" (404). The original exception type
        is preserved in the message so on-call diagnosis is unaffected.
        """
        return ReadStoreUnavailableError(
            f"Elasticsearch read store unavailable during {operation}: "
            f"{type(exc).__name__}: {exc}",
            operation=operation,
        )

    async def upsert_order(self, document: dict) -> None:
        try:
            await self._ensure_index()
            order_id = str(document["order_id"])
            await self._call(
                "index",
                lambda: self.client.index(
                    index=self._index,
                    id=order_id,
                    document=document,
                    refresh="wait_for",
                ),
                **{"db.document.id": order_id},
            )
        except Exception as exc:
            raise self._unavailable("upsert_order", exc) from exc

    async def update_status(self, order_id: str, status: str) -> None:
        try:
            await self._ensure_index()
            # upsert: even if the status event beats the create event through the
            # pipeline, the eventual full order.created projection will overwrite
            # the whole document (upsert_order replaces by id).
            await self._call(
                "update",
                lambda: self.client.update(
                    index=self._index,
                    id=str(order_id),
                    doc={"status": status},
                    upsert={"order_id": str(order_id), "status": status, "items": []},
                    refresh="wait_for",
                ),
                **{"db.document.id": str(order_id), "order.status": status},
            )
        except Exception as exc:
            raise self._unavailable("update_status", exc) from exc

    async def get_order(self, order_id: str) -> dict | None:
        """Return the projected document, or None if it is genuinely absent.

        Week 11: a missing document (`NotFoundError`) and an unreachable cluster
        are now distinct outcomes. Previously a single `except Exception: return
        None` covered both, so a full Elasticsearch outage surfaced to clients
        as a plain 404 "order not found" — and `_ensure_index()` sat outside the
        try entirely, so the same outage on the pre-flight produced a raw 500.
        """
        try:
            await self._ensure_index()
            resp = await self._call(
                "get",
                lambda: self.client.get(index=self._index, id=str(order_id)),
                **{"db.document.id": str(order_id)},
            )
        except NotFoundError:
            return None  # document really is not projected yet
        except Exception as exc:
            raise self._unavailable("get_order", exc) from exc
        source = resp.get("_source")
        return dict(source) if source else None

    async def list_by_customer(
        self, customer_id: str, limit: int = 20, offset: int = 0
    ) -> list[dict]:
        try:
            await self._ensure_index()
            resp = await self._call(
                "search",
                lambda: self.client.search(
                    index=self._index,
                    query={"term": {"customer_id": str(customer_id)}},
                    sort=[{"created_at": {"order": "desc"}}],
                    from_=offset,
                    size=limit,
                ),
                **{"customer.id": str(customer_id), "db.query.limit": limit, "db.query.offset": offset},
            )
            return [dict(hit["_source"]) for hit in resp["hits"]["hits"]]
        except Exception as exc:
            raise self._unavailable("list_by_customer", exc) from exc

    async def count(self) -> int:
        try:
            await self._ensure_index()
            resp = await self._call("count", lambda: self.client.count(index=self._index))
            return int(resp["count"])
        except Exception as exc:
            raise self._unavailable("count", exc) from exc

    async def ping(self) -> bool:
        try:
            return bool(await self.client.ping())
        except Exception:
            return False

    async def refresh(self) -> None:
        try:
            await self._ensure_index()
            await self._call("indices.refresh", lambda: self.client.indices.refresh(index=self._index))
        except Exception as exc:
            raise self._unavailable("refresh", exc) from exc

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None