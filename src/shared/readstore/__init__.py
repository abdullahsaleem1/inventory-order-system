"""CQRS dedicated read store (Week 8).

The read store holds **denormalized order documents** — one per order, line
items embedded, materialized totals — and is the ONLY store query handlers
read from (write endpoints and the write database are never consulted for
order reads). It is populated solely by the sync worker
(`scripts/read_projector.py`), which consumes events from the broker and
upserts documents, giving **eventual consistency** between the write model and
the query-visible read model.

Backends:
  * `ElasticsearchOrderReadStore` — production store (docker-compose `es`).
    Each order maps to a single indexed document.
  * `InMemoryOrderReadStore` — zero-dependency fallback used for local
    runs and the deterministic integration-test suite.
"""
from src.shared.readstore.base import OrderReadStore
from src.shared.readstore.elasticsearch_store import ElasticsearchOrderReadStore
from src.shared.readstore.factory import build_read_store, close_read_store, get_read_store
from src.shared.readstore.inmemory_store import InMemoryOrderReadStore

__all__ = [
    "ElasticsearchOrderReadStore",
    "InMemoryOrderReadStore",
    "OrderReadStore",
    "build_read_store",
    "close_read_store",
    "get_read_store",
]