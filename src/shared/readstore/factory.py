"""
Wiring between settings and the read store (CQRS Week 8).

- `build_read_store()` — returns the configured `OrderReadStore`: the
  Elasticsearch store when `READ_STORE_TYPE=elasticsearch` + `READ_STORE_URL`
  is set, otherwise the zero-dependency in-memory store.
- `get_read_store()` — FastAPI dependency used by query handlers. Tests
  override this with a fresh in-memory store per fixture.
- `close_read_store()` — called from app shutdown.
"""
import logging

from src.core.config import get_settings
from src.shared.readstore.base import OrderReadStore
from src.shared.readstore.elasticsearch_store import ElasticsearchOrderReadStore
from src.shared.readstore.inmemory_store import InMemoryOrderReadStore

logger = logging.getLogger("readstore.factory")

_store: OrderReadStore | None = None
_store_key: str | None = None


def build_read_store() -> OrderReadStore:
    """Create (once) the process-wide read store from current settings."""
    global _store, _store_key
    settings = get_settings()
    store_type = (settings.READ_STORE_TYPE or "inmemory").strip().lower()
    key = f"{store_type}:{settings.READ_STORE_URL or ''}:{settings.READ_STORE_INDEX}"
    if _store is None or _store_key != key:
        if store_type == "elasticsearch":
            if not (settings.READ_STORE_URL or "").strip():
                raise ValueError(
                    "READ_STORE_TYPE=elasticsearch requires READ_STORE_URL "
                    "(e.g. http://elasticsearch:9200)"
                )
            _store = ElasticsearchOrderReadStore(
                settings.READ_STORE_URL.strip(),
                settings.READ_STORE_INDEX,
            )
            logger.info("read_store_initialized", extra={"type": "elasticsearch", "url": settings.READ_STORE_URL})
        else:
            _store = InMemoryOrderReadStore()
            logger.info("read_store_initialized", extra={"type": "inmemory"})
        _store_key = key
    return _store


async def get_read_store() -> OrderReadStore:
    """FastAPI dependency: the read store the query handlers read from."""
    return build_read_store()


async def close_read_store() -> None:
    global _store, _store_key
    if _store is not None:
        await _store.close()
        logger.info("read_store_closed")
    _store = None
    _store_key = None