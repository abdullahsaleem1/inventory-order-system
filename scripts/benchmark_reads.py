"""
Read-latency benchmark — CQRS read circumference (Week 8).

Compares order-read latency across the three read strategies this project has
evolved through, so the CQRS read phase can be measured, not just asserted:

  1. **Pre-CQRS baseline** — reading the NORMALIZED write tables directly
     (aggregate + line join). Orders at the time had to be read only from the
     normalized model; per-customer listing was a scan across the order table.
  2. **Week-7 read projection** — the `orders_read_orders` denormalized
     Postgres/SQLite row (one table, JSON items inline, customer_id index).
  3. **Week-8 dedicated read store** — a document read from a store separate
     from the database (in-memory store by default; pass `--es-url` to run the
     same workload against the Elasticsearch store compose brings up).

Output is a markdown table with p50/p95/p99/mean latencies in milliseconds,
which is pasted verbatim into README.md.

One caveat that is documented in the README analysis: the dedicated store
numbers are the query-handler fetch cost only (the sync worker has already
projected the data in the background); the latency of keeping it projected is
measured separately by the vector of transition-to-visible events.

Usage:
    python -m scripts.benchmark_reads --orders 5000 --iterations 5000
    python -m scripts.benchmark_reads --es-url http://localhost:9200
"""
import argparse
import asyncio
import statistics
import time
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from src.contexts.orders.domain.order import Order, OrderLine
from src.contexts.orders.events import build_order_created_event
from src.contexts.orders.repositories.order_read_repository import OrderReadRepository
from src.contexts.orders.repositories.order_read_store_repository import (
    OrderReadStoreRepository,
    record_to_document,
)
from src.contexts.orders.repositories.order_write_repository import OrderWriteRepository
from src.contexts.orders.services.order_event_handler import PersistOrderCreatedHandler
from src.shared.infrastructure.database import Base
from src.shared.readstore import InMemoryOrderReadStore

CUSTOMER_IDS = [uuid4() for _ in range(20)]


def _percentile(sorted_latencies: list[float], pct: float) -> float:
    if not sorted_latencies:
        return 0.0
    idx = min(len(sorted_latencies) - 1, int(len(sorted_latencies) * pct / 100.0))
    return sorted_latencies[idx]


def _summarize(latencies: list[float]) -> dict[str, float]:
    latencies = sorted(latencies)
    return {
        "mean_ms": statistics.mean(latencies) * 1000,
        "p50_ms": _percentile(latencies, 50) * 1000,
        "p95_ms": _percentile(latencies, 95) * 1000,
        "p99_ms": _percentile(latencies, 99) * 1000,
    }


def _fmt(row: dict[str, float]) -> str:
    return (
        f"{row['mean_ms']:.4f} | {row['p50_ms']:.4f} | "
        f"{row['p95_ms']:.4f} | {row['p99_ms']:.4f}"
    )


async def _run_benchmark(orders_count: int, iterations: int, es_url: str | None) -> None:
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # --- seed the write model (+ Week-7 projection) and the read store ------
    persist = PersistOrderCreatedHandler(session_factory)
    read_store = InMemoryOrderReadStore()
    pick = [uuid4() for _ in range(orders_count)]
    seeded_at = time.perf_counter()
    for order_id in pick:
        order = Order(
            id=order_id,
            customer_id=CUSTOMER_IDS[order_id.int % len(CUSTOMER_IDS)],
            lines=[OrderLine(product_id=uuid4(), quantity=2, unit_price_cents=1500)],
        )
        await persist.handle(build_order_created_event(order))
        # Week-8 read store is populated by the async read projector in
        # production; here we project directly to isolate read latency.
        from src.contexts.orders.repositories.order_read_repository import record_from_order

        await read_store.upsert_order(
            record_to_document(record_from_order(order), occurred_at=order.created_at)
        )
    seeded = time.perf_counter() - seeded_at
    print(f"seeded {orders_count} orders in {seeded:.1f}s", flush=True)

    if es_url:
        from src.shared.readstore import ElasticsearchOrderReadStore

        es_store = ElasticsearchOrderReadStore(es_url)
        await es_store.refresh()
        for doc in read_store.all_documents().values():
            await es_store.upsert_order(doc)
        await es_store.refresh()
    else:
        es_store = None

    async with session_factory() as session:
        write_repo = OrderWriteRepository(session)
        # baseline per-customer listing: the write table has no customer_id
        # index (moved to the read side in Week 7) — this is the pre-CQRS scan.
        from sqlalchemy import select
        from src.contexts.orders.infrastructure.models import OrderModel as WriteOrderModel

        async def _baseline_list(customer_id) -> list:
            rows = (await session.execute(select(WriteOrderModel))).scalars().all()
            return [r for r in rows if str(r.customer_id) == str(customer_id)]

        projection_repo = OrderReadRepository(session)
        read_store_repo = OrderReadStoreRepository(read_store)

        target = pick

        async def probe_write_get(order_id) -> None:
            await write_repo.get_by_id(order_id)

        async def probe_projection_get(order_id) -> None:
            await projection_repo.get_by_id(order_id)

        async def probe_read_store_get(order_id) -> None:
            await read_store_repo.get_by_id(order_id)

        async def probe_write_list() -> None:
            await _baseline_list(CUSTOMER_IDS[0])

        async def probe_projection_list() -> None:
            await projection_repo.list_by_customer(CUSTOMER_IDS[0])

        async def probe_read_store_list() -> None:
            await read_store_repo.list_by_customer(CUSTOMER_IDS[0])

        async def probe_es_get(order_id) -> None:
            await OrderReadStoreRepository(es_store).get_by_id(order_id)

        async def probe_es_list() -> None:
            await OrderReadStoreRepository(es_store).list_by_customer(CUSTOMER_IDS[0])

        async def timeit(probe, arg=None) -> list[float]:
            latencies: list[float] = []
            for _ in range(iterations):
                started = time.perf_counter()
                if arg is None:
                    await probe()
                else:
                    await probe(arg)
                latencies.append(time.perf_counter() - started)
            return latencies

        rows = []
        phase = time.perf_counter()
        rows.append(("GET /orders/{id} (write-engine join - pre-CQRS)", _summarize(await timeit(probe_write_get, target[0]))))
        print(f"  write get done in {time.perf_counter()-phase:.1f}s", flush=True)
        phase = time.perf_counter()
        rows.append(("GET /orders/{id} (Week-7 projection, one table)", _summarize(await timeit(probe_projection_get, target[0]))))
        print(f"  projection get done in {time.perf_counter()-phase:.1f}s", flush=True)
        rows.append(("GET /orders/{id} (Week-8 in-memory read store)", _summarize(await timeit(probe_read_store_get, target[0]))))
        if es_url:
            rows.append(("GET /orders/{id} (Week-8 Elasticsearch read store)", _summarize(await timeit(probe_es_get, target[0]))))

        phase = time.perf_counter()
        rows.append(("List by customer (write-engine full scan - pre-CQRS)", _summarize(await timeit(probe_write_list))))
        print(f"  write list done in {time.perf_counter()-phase:.1f}s", flush=True)
        phase = time.perf_counter()
        rows.append(("List by customer (Week-7 projection index)", _summarize(await timeit(probe_projection_list))))
        print(f"  projection list done in {time.perf_counter()-phase:.1f}s", flush=True)
        rows.append(("List by customer (Week-8 in-memory read store)", _summarize(await timeit(probe_read_store_list))))
        if es_url:
            rows.append(("List by customer (Week-8 Elasticsearch read store)", _summarize(await timeit(probe_es_list))))

    print(f"\nBenchmark: {orders_count} orders seeded, {iterations} iterations per probe\n")
    print("| Read strategy | mean (ms) | p50 (ms) | p95 (ms) | p99 (ms) |")
    print("| ------------- | --------- | -------- | -------- | -------- |")
    for label, stats in rows:
        print(f"| {label} | {_fmt(stats)} |")

    await engine.dispose()
    if es_store is not None:
        await es_store.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description="CQRS read-latency benchmark")
    parser.add_argument("--orders", type=int, default=5000, help="number of seeded orders")
    parser.add_argument("--iterations", type=int, default=5000, help="probe iterations per strategy")
    parser.add_argument("--es-url", default=None, help="run ES probes against this URL (default: skip)")
    args = parser.parse_args()
    await _run_benchmark(args.orders, args.iterations, args.es_url)


if __name__ == "__main__":
    asyncio.run(main())