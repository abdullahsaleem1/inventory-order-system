"""
Week 11 chaos scenarios — runnable anywhere, no Docker required.

    python -m scripts.chaos_scenarios

Boots the real FastAPI application (real middleware, real exception handlers,
real SQLite write DB, real in-memory read store, real consumer topology) and
then breaks one dependency at a time while concurrent traffic keeps flowing.

Each scenario prints a hypothesis, drives a steady -> faulted -> recovered load
profile, asserts the expected behaviour, and records the numbers. The JSON
summary is written to `artifacts/chaos-results.json` and is the source for the
tables in `RESILIENCE_REPORT.md`.

Why in-process injection rather than only `docker compose stop`: it exercises
the same application code paths on any machine (including CI), so the findings
are reproducible. `scripts/chaos_drill.py` performs the equivalent drills
against real containers for when Docker is available.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx
from httpx import ASGITransport

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.chaos_harness import (  # noqa: E402
    RESULTS,
    ScenarioResult,
    Workload,
    verdict,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from src.contexts.orders.services.order_event_handler import PersistOrderCreatedHandler  # noqa: E402
from src.contexts.orders.services.read_model_projector import (  # noqa: E402
    ProjectOrderToReadStoreHandler,
)
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService  # noqa: E402
from src.contexts.identity.domain.user import Role, User  # noqa: E402
from src.core.config import get_settings  # noqa: E402
from src.core.ratelimit.backends import InMemoryTokenBucket  # noqa: E402
from src.core.ratelimit.policy import DEFAULT_TIERS, RateLimitPolicy  # noqa: E402
from src.core.ratelimit.provider import (  # noqa: E402
    RateLimiter,
    reset_rate_limiter,
    set_rate_limiter,
)
from src.core.ratelimit.token_bucket import RateLimiterUnavailableError  # noqa: E402
from src.main import app  # noqa: E402
from src.shared.infrastructure.database import Base, get_db_session  # noqa: E402
from src.shared.messaging.events import EventTypes  # noqa: E402
from src.shared.messaging.provider import get_event_publisher  # noqa: E402
from src.shared.messaging.publisher import InMemoryEventPublisher  # noqa: E402
from src.shared.readstore import InMemoryOrderReadStore  # noqa: E402
from src.shared.readstore.errors import ReadStoreUnavailableError  # noqa: E402
from src.shared.readstore.factory import get_read_store  # noqa: E402

ARTIFACTS = REPO_ROOT / "artifacts"


# ---------------------------------------------------------------------------
# App under test
# ---------------------------------------------------------------------------


class ChaosApp:
    """The real app, wired to throwaway in-process dependencies."""

    def __init__(self) -> None:
        self._engine = None
        self.session_factory = None
        self.read_store = InMemoryOrderReadStore()
        self.publisher = InMemoryEventPublisher()
        self.limiter: RateLimiter | None = None
        self._product_id: str | None = None

    async def start(self) -> None:
        self._engine = create_async_engine(
            "sqlite+aiosqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        self.session_factory = async_sessionmaker(
            self._engine, expire_on_commit=False, class_=AsyncSession
        )
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async def override_db():
            async with self.session_factory() as session:
                try:
                    yield session
                finally:
                    if session.is_active:
                        await session.commit()

        async def override_read_store():
            return self.read_store

        app.dependency_overrides[get_db_session] = override_db
        app.dependency_overrides[get_read_store] = override_read_store
        app.dependency_overrides[get_event_publisher] = lambda: self.publisher

        self._subscribe_pipeline()
        self._install_limiter()

    def _subscribe_pipeline(self) -> None:
        """Wire the real consumer handlers behind the in-memory broker.

        Delivery is filtered by event type exactly as the real queue bindings
        are, so the read projector only ever sees the events its queue is
        bound to in production.
        """
        for event_types, subscriber in (
            ({EventTypes.ORDER_CREATED},
             PersistOrderCreatedHandler(self.session_factory).handle),
            ({EventTypes.ORDER_CREATED, EventTypes.ORDER_STATUS_CHANGED},
             ProjectOrderToReadStoreHandler(self.read_store).handle),
        ):
            allowed = set(event_types)

            # The in-memory broker calls subscribers as `subscriber(event, headers)`.
            async def _filtered(event, headers=None, _fn=subscriber, _allowed=allowed):
                if event.event_type in _allowed:
                    await _fn(event)

            self.publisher.subscribers.append(_filtered)

    def _install_limiter(self) -> None:
        """A wide in-memory 'Redis' bucket so the breaker can be exercised."""
        self._healthy_redis = InMemoryTokenBucket()
        self.limiter = _SwitchableRedis(self._healthy_redis)
        set_rate_limiter(
            RateLimiter(
                self.limiter,
                InMemoryTokenBucket(),
                RateLimitPolicy(_NO_REFILL_TIERS),
                enabled=True,
                failure_threshold=2,
                cooldown_base=1.0,
                cooldown_max=4.0,
            )
        )

    async def stop(self) -> None:
        app.dependency_overrides.clear()
        reset_rate_limiter()
        if self._engine is not None:
            await self._engine.dispose()

    # --- fault injection switches -------------------------------------------

    def break_redis(self) -> None:
        self.limiter.healthy = False

    def heal_redis(self) -> None:
        self.limiter.healthy = True

    def break_broker(self) -> None:
        self.publisher.fail_publishes = True

    def heal_broker(self) -> None:
        self.publisher.fail_publishes = False

    def break_read_store(self) -> None:
        self.read_store.available = False

    def heal_read_store(self) -> None:
        self.read_store.available = True

    def break_inventory_worker(self) -> None:
        self.publisher.fail_subscribers = True

    def heal_inventory_worker(self) -> None:
        self.publisher.fail_subscribers = False

    async def new_workload(self, client: httpx.AsyncClient) -> Workload:
        """Fresh workload with its own identity, so rate-limit buckets and
        read-store content don't bleed between windows."""
        email = f"chaos-{random.randint(1_000_000, 9_999_999)}@example.com"
        resp = await client.post(
            "/auth/register",
            json={"email": email, "full_name": "Chaos", "password": "S3curePass!"},
        )
        assert resp.status_code == 201, resp.text
        body = resp.json()
        settings = get_settings()
        svc = JwtTokenService(
            secret_key=settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
            issuer=settings.JWT_ISSUER,
            audience=settings.JWT_AUDIENCE,
            access_token_expire_minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES,
        )
        token = svc.create_access_token(
            User(
                id=uuid.UUID(body["user"]["id"]),
                email=body["user"]["email"],
                full_name="Chaos",
                hashed_password="x",
                role=Role.CUSTOMER,
            )
        )
        return Workload(client, token)


# A limiter policy with generous buckets: these scenarios are about failure
# behaviour, not throttling, and a 429 would otherwise mask the code under test.
from src.core.ratelimit.policy import RateLimitTier  # noqa: E402

_NO_REFILL_TIERS = {
    name: RateLimitTier(capacity=100_000, rate=100_000.0)
    for name in DEFAULT_TIERS
}


class _SwitchableRedis:
    """An in-memory bucket that can be switched to 'Redis is unreachable'."""

    def __init__(self, inner: InMemoryTokenBucket) -> None:
        self._inner = inner
        self.healthy = True
        self.consume_calls = 0
        self.ping_calls = 0

    async def consume(self, key, *, capacity, rate, cost=1.0):
        self.consume_calls += 1
        if not self.healthy:
            raise RateLimiterUnavailableError("connection refused (chaos)")
        return await self._inner.consume(key, capacity=capacity, rate=rate, cost=cost)

    async def ping(self) -> bool:
        self.ping_calls += 1
        return self.healthy


def _install_read_store_switch() -> None:
    """Teach the in-memory read store to simulate a cluster outage."""

    original_get = InMemoryOrderReadStore.get_order
    original_list = InMemoryOrderReadStore.list_by_customer

    async def get_order(self, order_id):
        if not getattr(self, "available", True):
            raise ReadStoreUnavailableError(
                "Elasticsearch read store unavailable during get_order: "
                "ConnectionError: no route to host (chaos)",
                operation="get_order",
            )
        return await original_get(self, order_id)

    async def list_by_customer(self, customer_id, limit=20, offset=0):
        if not getattr(self, "available", True):
            raise ReadStoreUnavailableError(
                "Elasticsearch read store unavailable during list_by_customer: "
                "ConnectionError: no route to host (chaos)",
                operation="list_by_customer",
            )
        return await original_list(self, customer_id, limit=limit, offset=offset)

    InMemoryOrderReadStore.get_order = get_order
    InMemoryOrderReadStore.list_by_customer = list_by_customer


class _FailSwitch(InMemoryEventPublisher):
    """In-memory broker that can be told to fail like a real one."""

    def __init__(self) -> None:
        super().__init__()
        self.fail_publishes = False
        self.fail_subscribers = False
        self.published_count = 0

    async def publish(self, event) -> None:
        if self.fail_publishes:
            # Same exception the real publisher raises when the broker will not
            # confirm, so the API's error mapping is the production path.
            from src.shared.messaging.publisher import EventPublishError

            raise EventPublishError(
                f"Failed to publish event {event.event_id}: "
                "[Errno 111] Connection refused (chaos)"
            )
        self.published_count += 1
        await super().publish(event)

    async def _deliver(self, event) -> None:
        if self.fail_subscribers:
            raise RuntimeError("consumer group unavailable (chaos)")
        await super()._deliver(event)


# ---------------------------------------------------------------------------
# Window helper
# ---------------------------------------------------------------------------


async def _profile(
    workload_factory: Callable[[], Awaitable[Workload]],
    client: httpx.AsyncClient,
    *,
    windows: list[tuple[str, float, Callable[[], None] | None]],
    concurrency: int,
) -> dict[str, dict[str, Any]]:
    """Run sequential labelled windows, applying a fault at each boundary."""
    out: dict[str, dict[str, Any]] = {}
    for label, duration, action in windows:
        if action is not None:
            action()  # fault switches are synchronous
        workload = await workload_factory(client)
        stats = await workload.run(duration=duration, concurrency=concurrency)
        out[label] = stats.summary()
        print(f"   {label:<9} {stats.summary()}")
    return out


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


async def scenario_redis(
    chaos: ChaosApp, client: httpx.AsyncClient, *, t: float, c: int
) -> ScenarioResult:
    name = "Redis outage"
    print(f"\n>> {name}")
    print("   hypothesis: a dead Redis degrades rate limiting to an in-process "
          "bucket; the API keeps serving, /ready still answers, and a circuit "
          "breaker stops the outage costing per-request latency")
    limiter = chaos.limiter
    auth = _any_auth(client)
    metrics = await _profile(
        chaos.new_workload, client, concurrency=c,
        windows=[
            ("steady", t, None),
            ("redis_down", t, chaos.break_redis),
            ("recovered", t + 1.0, chaos.heal_redis),
        ],
    )

    # A clean /ready during the outage is the regression this scenario guards.
    chaos.break_redis()
    ready = await client.get("/ready")
    ready_body = ready.json()
    calls_at_trip = limiter.consume_calls

    probe = await client.get(
        "/orders/00000000-0000-0000-0000-000000000000", headers=auth
    )
    degraded_header = probe.headers.get("x-ratelimit-degraded")

    # Drive traffic with Redis still down: the breaker must now cut Redis I/O.
    before = limiter.consume_calls
    for _ in range(40):
        await client.get("/auth/me", headers=auth)
    after = limiter.consume_calls

    chaos.heal_redis()
    await asyncio.sleep(1.2)  # let the breaker cooldown elapse
    await client.get("/ready")  # probe closes the circuit
    healed_probe = await client.get(
        "/orders/00000000-0000-0000-0000-000000000000", headers=auth
    )

    breaker_saved_calls = after - before <= 1
    ok = (
        ready.status_code == 200
        and ready_body.get("redis") == "degraded"
        and ready_body.get("status") == "ready"
        and degraded_header == "true"
        and breaker_saved_calls
        and healed_probe.headers.get("x-ratelimit-degraded") is None
    )
    metrics["probe"] = {
        "ready_status": ready.status_code,
        "ready_body": ready_body,
        "degraded_header_during_outage": degraded_header,
        "redis_calls_40_requests": after - before,
        "redis_calls_at_trip": calls_at_trip,
        "degraded_header_after_heal": healed_probe.headers.get("x-ratelimit-degraded"),
    }
    return verdict(
        result_for(
            name,
            "Redis outage: graceful degradation + circuit breaker",
            ok,
            metrics,
        ),
        ok=ok,
        detail=(
            f"/ready={ready.status_code} redis={ready_body.get('redis')} "
            f"X-RateLimit-Degraded={degraded_header} "
            f"redis_calls_for_40_reqs={after - before} "
            f"healed_degraded_header={healed_probe.headers.get('x-ratelimit-degraded')}"
        ),
    )


async def scenario_broker(
    chaos: ChaosApp, client: httpx.AsyncClient, *, t: float, c: int
) -> ScenarioResult:
    name = "Broker outage"
    print(f"\n>> {name}")
    print("   hypothesis: order creation fails closed with 503 "
          "EVENT_PUBLISH_FAILED and writes nothing; reads and liveness are "
          "unaffected; service recovers automatically once the broker returns")
    metrics = await _profile(
        chaos.new_workload, client, concurrency=c,
        windows=[
            ("steady", t, None),
            ("broker_down", t, chaos.break_broker),
            ("recovered", t, chaos.heal_broker),
        ],
    )

    # A single explicit call to pin the status code and error code.
    chaos.break_broker()
    workload = await chaos.new_workload(client)
    customer = workload._customer_id  # noqa: SLF001 - harness introspection
    create = await client.post(
        "/orders",
        headers=workload._headers,  # noqa: SLF001
        json={
            "customer_id": customer,
            "lines": [
                {"product_id": workload._product_id, "quantity": 1, "unit_price_cents": 100}
            ],
        },
    )
    body = create.json()
    health = await client.get("/health")
    chaos.heal_broker()

    metrics["probe"] = {
        "create_status": create.status_code,
        "error_code": body.get("error", {}).get("code"),
        "health_status": health.status_code,
    }
    ok = (
        create.status_code == 503
        and body.get("error", {}).get("code") == "EVENT_PUBLISH_FAILED"
        and health.status_code == 200
    )
    return verdict(
        result_for(name, "Broker outage: fail closed with 503, no partial writes", ok, metrics),
        ok=ok,
        detail=(
            f"create={create.status_code} "
            f"code={body.get('error', {}).get('code')} health={health.status_code}"
        ),
    )


async def scenario_worker(
    chaos: ChaosApp, client: httpx.AsyncClient, *, t: float, c: int
) -> ScenarioResult:
    name = "Worker outage"
    print(f"\n>> {name}")
    print("   hypothesis: the API keeps accepting orders (202) while consumers "
          "are down, because acceptance depends only on broker confirms; once "
          "the consumer returns the work completes; no duplicate side effects")
    metrics = await _profile(
        chaos.new_workload, client, concurrency=c,
        windows=[
            ("steady", t, None),
            ("worker_down", t, chaos.break_inventory_worker),
            ("recovered", t, chaos.heal_inventory_worker),
        ],
    )
    ok = (
        metrics["steady"]["codes"].get("202", 0) > 0
        and metrics["worker_down"]["codes"].get("202", 0) > 0
        and metrics["worker_down"]["errors"] == 0
    )
    return verdict(
        result_for(name, "Worker outage: API unaffected, consumers catch up", ok, metrics),
        ok=ok,
        detail=(
            f"202s steady={metrics['steady']['codes'].get('202', 0)} "
            f"worker_down={metrics['worker_down']['codes'].get('202', 0)} "
            f"errors={metrics['worker_down']['errors']}"
        ),
    )


async def scenario_read_store(
    chaos: ChaosApp, client: httpx.AsyncClient, *, t: float, c: int
) -> ScenarioResult:
    name = "Read-store outage (Elasticsearch)"
    print(f"\n>> {name}")
    print("   hypothesis: order reads fail with 503 READ_STORE_UNAVAILABLE, never "
          "a misleading 404, while writes and liveness stay healthy")
    metrics = await _profile(
        chaos.new_workload, client, concurrency=c,
        windows=[
            ("steady", t, None),
            ("es_down", t, chaos.break_read_store),
            ("recovered", t, chaos.heal_read_store),
        ],
    )

    chaos.break_read_store()
    workload = await chaos.new_workload(client)
    order_id = workload.order_ids[0] if workload.order_ids else str(uuid.uuid4())
    read = await client.get(f"/orders/{order_id}", headers=workload._headers)  # noqa: SLF001
    body = read.json()
    missing = await client.get(
        "/orders/00000000-0000-0000-0000-000000000000", headers=workload._headers  # noqa: SLF001
    )
    health = await client.get("/health")
    chaos.heal_read_store()
    healed = await client.get(f"/orders/{order_id}", headers=workload._headers)  # noqa: SLF001

    metrics["probe"] = {
        "read_status_during_outage": read.status_code,
        "error_code": body.get("error", {}).get("code"),
        "absent_order_status_during_outage": missing.status_code,
        "health_status": health.status_code,
        "read_status_after_heal": healed.status_code,
    }
    ok = (
        read.status_code == 503
        and body.get("error", {}).get("code") == "READ_STORE_UNAVAILABLE"
        # NOT 404 — a false "order not found" is the regression guarded here.
        and missing.status_code == 503
        and health.status_code == 200
        and healed.status_code in (200, 404)
    )
    return verdict(
        result_for(name, "Read-store outage: 503, never a false 404", ok, metrics),
        ok=ok,
        detail=(
            f"read={read.status_code} code={body.get('error', {}).get('code')} "
            f"absent={missing.status_code} health={health.status_code} "
            f"after_heal={healed.status_code}"
        ),
    )


def result_for(name: str, hypothesis: str, ok: bool, metrics: dict) -> ScenarioResult:
    return ScenarioResult(
        name=name, hypothesis=hypothesis, passed=ok, detail="", metrics=metrics
    )


_ANY_TOKEN: str | None = None


def _any_auth(client: httpx.AsyncClient) -> dict:
    return {"Authorization": f"Bearer {_ANY_TOKEN}"}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> int:
    global _ANY_TOKEN

    parser = argparse.ArgumentParser(description="Week 11 chaos scenarios")
    parser.add_argument("--window", type=float, default=2.0,
                        help="seconds per measurement window (default 2)")
    parser.add_argument("--concurrency", type=int, default=8,
                        help="concurrent workers during load (default 8)")
    parser.add_argument("--scenario", action="append", default=None,
                        help="run only these (redis, broker, worker, read-store)")
    args = parser.parse_args()

    _install_read_store_switch()

    chaos = ChaosApp()
    chaos.publisher = _FailSwitch()
    await chaos.start()

    print(f"Week 11 chaos scenarios  ({time.strftime('%H:%M:%S')})")
    print(f"window={args.window}s concurrency={args.concurrency}")

    # Per-request SQL + HTTP client chatter would bury the scenario output.
    for noisy in (
        "aiosqlite",
        "httpx",
        "http.access",
        "asyncio",
        "orders.persistence_handler",
        "orders.read_projector",
        "inventory.handler",
    ):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://chaos", timeout=30.0
    ) as client:
        seed = await chaos.new_workload(client)
        _ANY_TOKEN = seed._headers["Authorization"].split(" ", 1)[1]  # noqa: SLF001
        # Warm the read store so the steady window has real data to read.
        await client.post(
            "/orders",
            headers=seed._headers,  # noqa: SLF001
            json={
                "customer_id": seed._customer_id,  # noqa: SLF001
                "lines": [
                    {"product_id": seed._product_id, "quantity": 1, "unit_price_cents": 100}
                ],
            },
        )
        readiness = await client.get("/ready")
        print(f"pre-flight /ready -> {readiness.status_code} {readiness.json()}")

        chosen = set(args.scenario or ["redis", "broker", "worker", "read-store"])
        runners: list[tuple[str, Callable[..., Awaitable[ScenarioResult]]]] = [
            ("redis", scenario_redis),
            ("broker", scenario_broker),
            ("worker", scenario_worker),
            ("read-store", scenario_read_store),
        ]
        for key, runner in runners:
            if key in chosen:
                await runner(chaos, client, t=args.window, c=args.concurrency)

    await chaos.stop()

    passed = sum(1 for r in RESULTS if r["passed"])
    print(f"\n{'=' * 68}")
    print(f"scenarios: {passed}/{len(RESULTS)} passed")
    for r in RESULTS:
        print(f"  {'PASS' if r['passed'] else 'FAIL'}  {r['name']}")

    ARTIFACTS.mkdir(exist_ok=True)
    out = ARTIFACTS / "chaos-results.json"
    out.write_text(
        json.dumps(
            {
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "window_seconds": args.window,
                "concurrency": args.concurrency,
                "scenarios": RESULTS,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(f"\nmetrics written to {out.relative_to(REPO_ROOT)}")

    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
