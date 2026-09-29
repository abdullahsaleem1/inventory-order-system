"""Readiness + read-store outage tests (Week 11 chaos engineering).

Two defects found by fault injection, both pinned here:

1. ``/ready`` returned **500 INTERNAL_ERROR** when Redis was down.
   ``RateLimiter.probe()`` emits ``"degraded"`` but ``ReadinessResponse.redis``
   only allowed ``up``/``down``/``disabled``. The pydantic ValidationError was
   raised *after* the endpoint returned, so it discarded the readiness body and
   overrode the intended 503 — readiness went blind exactly when an operator
   needed it most.

2. A read-store outage was reported as **404 order-not-found** on
   ``GET /orders/{id}``, because a bare ``except Exception: return None`` could
   not tell "not projected yet" from "Elasticsearch is unreachable". It is now
   503 ``READ_STORE_UNAVAILABLE``.
"""
import pytest
from httpx import AsyncClient

from src.core.ratelimit.provider import RateLimiter, set_rate_limiter
from src.core.ratelimit.token_bucket import RateLimiterUnavailableError
from src.shared.readstore.errors import ReadStoreUnavailableError


class _UnreachableRedis:
    """A Redis bucket that is always down."""

    def __init__(self) -> None:
        self.consume_calls = 0

    async def consume(self, key, *, capacity, rate, cost=1.0):
        self.consume_calls += 1
        raise RateLimiterUnavailableError("redis unreachable")

    async def ping(self) -> bool:
        return False


@pytest.fixture
def dead_redis_limiter(client: AsyncClient):
    """Install a limiter whose Redis is permanently unreachable."""
    from src.core.ratelimit.backends import InMemoryTokenBucket
    from src.core.ratelimit.policy import DEFAULT_TIERS, RateLimitPolicy

    limiter = RateLimiter(
        _UnreachableRedis(),
        InMemoryTokenBucket(),
        RateLimitPolicy(DEFAULT_TIERS),
        enabled=True,
    )
    set_rate_limiter(limiter)
    try:
        yield limiter
    finally:
        from src.core.ratelimit.provider import reset_rate_limiter

        reset_rate_limiter()


class TestReadinessSurvivesRedisOutage:
    """Regression for the 500-instead-of-503 defect."""

    async def test_ready_does_not_500_when_redis_is_down(self, client, dead_redis_limiter):
        resp = await client.get("/ready")
        assert resp.status_code != 500, (
            f"readiness must never 500 on a Redis outage; got "
            f"{resp.status_code} {resp.text}"
        )

    async def test_ready_still_reports_200_when_only_redis_is_down(
        self, client, dead_redis_limiter
    ):
        """Redis is explicitly non-fatal: the API degrades, it does not go unready."""
        resp = await client.get("/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        assert body["redis"] == "degraded"
        assert "database" in body and "broker" in body

    async def test_ready_body_survives_a_redis_outage(self, client, dead_redis_limiter):
        """The operator must still see database and broker status."""
        body = (await client.get("/ready")).json()
        assert body["database"] in ("up", "down")
        assert body["broker"] in ("up", "down", "disabled")

    async def test_health_is_unaffected_by_redis_outage(self, client, dead_redis_limiter):
        """Liveness must never depend on a degraded-but-serving limiter."""
        for _ in range(5):
            resp = await client.get("/health")
            assert resp.status_code == 200
            assert resp.json() == {"status": "ok"}

    async def test_readiness_does_not_hammer_redis(self, client, dead_redis_limiter):
        """The probe must not open the breaker itself, or repeated /ready polls
        would ratchet the cooldown and delay recovery."""
        assert dead_redis_limiter.circuit_open is False
        for _ in range(10):
            await client.get("/ready")
        assert dead_redis_limiter.circuit_open is True
        assert dead_redis_limiter._cooldown_seconds == (
            dead_redis_limiter._cooldown_base
        ), "polling /ready must not inflate the recovery cooldown"


@pytest.fixture
async def auth_client(client: AsyncClient):
    """`client` plus an authenticated Authorization header for order reads."""
    import uuid

    from src.contexts.identity.domain.user import Role, User
    from src.contexts.identity.infrastructure.jwt_service import JwtTokenService
    from src.core.config import get_settings

    settings = get_settings()
    resp = await client.post(
        "/auth/register",
        json={
            "email": f"resilience-{uuid.uuid4().hex[:8]}@example.com",
            "full_name": "Resilience Test",
            "password": "S3curePass!",
        },
    )
    assert resp.status_code == 201, resp.text
    user = resp.json()["user"]

    svc = JwtTokenService(
        secret_key=settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
        issuer=settings.JWT_ISSUER,
        audience=settings.JWT_AUDIENCE,
        access_token_expire_minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES,
    )
    token = svc.create_access_token(
        User(
            id=uuid.UUID(user["id"]),
            email=user["email"],
            full_name="Resilience",
            hashed_password="x",
            role=Role.CUSTOMER,
        )
    )
    return client, {"Authorization": f"Bearer {token}"}


class TestReadStoreOutageIsNotA404:
    """Regression for the outage-reported-as-not-found defect.

    These drive the *read store* (the dependency) rather than the repository
    (an internal collaborator), so the assertions hold for any implementation.
    """

    async def _orders(self, client) -> dict:
        """Create one order so authenticated reads have a real subject."""
        import uuid

        body = await client.post(
            "/auth/register",
            json={
                "email": f"rs-{uuid.uuid4().hex[:8]}@example.com",
                "full_name": "RS",
                "password": "S3curePass!",
            },
        )
        assert body.status_code == 201, body.text
        return body.json()

    async def test_get_order_reports_503_not_404_when_read_store_is_down(
        self, auth_client, monkeypatch
    ):
        client, headers = auth_client
        from src.shared.readstore import InMemoryOrderReadStore

        async def _down(self, order_id):
            raise ReadStoreUnavailableError(
                "Elasticsearch read store unavailable during get_order: "
                "ConnectionError: connection refused",
                operation="get_order",
            )

        monkeypatch.setattr(InMemoryOrderReadStore, "get_order", _down, raising=True)

        resp = await client.get(
            "/orders/00000000-0000-0000-0000-000000000000", headers=headers
        )
        assert resp.status_code == 503, (
            f"a read-store outage must be 503, got {resp.status_code}: {resp.text}"
        )
        assert resp.json()["error"]["code"] == "READ_STORE_UNAVAILABLE"

    async def test_get_order_still_404s_when_genuinely_absent(self, auth_client):
        """The fix must not break the legitimate eventual-consistency 404."""
        client, headers = auth_client
        resp = await client.get(
            "/orders/00000000-0000-0000-0000-000000000000", headers=headers
        )
        assert resp.status_code == 404, resp.text
        assert resp.json()["error"]["code"] == "NOT_FOUND"

    async def test_list_by_customer_reports_503_when_read_store_is_down(
        self, auth_client, monkeypatch
    ):
        """`ListOrdersByCustomerQueryHandler` has no HTTP route yet, so assert the
        handler's contract directly: it must surface a 503-mapped AppError."""
        import uuid as _uuid

        from src.contexts.orders.queries import ListOrdersByCustomerQuery
        from src.contexts.orders.query_handlers import ListOrdersByCustomerQueryHandler
        from src.contexts.orders.repositories.order_read_store_repository import (
            OrderReadStoreRepository,
        )
        from src.shared.exceptions import ServiceUnavailableError
        from src.shared.readstore import InMemoryOrderReadStore

        async def _down(self, customer_id, *, limit=20, offset=0):
            raise ReadStoreUnavailableError(
                "Elasticsearch read store unavailable during list_by_customer",
                operation="list_by_customer",
            )

        monkeypatch.setattr(
            InMemoryOrderReadStore, "list_by_customer", _down, raising=True
        )

        handler = ListOrdersByCustomerQueryHandler(
            OrderReadStoreRepository(InMemoryOrderReadStore())
        )
        query = ListOrdersByCustomerQuery(
            customer_id=_uuid.UUID(int=0), limit=20, offset=0
        )
        with pytest.raises(ServiceUnavailableError) as excinfo:
            await handler.handle(query)
        assert excinfo.value.status_code == 503
        assert excinfo.value.code == "READ_STORE_UNAVAILABLE"


class TestElasticsearchStoreErrorClassification:
    """Unit-level checks on the store itself, independent of HTTP."""

    def _store(self, client_double):
        from src.shared.readstore.elasticsearch_store import ElasticsearchOrderReadStore

        # Real constructor (so the tracer is wired), then swap the driver.
        store = ElasticsearchOrderReadStore("http://es.invalid:9200", index="orders")
        store._client = client_double
        return store

    @staticmethod
    def _not_found() -> Exception:
        """A real client-shaped 404, as the ES driver raises for a missing doc."""
        from elasticsearch import NotFoundError
        from elastic_transport import ApiResponseMeta, HttpHeaders

        return NotFoundError(
            "not found",
            meta=ApiResponseMeta(
                status=404,
                http_version="1.1",
                headers=HttpHeaders({"content-type": "application/json"}),
                duration=0.0,
                node=None,
            ),
            body={"error": {"type": "document_missing_exception"}},
        )

    async def test_missing_document_returns_none(self):
        not_found = self._not_found()

        class _Client:
            @property
            def indices(self):
                class _Indices:
                    async def exists(self, index):
                        return True

                return _Indices()

            async def get(self, index, id):
                raise not_found

        store = self._store(_Client())
        assert await store.get_order("missing") is None

    async def test_cluster_outage_raises_unavailable_not_none(self):
        class _Indices:
            async def exists(self, index):
                raise ConnectionError("connection refused")

        class _Client:
            indices = _Indices()

        store = self._store(_Client())
        with pytest.raises(ReadStoreUnavailableError) as excinfo:
            await store.get_order("any")
        assert excinfo.value.operation == "get_order"
        assert "connection refused" in str(excinfo.value)

    async def test_write_side_outage_raises_unavailable(self):
        class _Indices:
            async def exists(self, index):
                return True

        class _Client:
            indices = _Indices()

            async def index(self, **kwargs):
                raise ConnectionError("no route to host")

        store = self._store(_Client())
        with pytest.raises(ReadStoreUnavailableError):
            await store.upsert_order({"order_id": "x", "customer_id": "y"})
