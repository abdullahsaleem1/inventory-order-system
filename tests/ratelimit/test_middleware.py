"""Middleware + provider integration tests (Week 9).

Exercises the real HTTP stack (RateLimitMiddleware in the ASGI app) with the
limiter *enabled* and backed by fresh in-memory buckets, plus the degraded
Redis-failure path. Roles come from real signed JWTs.
"""
import uuid

import pytest
from httpx import AsyncClient

from src.contexts.identity.domain.user import Role, User
from src.contexts.identity.infrastructure.jwt_service import JwtTokenService
from src.core.config import get_settings
from src.core.ratelimit.token_bucket import RateLimiterUnavailableError

settings = get_settings()


async def _register(client: AsyncClient, email: str) -> dict:
    resp = await client.post(
        "/auth/register",
        json={"email": email, "full_name": "Rate Limit Test", "password": "S3curePass!"},
    )
    assert resp.status_code == 201
    return resp.json()


def _mint_token(user_id: str, email: str, role: Role) -> str:
    svc = JwtTokenService(
        secret_key=settings.JWT_SECRET_KEY,
        algorithm=settings.JWT_ALGORITHM,
        issuer=settings.JWT_ISSUER,
        audience=settings.JWT_AUDIENCE,
        access_token_expire_minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES,
    )
    return svc.create_access_token(
        User(
            id=uuid.UUID(user_id),
            email=email,
            full_name="Mint",
            hashed_password="x",
            role=role,
        )
    )


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


async def _burst(client: AsyncClient, url: str, *, headers: dict | None = None, emails=None) -> list[int]:
    """Fire N sequential requests fast; return the status codes."""
    codes = []
    for i in range(30):
        if emails is not None:
            resp = await client.post("/auth/register", json={
                "email": emails[i],
                "full_name": "Burst",
                "password": "S3curePass!",
            })
        else:
            resp = await client.get(url, headers=headers)
        codes.append(resp.status_code)
    return codes


class TestAnonymousIpLimits:
    async def test_anonymous_burst_hits_anonymous_capacity(self, rate_limited_client):
        client, _, _, _ = rate_limited_client
        codes = await _burst(
            client,
            "/orders/doesnotmatter",
            emails=[f"anon{i}@example.com" for i in range(30)],
        )
        # Register endpoint answers each fresh email with 201; the 6th request
        # (after the capacity-5 anonymous bucket empties) is rate limited.
        assert codes.count(201) == 5
        assert codes[5] == 429
        assert all(c == 429 for c in codes[5:])

    async def test_429_uses_standard_envelope_and_headers(self, rate_limited_client):
        client, _, _, _ = rate_limited_client
        codes = await _burst(
            client, "/orders/x", emails=[f"env{i}@example.com" for i in range(30)]
        )
        # Re-check with explicit request to assert the response body.
        extra = await client.post("/auth/register", json={
            "email": "env-extra@example.com",
            "full_name": "Burst",
            "password": "S3curePass!",
        })
        assert extra.status_code == 429
        body = extra.json()
        assert body["error"]["code"] == "TOO_MANY_REQUESTS"
        assert "Retry in" in body["error"]["message"]
        assert body["status"] == 429
        assert body["path"] == "/auth/register"
        assert "request_id" in body
        assert extra.headers.get("retry-after") is not None
        assert extra.headers.get("x-ratelimit-limit") == "0"
        assert extra.headers.get("x-ratelimit-remaining") == "0"
        assert extra.headers.get("x-ratelimit-retry-after") is not None


class TestTieredLimitsByRole:
    async def test_admin_gets_larger_burst_than_anonymous(self, rate_limited_client):
        client, _, _, _ = rate_limited_client
        body = await _register(client, "tier-admin@example.com")
        admin_token = _mint_token(body["user"]["id"], body["user"]["email"], Role.ADMIN)
        codes = await _burst(client, "/orders/doesnotmatter", headers=_auth(admin_token))
        # ADMIN capacity (DEFAULT_TIERS) == 100; we only fired 30 requests so
        # none should be rate limited — unlike anonymous which dies at 5.
        assert 429 not in codes

    async def test_staff_burst_matches_staff_capacity(self, rate_limited_client):
        client, limiter, backend, _ = rate_limited_client
        body = await _register(client, "tier-staff@example.com")
        staff_token = _mint_token(body["user"]["id"], body["user"]["email"], Role.STAFF)
        codes = await _burst(client, "/orders/doesnotmatter", headers=_auth(staff_token))
        # STAFF capacity == 20.
        assert sum(1 for c in codes if c != 429) == 20
        assert codes[20] == 429

    async def test_user_keyed_by_user_id_not_ip(self, rate_limited_client):
        client, limiter, backend, _ = rate_limited_client
        # Two distinct roles share the same client IP; the buckets must be
        # per-user, so one user exhausting their budget can't starve the other.
        a = await _register(client, "k1@example.com")
        b = await _register(client, "k2@example.com")
        token_a = _mint_token(a["user"]["id"], a["user"]["email"], Role.CUSTOMER)
        token_b = _mint_token(b["user"]["id"], b["user"]["email"], Role.CUSTOMER)
        codes_a = await _burst(client, "/orders/x", headers=_auth(token_a))  # burns 10 of 10
        assert codes_a[9] != 429 and codes_a[10] == 429
        # Fresh CUSTOMER for user B on the same IP still serves normally.
        resp = await client.get("/orders/y", headers=_auth(token_b))
        assert resp.status_code != 429


class TestExemptions:
    async def test_health_never_rate_limited(self, rate_limited_client):
        client, _, _, _ = rate_limited_client
        for _ in range(12):
            resp = await client.get("/health")
            assert resp.status_code == 200
            assert "x-ratelimit-remaining" not in resp.headers

    async def test_ready_reports_redis_status(self, rate_limited_client):
        client, _, _, _ = rate_limited_client
        resp = await client.get("/ready")
        assert resp.status_code in (200, 503)  # readiness semantics unchanged
        body = resp.json()
        # In-memory backend pings OK, so the limiter reports `up` (enabled).
        assert body["redis"] == "up"
        assert "x-ratelimit-remaining" not in resp.headers

    async def test_ready_never_rate_limited(self, rate_limited_client):
        client, _, _, _ = rate_limited_client
        for _ in range(12):
            resp = await client.get("/ready")
            assert resp.status_code in (200, 503)  # readiness semantics unchanged
            assert "x-ratelimit-remaining" not in resp.headers


class TestGracefulDegradation:
    async def test_redis_failure_falls_back_and_stamps_degraded(self, rate_limited_client):
        client, limiter, backend, fallback = rate_limited_client

        class _BrokenBackend:
            async def consume(self, key, *, capacity, rate, cost=1.0):
                raise RateLimiterUnavailableError("redis unreachable")

            async def ping(self) -> bool:
                return False

        limiter._backend = _BrokenBackend()

        # Requests keep being served (degraded) instead of crashing.
        resp = await client.get("/orders/x")  # anonymous, no token
        assert resp.status_code != 500
        assert resp.headers.get("x-ratelimit-degraded") == "true"

        # The in-process fallback bucket is what actually counted the request.
        assert fallback.size() == 1

    async def test_redis_recovery_clears_degraded_flag(self, rate_limited_client):
        client, limiter, _, backend = rate_limited_client

        assert limiter.degraded is False
        # Simulate transient failure then recovery.
        limiter._degraded = True
        await client.get("/orders/x")
        assert limiter.degraded is False


class TestDisabledLimiterPassthrough:
    async def test_disabled_limiter_never_limits(self, client):
        codes = await _burst(
            client, "/orders/doesnotmatter",
            emails=[f"dis{i}@example.com" for i in range(30)],
        )
        assert 429 not in codes