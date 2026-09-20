"""Fixtures for Week 9 rate-limit integration tests.

Reuses the project-wide `client` fixture (DB + read store wired) but replaces
the disabled limiter installed by tests/conftest.py with an *enabled* limiter
backed by fresh in-memory buckets, so middleware behavior is exercised
deterministically without Redis.

Burst tests are made deterministic by using zero-refill tiers: with ``rate=0``
the bucket never accrues fresh tokens during a test, so "exactly `capacity`
requests succeed, then 429" holds regardless of wall-clock timing.
"""
import pytest

from tests.conftest import client  # noqa: F401 (re-export the base fixture)

from src.core.ratelimit.backends import InMemoryTokenBucket
from src.core.ratelimit.policy import DEFAULT_TIERS, RateLimitPolicy, RateLimitTier
from src.core.ratelimit.provider import RateLimiter, reset_rate_limiter, set_rate_limiter

# Copy of DEFAULT_TIERS with every refill rate set to 0 (see module docstring).
_ZERO_REFILL_TIERS = {
    name: RateLimitTier(capacity=tier.capacity, rate=0.0)
    for name, tier in DEFAULT_TIERS.items()
}


@pytest.fixture
async def rate_limited_client(client):
    """Yield (async client, limiter, primary backend, fallback backend) with
    tiny zero-refill buckets so bursts are strictly `capacity`-bounded."""
    backend = InMemoryTokenBucket()
    fallback = InMemoryTokenBucket()
    limiter = RateLimiter(
        backend, fallback, RateLimitPolicy(_ZERO_REFILL_TIERS), enabled=True
    )
    set_rate_limiter(limiter)
    try:
        yield client, limiter, backend, fallback
    finally:
        reset_rate_limiter()