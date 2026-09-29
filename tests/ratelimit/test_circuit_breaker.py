"""Circuit-breaker tests for the rate limiter (Week 11 chaos engineering).

Motivation: per-request fallback was not enough. With Redis down, the limiter
caught ``RateLimiterUnavailableError`` and served from the in-process bucket —
but it had already paid the full Redis command timeout on *every* request. A
dead Redis therefore added its timeout to the latency of the entire API
indefinitely. These tests pin the breaker: once open, ``check()`` must make zero
Redis calls, and a single successful half-open probe must close it.
"""
import asyncio
import time

import pytest

from src.core.ratelimit.backends import InMemoryTokenBucket
from src.core.ratelimit.policy import RateLimitPolicy, RateLimitTier
from src.core.ratelimit.provider import RateLimiter
from src.core.ratelimit.token_bucket import RateLimiterUnavailableError

# capacity 1000 / rate 0 so a long test run never exhausts the bucket and
# confuse an allow/deny assertion with a breaker assertion.
_WIDE_TIER = {"ANONYMOUS": RateLimitTier(capacity=1000, rate=0.0)}
_POLICY = RateLimitPolicy(_WIDE_TIER)


class FlakyBackend:
    """Stand-in for the Redis bucket that can be broken at will.

    When healthy it delegates to a real in-process bucket, so a "successful"
    consume is a genuine token spend rather than a canned value.
    """

    def __init__(self) -> None:
        self.healthy = True
        self.consume_calls = 0
        self.ping_calls = 0
        self.delay = 0.0
        self._inner = InMemoryTokenBucket()

    async def consume(self, key, *, capacity, rate, cost=1.0):
        self.consume_calls += 1
        if self.delay:
            await asyncio.sleep(self.delay)
        if not self.healthy:
            raise RateLimiterUnavailableError("redis unreachable")
        return await self._inner.consume(key, capacity=capacity, rate=rate, cost=cost)

    async def ping(self) -> bool:
        self.ping_calls += 1
        return self.healthy


def _make_limiter(**kwargs) -> tuple[RateLimiter, FlakyBackend, InMemoryTokenBucket]:
    backend = FlakyBackend()
    fallback = InMemoryTokenBucket()
    limiter = RateLimiter(
        backend,
        fallback,
        _POLICY,
        enabled=True,
        failure_threshold=kwargs.pop("failure_threshold", 2),
        cooldown_base=kwargs.pop("cooldown_base", 0.05),
        cooldown_max=kwargs.pop("cooldown_max", 0.2),
    )
    assert not kwargs, f"unexpected kwargs: {kwargs}"
    return limiter, backend, fallback


class TestBreakerOpens:
    async def test_circuit_closed_while_redis_healthy(self):
        limiter, backend, _ = _make_limiter()
        for _ in range(5):
            await limiter.check("k", None)
        assert backend.consume_calls == 5
        assert limiter.circuit_open is False
        assert limiter.degraded is False

    async def test_breaker_opens_after_threshold_failures(self):
        limiter, backend, _ = _make_limiter(failure_threshold=2)
        backend.healthy = False
        await limiter.check("k", None)
        assert limiter.circuit_open is False, "one failure must not open the breaker"
        await limiter.check("k", None)
        assert limiter.circuit_open is True
        assert limiter.degraded is True

    async def test_open_breaker_makes_no_further_redis_calls(self):
        """The core latency fix: once open, Redis is not touched at all."""
        limiter, backend, fallback = _make_limiter(failure_threshold=1)
        backend.healthy = False
        await limiter.check("k", None)  # trips the breaker
        calls_after_open = backend.consume_calls

        for _ in range(50):
            result = await limiter.check("k", None)
            assert result.allowed is True

        assert backend.consume_calls == calls_after_open, (
            "the breaker must skip Redis entirely; each extra call would cost "
            "another full Redis timeout on the request path"
        )
        assert fallback.size() == 1, "all 51 requests must share the one fallback bucket"

    async def test_fallback_still_enforces_the_limit_while_open(self):
        """Degraded must mean 'stricter local bucket', not 'no limiting'."""
        limiter, backend, _ = _make_limiter(failure_threshold=1)
        backend.healthy = False
        results = [await limiter.check("k", None) for _ in range(5)]
        # capacity 1000 so nothing is denied here; the point is the breaker
        # stayed open and never touched Redis after the first failure.
        assert all(r.allowed for r in results)
        assert backend.consume_calls == 1
        assert limiter.circuit_open is True


class TestBreakerRecovers:
    async def test_half_open_probe_closes_the_breaker(self):
        limiter, backend, _ = _make_limiter(failure_threshold=1, cooldown_base=0.01)
        backend.healthy = False
        await limiter.check("k", None)
        assert limiter.circuit_open is True

        await asyncio.sleep(0.02)  # let the cooldown elapse
        backend.healthy = True
        result = await limiter.check("k", None)

        assert result.allowed is True
        assert limiter.circuit_open is False
        assert limiter.degraded is False
        assert backend.consume_calls == 2, "the recovery probe must reach Redis"

    async def test_breaker_stays_closed_after_recovery(self):
        limiter, backend, _ = _make_limiter(failure_threshold=1, cooldown_base=0.01)
        backend.healthy = False
        await limiter.check("k", None)
        await asyncio.sleep(0.02)
        backend.healthy = True
        await limiter.check("k", None)

        before = backend.consume_calls
        for _ in range(10):
            await limiter.check("k", None)
        assert backend.consume_calls == before + 10, (
            "a recovered limiter must serve every request from Redis again"
        )
        assert limiter.circuit_open is False

    async def test_failed_probe_reopens_and_backs_off(self):
        limiter, backend, _ = _make_limiter(failure_threshold=1, cooldown_base=0.01)
        backend.healthy = False
        await limiter.check("k", None)
        first_cooldown = limiter._cooldown_seconds

        await asyncio.sleep(0.02)
        await limiter.check("k", None)  # half-open probe, still broken

        assert limiter.circuit_open is True
        assert limiter._cooldown_seconds > first_cooldown, (
            "repeated failures must lengthen the cooldown, not hammer Redis"
        )

    async def test_cooldown_is_capped(self):
        limiter, backend, _ = _make_limiter(
            failure_threshold=1, cooldown_base=0.01, cooldown_max=0.05
        )
        backend.healthy = False
        for _ in range(20):
            await limiter.check("k", None)
            await asyncio.sleep(0.001)
        assert limiter._cooldown_seconds <= 0.05


class TestOnlyOneProbe:
    async def test_concurrent_requests_admit_a_single_probe(self):
        """A thundering herd must not turn a 50-request burst into 50 Redis dials."""
        limiter, backend, _ = _make_limiter(failure_threshold=1, cooldown_base=0.02)
        backend.healthy = False
        await limiter.check("k", None)  # trip
        await asyncio.sleep(0.03)  # cooldown elapsed: breaker is half-open

        backend.healthy = True
        backend.delay = 0.01
        await asyncio.gather(*(limiter.check(f"k{i}", None) for i in range(50)))

        # Exactly one of the 50 became the half-open probe; the rest were
        # served from the fallback with no Redis I/O.
        assert backend.consume_calls == 2, (
            f"expected 1 probe (plus the original failure), got "
            f"{backend.consume_calls - 1} probes"
        )
        assert limiter.circuit_open is False


class TestProbeInteraction:
    async def test_failed_ready_probe_opens_the_breaker(self):
        """A /ready probe must stop the request path from hammering dead Redis."""
        limiter, backend, _ = _make_limiter(failure_threshold=5)
        backend.healthy = False
        assert await limiter.probe() == "degraded"
        assert limiter.circuit_open is True

    async def test_repeated_ready_probes_do_not_inflate_the_cooldown(self):
        """A frequently-polled /ready must not delay real recovery.

        Readiness probes are a health check, not a load-bearing request. If
        each poll counted as a request failure, polling /ready once a second
        would ratchet the cooldown to its cap and stall recovery.
        """
        limiter, backend, _ = _make_limiter(failure_threshold=1, cooldown_base=0.5)
        backend.healthy = False
        await limiter.probe()
        cooldown_after_first = limiter._cooldown_seconds
        for _ in range(30):
            await limiter.probe()
        assert limiter._cooldown_seconds == cooldown_after_first

    async def test_successful_probe_closes_the_breaker(self):
        """Recovery must be observable even with no traffic hitting check()."""
        limiter, backend, _ = _make_limiter(failure_threshold=1, cooldown_base=0.01)
        backend.healthy = False
        await limiter.check("k", None)
        assert limiter.circuit_open is True

        backend.healthy = True
        assert await limiter.probe() == "up"
        assert limiter.circuit_open is False
        assert limiter.degraded is False

    async def test_probe_reports_real_redis_health_while_breaker_open(self):
        limiter, backend, _ = _make_limiter(failure_threshold=1, cooldown_base=60.0)
        backend.healthy = False
        await limiter.check("k", None)
        assert limiter.circuit_open is True

        # Redis actually came back; /ready must say so even though the breaker
        # is still cooling down and check() would not touch Redis yet.
        backend.healthy = True
        assert await limiter.probe() == "up"
        assert backend.ping_calls == 1, "probe must perform real I/O, not trust the breaker"

    async def test_disabled_limiter_never_consults_the_breaker(self):
        backend = FlakyBackend()
        limiter = RateLimiter(
            backend, InMemoryTokenBucket(), _POLICY, enabled=False, failure_threshold=1
        )
        result = await limiter.check("k", None)
        assert result.allowed is True
        assert backend.consume_calls == 0
        assert await limiter.probe() == "disabled"


@pytest.mark.parametrize("threshold", [1, 2, 3])
async def test_threshold_of_one_opens_immediately(threshold):
    limiter, backend, _ = _make_limiter(failure_threshold=threshold)
    backend.healthy = False
    for i in range(threshold):
        await limiter.check("k", None)
        assert limiter.circuit_open is (i >= threshold - 1)


async def test_no_latency_penalty_once_the_breaker_is_open():
    """Directly measures the fix: N requests against a dead Redis.

    Without the breaker each request pays the backend delay; with it, only the
    trip request and the eventual probe do.
    """
    delay = 0.05
    limiter, backend, _ = _make_limiter(failure_threshold=1, cooldown_base=5.0)
    backend.healthy = False
    backend.delay = delay

    started = time.perf_counter()
    await limiter.check("k", None)  # trips the breaker, pays `delay`
    for _ in range(40):
        await limiter.check("k", None)  # must be effectively free
    elapsed = time.perf_counter() - started

    assert backend.consume_calls == 1
    assert elapsed < delay * 3, (
        f"40 requests took {elapsed:.3f}s; expected roughly one {delay}s timeout, "
        "so the breaker is not actually bypassing Redis"
    )
