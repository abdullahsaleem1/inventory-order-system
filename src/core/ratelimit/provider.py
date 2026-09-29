"""
Process-wide rate-limiter wiring (Week 9).

Builds the singleton ``RateLimiter`` at startup, exposes it to the middleware
via ``get_rate_limiter()``, probes Redis for ``/ready``, and closes the Redis
client at shutdown. Redis is optional at runtime: if it is unreachable when the
limiter is first used (or ever goes down mid-flight), a ``RedisUnavailableError``
triggers an in-process fallback so the API degrades gracefully instead of
crashing.
"""
import asyncio
import time

import redis.asyncio as aioredis

from src.core.config import get_settings
from src.core.logging_config import get_logger
from src.core.ratelimit.backends import InMemoryTokenBucket, RedisTokenBucket, TokenBucket
from src.core.ratelimit.policy import RateLimitPolicy, tiers_from_settings
from src.core.ratelimit.token_bucket import RateLimiterUnavailableError, TokenBucketResult

logger = get_logger("ratelimit.provider")

_limiter: "RateLimiter" | None = None
_redis_client: aioredis.Redis | None = None


class RateLimiter:
    """Tiered token-bucket rate limiter with graceful Redis degradation.

    - ``check(key, role)`` consumes one token from the bucket for the identity
      represented by ``key``, using the token-bucket tier for ``role``.
    - If the Redis backend is unavailable the call is served by the in-process
      fallback bucket and ``degraded`` is set, so the API keeps running in a
      degraded state.

    A circuit breaker (Week 11) sits in front of the Redis backend. Falling
    back per-request is not enough on its own: every request would still pay
    the full Redis command timeout, so a dead Redis would add that latency to
    the entire API indefinitely. Once ``failure_threshold`` consecutive calls
    fail the breaker *opens* and ``check()`` serves from the fallback bucket
    with zero Redis I/O. After ``cooldown_base`` seconds a single half-open
    probe is allowed through; success closes the breaker, failure re-opens it
    with an exponentially longer cooldown (capped).
    """

    def __init__(
        self,
        backend: TokenBucket,
        fallback: TokenBucket,
        policy: RateLimitPolicy,
        *,
        enabled: bool = True,
        failure_threshold: int = 2,
        cooldown_base: float = 5.0,
        cooldown_max: float = 60.0,
    ) -> None:
        self._backend = backend
        self._fallback = fallback
        self._policy = policy
        self.enabled = enabled
        self._degraded = False
        self._failure_threshold = max(1, failure_threshold)
        self._cooldown_base = max(0.0, cooldown_base)
        self._cooldown_max = max(self._cooldown_base, cooldown_max)
        # Circuit-breaker state.
        self._consecutive_failures = 0
        self._cooldown_seconds = self._cooldown_base
        self._opened_at = 0.0
        self._half_open_in_flight = False
        self._circuit_lock = asyncio.Lock()

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def policy(self) -> RateLimitPolicy:
        return self._policy

    @property
    def circuit_open(self) -> bool:
        """True while the breaker is open (Redis skipped entirely)."""
        return self._consecutive_failures >= self._failure_threshold

    async def _allow_redis_call(self) -> bool:
        """Decide whether this request may touch Redis; may claim the probe slot.

        Returns True when the caller should attempt the Redis backend. While
        the breaker is open the answer is False until the cooldown elapses, at
        which point exactly one caller is admitted as a half-open probe.
        """
        async with self._circuit_lock:
            if self._consecutive_failures < self._failure_threshold:
                return True
            if time.monotonic() - self._opened_at < self._cooldown_seconds:
                return False  # still cooling down: serve from the fallback
            if self._half_open_in_flight:
                return False  # another caller is already probing
            self._half_open_in_flight = True
            return True

    def _record_redis_success(self) -> None:
        self._consecutive_failures = 0
        self._cooldown_seconds = self._cooldown_base
        self._half_open_in_flight = False
        self._degraded = False

    def _record_redis_failure(self, *, key: str) -> None:
        first = not self._degraded
        self._consecutive_failures += 1
        self._half_open_in_flight = False
        if first:
            logger.warning(
                "rate_limiter_degraded",
                extra={"reason": "redis unavailable", "key": key},
            )
        if self._consecutive_failures >= self._failure_threshold:
            if self._opened_at == 0.0:
                # First transition into the open state: start the clock.
                self._opened_at = time.monotonic()
            self._cooldown_seconds = min(self._cooldown_max, self._cooldown_base * (2 ** min(
                self._consecutive_failures - self._failure_threshold, 8
            )))
            if first:
                logger.warning(
                    "rate_limiter_circuit_open",
                    extra={
                        "cooldown_seconds": self._cooldown_seconds,
                        "consecutive_failures": self._consecutive_failures,
                    },
                )
        self._degraded = True

    async def check(self, key: str, role: str | None) -> TokenBucketResult:
        if not self.enabled:
            return TokenBucketResult(allowed=True, balance=0.0, retry_after_seconds=0.0)
        tier = self._policy.tier(role)
        if not await self._allow_redis_call():
            # Breaker open: no Redis I/O at all, serve from the fallback bucket.
            return await self._fallback.consume(
                key, capacity=tier.capacity, rate=tier.rate
            )
        try:
            result = await self._backend.consume(
                key, capacity=tier.capacity, rate=tier.rate
            )
        except RateLimiterUnavailableError:
            self._record_redis_failure(key=key)
            result = await self._fallback.consume(
                key, capacity=tier.capacity, rate=tier.rate
            )
        else:
            self._record_redis_success()
        return result

    def _record_probe_failure(self) -> None:
        """Record a failed /ready probe.

        A probe is a health check, not a load-bearing request, so it must open
        the circuit (to stop the request path hammering a dead Redis) but must
        NOT ratchet the backoff: a frequently-polled /ready would otherwise
        inflate the cooldown and delay genuine recovery.
        """
        self._degraded = True
        if self._consecutive_failures < self._failure_threshold:
            self._consecutive_failures = self._failure_threshold
            self._opened_at = time.monotonic()
            self._cooldown_seconds = self._cooldown_base
            logger.warning(
                "rate_limiter_circuit_open",
                extra={
                    "cooldown_seconds": self._cooldown_seconds,
                    "trigger": "readiness_probe",
                },
            )

    async def probe(self) -> str:
        """Return 'up', 'degraded' or 'disabled' for the /ready endpoint."""
        if not self.enabled:
            return "disabled"
        try:
            up = await asyncio.wait_for(self._backend.ping(), timeout=1.0)
        except Exception:
            up = False
        # Readiness must report real Redis health even while the breaker is
        # open, so the probe always pings Redis; a successful probe doubles as
        # the recovery signal that closes the circuit.
        if up:
            self._record_redis_success()
        else:
            self._record_probe_failure()
        return "up" if up else "degraded"


def build_rate_limiter() -> RateLimiter:
    """Create (once) the process-wide limiter based on current settings."""
    global _limiter, _redis_client
    if _limiter is not None:
        return _limiter

    settings = get_settings()
    policy = RateLimitPolicy(tiers_from_settings(settings))

    if settings.RATE_LIMIT_ENABLED:
        _redis_client = aioredis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=max(0.25, settings.RATE_LIMIT_REDIS_TIMEOUT_SECONDS),
            socket_timeout=max(0.25, settings.RATE_LIMIT_REDIS_TIMEOUT_SECONDS),
        )
        backend: TokenBucket = RedisTokenBucket(
            _redis_client, ttl_seconds=settings.RATE_LIMIT_BUCKET_TTL_SECONDS
        )
        logger.info(
            "rate_limiter_initialized",
            extra={"backend": "redis", "redis_url": settings.REDIS_URL},
        )
    else:
        backend = InMemoryTokenBucket()
        logger.info("rate_limiter_disabled", extra={"reason": "RATE_LIMIT_ENABLED=false"})

    _limiter = RateLimiter(
        backend,
        InMemoryTokenBucket(),
        policy,
        enabled=settings.RATE_LIMIT_ENABLED,
        failure_threshold=settings.RATE_LIMIT_REDIS_CIRCUIT_FAILURES,
        cooldown_base=settings.RATE_LIMIT_REDIS_CIRCUIT_COOLDOWN_SECONDS,
        cooldown_max=settings.RATE_LIMIT_REDIS_CIRCUIT_MAX_COOLDOWN_SECONDS,
    )
    return _limiter


def get_rate_limiter() -> RateLimiter:
    return build_rate_limiter()


def set_rate_limiter(limiter: RateLimiter) -> None:
    """Install a pre-built limiter (tests / embedding)."""
    global _limiter
    _limiter = limiter


def reset_rate_limiter() -> None:
    """Drop the singleton (used by tests)."""
    global _limiter
    _limiter = None


async def close_rate_limiter() -> None:
    """Close the Redis client and drop the singleton (app shutdown / tests)."""
    global _limiter, _redis_client
    if _redis_client is not None:
        try:
            await _redis_client.aclose()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("rate_limiter_close_failed", extra={"error": str(exc)})
    _redis_client = None
    _limiter = None


async def probe_rate_limiter() -> str:
    """Never raises; returns 'up' | 'degraded' | 'disabled'."""
    try:
        return await get_rate_limiter().probe()
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("rate_limiter_probe_failed", extra={"error": str(exc)})
        return "degraded"