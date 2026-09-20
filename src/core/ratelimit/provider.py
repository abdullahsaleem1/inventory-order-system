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
    """

    def __init__(
        self,
        backend: TokenBucket,
        fallback: TokenBucket,
        policy: RateLimitPolicy,
        *,
        enabled: bool = True,
    ) -> None:
        self._backend = backend
        self._fallback = fallback
        self._policy = policy
        self.enabled = enabled
        self._degraded = False

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def policy(self) -> RateLimitPolicy:
        return self._policy

    async def check(self, key: str, role: str | None) -> TokenBucketResult:
        if not self.enabled:
            return TokenBucketResult(allowed=True, balance=0.0, retry_after_seconds=0.0)
        tier = self._policy.tier(role)
        try:
            result = await self._backend.consume(
                key, capacity=tier.capacity, rate=tier.rate
            )
        except RateLimiterUnavailableError:
            if not self._degraded:
                logger.warning(
                    "rate_limiter_degraded",
                    extra={"reason": "redis unavailable", "key": key},
                )
            self._degraded = True
            result = await self._fallback.consume(
                key, capacity=tier.capacity, rate=tier.rate
            )
        else:
            self._degraded = False
        return result

    async def probe(self) -> str:
        """Return 'up', 'degraded' or 'disabled' for the /ready endpoint."""
        if not self.enabled:
            return "disabled"
        try:
            up = await asyncio.wait_for(self._backend.ping(), timeout=1.0)
        except Exception:
            up = False
        self._degraded = not up
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

    _limiter = RateLimiter(backend, InMemoryTokenBucket(), policy, enabled=settings.RATE_LIMIT_ENABLED)
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