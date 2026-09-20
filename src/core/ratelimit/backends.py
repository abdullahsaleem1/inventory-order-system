"""
Token-bucket backends (Week 9).

Redis is the strict, atomic backend: the entire read-refill-spend-write runs
inside one Lua script that Redis executes atomically, so concurrent requests
can never over-spend a bucket. When Redis is unreachable we fall back to the
process-local in-memory bucket so the API keeps serving (degraded state).
"""
import asyncio
import time
from typing import Protocol

import redis.asyncio as aioredis
from redis.exceptions import RedisError, ResponseError, TimeoutError

from src.core.logging_config import get_logger
from src.core.ratelimit.token_bucket import (
    RateLimiterUnavailableError,
    TokenBucketResult,
    attempt,
)

logger = get_logger(__name__)


class TokenBucket(Protocol):
    async def consume(
        self, key: str, *, capacity: int, rate: float, cost: float = 1.0
    ) -> TokenBucketResult: ...

    async def ping(self) -> bool: ...


# ---------------------------------------------------------------------------
# Redis backend
# ---------------------------------------------------------------------------
#
# The Lua script does the whole token-bucket step atomically for one key:
#   1. load stored tokens + refill timestamp (defaults to a full bucket)
#   2. refill by (now - last) * rate, capped at capacity
#   3. spend `cost` if available; otherwise compute retry_after
#   4. persist new tokens + timestamp and refresh the key TTL
#
# Because Redis executes scripts serially, two concurrent EVALSHA calls for the
# same key can never both observe a bucket with one token left — this is what
# makes the limiter *strict* under concurrency.
_TOKEN_BUCKET_LUA = r"""
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local cost     = tonumber(ARGV[3])
local now      = tonumber(ARGV[4])
local ttl_s    = tonumber(ARGV[5])

local tokens, last_refill = capacity, now
local stored = redis.call('HGET', key, 'tokens')
local ts     = redis.call('HGET', key, 'ts')
if stored then tokens = tonumber(stored) end
if ts then last_refill = tonumber(ts) end

local elapsed = now - last_refill
if elapsed < 0 then elapsed = 0 end
tokens = tokens + elapsed * rate
if tokens > capacity then tokens = capacity end

local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = (cost - tokens) / rate
  if retry_after < 0 then retry_after = 0 end
  retry_after = math.ceil(retry_after)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, ttl_s * 1000)
return {allowed, tokens, retry_after}
"""


def _to_int(value) -> int:
    return int(value) if not isinstance(value, bytes) else int(value.decode())


def _to_float(value) -> float:
    if isinstance(value, bytes):
        value = value.decode()
    return float(value)


class RedisTokenBucket:
    """Strict token bucket whose state lives in Redis behind one Lua script."""

    def __init__(self, client: aioredis.Redis, *, ttl_seconds: int = 60) -> None:
        self._client = client
        self._ttl_seconds = ttl_seconds
        self._script = client.register_script(_TOKEN_BUCKET_LUA)

    async def consume(
        self, key: str, *, capacity: int, rate: float, cost: float = 1.0
    ) -> TokenBucketResult:
        now = time.time()
        try:
            result = await asyncio.wait_for(
                self._script(
                    keys=[key],
                    args=[
                        str(capacity),
                        str(rate),
                        str(cost),
                        repr(now),
                        str(self._ttl_seconds),
                    ],
                ),
                timeout=0.5,
            )
        except (RedisError, TimeoutError, ResponseError, OSError, asyncio.TimeoutError) as exc:
            logger.warning(
                "rate_limit_redis_unavailable",
                extra={"key": key, "error_type": type(exc).__name__},
            )
            raise RateLimiterUnavailableError(str(exc)) from exc

        allowed = _to_int(result[0])
        balance = _to_float(result[1])
        retry_after = _to_float(result[2])
        return TokenBucketResult(
            allowed=bool(allowed),
            balance=balance,
            retry_after_seconds=retry_after,
        )

    async def ping(self) -> bool:
        try:
            await asyncio.wait_for(self._client.ping(), timeout=0.5)
            return True
        except Exception:
            return False


# ---------------------------------------------------------------------------
# In-memory backend (degraded fallback + deterministic tests)
# ---------------------------------------------------------------------------


class InMemoryTokenBucket:
    """Process-local token bucket. Not cluster-wide — used only when Redis is
    down, so the API keeps rate limiting (per instance) instead of crashing."""

    def __init__(self) -> None:
        self._buckets: dict[str, tuple[float, float]] = {}
        self._lock = asyncio.Lock()

    async def consume(
        self, key: str, *, capacity: int, rate: float, cost: float = 1.0
    ) -> TokenBucketResult:
        now = time.time()
        async with self._lock:
            tokens, last_refill = self._buckets.get(key, (float(capacity), now))
            result = attempt(
                tokens,
                last_refill,
                now,
                capacity=capacity,
                rate=rate,
                cost=cost,
            )
            self._buckets[key] = (result.balance, now)
        return result

    async def ping(self) -> bool:
        return True

    # --- test/observability helpers -----------------------------------------

    async def clear(self) -> None:
        async with self._lock:
            self._buckets.clear()

    def size(self) -> int:
        return len(self._buckets)