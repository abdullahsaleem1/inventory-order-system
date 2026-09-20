"""Redis token-bucket backend tests (Week 9).

The Lua script itself is executed by Redis; these unit tests drive the
``RedisTokenBucket`` wrapper against a fake async Redis client that executes
the same ``attempt()`` math in Python (mirroring the Lua) and records the exact
KEYS/ARGV it is handed. This covers the wrapper's marshalling, result parsing
and error mapping without requiring a live Redis.
"""
import asyncio
import time

import pytest
from redis.exceptions import ConnectionError, ResponseError

from src.core.ratelimit.backends import RedisTokenBucket
from src.core.ratelimit.token_bucket import RateLimiterUnavailableError, TokenBucketResult, attempt


class FakeLuaExecutor:
    """Runs the token-bucket step in Python, mirroring _TOKEN_BUCKET_LUA.

    State persists across calls (like Redis) so drained buckets stay drained
    and refill over time, per (tokens, ts) stored in the fake.
    """

    def __init__(self):
        self._state: dict[str, tuple[float, float]] = {}
        self.calls: list[tuple[list, list]] = []

    async def __call__(self, *, keys: list, args: list):
        self.calls.append((list(keys), list(args)))
        key = keys[0]
        capacity = args[0]
        rate = args[1]
        cost = args[2]
        now = args[3]
        # ttl arg (args[4]) intentionally ignored by the fake.
        tokens, last_refill = self._state.get(key, (float(capacity), float(now)))
        result = attempt(tokens, last_refill, float(now), capacity=int(capacity), rate=float(rate), cost=float(cost))
        self._state[key] = (result.balance, float(now))
        # Redis returns the same [allowed, tokens, retry_after] triple.
        return [1 if result.allowed else 0, result.balance, result.retry_after_seconds]


class FakeAsyncRedis:
    def __init__(self):
        self.executor = FakeLuaExecutor()
        self.registered_scripts: list[str] = []
        self.closed = False

    def register_script(self, script: str):
        self.registered_scripts.append(script)
        redis = self

        class _FakeScript:
            async def __call__(self, *, keys: list, args: list):
                return await redis.executor(keys=keys, args=args)

        return _FakeScript()

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        self.closed = True


def test_fresh_key_starts_full():
    r = FakeAsyncRedis()
    bucket = RedisTokenBucket(r, ttl_seconds=60)
    result = asyncio.run(bucket.consume("user:1", capacity=5, rate=1.0))
    assert result.allowed
    assert result.remaining == 4


def test_consecutive_consumes_drain_until_denied():
    r = FakeAsyncRedis()
    bucket = RedisTokenBucket(r, ttl_seconds=60)

    async def run():
        return [await bucket.consume("u", capacity=3, rate=1.0) for _ in range(5)]

    results = asyncio.run(run())
    assert [res.allowed for res in results] == [True, True, True, False, False]


def test_pass_through_matches_raw_attempt(math_checked=True):
    """The key passes through and the returned triple equals the pure math."""
    r = FakeAsyncRedis()
    bucket = RedisTokenBucket(r, ttl_seconds=120)

    async def run():
        res = await bucket.consume("user:42", capacity=7, rate=2.5)
        return res, r.executor.calls

    result, calls = asyncio.run(run())
    (keys, args) = calls[0]
    assert keys == ["user:42"]
    assert args[0] == "7"
    assert args[1] == "2.5"
    assert args[2] == "1.0"  # cost
    float(args[3])  # now repr parses as float
    assert args[4] == "120"  # ttl seconds
    assert isinstance(result, TokenBucketResult)
    assert result.balance == 7.0 - 1.0  # first call: full bucket - cost


def test_wraps_redis_exception_as_unavailable():
    class _Flaky(FakeAsyncRedis):
        def register_script(self, script: str):
            class _Bad:
                async def __call__(self, *, keys, args):
                    raise ConnectionError("redis is down")

            return _Bad()

    bucket = RedisTokenBucket(_Flaky(), ttl_seconds=60)

    with pytest.raises(RateLimiterUnavailableError, match="redis is down"):
        asyncio.run(bucket.consume("u", capacity=5, rate=1.0))


def test_wraps_response_error_as_unavailable():
    class _BadResponse(FakeAsyncRedis):
        def register_script(self, script: str):
            class _Bad:
                async def __call__(self, *, keys, args):
                    raise ResponseError("wrong number of args")

            return _Bad()

    bucket = RedisTokenBucket(_BadResponse(), ttl_seconds=60)

    with pytest.raises(RateLimiterUnavailableError, match="wrong number of args"):
        asyncio.run(bucket.consume("u", capacity=5, rate=1.0))


def test_ping_success_and_close_surface():
    r = FakeAsyncRedis()
    bucket = RedisTokenBucket(r, ttl_seconds=60)

    async def run():
        assert await bucket.ping() is True
        await r.aclose()
        return r.closed

    assert asyncio.run(run()) is True


def test_ping_does_not_raise_when_redis_down():
    class _Dead(FakeAsyncRedis):
        async def ping(self) -> bool:
            raise ConnectionError("nope")

    async def run():
        return await RedisTokenBucket(_Dead(), ttl_seconds=60).ping()

    assert asyncio.run(run()) is False


def test_now_arg_is_monotonic_timestamp():
    r = FakeAsyncRedis()
    bucket = RedisTokenBucket(r, ttl_seconds=60)

    async def run():
        before = time.time()
        await bucket.consume("u", capacity=3, rate=1.0)
        after = time.time()
        _, args = r.executor.calls[0]
        return before <= float(args[3]) <= after

    assert asyncio.run(run())


def test_registers_the_lua_script_once():
    r = FakeAsyncRedis()
    RedisTokenBucket(r, ttl_seconds=60)
    # register_script was called exactly once with the Lua source
    assert len(r.registered_scripts) == 1
    assert r.registered_scripts[0].lstrip().startswith("local key")