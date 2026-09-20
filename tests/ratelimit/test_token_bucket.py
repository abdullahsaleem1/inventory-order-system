"""Unit tests for the from-scratch Token Bucket algorithm (Week 9)."""
import pytest

from src.core.ratelimit.backends import InMemoryTokenBucket
from src.core.ratelimit.token_bucket import attempt

T0 = 1_000_000.0  # arbitrary reference "now"


class TestAttemptPureMath:
    def test_fresh_bucket_allows_full_burst(self):
        balance = float(5)  # fresh bucket holds `capacity` tokens
        for i in range(5):
            r = attempt(balance, T0, T0, capacity=5, rate=1.0)
            assert r.allowed
            balance = r.balance
        r = attempt(balance, T0, T0, capacity=5, rate=1.0)
        assert not r.allowed

    def test_burst_is_capped_at_capacity(self):
        r = attempt(999.0, T0, T0, capacity=5, rate=100)  # absurd balance
        assert r.allowed
        # The bucket can never hold more than `capacity`
        assert r.balance <= 5.0

    def test_refill_accumulates_over_time(self):
        # Start with an empty bucket + rate 2/s:
        # (capacity 10 but only 0 tokens stored and no time has passed)
        empty = attempt(0.0, T0, T0, capacity=10, rate=2.0)
        assert not empty.allowed
        assert empty.retry_after_seconds == 1  # ceil((1-0)/2)

        # ...wait half a second => +1 token
        r = attempt(0.0, T0, T0 + 0.5, capacity=10, rate=2.0)
        assert r.allowed
        assert r.balance == 0.0

    def test_deny_reports_retry_after(self):
        r = attempt(0.5, T0, T0, capacity=10, rate=2.0)
        assert not r.allowed
        assert r.retry_after_seconds == pytest.approx(1.0)  # ceil((1-0.5)/2)

    def test_rate_zero_denies_with_infinity(self):
        r = attempt(0.0, T0, T0, capacity=2, rate=0)
        assert not r.allowed
        assert r.retry_after_seconds == float("inf")

    def test_negative_elapsed_is_clamped(self):
        # Clock moved backwards (e.g. NTP correction): no refill is granted;
        # the bucket only keeps whatever tokens it already held.
        r = attempt(1.0, T0 + 10.0, T0, capacity=5, rate=1.0)
        assert r.allowed
        assert r.balance == 0.0  # spent the single stored token, no refill

    def test_remaining_is_floor_of_balance(self):
        r = attempt(10.0, T0, T0 + 0.1, capacity=5, rate=1.0)  # balance 5.1 -> cap 5
        assert r.remaining == 4  # after spending 1 from a capped 5.0
        assert r.balance == 4.0


class TestInMemoryTokenBucket:
    @pytest.mark.asyncio
    async def test_burst_exactly_capacity_succeeds(self):
        bucket = InMemoryTokenBucket()
        results = [
            await bucket.consume("k1", capacity=5, rate=100.0) for _ in range(5)
        ]
        assert all(r.allowed for r in results)
        assert [r.remaining for r in results] == [4, 3, 2, 1, 0]

    @pytest.mark.asyncio
    async def test_burst_over_capacity_is_denied(self):
        bucket = InMemoryTokenBucket()
        results = [
            await bucket.consume("k1", capacity=3, rate=100.0) for _ in range(5)
        ]
        assert sum(r.allowed for r in results) == 3
        assert not results[3].allowed
        assert not results[4].allowed
        assert results[4].retry_after_seconds > 0

    @pytest.mark.asyncio
    async def test_concurrent_burst_never_exceeds_capacity(self):
        """The in-memory backend must NEVER overspend under concurrency: a
        burst of 200 concurrent requests against a capacity-10 bucket with a
        0 refill rate admits exactly 10 — no double-spend of a shared token."""
        bucket = InMemoryTokenBucket()

        async def fire(_: int):
            return await bucket.consume("burst", capacity=10, rate=0.0, cost=1)

        import asyncio

        results = await asyncio.gather(*(fire(i) for i in range(200)))
        allowed = sum(1 for r in results if r.allowed)
        assert allowed == 10

    @pytest.mark.asyncio
    async def test_distinct_keys_are_isolated(self):
        bucket = InMemoryTokenBucket()
        await bucket.consume("a", capacity=2, rate=1)
        await bucket.consume("a", capacity=2, rate=1)
        r = await bucket.consume("a", capacity=2, rate=1)
        assert not r.allowed
        r2 = await bucket.consume("b", capacity=2, rate=1)
        assert r2.allowed  # untouched sibling bucket

    @pytest.mark.asyncio
    async def test_refill_unblocks_denied_bucket(self):
        import asyncio

        bucket = InMemoryTokenBucket()
        for _ in range(2):
            await bucket.consume("k", capacity=2, rate=2.0)
        denied = await bucket.consume("k", capacity=2, rate=2.0)
        assert not denied.allowed
        await asyncio.sleep(0.6)  # refills >= 1 token
        regained = await bucket.consume("k", capacity=2, rate=2.0)
        assert regained.allowed