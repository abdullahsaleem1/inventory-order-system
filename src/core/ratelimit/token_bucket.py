"""
Token Bucket — implemented from scratch (Week 9).

A bucket holds up to ``capacity`` tokens. Tokens accrue continuously at
``rate`` tokens/second (capped at capacity); each request costs one token.
Allowed iff the bucket holds at least one token; when denied, ``retry_after``
is the time until a token is available again.

State is ``(tokens, last_refill_timestamp)``. ``attempt()`` is the pure,
framework-agnostic step shared by every backend so the math is tested once.
"""
import math
from dataclasses import dataclass


class RateLimiterUnavailableError(Exception):
    """The Redis-backed limiter cannot reach Redis. Callers should degrade."""


@dataclass(frozen=True)
class TokenBucketResult:
    allowed: bool
    balance: float  # exact token balance in the bucket after refill+spend
    retry_after_seconds: float  # 0.0 when allowed, else seconds until a token frees

    @property
    def remaining(self) -> int:
        return int(math.floor(self.balance))


def attempt(
    tokens: float,
    last_refill: float,
    now: float,
    *,
    capacity: int,
    rate: float,
    cost: float = 1.0,
) -> TokenBucketResult:
    """One pure token-bucket step (no I/O, no locks)."""
    elapsed = max(0.0, now - last_refill)
    balance = min(float(capacity), float(tokens) + elapsed * rate)
    if balance >= cost:
        return TokenBucketResult(
            allowed=True,
            balance=balance - cost,
            retry_after_seconds=0.0,
        )
    retry_after = math.ceil((cost - balance) / rate) if rate > 0 else math.inf
    return TokenBucketResult(
        allowed=False,
        balance=balance,
        retry_after_seconds=retry_after,
    )