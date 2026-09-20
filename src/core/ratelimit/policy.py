"""
Rate-limit tiers (Week 9).

Role-based token-bucket configuration: ADMIN > MANAGER > STAFF > CUSTOMER >
anonymous. Higher-privilege roles get a larger burst (`capacity`) and a faster
refill (`rate`, requests per second).
"""
from dataclasses import dataclass

from src.contexts.identity.domain.user import Role


@dataclass(frozen=True)
class RateLimitTier:
    """Token-bucket parameters for one identity class."""

    capacity: int  # maximum burst of tokens (= requests)
    rate: float  # tokens (requests) refilled per second


# Default tiers — overridden by settings via `tiers_from_settings` below.
DEFAULT_TIERS: dict[str, RateLimitTier] = {
    "ANONYMOUS": RateLimitTier(capacity=5, rate=1.0),
    Role.CUSTOMER.value: RateLimitTier(capacity=10, rate=2.0),
    Role.STAFF.value: RateLimitTier(capacity=20, rate=5.0),
    Role.MANAGER.value: RateLimitTier(capacity=50, rate=10.0),
    Role.ADMIN.value: RateLimitTier(capacity=100, rate=20.0),
}

# Roles embedded in JWTs; anything else (missing/expired token) is anonymous.
ALL_ROLES = {role.value for role in Role}


def tiers_from_settings(settings) -> dict[str, RateLimitTier]:
    """Build the tier table from configuration values."""
    return {
        "ANONYMOUS": RateLimitTier(
            capacity=settings.RATE_LIMIT_ANON_CAPACITY,
            rate=settings.RATE_LIMIT_ANON_RATE,
        ),
        Role.CUSTOMER.value: RateLimitTier(
            capacity=settings.RATE_LIMIT_CUSTOMER_CAPACITY,
            rate=settings.RATE_LIMIT_CUSTOMER_RATE,
        ),
        Role.STAFF.value: RateLimitTier(
            capacity=settings.RATE_LIMIT_STAFF_CAPACITY,
            rate=settings.RATE_LIMIT_STAFF_RATE,
        ),
        Role.MANAGER.value: RateLimitTier(
            capacity=settings.RATE_LIMIT_MANAGER_CAPACITY,
            rate=settings.RATE_LIMIT_MANAGER_RATE,
        ),
        Role.ADMIN.value: RateLimitTier(
            capacity=settings.RATE_LIMIT_ADMIN_CAPACITY,
            rate=settings.RATE_LIMIT_ADMIN_RATE,
        ),
    }


class RateLimitPolicy:
    """Resolves a JWT role (or anonymity) to a token-bucket tier."""

    def __init__(self, tiers: dict[str, RateLimitTier] | None = None) -> None:
        self._tiers = dict(tiers or DEFAULT_TIERS)

    def tier(self, role: str | None) -> RateLimitTier:
        if role in ALL_ROLES:
            return self._tiers[role]
        return self._tiers["ANONYMOUS"]