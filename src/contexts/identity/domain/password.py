"""
Identity bounded context — domain layer.
Password hashing interface. The domain layer depends only on this protocol;
the concrete bcrypt implementation lives in infrastructure and is injected
at runtime, keeping the domain pure and framework-agnostic.
"""
from typing import Protocol


class PasswordHasher(Protocol):
    """Interface for one-way password hashing / verification.

    Implementations must use a per-password random salt (bcrypt embeds the
    salt in the resulting hash string, so no separate salt column is needed).
    """

    def hash_password(self, plain: str) -> str:
        """Return a salted, one-way hash of `plain`."""
        ...

    def verify_password(self, plain: str, hashed: str) -> bool:
        """Return True if `plain` matches the stored `hashed` value."""
        ...
