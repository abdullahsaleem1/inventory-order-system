"""
Identity bounded context — domain layer.

User entity and roles. Password storage/verification is delegated to an
injected PasswordHasher so this file stays free of library dependencies.
"""
from dataclasses import dataclass
from enum import Enum

from src.shared.domain.base import Entity


class Role(str, Enum):
    ADMIN = "ADMIN"
    MANAGER = "MANAGER"
    STAFF = "STAFF"
    CUSTOMER = "CUSTOMER"


@dataclass(kw_only=True)
class User(Entity):
    email: str
    full_name: str
    hashed_password: str
    role: Role = Role.CUSTOMER
    is_active: bool = True

    def verify_password(self, plain_password: str, hasher) -> bool:
        """Check a plaintext password against the stored hash."""
        return hasher.verify_password(plain_password, self.hashed_password)

    def set_password(self, plain_password: str, hasher) -> None:
        """Replace the stored hash with a freshly salted hash of `plain_password`."""
        self.hashed_password = hasher.hash_password(plain_password)
