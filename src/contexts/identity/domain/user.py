"""
Identity bounded context — domain layer.

This context is intentionally minimal for Week 2: it exists only so we have
a real User entity/table to seed realistic role-based test data with. Full
auth behavior (password verification, token issuance, RBAC permission
checks) is added in the OAuth2/JWT week — this file will grow then, not
get replaced.
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