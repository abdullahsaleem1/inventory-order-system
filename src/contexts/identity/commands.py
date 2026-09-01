"""
Identity bounded context — CQRS write side (commands, Week 7).

Each command is a frozen dataclass describing an intention to change auth
state: register a user, log in (issue tokens — it mutates the refresh-token
store), refresh a token pair, or revoke sessions.
"""
from dataclasses import dataclass, field
from uuid import UUID

from src.contexts.identity.domain.user import Role
from src.shared.cqrs import Command


@dataclass(frozen=True)
class RegisterUserCommand(Command):
    email: str
    full_name: str
    password: str
    role: Role = Role.CUSTOMER


@dataclass(frozen=True)
class LoginCommand(Command):
    email: str
    password: str


@dataclass(frozen=True)
class RefreshTokensCommand(Command):
    refresh_token: str


@dataclass(frozen=True)
class LogoutCommand(Command):
    access_token: str | None = None
    refresh_token: str | None = None


@dataclass(frozen=True)
class LogoutAllCommand(Command):
    user_id: UUID | None = None