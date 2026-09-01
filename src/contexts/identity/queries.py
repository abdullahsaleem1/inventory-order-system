"""
Identity bounded context — CQRS read side (queries, Week 7).

Queries are frozen dataclasses describing a read: resolve the current user from
a token, or look a user up for the shared auth dependency. Handlers
(query_handlers.py) answer them.
"""
from dataclasses import dataclass
from uuid import UUID

from src.shared.cqrs import Query


@dataclass(frozen=True)
class GetCurrentUserQuery(Query):
    """Resolve the authenticated user from a Bearer access token."""
    token: str


@dataclass(frozen=True)
class GetUserQuery(Query):
    user_id: UUID