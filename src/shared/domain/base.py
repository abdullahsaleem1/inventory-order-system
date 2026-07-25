"""
Shared domain-layer building blocks used across bounded contexts.
These are plain Python objects — no ORM, no framework dependency.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID, uuid4


@dataclass(kw_only=True)
class Entity:
    """Base class for domain entities. Identity-based equality."""
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Entity):
            return NotImplemented
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class DomainError(Exception):
    """Base class for all domain-level rule violations."""
    pass
