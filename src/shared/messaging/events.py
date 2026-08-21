"""
Event envelope shared by every publisher and consumer.

The wire format is a single JSON document (inspired by CloudEvents) so any
language can produce/consume it:

    {
      "event_id":      "b3c1... (UUIDv4, unique per event occurrence)",
      "event_type":    "order.created",
      "occurred_at":   "2026-08-20T12:00:00.000000+00:00",
      "correlation_id":"<usually the HTTP X-Request-ID that caused the event>",
      "payload":       { ... event-specific data ... }
    }

Consumers never trust payload contents — malformed payloads raise
MessageDecodeError / PermanentMessageError which route the message to the DLQ
instead of crashing the consumer loop.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import json


class EventTypes:
    """Registry of all event types in the system (one per use case)."""

    ORDER_CREATED = "order.created"


class MessageDecodeError(Exception):
    """Raised when a raw broker message cannot be parsed into a DomainEvent."""


@dataclass(kw_only=True)
class DomainEvent:
    event_type: str
    payload: dict[str, Any]
    event_id: UUID = field(default_factory=uuid4)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: str | None = None

    def to_json(self) -> bytes:
        doc = {
            "event_id": str(self.event_id),
            "event_type": self.event_type,
            "occurred_at": self.occurred_at.isoformat(),
            "correlation_id": self.correlation_id,
            "payload": self.payload,
        }
        return json.dumps(doc, default=str).encode("utf-8")

    @classmethod
    def from_json(cls, raw: bytes | str) -> "DomainEvent":
        try:
            doc = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise MessageDecodeError(f"body is not valid JSON: {exc}") from exc
        if not isinstance(doc, dict):
            raise MessageDecodeError("event body must be a JSON object")

        missing = [key for key in ("event_id", "event_type", "occurred_at", "payload") if key not in doc]
        if missing:
            raise MessageDecodeError(f"event envelope missing required keys: {missing}")

        try:
            event_id = UUID(str(doc["event_id"]))
            occurred_at = datetime.fromisoformat(str(doc["occurred_at"]))
        except ValueError as exc:
            raise MessageDecodeError(f"invalid envelope field: {exc}") from exc

        if not isinstance(doc["payload"], dict):
            raise MessageDecodeError("event payload must be a JSON object")
        if not occurred_at.tzinfo:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)

        return cls(
            event_type=str(doc["event_type"]),
            payload=doc["payload"],
            event_id=event_id,
            occurred_at=occurred_at,
            correlation_id=doc.get("correlation_id"),
        )
