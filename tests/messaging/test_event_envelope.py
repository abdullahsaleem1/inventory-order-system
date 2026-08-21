"""
Unit/integration tests for the shared event envelope (DomainEvent):
serialization round-trips, defaults, and strict decode validation.
"""
from datetime import datetime, timezone
from uuid import UUID

import pytest

from src.shared.messaging.events import DomainEvent, MessageDecodeError


def _sample_event() -> DomainEvent:
    return DomainEvent(
        event_type="order.created",
        correlation_id="req-123",
        payload={"order_id": "00000000-0000-0000-0000-00000000abcd", "total_cents": 4200},
    )


def test_round_trip_preserves_every_envelope_field() -> None:
    event = _sample_event()
    decoded = DomainEvent.from_json(event.to_json())

    assert decoded.event_id == event.event_id
    assert decoded.event_type == event.event_type
    assert decoded.correlation_id == event.correlation_id
    assert decoded.payload == event.payload
    assert decoded.occurred_at == event.occurred_at


def test_event_defaults_are_unique_and_utc() -> None:
    a, b = DomainEvent(event_type="t", payload={}), DomainEvent(event_type="t", payload={})

    assert a.event_id != b.event_id  # UUIDv4 per occurrence
    assert isinstance(a.event_id, UUID)
    assert a.occurred_at.tzinfo is not None
    assert a.occurred_at.utcoffset() is not None
    now = datetime.now(timezone.utc)
    assert abs((now - a.occurred_at).total_seconds()) < 5


@pytest.mark.parametrize(
    "raw",
    [
        b"not json at all",
        b'["a", "json", "array"]',  # must be an object
        b'{"event_type": "order.created"}',  # missing keys
        b'{"event_id": "nope", "event_type": "t", "occurred_at": "2026-01-01T00:00:00+00:00", "payload": {}}',
        b'{"event_id": "00000000-0000-0000-0000-000000000001", "event_type": "t", '
        b'"occurred_at": "not-a-date", "payload": {}}',
        b'{"event_id": "00000000-0000-0000-0000-000000000001", "event_type": "t", '
        b'"occurred_at": "2026-01-01T00:00:00+00:00", "payload": [1, 2]}',
    ],
)
def test_decode_rejects_malformed_messages(raw: bytes) -> None:
    with pytest.raises(MessageDecodeError):
        DomainEvent.from_json(raw)


def test_naive_timestamp_is_normalized_to_utc() -> None:
    raw = (
        b'{"event_id": "00000000-0000-0000-0000-000000000001", "event_type": "t",'
        b' "occurred_at": "2026-01-01T12:00:00", "payload": {}}'
    )
    decoded = DomainEvent.from_json(raw)
    assert decoded.occurred_at.tzinfo is not None
