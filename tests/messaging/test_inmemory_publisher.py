"""
Tests for the InMemoryEventPublisher test double used across the suite:
recording, ordered inline delivery to subscribers, and failure semantics
that mirror a real broker outage.
"""
import pytest

from src.shared.messaging.events import DomainEvent
from src.shared.messaging.publisher import InMemoryEventPublisher


async def test_publish_records_every_event_in_order() -> None:
    publisher = InMemoryEventPublisher()
    events = [DomainEvent(event_type="order.created", payload={"n": i}) for i in range(3)]

    for event in events:
        await publisher.publish(event)

    assert [e.payload["n"] for e in publisher.published] == [0, 1, 2]


async def test_subscribers_receive_events_inline() -> None:
    publisher = InMemoryEventPublisher()
    received: list[str] = []

    async def subscriber_a(event: DomainEvent) -> None:
        received.append(f"a:{event.payload['n']}")

    async def subscriber_b(event: DomainEvent) -> None:
        received.append(f"b:{event.payload['n']}")

    publisher.subscribers.extend([subscriber_a, subscriber_b])

    await publisher.publish(DomainEvent(event_type="order.created", payload={"n": 1}))

    assert received == ["a:1", "b:1"]


async def test_subscriber_failure_propagates_like_a_broker_outage() -> None:
    """The API maps EventPublishError to 503; inline delivery must therefore
    surface subscriber crashes to the publish() caller."""
    publisher = InMemoryEventPublisher()

    async def broken(_event: DomainEvent) -> None:
        raise RuntimeError("simulated broker outage")

    publisher.subscribers.append(broken)

    with pytest.raises(RuntimeError):
        await publisher.publish(DomainEvent(event_type="order.created", payload={}))

    # The event was recorded before delivery failed (same as a broker that
    # accepted then dropped) — consumers must be idempotent regardless.
    assert len(publisher.published) == 1


def test_clear_resets_the_recording() -> None:
    import asyncio

    publisher = InMemoryEventPublisher()

    async def run():
        await publisher.publish(DomainEvent(event_type="t", payload={}))
        publisher.clear()

    asyncio.run(run())
    assert publisher.published == []
