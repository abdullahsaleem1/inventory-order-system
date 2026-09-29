"""Consumer resilience tests (Week 11 chaos engineering).

Covers the failure-handling behaviour that fault injection exercised but the
Week 6-10 suites did not pin:

* the retry stairway dead-letters each event back under **its own** routing key
  (a single shared stairway fanned retried `order.status.changed` events out to
  the persistence/audit/inventory groups);
* an unretriable payload is dead-lettered immediately, burning no retries;
* a transient failure is retried and then dead-lettered once the budget is spent;
* the retry budget and exponential backoff are what the report claims.
"""
import pytest

from src.shared.messaging.consumer import (
    PermanentMessageError,
    QueueGroupSpec,
    RabbitMQEventConsumer,
)
from src.shared.messaging.events import DomainEvent, EventTypes

BROKER = "amqp://guest:guest@localhost:5672/"


class _Channel:
    """Stands in for a queue's channel and its default exchange."""

    def __init__(self, name: str = "chan") -> None:
        self.default_exchange = self
        self.name = name
        self.published: list[tuple[str, object]] = []

    async def publish(self, message, routing_key: str, timeout: int | None = None) -> None:
        self.published.append((routing_key, message))

    def queue(self, name: str) -> "_Queue":
        return _Queue(name, self)


class _Queue:
    def __init__(self, name: str, channel: _Channel) -> None:
        self.name = name
        self.channel = channel


class _Message:
    """Minimal stand-in for aio_pika.IncomingMessage."""

    def __init__(self, body: bytes, *, routing_key: str, headers: dict | None = None) -> None:
        self.body = body
        self.routing_key = routing_key
        self.headers = headers or {}
        self.message_id = "msg-1"
        self.correlation_id = "corr-1"
        self.redelivered = False
        self.acked = 0
        self.rejected: list[bool] = []

    async def ack(self) -> None:
        self.acked += 1

    async def reject(self, requeue: bool = True) -> None:
        self.rejected.append(requeue)


def _body(event_type: str = EventTypes.ORDER_CREATED) -> bytes:
    return DomainEvent(
        event_type=event_type, payload={"order_id": "o-1"}, correlation_id="corr-1"
    ).to_json()


def _spec(*routing_keys: str) -> QueueGroupSpec:
    async def _noop(_event) -> None:
        return None

    return QueueGroupSpec(
        name="read",
        queue_name="orders.order-created.read",
        routing_keys=tuple(routing_keys),
        handler=_noop,
    )


def _consumer(spec: QueueGroupSpec, *, max_retries: int = 3, backoff_base: float = 0.0):
    con = RabbitMQEventConsumer(
        BROKER, [spec], max_retries=max_retries,
        backoff_base_seconds=backoff_base, backoff_max_seconds=60.0,
    )
    # Build the retry stairway exactly as _declare_topology does.
    for routing_key in spec.routing_keys:
        for attempt in range(1, max_retries + 1):
            spec.retry_queues[(routing_key, attempt)] = _Channel().queue(
                f"{spec.queue_name}.retry.{routing_key}.{attempt}"
            )
    return con


def _deliver(con, spec, routing_key: str, *, headers: dict | None = None) -> _Message:
    message = _Message(_body(), routing_key=routing_key, headers=headers)
    return message


async def _run(con, spec, message: _Message) -> _Message:
    await con._make_on_message(spec)(message)
    return message


class TestRetryRoutingKeyIsPreserved:
    """The fan-out defect: a shared stairway re-entered the exchange under
    `routing_keys[0]`, so a retried order.status.changed arrived as
    order.created and hit every group bound to it."""

    async def test_status_retry_uses_its_own_stairway(self):
        spec = _spec(EventTypes.ORDER_CREATED, EventTypes.ORDER_STATUS_CHANGED)
        con = _consumer(spec)

        async def handler(_e):
            raise RuntimeError("read store down")

        spec.handler = handler
        message = await _run(
            con, spec, _deliver(con, spec, EventTypes.ORDER_STATUS_CHANGED)
        )

        assert len(spec.retry_queues[(EventTypes.ORDER_STATUS_CHANGED, 1)].channel.published) == 1
        assert spec.retry_queues[(EventTypes.ORDER_CREATED, 1)].channel.published == [], (
            "a retried order.status.changed must never be parked on the "
            "order.created stairway, or it re-enters the exchange as "
            "order.created and fans out to the persistence, audit and "
            "inventory groups"
        )
        assert message.acked == 1, "the original must be acked once the retry is parked"

    async def test_created_retry_uses_created_stairway(self):
        spec = _spec(EventTypes.ORDER_CREATED, EventTypes.ORDER_STATUS_CHANGED)
        con = _consumer(spec)

        async def handler(_e):
            raise RuntimeError("boom")

        spec.handler = handler
        await _run(con, spec, _deliver(con, spec, EventTypes.ORDER_CREATED))

        assert len(spec.retry_queues[(EventTypes.ORDER_CREATED, 1)].channel.published) == 1
        assert spec.retry_queues[(EventTypes.ORDER_STATUS_CHANGED, 1)].channel.published == []

    async def test_each_attempt_uses_its_own_level(self):
        spec = _spec(EventTypes.ORDER_STATUS_CHANGED)
        con = _consumer(spec)

        async def handler(_e):
            raise RuntimeError("boom")

        spec.handler = handler
        for attempt in range(3):
            await _run(
                con, spec,
                _deliver(con, spec, EventTypes.ORDER_STATUS_CHANGED,
                         headers={"x-retry-count": attempt}),
            )
        for attempt in (1, 2, 3):
            assert len(
                spec.retry_queues[(EventTypes.ORDER_STATUS_CHANGED, attempt)].channel.published
            ) == 1

    def test_retry_queue_names_encode_the_routing_key(self):
        """Queue names are operator-visible in the RabbitMQ UI and in the DLQ
        drain tooling, so the routing key must be legible in them."""
        spec = _spec(EventTypes.ORDER_CREATED, EventTypes.ORDER_STATUS_CHANGED)
        _consumer(spec)
        names = {key: q.name for key, q in spec.retry_queues.items()}
        assert names[(EventTypes.ORDER_STATUS_CHANGED, 1)] == (
            "orders.order-created.read.retry.order.status.changed.1"
        )
        assert names[(EventTypes.ORDER_CREATED, 2)] == (
            "orders.order-created.read.retry.order.created.2"
        )

    async def test_undeclared_routing_key_is_rejected_not_guessed(self):
        """A message under a binding the group never declared must not be
        parked on an arbitrary stairway."""
        spec = _spec(EventTypes.ORDER_CREATED)
        con = _consumer(spec)

        async def handler(_e):
            raise RuntimeError("boom")

        spec.handler = handler
        message = await _run(con, spec, _deliver(con, spec, "order.somethingElse"))

        assert message.rejected == [False], "must be dead-lettered, not retried"
        assert message.acked == 0
        assert all(q.channel.published == [] for q in spec.retry_queues.values())


class TestDeadLettering:
    async def test_permanent_failure_skips_retries_entirely(self):
        spec = _spec(EventTypes.ORDER_CREATED)
        con = _consumer(spec)

        async def handler(_e):
            raise PermanentMessageError("unknown product")

        spec.handler = handler
        message = await _run(con, spec, _deliver(con, spec, EventTypes.ORDER_CREATED))

        assert message.rejected == [False], "a permanent error must not be retried"
        assert message.acked == 0
        assert all(q.channel.published == [] for q in spec.retry_queues.values())

    async def test_transient_failure_is_dead_lettered_once_budget_is_spent(self):
        spec = _spec(EventTypes.ORDER_CREATED)
        con = _consumer(spec, max_retries=3)

        async def handler(_e):
            raise RuntimeError("still down")

        spec.handler = handler
        message = await _run(
            con, spec,
            _deliver(con, spec, EventTypes.ORDER_CREATED, headers={"x-retry-count": 3}),
        )

        assert message.rejected == [False]
        assert message.acked == 0
        assert all(q.channel.published == [] for q in spec.retry_queues.values())

    async def test_transient_failure_below_budget_is_retried(self):
        spec = _spec(EventTypes.ORDER_CREATED)
        con = _consumer(spec, max_retries=3)

        async def handler(_e):
            raise RuntimeError("still down")

        spec.handler = handler
        message = await _run(
            con, spec,
            _deliver(con, spec, EventTypes.ORDER_CREATED, headers={"x-retry-count": 2}),
        )

        assert message.acked == 1
        assert message.rejected == []
        assert len(spec.retry_queues[(EventTypes.ORDER_CREATED, 3)].channel.published) == 1

    async def test_undecodable_body_goes_straight_to_the_dlq(self):
        spec = _spec(EventTypes.ORDER_CREATED)
        con = _consumer(spec)

        message = _Message(b"not json at all", routing_key=EventTypes.ORDER_CREATED)
        await _run(con, spec, message)

        assert message.rejected == [False]
        assert message.acked == 0
        assert all(q.channel.published == [] for q in spec.retry_queues.values())

    async def test_successful_handler_is_acked_and_not_retried(self):
        spec = _spec(EventTypes.ORDER_CREATED)
        con = _consumer(spec)
        seen: list[str] = []

        async def handler(event):
            seen.append(event.event_type)

        spec.handler = handler
        message = await _run(con, spec, _deliver(con, spec, EventTypes.ORDER_CREATED))

        assert seen == [EventTypes.ORDER_CREATED]
        assert message.acked == 1
        assert message.rejected == []
        assert all(q.channel.published == [] for q in spec.retry_queues.values())


class TestBackoff:
    def test_backoff_is_exponential_and_capped(self):
        con = RabbitMQEventConsumer(
            BROKER, [], max_retries=3,
            backoff_base_seconds=1.0, backoff_max_seconds=60.0,
        )
        # The schedule the resilience report documents: 1s, 2s, 4s.
        assert con._backoff_seconds(1) == 1.0
        assert con._backoff_seconds(2) == 2.0
        assert con._backoff_seconds(3) == 4.0
        assert con._backoff_seconds(20) == 60.0, "must clamp to backoff_max"

    async def test_retry_message_carries_the_delay_and_attempt(self):
        spec = _spec(EventTypes.ORDER_CREATED)
        con = _consumer(spec, backoff_base=1.0)

        async def handler(_e):
            raise RuntimeError("boom")

        spec.handler = handler
        await _run(con, spec, _deliver(con, spec, EventTypes.ORDER_CREATED))

        _routing_key, retry = spec.retry_queues[(EventTypes.ORDER_CREATED, 1)].channel.published[0]
        assert retry.headers["x-retry-count"] == 1
        assert retry.expiration == "1000", "attempt 1 must be parked for 1s"


@pytest.mark.parametrize("max_retries", [1, 3, 5])
def test_stairway_is_built_per_routing_key_and_attempt(max_retries):
    spec = _spec(EventTypes.ORDER_CREATED, EventTypes.ORDER_STATUS_CHANGED)
    _consumer(spec, max_retries=max_retries)
    assert len(spec.retry_queues) == 2 * max_retries
