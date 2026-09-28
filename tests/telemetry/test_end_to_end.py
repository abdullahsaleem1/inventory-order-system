"""
Week 10 — the end-to-end trace across the broker, using the real classes.

Nothing here is mocked at the telemetry layer. The real
`RabbitMQEventPublisher.publish()` runs (only the aio-pika connection is
replaced), the W3C `traceparent` it writes into the AMQP headers is read back by
the real `RabbitMQEventConsumer` delivery path, and the resulting spans are
asserted for trace id and parent/child wiring.

This is the regression test for the single most important property of the
week: a request that crosses a message boundary still produces ONE trace, not
two disconnected ones.
"""
from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from opentelemetry.trace import SpanKind

from src.core.telemetry import get_tracer
from src.shared.messaging.consumer import (
    PermanentMessageError,
    QueueGroupSpec,
    RabbitMQEventConsumer,
)
from src.shared.messaging.events import DomainEvent
from src.shared.messaging.publisher import RabbitMQEventPublisher

QUEUE_NAME = "orders.order-created.inventory"
GROUP_NAME = "inventory"


# --- fakes for aio-pika, so no broker is needed -------------------------------


class FakeExchange:
    """Captures published `Message` objects instead of talking to RabbitMQ."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.published: list[Any] = []

    async def publish(self, message, routing_key: str, timeout: int | None = None) -> None:
        self.published.append((routing_key, message))


class FakeChannel:
    def __init__(self) -> None:
        self.default_exchange = self

    async def publish(self, message, routing_key: str, timeout: int | None = None) -> None:
        self.published.append((routing_key, message))  # type: ignore[attr-defined]

    published: list[Any] = []


class FakeMessage:
    """Minimal stand-in for `aio_pika.IncomingMessage`."""

    def __init__(self, body: bytes, headers: dict[str, Any] | None = None, **kwargs) -> None:
        self.body = body
        self.headers = headers or {}
        self.message_id = kwargs.get("message_id", str(uuid4()))
        self.correlation_id = kwargs.get("correlation_id", "corr-test")
        self.routing_key = kwargs.get("routing_key", "order.created")
        self.redelivered = kwargs.get("redelivered", False)
        self.acked = 0
        self.rejected: list[bool] = []

    async def ack(self) -> None:
        self.acked += 1

    async def reject(self, requeue: bool = False) -> None:
        self.rejected.append(requeue)


def _make_event(event_type: str = "order.created") -> DomainEvent:
    order_id = str(uuid4())
    return DomainEvent(
        event_type=event_type,
        correlation_id="corr-e2e",
        payload={
            "order_id": order_id,
            "customer_id": str(uuid4()),
            "status": "PENDING",
            "total_cents": 3000,
            "lines": [
                {
                    "product_id": str(uuid4()),
                    "quantity": 3,
                    "unit_price_cents": 1000,
                }
            ],
        },
    )


@pytest.fixture
def publisher():
    """A real publisher whose broker connection is short-circuited."""
    pub = RabbitMQEventPublisher("amqp://guest:guest@localhost:5672/")
    exchange = FakeExchange("inventory.orders.events")

    async def fake_ensure():
        return exchange

    pub._ensure_exchange = fake_ensure
    return pub, exchange


@pytest.fixture
def consumer():
    """A real consumer wired to one group, with the retry stairway faked."""
    def build(handler, *, max_retries: int = 3):
        spec = QueueGroupSpec(
            name=GROUP_NAME,
            queue_name=QUEUE_NAME,
            routing_keys=("order.created",),
            handler=handler,
        )
        con = RabbitMQEventConsumer(
            "amqp://guest:guest@localhost:5672/",
            [spec],
            max_retries=max_retries,
        )
        # Retry queue 1..max, as `_declare_topology` would have created them.
        for attempt in range(1, max_retries + 1):
            channel = FakeChannel()
            channel.published = []
            retry_queue = FakeChannel()
            retry_queue.name = f"{QUEUE_NAME}.retry.{attempt}"
            retry_queue.channel = channel
            spec.retry_queues[attempt] = retry_queue
        return con, spec

    return build


async def _publish_and_deliver(publisher, consumer, event, *, headers=None):
    """Publish through the real publisher, then deliver the real message bytes.

    `consumer` is a built `(RabbitMQEventConsumer, QueueGroupSpec)` pair.
    """
    pub, exchange = publisher
    con, spec = consumer

    await pub.publish(event)
    _, published = exchange.published[-1]
    delivered_headers = headers if headers is not None else dict(published.headers or {})
    message = FakeMessage(
        published.body,
        headers=delivered_headers,
        message_id=published.message_id,
        correlation_id=published.correlation_id,
    )
    await con._make_on_message(spec)(message)
    return message


# --- the headline test --------------------------------------------------------


class TestEndToEndTrace:
    async def test_one_trace_spans_api_producer_and_consumer(self, spans, publisher, consumer) -> None:
        """`POST /orders` -> publish -> consume must all be a single trace."""
        handled: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            # Stand in for the worker's database calls.
            with get_tracer().start_as_current_span("SELECT products", kind=SpanKind.CLIENT):
                handled.append(event)

        tracer = get_tracer()
        with tracer.start_as_current_span("POST /orders", kind=SpanKind.SERVER) as server:
            with tracer.start_as_current_span("command CreateOrder"):
                message = await _publish_and_deliver(publisher, consumer(handler), _make_event(), )

        assert len(handled) == 1, "the consumer handler must have run"

        producer = spans.first("publish")
        command = spans.first("command CreateOrder")
        consume_span = spans.first(QUEUE_NAME, "process")
        db_span = spans.first("SELECT products")

        # One trace id everywhere.
        assert len({spans.trace_id(server), spans.trace_id(command),
                    spans.trace_id(producer), spans.trace_id(consume_span),
                    spans.trace_id(db_span)}) == 1

        # Correct ancestry: server -> command -> producer -> consumer -> db.
        assert command.parent.span_id == server.context.span_id
        assert producer.parent.span_id == command.context.span_id
        assert consume_span.parent.span_id == producer.context.span_id
        assert db_span.parent.span_id == consume_span.context.span_id

        # And the message was acked on the happy path.
        assert message.acked == 1
        assert message.rejected == []

    async def test_span_kinds_follow_the_messaging_convention(self, spans, publisher, consumer) -> None:
        async def handler(event: DomainEvent) -> None:
            return None

        with get_tracer().start_as_current_span("POST /orders", kind=SpanKind.SERVER):
            await _publish_and_deliver(publisher, consumer(handler), _make_event())

        assert spans.first("publish").kind == SpanKind.PRODUCER
        assert spans.first(QUEUE_NAME, "process").kind == SpanKind.CONSUMER

    async def test_publish_stamps_traceparent_onto_the_message(self, spans, publisher, consumer) -> None:
        async def handler(event: DomainEvent) -> None:
            return None

        with get_tracer().start_as_current_span("POST /orders"):
            await _publish_and_deliver(publisher, consumer(handler), _make_event())

        # And the AMQP message really does carry it, as a str-valued header.
        pub, exchange = publisher
        _, message = exchange.published[-1]
        traceparent = message.headers["traceparent"]
        assert isinstance(traceparent, str)
        assert traceparent.split("-")[1] == spans.trace_id(spans.first("POST /orders"))

    async def test_business_attributes_survive_the_hop(self, spans, publisher, consumer) -> None:
        """The spans must be searchable by message id / event type, which is how
        you go from a Jaeger trace to the order it was about."""
        event = _make_event()

        async def handler(_event: DomainEvent) -> None:
            return None

        with get_tracer().start_as_current_span("POST /orders"):
            await _publish_and_deliver(publisher, consumer(handler), event)

        producer = spans.first("publish")
        consume_span = spans.first(QUEUE_NAME, "process")
        for span, label in ((producer, "producer"), (consume_span, "consumer")):
            assert span.attributes["messaging.system"] == "rabbitmq"
            assert span.attributes["messaging.message.id"] == str(event.event_id)
            assert span.attributes["event.type"] == "order.created", label

        assert consume_span.attributes["messaging.consumer.group.name"] == GROUP_NAME
        assert consume_span.attributes["messaging.destination.name"] == QUEUE_NAME
        assert consume_span.attributes["messaging.message.redelivered"] is False


# --- failure paths, which is where traces earn their keep ---------------------


class TestConsumerFailurePaths:
    async def test_transient_failure_retries_inside_the_same_trace(
        self, spans, publisher, consumer
    ) -> None:
        """A retry re-enters the queue after a TTL, so the second attempt is a
        sibling span hanging off the same producer — not a separate trace."""
        attempts: list[int] = []

        async def handler(_event: DomainEvent) -> None:
            attempts.append(1)
            if len(attempts) < 2:
                raise RuntimeError("insufficient stock")

        con, spec = consumer(handler)
        pub, exchange = publisher
        event = _make_event()

        with get_tracer().start_as_current_span("POST /orders"):
            await pub.publish(event)
            _, published = exchange.published[-1]
            first_delivery = FakeMessage(published.body, headers=dict(published.headers))
            await con._make_on_message(spec)(first_delivery)

            # The retry publish parked a copy in `.retry.1` with its own
            # traceparent; simulate the TTL expiring and the broker redelivering.
            retry_copy = spec.retry_queues[1].channel.published[-1][1]
            assert retry_copy.headers["x-retry-count"] == 1
            redelivered = FakeMessage(
                retry_copy.body,
                headers=dict(retry_copy.headers),
                redelivered=True,
                routing_key="order.created",
            )
            await con._make_on_message(spec)(redelivered)

        assert len(attempts) == 2
        trace_ids = {spans.trace_id(s) for s in spans.all}
        assert len(trace_ids) == 1, f"retry escaped the trace: {trace_ids}"

    async def test_retry_publish_creates_a_producer_child_span(
        self, spans, publisher, consumer
    ) -> None:
        async def handler(_event: DomainEvent) -> None:
            raise RuntimeError("downstream unavailable")

        with get_tracer().start_as_current_span("POST /orders"):
            await _publish_and_deliver(publisher, consumer(handler), _make_event())

        retry_publish = spans.first(".retry.1 publish")
        failed_attempt = spans.first(QUEUE_NAME, "process")

        assert retry_publish.kind == SpanKind.PRODUCER
        # The retry republish is a child of the attempt that failed, so the retry
        # chain hangs off the original trace instead of starting a new one.
        assert retry_publish.parent.span_id == failed_attempt.context.span_id
        assert spans.trace_id(retry_publish) == spans.trace_id(failed_attempt)
        assert retry_publish.attributes["messaging.retry.attempt"] == 1
        # The failing attempt is marked, and flagged as going to be retried.
        assert failed_attempt.status.status_code.name in {"ERROR", "UNSET"}
        assert failed_attempt.attributes["error_type"] == "RuntimeError"
        assert failed_attempt.attributes["will_retry"] is True
        # `dead_lettered` is only set on the terminal paths, so its absence here
        # is what says "this attempt was not the last one".
        assert "dead_lettered" not in failed_attempt.attributes

    async def test_exhausted_retries_dead_letter_and_mark_the_span(self, spans, publisher, consumer) -> None:
        async def handler(_event: DomainEvent) -> None:
            raise RuntimeError("always broken")

        # max_retries=3, and the message arrives already stamped as attempt 3,
        # so this delivery is the last one allowed.
        con, spec = consumer(handler, max_retries=3)
        pub, exchange = publisher
        event = _make_event()
        with get_tracer().start_as_current_span("POST /orders"):
            await pub.publish(event)
        _, published = exchange.published[-1]
        message = FakeMessage(
            published.body,
            headers={**dict(published.headers), "x-retry-count": 3},
            redelivered=True,
        )
        await con._make_on_message(spec)(message)

        span = spans.first(QUEUE_NAME, "process")
        assert span.attributes["retries_exhausted"] is True
        assert span.attributes["dead_lettered"] is True
        assert message.rejected == [False], "must be rejected without requeue -> DLQ"
        # One more attempt must not have been scheduled.
        assert spans.named(".retry.") == []

    async def test_permanent_error_skips_retries(self, spans, publisher, consumer) -> None:
        async def handler(_event: DomainEvent) -> None:
            raise PermanentMessageError("malformed payload")

        with get_tracer().start_as_current_span("POST /orders"):
            message = await _publish_and_deliver(
                publisher, consumer(handler), _make_event()
            )

        assert message.rejected == [False]
        span = spans.first(QUEUE_NAME, "process")
        assert span.attributes["dead_lettered"] is True
        assert span.attributes["error_type"] == "PermanentMessageError"
        # No retry publish span was created.
        assert spans.named(".retry.") == []

    async def test_undecodable_envelope_is_dead_lettered(self, spans, publisher, consumer) -> None:
        async def handler(_event: DomainEvent) -> None:
            raise AssertionError("must not be called")

        con, spec = consumer(handler)
        message = FakeMessage(
            b"{not json", headers={"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}
        )

        with get_tracer().start_as_current_span("POST /orders"):
            await con._make_on_message(spec)(message)

        assert message.rejected == [False]
        span = spans.first(QUEUE_NAME, "process")
        assert span.attributes["dead_lettered"] is True


# --- no-collector / legacy-message behaviour ----------------------------------


class TestUntracedMessages:
    async def test_message_without_traceparent_starts_its_own_trace(
        self, spans, publisher, consumer
    ) -> None:
        """A message published before instrumentation (or by another system) has
        no traceparent. The worker must still emit a span, as a new root — not
        crash, and not attach itself to whatever span happens to be active."""
        seen: list[bool] = []

        async def handler(_event: DomainEvent) -> None:
            from src.core.telemetry import current_trace_identifiers

            seen.append(bool(current_trace_identifiers().trace_id))

        con, spec = consumer(handler)
        message = FakeMessage(_make_event().to_json(), headers={"x-event-version": 1})

        with get_tracer().start_as_current_span("an-unrelated-trace"):
            await con._make_on_message(spec)(message)

        assert seen == [True], "the consumer must have its own active trace"
        span = spans.first(QUEUE_NAME, "process")
        assert span.parent is None, "must not adopt an unrelated in-process span"
        assert spans.trace_id(span) != spans.trace_id(spans.first("an-unrelated-trace"))

    async def test_corrupt_traceparent_does_not_break_processing(
        self, spans, publisher, consumer
    ) -> None:
        handled: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            handled.append(event)

        con, spec = consumer(handler)
        message = FakeMessage(
            _make_event().to_json(), headers={"traceparent": "totally-corrupt"}
        )
        await con._make_on_message(spec)(message)

        assert len(handled) == 1, "a bad header must not stop message processing"
        assert message.acked == 1
