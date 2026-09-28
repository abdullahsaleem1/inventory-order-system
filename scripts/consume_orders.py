"""
Event consumer entrypoint (Week 5 — Event-Driven Architecture).

Runs OUTSIDE the API process — this is the async side of order creation.
Each consumer group is its own durable queue bound to the
`inventory.orders.events` topic exchange:

    python -m scripts.consume_orders --group persistence
        Persists every `order.created` event into the orders write model
        (idempotent). Run several instances for competing consumers:
        docker compose up --scale consumer=3

    python -m scripts.consume_orders --group audit
        Second consumer group on the same routing key — logs every event,
        demonstrating exchange fan-out to multiple independent queues.

    python -m scripts.consume_orders --drain-dlq
        Inspection tool: prints and acknowledges every message currently in a
        group's dead-letter queue.

Graceful shutdown on SIGINT/SIGTERM: stops consuming, drains in-flight
messages, closes the broker connection.

Week 10 — tracing. `init_tracing("order-event-consumer")` installs the provider
before the database engine is imported, so the writes this consumer performs
appear inside the trace of the `POST /orders` request that triggered them — the
CONSUMER span picks up the `traceparent` from the message headers and the `db.*`
spans hang off it.

One service name covers both `--group persistence` and `--group audit`; the
group is recorded per span as `messaging.consumer.group.name` instead. A single
TracerProvider per process is not negotiable (the SQLAlchemy instrumentation
binds to whichever provider is current when the engine is created), so the
service is fixed at import time and relabelled per deployment via
`OTEL_SERVICE_NAME` if a distinct Jaeger service is wanted.
"""
import argparse
import asyncio
import signal

from src.core.config import get_settings
from src.core.logging_config import configure_logging, get_logger
from src.core.telemetry import init_tracing, shutdown_tracing
from src.contexts.orders.services.order_event_handler import PersistOrderCreatedHandler
from src.shared.infrastructure.database import AsyncSessionLocal
from src.shared.messaging.consumer import QueueGroupSpec, RabbitMQEventConsumer
from src.shared.messaging.events import DomainEvent

configure_logging()
init_tracing("order-event-consumer")
logger = get_logger("consumer.main")


class AuditLogHandler:
    """Second consumer group: records every event for observability."""

    def __init__(self) -> None:
        self._logger = get_logger("orders.audit_handler")

    async def handle(self, event: DomainEvent) -> None:
        self._logger.info(
            "event_audit",
            extra={
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "correlation_id": event.correlation_id,
                "occurred_at": event.occurred_at.isoformat(),
                "order_id": event.payload.get("order_id"),
                "total_cents": event.payload.get("total_cents"),
            },
        )


def build_groups() -> list[QueueGroupSpec]:
    settings = get_settings()
    return [
        QueueGroupSpec(
            name="persistence",
            queue_name=settings.ORDERS_PERSISTENCE_QUEUE,
            routing_keys=(settings.ORDER_CREATED_ROUTING_KEY,),
            handler=PersistOrderCreatedHandler(AsyncSessionLocal).handle,
        ),
        QueueGroupSpec(
            name="audit",
            queue_name=settings.ORDERS_AUDIT_QUEUE,
            routing_keys=(settings.ORDER_CREATED_ROUTING_KEY,),
            handler=AuditLogHandler().handle,
        ),
    ]


async def drain_dlq(group_name: str) -> int:
    """Log + ack everything sitting in a group's DLQ, then report the count."""
    import aio_pika

    settings = get_settings()
    spec = next(g for g in build_groups() if g.name == group_name)
    dlq_name = f"{spec.queue_name}.dlq"
    connection = await aio_pika.connect_robust(settings.BROKER_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue(dlq_name, durable=True)

    drained = 0
    while True:
        message = await queue.get(fail=False)
        if message is None:
            break
        try:
            event = DomainEvent.from_json(message.body)
            logger.warning(
                "dlq_message_drained",
                extra={
                    "queue": dlq_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "payload": event.payload,
                },
            )
        except Exception as exc:
            logger.warning("dlq_message_undecodable", extra={"queue": dlq_name, "error": str(exc)})
        await message.ack()
        drained += 1
    await connection.close()
    return drained


async def run_consumer(group_name: str | None) -> None:
    settings = get_settings()
    if not (settings.BROKER_URL or "").strip():
        logger.error("broker_url_missing", extra={"hint": "Set BROKER_URL before starting a consumer"})
        return

    groups = build_groups()
    if group_name is not None:
        groups = [g for g in groups if g.name == group_name]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows / proactor loop
            signal.signal(sig, lambda *_: stop.set())

    consumer = RabbitMQEventConsumer(
        settings.BROKER_URL,
        groups,
        prefetch_count=settings.CONSUMER_PREFETCH_COUNT,
        max_retries=settings.CONSUMER_MAX_RETRIES,
        backoff_base_seconds=settings.CONSUMER_BACKOFF_BASE_SECONDS,
        backoff_max_seconds=settings.CONSUMER_BACKOFF_MAX_SECONDS,
    )
    await consumer.start()
    logger.info(
        "consumer_running",
        extra={"groups": [g.name for g in groups], "broker": "rabbitmq"},
    )
    await stop.wait()
    await consumer.stop()


async def main() -> None:
    parser = argparse.ArgumentParser(description="RabbitMQ event consumer")
    parser.add_argument("--group", choices=["persistence", "audit"], help="consumer group to run (default: all)")
    parser.add_argument("--drain-dlq", metavar="GROUP", help="log+ack all messages in GROUP's dead-letter queue and exit")
    args = parser.parse_args()

    try:
        if args.drain_dlq:
            drained = await drain_dlq(args.drain_dlq)
            logger.info("dlq_drained", extra={"group": args.drain_dlq, "messages": drained})
            return

        await run_consumer(args.group)
    finally:
        shutdown_tracing()


if __name__ == "__main__":
    asyncio.run(main())
