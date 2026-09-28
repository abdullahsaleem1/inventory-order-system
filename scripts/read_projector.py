"""
Read-projector sync worker (Week 8 — CQRS Read Phase).

Runs in its OWN process/container, separate from the API. It consumes order
events from the broker and **updates the dedicated read store** (Elasticsearch
in docker-compose), achieving **eventual consistency** between the write model
and the read store that serves all order queries:

    python -m scripts.read_projector

It subscribes to the `orders.order-created.read` consumer group, bound to BOTH:

  * `order.created`       -> full document upsert (initial read-store view),
  * `order.status.changed`-> in-place status update (confirm/cancel on the
    write side keep the read store eventually current).

Because delivery is at-least-once, the projector is idempotent: re-delivered
events simply re-upsert the same document / re-write the same status. The
shared RabbitMQEventConsumer provides durable queues, manual acks,
exponential-backoff retries and dead-lettering (poison/retry-exhausted
messages -> `orders.order-created.read.dlq`).

Graceful shutdown on SIGINT/SIGTERM: stops consuming, drains in-flight
messages, closes the broker connection.

Week 10 — tracing. `init_tracing("read-projector")` makes this worker's
Elasticsearch upserts visible inside the trace of the request that caused them.
The CONSUMER span re-parents onto the `traceparent` carried in the message
headers, and each `elasticsearch index` CLIENT span hangs off that — so a slow
read-store write is immediately attributable to the `POST /orders` (or
`POST /orders/{id}/confirm`) that triggered it.
"""
import argparse
import asyncio
import signal

from src.core.config import get_settings
from src.core.logging_config import configure_logging, get_logger
from src.core.telemetry import init_tracing, instrument_read_store, shutdown_tracing
from src.contexts.orders.services.read_model_projector import ProjectOrderToReadStoreHandler
from src.shared.messaging.consumer import QueueGroupSpec, RabbitMQEventConsumer
from src.shared.messaging.events import DomainEvent
from src.shared.readstore import build_read_store

configure_logging()
init_tracing("read-projector")
logger = get_logger("read_projector.main")


class DlvInspectionHandler:
    """Inspection tool: log + ack one message from the projector's DLQ."""

    async def handle(self, event: DomainEvent) -> None:
        logger.warning(
            "read_projector_dlq_message",
            extra={"event_id": str(event.event_id), "event_type": event.event_type},
        )


def build_projector_group() -> QueueGroupSpec:
    settings = get_settings()
    # Client spans for the read store are hand-written; this is the marker that
    # says so in the logs alongside the real auto-instrumented layers.
    instrument_read_store()
    read_store = build_read_store()
    return QueueGroupSpec(
        name="read",
        queue_name=settings.ORDERS_READ_QUEUE,
        # One work queue, two event types: creates upsert the document,
        # status changes update just the status field of that document.
        routing_keys=(
            settings.ORDER_CREATED_ROUTING_KEY,
            settings.ORDER_STATUS_CHANGED_ROUTING_KEY,
        ),
        handler=ProjectOrderToReadStoreHandler(read_store).handle,
    )


async def drain_dlq() -> int:
    """Log + ack everything sitting in the projector's DLQ, then report the count."""
    import aio_pika

    settings = get_settings()
    spec = build_projector_group()
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
                "read_projector_dlq_drained",
                extra={
                    "queue": dlq_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "payload": event.payload,
                },
            )
        except Exception as exc:
            logger.warning(
                "read_projector_dlq_undecodable", extra={"queue": dlq_name, "error": str(exc)}
            )
        await message.ack()
        drained += 1
    await connection.close()
    return drained


async def run_projector() -> None:
    settings = get_settings()
    if not (settings.BROKER_URL or "").strip():
        logger.error("broker_url_missing", extra={"hint": "Set BROKER_URL before starting the read projector"})
        return

    group = build_projector_group()

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows / proactor loop
            signal.signal(sig, lambda *_: stop.set())

    consumer = RabbitMQEventConsumer(
        settings.BROKER_URL,
        [group],
        prefetch_count=settings.CONSUMER_PREFETCH_COUNT,
        max_retries=settings.CONSUMER_MAX_RETRIES,
        backoff_base_seconds=settings.CONSUMER_BACKOFF_BASE_SECONDS,
        backoff_max_seconds=settings.CONSUMER_BACKOFF_MAX_SECONDS,
    )
    await consumer.start()
    logger.info(
        "read_projector_running",
        extra={
            "group": group.name,
            "queue": group.queue_name,
            "routing_keys": list(group.routing_keys),
            "read_store_type": settings.READ_STORE_TYPE,
            "read_store_url": settings.READ_STORE_URL,
            "broker": "rabbitmq",
        },
    )
    await stop.wait()
    await consumer.stop()
    logger.info("read_projector_stopped", extra={"group": group.name, "queue": group.queue_name})


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="CQRS read-projector sync worker (updates the read store from events)"
    )
    parser.add_argument("--drain-dlq", action="store_true", help="log+ack all messages in the read projector DLQ and exit")
    args = parser.parse_args()

    try:
        if args.drain_dlq:
            drained = await drain_dlq()
            logger.info("read_projector_dlq_drained", extra={"messages": drained})
            return

        await run_projector()
    finally:
        shutdown_tracing()


if __name__ == "__main__":
    asyncio.run(main())