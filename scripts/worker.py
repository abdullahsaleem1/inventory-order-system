"""
Async worker service (Week 6 — Async Processing, Phase 2).

Runs INSIDE its own process/container, separate from the API. It subscribes to
the `orders.order-created.inventory` consumer group and asynchronously deducts
stock for every `order.created` event:

    python -m scripts.worker

The worker reuses the shared RabbitMQEventConsumer, which provides:
  * durable queues + manual acks,
  * **exponential backoff** retries (per-attempt TTL-delayed retry queues),
  * dead-lettering to `orders.order-created.inventory.dlq` for poison /
    retry-exhausted messages.

Graceful shutdown on SIGINT/SIGTERM: stops consuming, drains in-flight
messages, closes the broker connection.

Week 10 — tracing. The worker calls `init_tracing("inventory-worker")` before
importing anything that talks to the database, so the stock deduction it
performs shows up inside the same trace as the API request that created the
order: the CONSUMER span re-parents itself onto the `traceparent` carried in
the message headers, and every SELECT/UPDATE it issues hangs off that.
"""
import argparse
import asyncio
import signal

from src.core.config import get_settings
from src.core.logging_config import configure_logging, get_logger
from src.core.telemetry import init_tracing, shutdown_tracing
from src.contexts.inventory.services.order_event_handler import DeductInventoryHandler
from src.shared.infrastructure.database import AsyncSessionLocal
from src.shared.messaging.consumer import QueueGroupSpec, RabbitMQEventConsumer
from src.shared.messaging.events import DomainEvent

configure_logging()

# Before the database engine is imported: `database.py` instruments its engine
# at import time and must bind to the provider this call installs.
init_tracing("inventory-worker")

logger = get_logger("worker.main")

# Matches OTEL_SERVICE_NAME in docker-compose.yml; Jaeger groups by this.
SERVICE_NAME = "inventory-worker"


class RequeueDlqHandler:
    """Inspection tool: log + ack one message from the worker's DLQ."""

    async def handle(self, event: DomainEvent) -> None:
        logger.warning(
            "worker_dlq_message",
            extra={"event_id": str(event.event_id), "event_type": event.event_type},
        )


def build_worker_group() -> QueueGroupSpec:
    settings = get_settings()
    return QueueGroupSpec(
        name="inventory",
        queue_name=settings.ORDERS_INVENTORY_QUEUE,
        routing_keys=(settings.ORDER_CREATED_ROUTING_KEY,),
        handler=DeductInventoryHandler(AsyncSessionLocal).handle,
    )


async def drain_dlq() -> int:
    """Log + ack everything sitting in the worker's DLQ, then report the count."""
    import aio_pika

    settings = get_settings()
    spec = build_worker_group()
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
                "worker_dlq_drained",
                extra={
                    "queue": dlq_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "payload": event.payload,
                },
            )
        except Exception as exc:
            logger.warning("worker_dlq_undecodable", extra={"queue": dlq_name, "error": str(exc)})
        await message.ack()
        drained += 1
    await connection.close()
    return drained


async def run_worker() -> None:
    settings = get_settings()
    if not (settings.BROKER_URL or "").strip():
        logger.error("broker_url_missing", extra={"hint": "Set BROKER_URL before starting the worker"})
        return

    group = build_worker_group()

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
        "worker_running",
        extra={
            "group": group.name,
            "queue": group.queue_name,
            "broker": "rabbitmq",
            "max_retries": settings.CONSUMER_MAX_RETRIES,
            "backoff_base_seconds": settings.CONSUMER_BACKOFF_BASE_SECONDS,
        },
    )
    await stop.wait()
    await consumer.stop()
    logger.info("worker_stopped", extra={"group": group.name, "queue": group.queue_name})


async def main() -> None:
    parser = argparse.ArgumentParser(description="Async order-processing worker (inventory deduction)")
    parser.add_argument("--drain-dlq", action="store_true", help="log+ack all messages in the worker DLQ and exit")
    args = parser.parse_args()

    try:
        if args.drain_dlq:
            drained = await drain_dlq()
            logger.info("worker_dlq_drained", extra={"messages": drained})
            return

        await run_worker()
    finally:
        # Flush before the process exits; the DLQ-drain tool emits spans too.
        shutdown_tracing()


if __name__ == "__main__":
    asyncio.run(main())
