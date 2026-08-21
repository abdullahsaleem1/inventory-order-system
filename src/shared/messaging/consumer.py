"""
RabbitMQ consumer side of the event pipeline.

Topology (all durable, declared on startup):
    inventory.orders.events  (topic exchange)
      ├─ order.created ──> orders.order-created.persistence   (consumer group 1: DB writes)
      └─ order.created ──> orders.order-created.audit         (consumer group 2: audit log)
    inventory.orders.dlx     (dead-letter topic exchange)
      └─ <group>.dead  ──> <queue>.dlq                        (poison messages)

Delivery semantics:
- Manual acknowledgement — a message is acked only after its handler succeeds.
- At-least-once: crashes between handler success and ack cause redelivery, so
  handlers must be idempotent (the persistence handler is — see Week 5 README).
- Transient failures are retried by republishing the same body onto the work
  queue with an incremented `x-retry-count` header (bounded backoff-free
  retry). After CONSUMER_MAX_RETRIES the message is rejected without requeue,
  which routes it through the dead-letter exchange into the group's DLQ.
- Permanent failures (undecodable envelope / malformed payload) skip retries
  and go straight to the DLQ.

Every lifecycle stage is logged as structured JSON:
event_receive_started -> event_retry_scheduled | event_ack | event_nack |
event_dead_lettered.
"""
import asyncio
import time
from dataclasses import dataclass
from typing import Any

import aio_pika
from aio_pika import DeliveryMode, Message

from src.core.config import get_settings
from src.core.logging_config import get_logger
from src.shared.messaging.events import DomainEvent, MessageDecodeError


class PermanentMessageError(Exception):
    """Handler-level validation failure — retrying will never succeed."""


@dataclass(kw_only=True)
class QueueGroupSpec:
    """One consumer group = one durable queue + the handler that serves it."""

    name: str
    queue_name: str
    routing_keys: tuple[str, ...]
    handler: Any  # async callable(DomainEvent) -> None


class RabbitMQEventConsumer:
    def __init__(
        self,
        broker_url: str,
        groups: list[QueueGroupSpec],
        *,
        prefetch_count: int = 10,
        max_retries: int = 3,
    ) -> None:
        settings = get_settings()
        self._broker_url = broker_url
        self._exchange_name = settings.EVENT_EXCHANGE
        self._dlx_name = settings.DEAD_LETTER_EXCHANGE
        self._groups = groups
        self._prefetch_count = prefetch_count
        self._max_retries = max_retries
        self._logger = get_logger("messaging.consumer")
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._consume_tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()

    # --- topology -----------------------------------------------------------

    async def _declare_topology(self, channel: aio_pika.abc.AbstractRobustChannel):
        exchange = await channel.declare_exchange(
            self._exchange_name, type=aio_pika.ExchangeType.TOPIC, durable=True
        )
        dlx = await channel.declare_exchange(self._dlx_name, type=aio_pika.ExchangeType.TOPIC, durable=True)
        for spec in self._groups:
            dlq_name = f"{spec.queue_name}.dlq"
            dlq = await channel.declare_queue(dlq_name, durable=True)
            dead_key = f"{spec.name}.dead"
            await dlq.bind(dlx, routing_key=dead_key)

            work_queue = await channel.declare_queue(
                spec.queue_name,
                durable=True,  # survives broker restart
                arguments={
                    "x-dead-letter-exchange": self._dlx_name,
                    "x-dead-letter-routing-key": dead_key,
                },
            )
            for routing_key in spec.routing_keys:
                await work_queue.bind(exchange, routing_key=routing_key)
            spec.declared_queue = work_queue  # type: ignore[attr-defined]
        return exchange

    # --- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        self._stop.clear()
        self._connection = await aio_pika.connect_robust(self._broker_url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch_count)
        await self._declare_topology(channel)

        for spec in self._groups:
            queue = spec.declared_queue  # type: ignore[attr-defined]
            task = asyncio.create_task(
                queue.consume(self._make_on_message(spec), no_ack=False),
                name=f"consume:{spec.queue_name}",
            )
            self._consume_tasks.append(task)
            self._logger.info(
                "consumer_group_started",
                extra={"queue": spec.queue_name, "routing_keys": list(spec.routing_keys)},
            )

    async def stop(self) -> None:
        self._stop.set()
        for task in self._consume_tasks:
            task.cancel()
        await asyncio.gather(*self._consume_tasks, return_exceptions=True)
        self._consume_tasks.clear()
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._logger.info("consumer_stopped")

    # --- message handling ---------------------------------------------------

    def _make_on_message(self, spec: QueueGroupSpec):
        async def on_message(message: aio_pika.IncomingMessage) -> None:
            started = time.perf_counter()
            headers = dict(message.headers or {})
            attempt = int(headers.get("x-retry-count", 0))
            log_common: dict[str, Any] = {
                "message_id": message.message_id,
                "queue": spec.queue_name,
                "attempt": attempt + 1,
                "redelivered": message.redelivered,
                "routing_key": message.routing_key,
            }
            self._logger.info("event_receive_started", extra=log_common)

            try:
                event = DomainEvent.from_json(message.body)
            except MessageDecodeError as exc:
                self._logger.exception(
                    "event_nack",
                    extra={**log_common, "reason": f"decode_error: {exc}", "retryable": False},
                )
                await message.reject(requeue=False)  # straight to DLQ
                self._logger.info("event_dead_lettered", extra={**log_common, "reason": str(exc)})
                return

            log_common["event_id"] = str(event.event_id)
            log_common["event_type"] = event.event_type
            log_common["correlation_id"] = event.correlation_id

            try:
                await spec.handler(event)
            except PermanentMessageError as exc:
                self._logger.exception(
                    "event_nack",
                    extra={**log_common, "reason": f"permanent_error: {exc}", "retryable": False},
                )
                await message.reject(requeue=False)
                self._logger.info("event_dead_lettered", extra={**log_common, "reason": str(exc)})
                return
            except Exception as exc:  # transient — bounded retry then DLQ
                if attempt >= self._max_retries:
                    reason = f"retries_exhausted ({self._max_retries}): {exc}"
                    self._logger.exception(
                        "event_nack", extra={**log_common, "reason": reason, "retryable": False}
                    )
                    await message.reject(requeue=False)
                    self._logger.info("event_dead_lettered", extra={**log_common, "reason": reason})
                    return
                next_attempt = attempt + 1
                self._logger.warning(
                    "event_retry_scheduled",
                    extra={
                        **log_common,
                        "attempt": next_attempt,
                        "max_retries": self._max_retries,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                )
                retry_message = Message(
                    body=message.body,
                    message_id=message.message_id,
                    correlation_id=message.correlation_id,
                    type=event.event_type,
                    content_type="application/json",
                    delivery_mode=DeliveryMode.PERSISTENT,
                    headers={**headers, "x-retry-count": next_attempt},
                )
                await spec.declared_queue.channel.default_exchange.publish(  # type: ignore[attr-defined]
                    retry_message, routing_key=str(message.routing_key)
                )
                await message.ack()  # original consumed; retry copy now in flight
                return

            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            await message.ack()
            self._logger.info("event_ack", extra={**log_common, "duration_ms": duration_ms})

        return on_message
