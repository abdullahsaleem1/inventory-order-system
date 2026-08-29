"""
RabbitMQ consumer side of the event pipeline.

Topology (all durable, declared on startup):
    inventory.orders.events  (topic exchange)
      ├─ order.created ──> orders.order-created.persistence   (consumer group 1: DB writes)
      ├─ order.created ──> orders.order-created.audit         (consumer group 2: audit log)
      └─ order.created ──> orders.order-created.inventory     (worker group 3: stock deduction)
    inventory.orders.dlx     (dead-letter topic exchange)
      └─ <group>.dead  ──> <queue>.dlq                        (poison / exhausted messages)

Delivery semantics:
- Manual acknowledgement — a message is acked only after its handler succeeds.
- At-least-once: crashes between handler success and ack cause redelivery, so
  handlers must be idempotent (see Week 6 README — the inventory worker keeps
  a dedup log; the persistence handler skips existing orders).
- Transient failures are retried with **exponential backoff**. Each consumer
  group owns a stairway of durable retry queues (`<queue>.retry.1`, `.retry.2`,
  ...). On a transient failure the message is parked in the retry queue for
  `base * 2^(n-1)` seconds (its per-message TTL / `expiration`). When that TTL
  expires RabbitMQ dead-letters it back onto the main exchange, which routes it
  back into the same work queue for another attempt. There is one retry queue
  per attempt, so each retry can carry a different, exponentially growing delay.
  After CONSUMER_MAX_RETRIES the message is rejected without requeue, which
  routes it through the dead-letter exchange into the group's DLQ.
- Permanent failures (undecodable envelope / malformed payload /
  `PermanentMessageError`) skip retries and go straight to the DLQ.

Every lifecycle stage is logged as structured JSON:
event_receive_started -> event_retry_scheduled | event_ack | event_nack |
event_dead_lettered.
"""
import asyncio
import time
from dataclasses import dataclass, field
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
    # Internal: the declared work queue, filled in during start().
    declared_queue: Any = field(default=None, init=False, repr=False)
    # Internal: per-attempt retry queues used for exponential backoff.
    retry_queues: dict[int, Any] = field(default_factory=dict, init=False, repr=False)


class RabbitMQEventConsumer:
    def __init__(
        self,
        broker_url: str,
        groups: list[QueueGroupSpec],
        *,
        prefetch_count: int = 10,
        max_retries: int = 3,
        backoff_base_seconds: float = 1.0,
        backoff_max_seconds: float = 60.0,
    ) -> None:
        settings = get_settings()
        self._broker_url = broker_url
        self._exchange_name = settings.EVENT_EXCHANGE
        self._dlx_name = settings.DEAD_LETTER_EXCHANGE
        self._groups = groups
        self._prefetch_count = prefetch_count
        self._max_retries = max_retries
        self._backoff_base = backoff_base_seconds
        self._backoff_max = backoff_max_seconds
        self._logger = get_logger("messaging.consumer")
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._consume_tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()

    # --- helpers ------------------------------------------------------------

    def _backoff_seconds(self, attempt: int) -> float:
        """Exponential backoff for a 1-based attempt number: base * 2^(n-1)."""
        delay = self._backoff_base * (2 ** (attempt - 1))
        return min(delay, self._backoff_max)

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
            spec.declared_queue = work_queue

            # Stairway of retry queues — one per attempt, each dead-lettering
            # back onto the main exchange after its per-message TTL elapses.
            # The expired message re-enters `exchange` under the group's
            # routing key and is routed back into `work_queue`.
            retry_routing_key = spec.routing_keys[0]
            for attempt in range(1, self._max_retries + 1):
                retry_queue = await channel.declare_queue(
                    f"{spec.queue_name}.retry.{attempt}",
                    durable=True,
                    arguments={
                        "x-dead-letter-exchange": self._exchange_name,
                        "x-dead-letter-routing-key": retry_routing_key,
                    },
                )
                spec.retry_queues[attempt] = retry_queue
        return exchange

    # --- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        self._stop.clear()
        self._connection = await aio_pika.connect_robust(self._broker_url)
        channel = await self._connection.channel()
        await channel.set_qos(prefetch_count=self._prefetch_count)
        await self._declare_topology(channel)

        for spec in self._groups:
            queue = spec.declared_queue
            task = asyncio.create_task(
                queue.consume(self._make_on_message(spec), no_ack=False),
                name=f"consume:{spec.queue_name}",
            )
            self._consume_tasks.append(task)
            self._logger.info(
                "consumer_group_started",
                extra={
                    "queue": spec.queue_name,
                    "routing_keys": list(spec.routing_keys),
                    "max_retries": self._max_retries,
                    "backoff_base_seconds": self._backoff_base,
                },
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

    async def _schedule_retry(
        self,
        spec: QueueGroupSpec,
        message: aio_pika.IncomingMessage,
        event: DomainEvent,
        headers: dict[str, Any],
        attempt: int,
        error: Exception,
        log_common: dict[str, Any],
    ) -> None:
        """Park the message in the `attempt`-th retry queue for a backoff delay."""
        delay = self._backoff_seconds(attempt)
        delay_ms = max(1, int(delay * 1000))
        self._logger.warning(
            "event_retry_scheduled",
            extra={
                **log_common,
                "attempt": attempt,
                "max_retries": self._max_retries,
                "backoff_seconds": round(delay, 3),
                "error_type": type(error).__name__,
                "error": str(error),
            },
        )
        retry_message = Message(
            body=message.body,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            type=event.event_type,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            expiration=str(delay_ms),  # TTL before the retry queue dead-letters it
            headers={**headers, "x-retry-count": attempt},
        )
        await spec.retry_queues[attempt].channel.default_exchange.publish(
            retry_message, routing_key=str(spec.retry_queues[attempt].name)
        )
        await message.ack()  # original consumed; retry copy parked for backoff

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
            except Exception as exc:  # transient — exponential backoff then DLQ
                if attempt >= self._max_retries:
                    reason = f"retries_exhausted ({self._max_retries}): {exc}"
                    self._logger.exception(
                        "event_nack", extra={**log_common, "reason": reason, "retryable": False}
                    )
                    await message.reject(requeue=False)
                    self._logger.info("event_dead_lettered", extra={**log_common, "reason": reason})
                    return
                await self._schedule_retry(spec, message, event, headers, attempt + 1, exc, log_common)
                return

            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            await message.ack()
            self._logger.info("event_ack", extra={**log_common, "duration_ms": duration_ms})

        return on_message
