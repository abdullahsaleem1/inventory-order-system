"""
Event publishing over RabbitMQ.

- RabbitMQEventPublisher — production implementation:
    * connects lazily via aio-pika's connect_robust (auto-reconnect),
    * enables **publisher confirms** so `publish()` only returns once the
      broker has accepted the message (no silent loss on publish),
    * declares the durable topic exchange on connect,
    * sends persistent messages routed by event type,
    * emits one structured log line per lifecycle stage
      (`event_publish_started` / `event_publish_succeeded` /
      `event_publish_failed`).
- InMemoryEventPublisher — test double: records every event and optionally
  forwards it to inline "subscriber" handlers, simulating immediate delivery.
"""
import time
from typing import Any, Awaitable, Callable, Protocol

import aio_pika
from aio_pika import DeliveryMode, Message

from src.core.config import get_settings
from src.core.logging_config import get_logger
from src.shared.messaging.events import DomainEvent


class EventPublishError(Exception):
    """Raised when an event could not be confirmed by the broker."""


class EventPublisher(Protocol):
    async def publish(self, event: DomainEvent) -> None: ...


EventSubscriber = Callable[[DomainEvent], Awaitable[None]]


class RabbitMQEventPublisher:
    """Publishes DomainEvents to a durable topic exchange with confirms on."""

    def __init__(self, broker_url: str, routing_keys: dict[str, str] | None = None) -> None:
        settings = get_settings()
        self._broker_url = broker_url
        self._exchange_name = settings.EVENT_EXCHANGE
        self._routing_keys = routing_keys or {  # event_type -> routing key
            "order.created": settings.ORDER_CREATED_ROUTING_KEY,
        }
        self._logger = get_logger("messaging.publisher")
        self._connection: aio_pika.abc.AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractRobustChannel | None = None
        self._exchange: aio_pika.abc.AbstractRobustExchange | None = None

    async def _ensure_exchange(self) -> aio_pika.abc.AbstractRobustExchange:
        if self._exchange is not None and not self._connection.is_closed:
            return self._exchange
        self._connection = await aio_pika.connect_robust(self._broker_url)
        self._channel = await self._connection.channel()
        # Publisher confirms: every publish is acked by the broker before
        # exchange.publish() returns, otherwise it raises.
        await self._channel.confirm_delivery()
        self._exchange = await self._channel.declare_exchange(
            self._exchange_name,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,  # survives broker restarts
        )
        self._logger.info(
            "broker_connection_established",
            extra={"exchange": self._exchange_name, "confirm_mode": True},
        )
        return self._exchange

    def _routing_key_for(self, event_type: str) -> str:
        try:
            return self._routing_keys[event_type]
        except KeyError as exc:
            raise EventPublishError(f"No routing key registered for event type '{event_type}'") from exc

    async def publish(self, event: DomainEvent) -> None:
        started = time.perf_counter()
        log_common: dict[str, Any] = {
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "correlation_id": event.correlation_id,
        }
        self._logger.info("event_publish_started", extra=log_common)
        try:
            exchange = await self._ensure_exchange()
            message = Message(
                body=event.to_json(),
                message_id=str(event.event_id),
                correlation_id=event.correlation_id,
                type=event.event_type,
                timestamp=event.occurred_at,
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,  # survive broker restart
                headers={"x-event-version": 1},
            )
            # Raises on nack/timeout because confirm_delivery() is enabled.
            await exchange.publish(message, routing_key=self._routing_key_for(event.event_type), timeout=10)
        except Exception as exc:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            self._logger.exception(
                "event_publish_failed",
                extra={**log_common, "duration_ms": duration_ms, "error_type": type(exc).__name__},
            )
            raise EventPublishError(f"Failed to publish event {event.event_id}: {exc}") from exc

        duration_ms = round((time.perf_counter() - started) * 1000, 2)
        self._logger.info("event_publish_succeeded", extra={**log_common, "duration_ms": duration_ms})

    async def close(self) -> None:
        if self._connection is not None and not self._connection.is_closed:
            await self._connection.close()
        self._connection = None
        self._channel = None
        self._exchange = None


class InMemoryEventPublisher:
    """Test double — records events and forwards them to inline subscribers.

    Subscriber exceptions propagate to the caller of publish(), mirroring the
    behaviour of a real broker outage (publish fails -> API returns 503).
    """

    def __init__(self) -> None:
        self.published: list[DomainEvent] = []
        self.subscribers: list[EventSubscriber] = []

    async def publish(self, event: DomainEvent) -> None:
        self.published.append(event)
        for subscriber in self.subscribers:
            await subscriber(event)

    def clear(self) -> None:
        self.published.clear()
