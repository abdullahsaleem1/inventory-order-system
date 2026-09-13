"""
Orders bounded context — CQRS command handlers (Week 7).

The write side. Every state-changing use case is a `Command` (commands.py)
handled by exactly one of these. Handlers operate on the write model through
`OrderWriteRepository` and keep the read projection in sync via
`OrderReadRepository.upsert_from/update_status` in the SAME transaction.

Errors surface as:
  * `OrderNotFoundError`        — unknown order (→ 404)
  * `EmptyOrderError` / `InvalidOrderTransitionError` — domain rule violation (→ 409)
  * `ServiceUnavailableError`   — event broker failed to confirm publication (→ 503)
"""
from dataclasses import dataclass
from uuid import UUID

from src.contexts.orders.commands import CancelOrderCommand, ConfirmOrderCommand, CreateOrderCommand
from src.contexts.orders.domain.order import Order, OrderLine
from src.contexts.orders.errors import OrderNotFoundError
from src.contexts.orders.events import build_order_created_event, build_order_status_changed_event
from src.contexts.orders.repositories.order_read_repository import OrderReadRepository
from src.contexts.orders.repositories.order_write_repository import OrderWriteRepository
from src.shared.exceptions import ServiceUnavailableError
from src.shared.messaging.publisher import EventPublishError, EventPublisher


@dataclass(frozen=True)
class CreateOrderResult:
    """Outcome of CreateOrderCommand: the accepted aggregate + published event id."""

    order: Order
    event_id: UUID


class CreateOrderCommandHandler:
    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher

    async def handle(self, command: CreateOrderCommand) -> CreateOrderResult:
        order = Order(
            customer_id=command.customer_id,
            lines=[OrderLine(**line) for line in command.lines],
        )
        event = build_order_created_event(order, correlation_id=command.correlation_id)
        try:
            await self._publisher.publish(event)
        except EventPublishError as exc:
            raise ServiceUnavailableError(
                "Order could not be accepted: event broker did not confirm publication",
                code="EVENT_PUBLISH_FAILED",
            ) from exc
        return CreateOrderResult(order=order, event_id=event.event_id)


class ConfirmOrderCommandHandler:
    """Confirm a PENDING order.

    Week 8: the write side commits the aggregate status, keeps the local
    projection in sync, and publishes an `order.status.changed` event so the
    read-projector sync worker can update the dedicated read store (eventual
    consistency). The publish happens BEFORE the HTTP session commits — a
    broker failure raises 503 and the dependency rolls the DB write back, so we
    never confirm an order the read store never learns about.
    """

    def __init__(
        self,
        write_repo: OrderWriteRepository,
        read_repo: OrderReadRepository,
        publisher: EventPublisher,
    ) -> None:
        self._write_repo = write_repo
        self._read_repo = read_repo
        self._publisher = publisher

    async def handle(self, command: ConfirmOrderCommand) -> Order:
        order = await self._load_aggregate(command.order_id)
        order.confirm()  # raises EmptyOrderError / InvalidOrderTransitionError
        await self._write_repo.update_status(order)
        await self._read_repo.upsert_from(order)
        await self._publish_status_changed(order, command.correlation_id)
        return order

    async def _publish_status_changed(self, order: Order, correlation_id: str | None) -> None:
        event = build_order_status_changed_event(order, correlation_id=correlation_id)
        try:
            await self._publisher.publish(event)
        except EventPublishError as exc:
            raise ServiceUnavailableError(
                "Order status could not be published: event broker did not confirm publication",
                code="EVENT_PUBLISH_FAILED",
            ) from exc

    async def _load_aggregate(self, order_id: UUID) -> Order:
        order = await self._write_repo.get_by_id(order_id)
        if order is None:
            raise OrderNotFoundError(f"Order {order_id} not found")
        return order


class CancelOrderCommandHandler:
    """Cancel an order: write the aggregate status, keep the local projection,
    and publish an `order.status.changed` event for the read-projector sync
    worker (same atomicity rationale as ConfirmOrderCommandHandler)."""

    def __init__(
        self,
        write_repo: OrderWriteRepository,
        read_repo: OrderReadRepository,
        publisher: EventPublisher,
    ) -> None:
        self._write_repo = write_repo
        self._read_repo = read_repo
        self._publisher = publisher

    async def handle(self, command: CancelOrderCommand) -> Order:
        order = await self._write_repo.get_by_id(command.order_id)
        if order is None:
            raise OrderNotFoundError(f"Order {command.order_id} not found")
        order.cancel()  # raises InvalidOrderTransitionError
        await self._write_repo.update_status(order)
        await self._read_repo.upsert_from(order)
        event = build_order_status_changed_event(order, correlation_id=command.correlation_id)
        try:
            await self._publisher.publish(event)
        except EventPublishError as exc:
            raise ServiceUnavailableError(
                "Order status could not be published: event broker did not confirm publication",
                code="EVENT_PUBLISH_FAILED",
            ) from exc
        return order