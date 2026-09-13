"""
Orders bounded context — CQRS write side (commands, Week 7).

Each command is a frozen dataclass describing an intention to change state.
Command handlers (in `command_handlers.py`) execute them against the write
model. The API never performs a write outside a command handler.
"""
from dataclasses import dataclass
from uuid import UUID

from src.shared.cqrs import Command


@dataclass(frozen=True)
class CreateOrderCommand(Command):
    """Intention to accept a new order.

    Event-driven (since Week 5): the handler builds + validates the aggregate
    and publishes `order.created`; persistence happens asynchronously in the
    `orders.order-created.persistence` consumer group.
    """

    customer_id: UUID
    lines: list[dict]
    correlation_id: str | None = None


@dataclass(frozen=True)
class ConfirmOrderCommand(Command):
    """Intention to transition a PENDING order to CONFIRMED."""

    order_id: UUID
    correlation_id: str | None = None


@dataclass(frozen=True)
class CancelOrderCommand(Command):
    """Intention to cancel an order."""

    order_id: UUID
    correlation_id: str | None = None