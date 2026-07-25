"""
Orders bounded context — domain layer.
Deliberately separate from Inventory's domain model: Orders references
products only by ID + a price snapshot, never by importing Inventory's
Product class. Cross-context communication belongs at the service/event
layer, not the domain layer.
"""
from dataclasses import dataclass, field
from enum import Enum
from uuid import UUID

from src.shared.domain.base import DomainError, Entity


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"
    FULFILLED = "FULFILLED"


class InvalidOrderTransitionError(DomainError):
    pass


class EmptyOrderError(DomainError):
    pass


@dataclass(kw_only=True)
class OrderLine:
    product_id: UUID
    quantity: int
    unit_price_cents: int

    @property
    def subtotal_cents(self) -> int:
        return self.quantity * self.unit_price_cents


@dataclass(kw_only=True)
class Order(Entity):
    customer_id: UUID
    lines: list[OrderLine] = field(default_factory=list)
    status: OrderStatus = OrderStatus.PENDING

    @property
    def total_cents(self) -> int:
        return sum(line.subtotal_cents for line in self.lines)

    def confirm(self) -> None:
        if not self.lines:
            raise EmptyOrderError("Cannot confirm an order with no line items")
        if self.status != OrderStatus.PENDING:
            raise InvalidOrderTransitionError(f"Cannot confirm order in status {self.status}")
        self.status = OrderStatus.CONFIRMED

    def cancel(self) -> None:
        if self.status in (OrderStatus.CANCELLED, OrderStatus.FULFILLED):
            raise InvalidOrderTransitionError(f"Cannot cancel order in status {self.status}")
        self.status = OrderStatus.CANCELLED

    def fulfill(self) -> None:
        if self.status != OrderStatus.CONFIRMED:
            raise InvalidOrderTransitionError("Order must be CONFIRMED before it can be fulfilled")
        self.status = OrderStatus.FULFILLED
