"""
Inventory bounded context — domain layer.
Pure business logic. Knows nothing about FastAPI, SQLAlchemy, or HTTP.
"""
from dataclasses import dataclass

from src.shared.domain.base import DomainError, Entity


class InsufficientStockError(DomainError):
    pass


class InvalidPriceError(DomainError):
    pass


@dataclass(kw_only=True)
class Product(Entity):
    sku: str
    name: str
    price_cents: int
    quantity_on_hand: int = 0

    def __post_init__(self) -> None:
        if self.price_cents <= 0:
            raise InvalidPriceError(f"Price must be positive, got {self.price_cents}")
        if self.quantity_on_hand < 0:
            raise InsufficientStockError("quantity_on_hand cannot start negative")

    def reserve_stock(self, quantity: int) -> None:
        """Deduct stock for a pending order. Raises if not enough stock."""
        if quantity <= 0:
            raise ValueError("Reservation quantity must be positive")
        if quantity > self.quantity_on_hand:
            raise InsufficientStockError(
                f"Cannot reserve {quantity} units of {self.sku}; only {self.quantity_on_hand} available"
            )
        self.quantity_on_hand -= quantity

    def restock(self, quantity: int) -> None:
        if quantity <= 0:
            raise ValueError("Restock quantity must be positive")
        self.quantity_on_hand += quantity
