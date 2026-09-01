"""
Inventory bounded context — CQRS write side (commands, Week 7).

Each command is a frozen dataclass describing an intention to change inventory
state. Command handlers (command_handlers.py) execute them against the write
model. The API never mutates a product outside a command handler.
"""
from dataclasses import dataclass
from uuid import UUID

from src.shared.cqrs import Command


@dataclass(frozen=True)
class CreateProductCommand(Command):
    sku: str
    name: str
    price_cents: int
    quantity_on_hand: int = 0


@dataclass(frozen=True)
class ReserveStockCommand(Command):
    product_id: UUID
    quantity: int


@dataclass(frozen=True)
class RestockCommand(Command):
    product_id: UUID
    quantity: int