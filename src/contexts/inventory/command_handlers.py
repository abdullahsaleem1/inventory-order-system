"""
Inventory bounded context — CQRS command handlers (Week 7).

The write side. Every state-changing use case is a `Command` (commands.py)
handled by exactly one of these against `ProductWriteRepository`.

Errors surface as:
  * `DuplicateSkuError`       — SKU already taken (→ 409)
  * `InvalidPriceError`       — domain rule (→ 422)
  * `ProductNotFoundError`    — unknown product (→ 404)
  * `InsufficientStockError`  — reserve exceeds quantity_on_hand (→ 409)
"""
from src.contexts.inventory.commands import (
    CreateProductCommand,
    ReserveStockCommand,
    RestockCommand,
)
from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.errors import DuplicateSkuError, ProductNotFoundError
from src.contexts.inventory.repositories.product_write_repository import ProductWriteRepository
from uuid import UUID


class CreateProductCommandHandler:
    def __init__(self, write_repo: ProductWriteRepository) -> None:
        self._write_repo = write_repo

    async def handle(self, command: CreateProductCommand) -> Product:
        if await self._write_repo.get_by_sku(command.sku) is not None:
            raise DuplicateSkuError(f"SKU '{command.sku}' already exists")
        product = Product(
            sku=command.sku,
            name=command.name,
            price_cents=command.price_cents,
            quantity_on_hand=command.quantity_on_hand,
        )
        return await self._write_repo.add(product)


class ReserveStockCommandHandler:
    def __init__(self, write_repo: ProductWriteRepository) -> None:
        self._write_repo = write_repo

    async def handle(self, command: ReserveStockCommand) -> Product:
        product = await self._load(command.product_id)
        product.reserve_stock(command.quantity)  # domain rule enforced here
        return await self._write_repo.update(product)

    async def _load(self, product_id: UUID) -> Product:
        product = await self._write_repo.get_by_id(product_id)
        if product is None:
            raise ProductNotFoundError(f"Product {product_id} not found")
        return product


class RestockCommandHandler:
    def __init__(self, write_repo: ProductWriteRepository) -> None:
        self._write_repo = write_repo

    async def handle(self, command: RestockCommand) -> Product:
        product = await self._write_repo.get_by_id(command.product_id)
        if product is None:
            raise ProductNotFoundError(f"Product {command.product_id} not found")
        product.restock(command.quantity)
        return await self._write_repo.update(product)