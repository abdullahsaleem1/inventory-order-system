"""
Inventory bounded context — service (application) layer.
Orchestrates use cases: loads entities via the repository, invokes domain
logic, persists results. Contains NO business rules itself — those live in
the domain layer (product.py).
"""
from uuid import UUID

from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.repositories.product_repository import ProductRepository


class ProductNotFoundError(Exception):
    pass


class DuplicateSkuError(Exception):
    pass


class ProductService:
    def __init__(self, repository: ProductRepository) -> None:
        self._repo = repository

    async def create_product(self, sku: str, name: str, price_cents: int, quantity_on_hand: int = 0) -> Product:
        if await self._repo.get_by_sku(sku) is not None:
            raise DuplicateSkuError(f"SKU '{sku}' already exists")
        product = Product(sku=sku, name=name, price_cents=price_cents, quantity_on_hand=quantity_on_hand)
        return await self._repo.add(product)

    async def get_product(self, product_id: UUID) -> Product:
        product = await self._repo.get_by_id(product_id)
        if product is None:
            raise ProductNotFoundError(f"Product {product_id} not found")
        return product

    async def list_products(self, limit: int = 100, offset: int = 0) -> list[Product]:
        return await self._repo.list_all(limit=limit, offset=offset)

    async def reserve_stock(self, product_id: UUID, quantity: int) -> Product:
        product = await self.get_product(product_id)
        product.reserve_stock(quantity)  # domain rule enforced here
        return await self._repo.update(product)

    async def restock(self, product_id: UUID, quantity: int) -> Product:
        product = await self.get_product(product_id)
        product.restock(quantity)
        return await self._repo.update(product)
