"""
Inventory bounded context — read-side repository (CQRS Week 7).

Answers product reads for API queries. Currently backed by the same product
table (single-table domain), but isolated so a future read-optimized copy can
stand in without touching write intent.
"""
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.infrastructure.models import ProductModel


class ProductReadRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @staticmethod
    def _to_domain(row: ProductModel) -> Product:
        return Product(
            id=row.id,
            sku=row.sku,
            name=row.name,
            price_cents=row.price_cents,
            quantity_on_hand=row.quantity_on_hand,
        )

    async def get_by_id(self, product_id: UUID) -> Product | None:
        row = await self._session.get(ProductModel, product_id)
        return self._to_domain(row) if row else None

    async def list_all(self, limit: int = 100, offset: int = 0) -> list[Product]:
        result = await self._session.execute(select(ProductModel).limit(limit).offset(offset))
        return [self._to_domain(row) for row in result.scalars().all()]