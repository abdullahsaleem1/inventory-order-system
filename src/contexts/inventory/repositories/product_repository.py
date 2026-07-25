"""
Inventory bounded context — repository layer.
Only layer allowed to talk to the database. Translates ORM rows <-> domain
entities so the service layer above never touches SQLAlchemy directly.
"""
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.infrastructure.models import ProductModel


class ProductRepository:
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

    @staticmethod
    def _to_model(entity: Product, row: ProductModel | None = None) -> ProductModel:
        row = row or ProductModel(id=entity.id)
        row.sku = entity.sku
        row.name = entity.name
        row.price_cents = entity.price_cents
        row.quantity_on_hand = entity.quantity_on_hand
        return row

    async def get_by_id(self, product_id: UUID) -> Product | None:
        row = await self._session.get(ProductModel, product_id)
        return self._to_domain(row) if row else None

    async def get_by_sku(self, sku: str) -> Product | None:
        result = await self._session.execute(select(ProductModel).where(ProductModel.sku == sku))
        row = result.scalar_one_or_none()
        return self._to_domain(row) if row else None

    async def list_all(self, limit: int = 100, offset: int = 0) -> list[Product]:
        result = await self._session.execute(select(ProductModel).limit(limit).offset(offset))
        return [self._to_domain(row) for row in result.scalars().all()]

    async def add(self, entity: Product) -> Product:
        row = self._to_model(entity)
        self._session.add(row)
        await self._session.flush()
        return self._to_domain(row)

    async def update(self, entity: Product) -> Product:
        row = await self._session.get(ProductModel, entity.id)
        if row is None:
            raise ValueError(f"Product {entity.id} not found")
        row = self._to_model(entity, row)
        await self._session.flush()
        return self._to_domain(row)
