"""
Inventory bounded context — CQRS query handlers (Week 7).

The read side. Answers product queries through `ProductReadRepository`.
"""
from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.errors import ProductNotFoundError
from src.contexts.inventory.queries import GetProductQuery, ListProductsQuery
from src.contexts.inventory.repositories.product_read_repository import ProductReadRepository


class GetProductQueryHandler:
    def __init__(self, read_repo: ProductReadRepository) -> None:
        self._read_repo = read_repo

    async def handle(self, query: GetProductQuery) -> Product:
        product = await self._read_repo.get_by_id(query.product_id)
        if product is None:
            raise ProductNotFoundError(f"Product {query.product_id} not found")
        return product


class ListProductsQueryHandler:
    def __init__(self, read_repo: ProductReadRepository) -> None:
        self._read_repo = read_repo

    async def handle(self, query: ListProductsQuery) -> list[Product]:
        return await self._read_repo.list_all(limit=query.limit, offset=query.offset)