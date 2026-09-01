"""
Inventory bounded context — controller layer.

CQRS (Week 7): holds a `CqrsBus` and only handles HTTP concerns — scheduling
`Command`/`Query` objects and mapping exceptions to HTTP-friendly errors.
"""
from uuid import UUID

from fastapi import HTTPException, status

from src.contexts.inventory.api.schemas import ProductCreateRequest, ProductResponse
from src.contexts.inventory.commands import CreateProductCommand, ReserveStockCommand, RestockCommand
from src.contexts.inventory.domain.product import InsufficientStockError, InvalidPriceError
from src.contexts.inventory.errors import DuplicateSkuError, ProductNotFoundError
from src.contexts.inventory.queries import GetProductQuery, ListProductsQuery
from src.shared.cqrs import CqrsBus


class ProductController:
    def __init__(self, bus: CqrsBus) -> None:
        self._bus = bus

    async def create_product(self, payload: ProductCreateRequest) -> ProductResponse:
        try:
            product = await self._bus.dispatch_command(
                CreateProductCommand(
                    sku=payload.sku,
                    name=payload.name,
                    price_cents=payload.price_cents,
                    quantity_on_hand=payload.quantity_on_hand,
                )
            )
        except DuplicateSkuError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except InvalidPriceError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
        return ProductResponse.model_validate(product, from_attributes=True)

    async def get_product(self, product_id: UUID) -> ProductResponse:
        try:
            product = await self._bus.dispatch_query(GetProductQuery(product_id))
        except ProductNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return ProductResponse.model_validate(product, from_attributes=True)

    async def list_products(self, limit: int, offset: int) -> list[ProductResponse]:
        products = await self._bus.dispatch_query(ListProductsQuery(limit=limit, offset=offset))
        return [ProductResponse.model_validate(p, from_attributes=True) for p in products]

    async def reserve_stock(self, product_id: UUID, quantity: int) -> ProductResponse:
        try:
            product = await self._bus.dispatch_command(ReserveStockCommand(product_id, quantity))
        except ProductNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except InsufficientStockError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return ProductResponse.model_validate(product, from_attributes=True)

    async def restock(self, product_id: UUID, quantity: int) -> ProductResponse:
        try:
            product = await self._bus.dispatch_command(RestockCommand(product_id, quantity))
        except ProductNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return ProductResponse.model_validate(product, from_attributes=True)