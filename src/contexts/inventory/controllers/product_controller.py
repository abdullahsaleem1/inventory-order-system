"""
Inventory bounded context — controller layer.
Sits between routes and services: converts API schemas to service calls,
and translates domain/service exceptions into HTTP-friendly errors.
Routes should stay a thin wiring layer that just calls into here.
"""
from uuid import UUID

from fastapi import HTTPException, status

from src.contexts.inventory.api.schemas import ProductCreateRequest, ProductResponse
from src.contexts.inventory.domain.product import InsufficientStockError, InvalidPriceError
from src.contexts.inventory.services.product_service import (
    DuplicateSkuError,
    ProductNotFoundError,
    ProductService,
)


class ProductController:
    def __init__(self, service: ProductService) -> None:
        self._service = service

    async def create_product(self, payload: ProductCreateRequest) -> ProductResponse:
        try:
            product = await self._service.create_product(
                sku=payload.sku,
                name=payload.name,
                price_cents=payload.price_cents,
                quantity_on_hand=payload.quantity_on_hand,
            )
        except DuplicateSkuError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except InvalidPriceError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
        return ProductResponse.model_validate(product, from_attributes=True)

    async def get_product(self, product_id: UUID) -> ProductResponse:
        try:
            product = await self._service.get_product(product_id)
        except ProductNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return ProductResponse.model_validate(product, from_attributes=True)

    async def list_products(self, limit: int, offset: int) -> list[ProductResponse]:
        products = await self._service.list_products(limit=limit, offset=offset)
        return [ProductResponse.model_validate(p, from_attributes=True) for p in products]

    async def reserve_stock(self, product_id: UUID, quantity: int) -> ProductResponse:
        try:
            product = await self._service.reserve_stock(product_id, quantity)
        except ProductNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except InsufficientStockError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return ProductResponse.model_validate(product, from_attributes=True)

    async def restock(self, product_id: UUID, quantity: int) -> ProductResponse:
        try:
            product = await self._service.restock(product_id, quantity)
        except ProductNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return ProductResponse.model_validate(product, from_attributes=True)
