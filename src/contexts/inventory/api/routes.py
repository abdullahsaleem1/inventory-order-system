"""
Inventory bounded context — routes layer.
Thin wiring only: defines HTTP endpoints and delegates immediately to the
controller. No business logic, no persistence logic here.
"""
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.inventory.api.schemas import (
    ProductCreateRequest,
    ProductResponse,
    StockAdjustmentRequest,
)
from src.contexts.inventory.controllers.product_controller import ProductController
from src.contexts.inventory.repositories.product_repository import ProductRepository
from src.contexts.inventory.services.product_service import ProductService
from src.shared.infrastructure.database import get_db_session

router = APIRouter(prefix="/inventory/products", tags=["Inventory"])


def get_product_controller(session: AsyncSession = Depends(get_db_session)) -> ProductController:
    """Dependency-injection chain: session -> repository -> service -> controller."""
    repository = ProductRepository(session)
    service = ProductService(repository)
    return ProductController(service)


@router.post("", response_model=ProductResponse, status_code=201)
async def create_product(
    payload: ProductCreateRequest,
    controller: ProductController = Depends(get_product_controller),
) -> ProductResponse:
    return await controller.create_product(payload)


@router.get("/{product_id}", response_model=ProductResponse)
async def get_product(
    product_id: UUID,
    controller: ProductController = Depends(get_product_controller),
) -> ProductResponse:
    return await controller.get_product(product_id)


@router.get("", response_model=list[ProductResponse])
async def list_products(
    limit: int = 100,
    offset: int = 0,
    controller: ProductController = Depends(get_product_controller),
) -> list[ProductResponse]:
    return await controller.list_products(limit=limit, offset=offset)


@router.post("/{product_id}/reserve", response_model=ProductResponse)
async def reserve_stock(
    product_id: UUID,
    payload: StockAdjustmentRequest,
    controller: ProductController = Depends(get_product_controller),
) -> ProductResponse:
    return await controller.reserve_stock(product_id, payload.quantity)


@router.post("/{product_id}/restock", response_model=ProductResponse)
async def restock(
    product_id: UUID,
    payload: StockAdjustmentRequest,
    controller: ProductController = Depends(get_product_controller),
) -> ProductResponse:
    return await controller.restock(product_id, payload.quantity)
