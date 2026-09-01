"""
Inventory bounded context — routes layer.

Thin wiring only: defines HTTP endpoints and assembles the CQRS bus. No
business logic, no persistence logic here.

RBAC:
  POST   /inventory/products           — ADMIN, MANAGER
  GET    /inventory/products/{id}      — any authenticated user
  GET    /inventory/products           — any authenticated user
  POST   /inventory/products/{id}/reserve — ADMIN, MANAGER, STAFF
  POST   /inventory/products/{id}/restock — ADMIN, MANAGER
"""
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.identity.api.schemas import ErrorResponse
from src.contexts.inventory.api.schemas import (
    ProductCreateRequest,
    ProductResponse,
    StockAdjustmentRequest,
)
from src.contexts.inventory.command_handlers import (
    CreateProductCommandHandler,
    ReserveStockCommandHandler,
    RestockCommandHandler,
)
from src.contexts.inventory.commands import CreateProductCommand, ReserveStockCommand, RestockCommand
from src.contexts.inventory.controllers.product_controller import ProductController
from src.contexts.inventory.query_handlers import (
    GetProductQueryHandler,
    ListProductsQueryHandler,
)
from src.contexts.inventory.queries import GetProductQuery, ListProductsQuery
from src.contexts.inventory.repositories.product_read_repository import ProductReadRepository
from src.contexts.inventory.repositories.product_write_repository import ProductWriteRepository
from src.core.auth import get_current_user, require_roles
from src.shared.cqrs import CqrsBus
from src.shared.infrastructure.database import get_db_session
from src.shared.infrastructure.read_database import get_read_db_session

router = APIRouter(prefix="/inventory/products", tags=["Inventory"])


def get_product_controller(
    write_session: AsyncSession = Depends(get_db_session),
    read_session: AsyncSession = Depends(get_read_db_session),
) -> ProductController:
    """Dependency chain: sessions -> repositories -> command/query handlers ->
    CqrsBus -> controller."""
    write_repo = ProductWriteRepository(write_session)
    read_repo = ProductReadRepository(read_session)

    bus = (
        CqrsBus()
        # --- write side (commands) ---
        .register_command(CreateProductCommand, CreateProductCommandHandler(write_repo))
        .register_command(ReserveStockCommand, ReserveStockCommandHandler(write_repo))
        .register_command(RestockCommand, RestockCommandHandler(write_repo))
        # --- read side (queries) ---
        .register_query(GetProductQuery, GetProductQueryHandler(read_repo))
        .register_query(ListProductsQuery, ListProductsQueryHandler(read_repo))
    )
    return ProductController(bus)


@router.post(
    "",
    response_model=ProductResponse,
    status_code=201,
    dependencies=[Depends(require_roles("ADMIN", "MANAGER"))],
    summary="Create a new product (ADMIN, MANAGER)",
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        403: {"model": ErrorResponse, "description": "Role not permitted (requires ADMIN or MANAGER)"},
        409: {"model": ErrorResponse, "description": "Product with this SKU already exists"},
        422: {"model": ErrorResponse, "description": "Request validation failed"},
    },
)
async def create_product(
    payload: ProductCreateRequest,
    controller: ProductController = Depends(get_product_controller),
) -> ProductResponse:
    return await controller.create_product(payload)


@router.get(
    "/{product_id}",
    response_model=ProductResponse,
    summary="Get a product by ID",
    description="Requires any authenticated role.",
    dependencies=[Depends(get_current_user)],
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        404: {"model": ErrorResponse, "description": "Product not found"},
    },
)
async def get_product(
    product_id: UUID,
    controller: ProductController = Depends(get_product_controller),
) -> ProductResponse:
    return await controller.get_product(product_id)


@router.get(
    "",
    response_model=list[ProductResponse],
    summary="List products",
    description="Requires any authenticated role.",
    dependencies=[Depends(get_current_user)],
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
    },
)
async def list_products(
    limit: int = 100,
    offset: int = 0,
    controller: ProductController = Depends(get_product_controller),
) -> list[ProductResponse]:
    return await controller.list_products(limit=limit, offset=offset)


@router.post(
    "/{product_id}/reserve",
    response_model=ProductResponse,
    dependencies=[Depends(require_roles("ADMIN", "MANAGER", "STAFF"))],
    summary="Reserve stock (ADMIN, MANAGER, STAFF)",
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        403: {"model": ErrorResponse, "description": "Role not permitted (requires ADMIN, MANAGER, or STAFF)"},
        404: {"model": ErrorResponse, "description": "Product not found"},
        409: {"model": ErrorResponse, "description": "Insufficient stock to reserve"},
    },
)
async def reserve_stock(
    product_id: UUID,
    payload: StockAdjustmentRequest,
    controller: ProductController = Depends(get_product_controller),
) -> ProductResponse:
    return await controller.reserve_stock(product_id, payload.quantity)


@router.post(
    "/{product_id}/restock",
    response_model=ProductResponse,
    dependencies=[Depends(require_roles("ADMIN", "MANAGER"))],
    summary="Restock a product (ADMIN, MANAGER)",
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        403: {"model": ErrorResponse, "description": "Role not permitted (requires ADMIN or MANAGER)"},
        404: {"model": ErrorResponse, "description": "Product not found"},
    },
)
async def restock(
    product_id: UUID,
    payload: StockAdjustmentRequest,
    controller: ProductController = Depends(get_product_controller),
) -> ProductResponse:
    return await controller.restock(product_id, payload.quantity)