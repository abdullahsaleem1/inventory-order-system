"""
Orders bounded context — routes layer.

RBAC:
  POST /orders              — any authenticated user
  GET  /orders/{order_id}   — any authenticated user
  POST /orders/{id}/confirm — ADMIN, MANAGER
  POST /orders/{id}/cancel  — ADMIN, MANAGER, STAFF
"""
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.identity.api.schemas import ErrorResponse
from src.contexts.orders.api.schemas import OrderCreateRequest, OrderResponse
from src.contexts.orders.controllers.order_controller import OrderController
from src.contexts.orders.repositories.order_repository import OrderRepository
from src.contexts.orders.services.order_service import OrderService
from src.core.auth import get_current_user, require_roles
from src.shared.infrastructure.database import get_db_session

router = APIRouter(prefix="/orders", tags=["Orders"])


def get_order_controller(session: AsyncSession = Depends(get_db_session)) -> OrderController:
    repository = OrderRepository(session)
    service = OrderService(repository)
    return OrderController(service)


@router.post(
    "",
    response_model=OrderResponse,
    status_code=201,
    summary="Create a new order",
    description="Requires any authenticated role.",
    dependencies=[Depends(get_current_user)],
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        422: {"model": ErrorResponse, "description": "Request validation failed"},
    },
)
async def create_order(
    payload: OrderCreateRequest,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.create_order(payload)


@router.get(
    "/{order_id}",
    response_model=OrderResponse,
    summary="Get an order by ID",
    description="Requires any authenticated role.",
    dependencies=[Depends(get_current_user)],
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        404: {"model": ErrorResponse, "description": "Order not found"},
    },
)
async def get_order(
    order_id: UUID,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.get_order(order_id)


@router.post(
    "/{order_id}/confirm",
    response_model=OrderResponse,
    dependencies=[Depends(require_roles("ADMIN", "MANAGER"))],
    summary="Confirm an order (ADMIN, MANAGER)",
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        403: {"model": ErrorResponse, "description": "Role not permitted (requires ADMIN or MANAGER)"},
        404: {"model": ErrorResponse, "description": "Order not found"},
        409: {"model": ErrorResponse, "description": "Order is not in a confirmable state"},
    },
)
async def confirm_order(
    order_id: UUID,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.confirm_order(order_id)


@router.post(
    "/{order_id}/cancel",
    response_model=OrderResponse,
    dependencies=[Depends(require_roles("ADMIN", "MANAGER", "STAFF"))],
    summary="Cancel an order (ADMIN, MANAGER, STAFF)",
    responses={
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        403: {"model": ErrorResponse, "description": "Role not permitted (requires ADMIN, MANAGER, or STAFF)"},
        404: {"model": ErrorResponse, "description": "Order not found"},
        409: {"model": ErrorResponse, "description": "Order is not in a cancellable state"},
    },
)
async def cancel_order(
    order_id: UUID,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.cancel_order(order_id)
