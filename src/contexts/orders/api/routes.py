"""
Orders bounded context — routes layer.

RBAC:
   POST /orders              — any authenticated user (publishes order.created)
   GET  /orders/{order_id}   — any authenticated user
   POST /orders/{id}/confirm — ADMIN, MANAGER
   POST /orders/{id}/cancel  — ADMIN, MANAGER, STAFF
"""
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.identity.api.schemas import ErrorResponse
from src.contexts.orders.api.schemas import (
    OrderAcceptedResponse,
    OrderCreateRequest,
    OrderResponse,
)
from src.contexts.orders.controllers.order_controller import OrderController
from src.contexts.orders.repositories.order_repository import OrderRepository
from src.contexts.orders.services.order_service import OrderService
from src.core.auth import get_current_user, require_roles
from src.shared.infrastructure.database import get_db_session
from src.shared.messaging.provider import get_event_publisher

router = APIRouter(prefix="/orders", tags=["Orders"])


def get_order_controller(
    session: AsyncSession = Depends(get_db_session),
    publisher=Depends(get_event_publisher),
) -> OrderController:
    repository = OrderRepository(session)
    service = OrderService(repository)
    return OrderController(service, publisher)


@router.post(
    "",
    response_model=OrderAcceptedResponse,
    status_code=202,
    summary="Create a new order (event-driven)",
    description=(
        "Validates the request, builds the order aggregate, publishes an "
        "`order.created` event to RabbitMQ and returns **202 Accepted**. "
        "No synchronous DB write happens here — persistence is performed "
        "asynchronously by the orders.order-created.persistence consumer."
    ),
    dependencies=[Depends(get_current_user)],
    responses={
        202: {"model": OrderAcceptedResponse, "description": "Order accepted; `order.created` event published"},
        401: {"model": ErrorResponse, "description": "Missing or invalid access token"},
        422: {"model": ErrorResponse, "description": "Request validation failed (no event published)"},
        503: {"model": ErrorResponse, "description": "Event broker unavailable or publication not confirmed"},
    },
)
async def create_order(
    payload: OrderCreateRequest,
    request: Request,
    controller: OrderController = Depends(get_order_controller),
) -> OrderAcceptedResponse:
    correlation_id = getattr(request.state, "request_id", None)
    return await controller.create_order(payload, correlation_id=correlation_id)


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
