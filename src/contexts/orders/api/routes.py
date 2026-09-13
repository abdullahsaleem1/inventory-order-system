"""
Orders bounded context — routes layer.

RBAC:
   POST /orders              — any authenticated user (publishes order.created)
   GET  /orders/{order_id}   — any authenticated user
   POST /orders/{id}/confirm — ADMIN, MANAGER
   POST /orders/{id}/cancel  — ADMIN, MANAGER, STAFF

CQRS (Weeks 7-8) dependency chain: the write side resolves a write session;
the read side resolves the **dedicated read store** (Elasticsearch in Docker,
in-memory in tests) — never the write database. Confirm/cancel command
handlers additionally publish `order.status.changed` events so the
read-projector sync worker keeps that read store eventually consistent.
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
from src.contexts.orders.command_handlers import (
    CancelOrderCommandHandler,
    ConfirmOrderCommandHandler,
    CreateOrderCommandHandler,
)
from src.contexts.orders.commands import CancelOrderCommand, ConfirmOrderCommand, CreateOrderCommand
from src.contexts.orders.controllers.order_controller import OrderController
from src.contexts.orders.query_handlers import (
    GetOrderQueryHandler,
    ListOrdersByCustomerQueryHandler,
)
from src.contexts.orders.queries import GetOrderQuery, ListOrdersByCustomerQuery
from src.contexts.orders.repositories.order_read_repository import OrderReadRepository
from src.contexts.orders.repositories.order_read_store_repository import OrderReadStoreRepository
from src.contexts.orders.repositories.order_write_repository import OrderWriteRepository
from src.core.auth import get_current_user, require_roles
from src.shared.cqrs import CqrsBus
from src.shared.infrastructure.database import get_db_session
from src.shared.messaging.provider import get_event_publisher
from src.shared.readstore.base import OrderReadStore
from src.shared.readstore.factory import get_read_store

router = APIRouter(prefix="/orders", tags=["Orders"])


def get_order_controller(
    write_session: AsyncSession = Depends(get_db_session),
    read_store: OrderReadStore = Depends(get_read_store),
    publisher=Depends(get_event_publisher),
) -> OrderController:
    """Assemble the CQRS bus.

    Write side: commands operate on the write session (and keep the local
    Postgres projection in sync transactionally). Read side: query handlers
    read EXCLUSIVELY from the dedicated read store via the store-backed
    repository — eventual consistency is expected when the read projector lags.
    """
    write_repo = OrderWriteRepository(write_session)
    read_repo = OrderReadStoreRepository(read_store)

    bus = (
        CqrsBus()
        # --- write side (commands) ---
        .register_command(CreateOrderCommand, CreateOrderCommandHandler(publisher))
        .register_command(
            ConfirmOrderCommand,
            ConfirmOrderCommandHandler(
                write_repo,
                OrderReadRepository(write_session),  # same-transaction projection sync
                publisher,  # publishes order.status.changed for the read store
            ),
        )
        .register_command(
            CancelOrderCommand,
            CancelOrderCommandHandler(
                write_repo,
                OrderReadRepository(write_session),
                publisher,
            ),
        )
        # --- read side (queries) — dedicated read store only ---
        .register_query(GetOrderQuery, GetOrderQueryHandler(read_repo))
        .register_query(
            ListOrdersByCustomerQuery,
            ListOrdersByCustomerQueryHandler(read_repo),
        )
    )
    return OrderController(bus)


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
    description="Reads from the CQRS read model (orders_read_orders).",
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
    request: Request,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.confirm_order(
        order_id, correlation_id=getattr(request.state, "request_id", None)
    )


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
    request: Request,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.cancel_order(
        order_id, correlation_id=getattr(request.state, "request_id", None)
    )