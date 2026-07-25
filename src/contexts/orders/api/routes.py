from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.contexts.orders.api.schemas import OrderCreateRequest, OrderResponse
from src.contexts.orders.controllers.order_controller import OrderController
from src.contexts.orders.repositories.order_repository import OrderRepository
from src.contexts.orders.services.order_service import OrderService
from src.shared.infrastructure.database import get_db_session

router = APIRouter(prefix="/orders", tags=["Orders"])


def get_order_controller(session: AsyncSession = Depends(get_db_session)) -> OrderController:
    repository = OrderRepository(session)
    service = OrderService(repository)
    return OrderController(service)


@router.post("", response_model=OrderResponse, status_code=201)
async def create_order(
    payload: OrderCreateRequest,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.create_order(payload)


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: UUID,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.get_order(order_id)


@router.post("/{order_id}/confirm", response_model=OrderResponse)
async def confirm_order(
    order_id: UUID,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.confirm_order(order_id)


@router.post("/{order_id}/cancel", response_model=OrderResponse)
async def cancel_order(
    order_id: UUID,
    controller: OrderController = Depends(get_order_controller),
) -> OrderResponse:
    return await controller.cancel_order(order_id)
