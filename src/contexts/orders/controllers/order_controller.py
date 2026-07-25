from uuid import UUID

from fastapi import HTTPException, status

from src.contexts.orders.api.schemas import OrderCreateRequest, OrderLineResponse, OrderResponse
from src.contexts.orders.domain.order import Order, EmptyOrderError, InvalidOrderTransitionError
from src.contexts.orders.services.order_service import OrderNotFoundError, OrderService


class OrderController:
    def __init__(self, service: OrderService) -> None:
        self._service = service

    @staticmethod
    def _to_response(order: Order) -> OrderResponse:
        return OrderResponse(
            id=order.id,
            customer_id=order.customer_id,
            status=order.status.value,
            lines=[
                OrderLineResponse(
                    product_id=ln.product_id,
                    quantity=ln.quantity,
                    unit_price_cents=ln.unit_price_cents,
                    subtotal_cents=ln.subtotal_cents,
                )
                for ln in order.lines
            ],
            total_cents=order.total_cents,
        )

    async def create_order(self, payload: OrderCreateRequest) -> OrderResponse:
        order = await self._service.create_order(
            customer_id=payload.customer_id,
            lines=[line.model_dump() for line in payload.lines],
        )
        return self._to_response(order)

    async def get_order(self, order_id: UUID) -> OrderResponse:
        try:
            order = await self._service.get_order(order_id)
        except OrderNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return self._to_response(order)

    async def confirm_order(self, order_id: UUID) -> OrderResponse:
        try:
            order = await self._service.confirm_order(order_id)
        except OrderNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except (EmptyOrderError, InvalidOrderTransitionError) as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return self._to_response(order)

    async def cancel_order(self, order_id: UUID) -> OrderResponse:
        try:
            order = await self._service.cancel_order(order_id)
        except OrderNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except InvalidOrderTransitionError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return self._to_response(order)
