"""
Orders bounded context — controller layer.

CQRS (Week 7): the controller holds a `CqrsBus` and translates HTTP concerns
(schema <-> response, exception mapping) only. All intent is dispatched as a
**command** (writes) or a **query** (reads) to the bus; the controller never
touches repositories or services directly.
"""
from uuid import UUID

from fastapi import HTTPException, status

from src.contexts.orders.api.schemas import (
    OrderAcceptedResponse,
    OrderCreateRequest,
    OrderLineResponse,
    OrderResponse,
)
from src.contexts.orders.commands import CancelOrderCommand, ConfirmOrderCommand, CreateOrderCommand
from src.contexts.orders.domain.order import EmptyOrderError, InvalidOrderTransitionError, Order
from src.contexts.orders.errors import OrderNotFoundError
from src.contexts.orders.queries import GetOrderQuery
from src.contexts.orders.repositories.order_read_repository import OrderReadRecord
from src.shared.cqrs import CqrsBus


class OrderController:
    def __init__(self, bus: CqrsBus) -> None:
        self._bus = bus

    @staticmethod
    def _to_response(order: Order | OrderReadRecord) -> OrderResponse:
        status_str = order.status.value if isinstance(order, Order) else order.status
        return OrderResponse(
            id=order.id,
            customer_id=order.customer_id,
            status=status_str,
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

    async def create_order(
        self, payload: OrderCreateRequest, *, correlation_id: str | None
    ) -> OrderAcceptedResponse:
        """Command: accept a new order via the event pipeline (202)."""
        result = await self._bus.dispatch_command(
            CreateOrderCommand(
                customer_id=payload.customer_id,
                lines=[line.model_dump() for line in payload.lines],
                correlation_id=correlation_id,
            )
        )
        return OrderAcceptedResponse(
            id=result.order.id,
            customer_id=result.order.customer_id,
            status=result.order.status.value,
            lines=[
                OrderLineResponse(
                    product_id=ln.product_id,
                    quantity=ln.quantity,
                    unit_price_cents=ln.unit_price_cents,
                    subtotal_cents=ln.subtotal_cents,
                )
                for ln in result.order.lines
            ],
            total_cents=result.order.total_cents,
            event_id=result.event_id,
        )

    async def get_order(self, order_id: UUID) -> OrderResponse:
        try:
            record = await self._bus.dispatch_query(GetOrderQuery(order_id))
        except OrderNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        return self._to_response(record)

    async def confirm_order(self, order_id: UUID, *, correlation_id: str | None = None) -> OrderResponse:
        try:
            order = await self._bus.dispatch_command(
                ConfirmOrderCommand(order_id, correlation_id=correlation_id)
            )
        except OrderNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except (EmptyOrderError, InvalidOrderTransitionError) as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return self._to_response(order)

    async def cancel_order(self, order_id: UUID, *, correlation_id: str | None = None) -> OrderResponse:
        try:
            order = await self._bus.dispatch_command(
                CancelOrderCommand(order_id, correlation_id=correlation_id)
            )
        except OrderNotFoundError as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except InvalidOrderTransitionError as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return self._to_response(order)