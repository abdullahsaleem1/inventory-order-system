from uuid import UUID

from fastapi import HTTPException, status

from src.contexts.orders.api.schemas import (
    OrderAcceptedResponse,
    OrderCreateRequest,
    OrderLineResponse,
    OrderResponse,
)
from src.contexts.orders.domain.order import Order, EmptyOrderError, InvalidOrderTransitionError
from src.contexts.orders.events import build_order_created_event
from src.contexts.orders.services.order_service import OrderNotFoundError, OrderService
from src.shared.exceptions import ServiceUnavailableError
from src.shared.messaging.publisher import EventPublishError, EventPublisher


class OrderController:
    def __init__(self, service: OrderService, publisher: EventPublisher) -> None:
        self._service = service
        self._publisher = publisher

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

    async def create_order(self, payload: OrderCreateRequest, *, correlation_id: str | None) -> OrderAcceptedResponse:
        """Event-driven creation (Week 5): validate + build the aggregate, then
        publish `order.created` — NO synchronous DB write. Persistence happens
        asynchronously in the orders.order-created.persistence consumer group.
        """
        order = self._service.build_order(
            customer_id=payload.customer_id,
            lines=[line.model_dump() for line in payload.lines],
        )
        event = build_order_created_event(order, correlation_id=correlation_id)
        try:
            await self._publisher.publish(event)
        except EventPublishError as exc:
            raise ServiceUnavailableError(
                "Order could not be accepted: event broker did not confirm publication",
                code="EVENT_PUBLISH_FAILED",
            ) from exc
        return self._to_accepted_response(order, event.event_id)

    @staticmethod
    def _to_accepted_response(order: Order, event_id: UUID) -> OrderAcceptedResponse:
        return OrderAcceptedResponse(
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
            event_id=event_id,
        )

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
