from uuid import UUID

from src.contexts.orders.domain.order import Order, OrderLine
from src.contexts.orders.repositories.order_repository import OrderRepository


class OrderNotFoundError(Exception):
    pass


class OrderService:
    def __init__(self, repository: OrderRepository) -> None:
        self._repo = repository

    def build_order(self, customer_id: UUID, lines: list[dict]) -> Order:
        """Construct + validate an Order aggregate WITHOUT persisting it.

        Since Week 5, creation is event-driven: the API builds and validates
        the aggregate here, then publishes `order.created` — persistence
        happens asynchronously in the orders.order-created.persistence
        consumer group.
        """
        return Order(
            customer_id=customer_id,
            lines=[OrderLine(**line) for line in lines],
        )

    async def get_order(self, order_id: UUID) -> Order:
        order = await self._repo.get_by_id(order_id)
        if order is None:
            raise OrderNotFoundError(f"Order {order_id} not found")
        return order

    async def confirm_order(self, order_id: UUID) -> Order:
        order = await self.get_order(order_id)
        order.confirm()
        # NOTE: this is where, in the event-driven pipeline week, we'd
        # publish an OrderConfirmed event (RabbitMQ/Kafka) so Inventory
        # can reserve stock asynchronously instead of a direct call.
        return await self._repo.update_status(order)

    async def cancel_order(self, order_id: UUID) -> Order:
        order = await self.get_order(order_id)
        order.cancel()
        return await self._repo.update_status(order)
