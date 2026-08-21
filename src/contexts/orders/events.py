"""
Orders <-> events bridge: builds the `order.created` DomainEvent from an
Order aggregate and reconstructs the aggregate back from its event payload.

Keeping both directions in ONE module means the wire contract has a single
source of truth — publisher and consumer can never drift apart silently.
Malformed payloads raise PermanentMessageError so the consumer dead-letters
them instead of retrying forever.
"""
from uuid import UUID

from src.contexts.orders.domain.order import Order, OrderLine
from src.shared.messaging.consumer import PermanentMessageError
from src.shared.messaging.events import DomainEvent, EventTypes


def build_order_created_event(order: Order, *, correlation_id: str | None = None) -> DomainEvent:
    """Serialize an Order aggregate into an `order.created` DomainEvent."""
    return DomainEvent(
        event_type=EventTypes.ORDER_CREATED,
        correlation_id=correlation_id,
        payload={
            "order_id": str(order.id),
            "customer_id": str(order.customer_id),
            "status": order.status.value,
            "total_cents": order.total_cents,
            "lines": [
                {
                    "product_id": str(line.product_id),
                    "quantity": line.quantity,
                    "unit_price_cents": line.unit_price_cents,
                }
                for line in order.lines
            ],
        },
    )


def _require_str(payload: dict, key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise PermanentMessageError(f"payload field '{key}' must be a string")
    return value


def _require_int(payload: dict, key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PermanentMessageError(f"payload field '{key}' must be an integer")
    return value


def order_from_created_event(event: DomainEvent) -> Order:
    """Rebuild the Order aggregate from an `order.created` event payload."""
    if event.event_type != EventTypes.ORDER_CREATED:
        raise PermanentMessageError(f"unsupported event type '{event.event_type}'")
    payload = event.payload
    try:
        lines = [
            OrderLine(
                product_id=UUID(_require_str(line, "product_id")),
                quantity=_require_int(line, "quantity"),
                unit_price_cents=_require_int(line, "unit_price_cents"),
            )
            for line in payload.get("lines", [])
        ]
        if not lines:
            raise PermanentMessageError("payload field 'lines' must contain at least one line")
        for line in lines:
            if line.quantity <= 0 or line.unit_price_cents <= 0:
                raise PermanentMessageError("line 'quantity' and 'unit_price_cents' must be positive")
        return Order(
            id=UUID(_require_str(payload, "order_id")),
            customer_id=UUID(_require_str(payload, "customer_id")),
            lines=lines,
        )
    except (ValueError, TypeError) as exc:
        raise PermanentMessageError(f"malformed order.created payload: {exc}") from exc
