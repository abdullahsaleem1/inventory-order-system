"""
Inventory <-> events bridge (Week 6 — Async Processing, Phase 2).

Reconstructs an inventory *deduction intent* from the `order.created` event
published by the Orders context. The inventory worker consumes that event and,
for every line, deducts the ordered quantity from the matching product.

This module is the single source of truth for the cross-context contract that
the worker depends on. Malformed payloads raise PermanentMessageError so the
consumer dead-letters them instead of burning retries — see the worker tests in
tests/messaging/test_inventory_worker.py.
"""
from dataclasses import dataclass
from uuid import UUID

from src.shared.messaging.consumer import PermanentMessageError
from src.shared.messaging.events import DomainEvent, EventTypes


@dataclass(frozen=True)
class StockDeductionLine:
    product_id: UUID
    quantity: int


@dataclass(frozen=True)
class StockDeductionIntent:
    order_id: UUID
    event_id: UUID
    lines: list[StockDeductionLine]

    @property
    def is_empty(self) -> bool:
        return not self.lines


def _require_uuid(field_name: str, value) -> UUID:
    if not isinstance(value, str):
        raise PermanentMessageError(f"payload field '{field_name}' must be a string")
    try:
        return UUID(value)
    except ValueError as exc:
        raise PermanentMessageError(f"payload field '{field_name}' is not a valid UUID") from exc


def deduction_intent_from_created_event(event: DomainEvent) -> StockDeductionIntent:
    """Extract + validate the stock-deduction intent from an `order.created` event."""
    if event.event_type != EventTypes.ORDER_CREATED:
        raise PermanentMessageError(f"unsupported event type '{event.event_type}'")
    payload = event.payload
    raw_lines = payload.get("lines")
    if not isinstance(raw_lines, list) or not raw_lines:
        raise PermanentMessageError("payload field 'lines' must be a non-empty list")

    lines: list[StockDeductionLine] = []
    for line in raw_lines:
        if not isinstance(line, dict):
            raise PermanentMessageError("each line must be a JSON object")
        quantity = line.get("quantity")
        if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity <= 0:
            raise PermanentMessageError("line 'quantity' must be a positive integer")
        lines.append(
            StockDeductionLine(product_id=_require_uuid("product_id", line.get("product_id")), quantity=quantity)
        )

    return StockDeductionIntent(
        order_id=_require_uuid("order_id", payload.get("order_id")),
        event_id=event.event_id,
        lines=lines,
    )
