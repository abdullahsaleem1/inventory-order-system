"""
Consumer-group handler that projects order events onto the dedicated read
store (CQRS Week 8).

Runs in its own process (`scripts/read_projector.py`), NOT in the API. It
consumes:

  * `order.created` → full document upsert (the read store's initial view of
    the order),
  * `order.status.changed` → in-place status update (keeps the read store
    eventually consistent with confirm/cancel transitions on the write side).

Each operation is idempotent: duplicate delivery of the same event is a
no-op (upsert replaces the document, status update writes the same value).
"""
import json
from typing import Any

from src.core.logging_config import get_logger
from src.contexts.orders.events import order_from_created_event
from src.contexts.orders.repositories.order_read_repository import (
    record_from_order,
)
from src.contexts.orders.repositories.order_read_store_repository import (
    record_to_document,
)
from src.shared.messaging.consumer import PermanentMessageError
from src.shared.messaging.events import DomainEvent, EventTypes
from src.shared.readstore.base import OrderReadStore


class ProjectOrderToReadStoreHandler:
    """Upserts order documents into the dedicated read store from events."""

    def __init__(self, read_store: OrderReadStore) -> None:
        self._read_store = read_store
        self._logger = get_logger("orders.read_projector")

    async def handle(self, event: DomainEvent) -> None:
        if event.event_type == EventTypes.ORDER_CREATED:
            await self._project_created(event)
        elif event.event_type == EventTypes.ORDER_STATUS_CHANGED:
            await self._project_status_changed(event)
        else:
            raise PermanentMessageError(
                f"read-projector does not handle event type '{event.event_type}'"
            )

    # --- order.created ------------------------------------------------------

    async def _project_created(self, event: DomainEvent) -> None:
        order = order_from_created_event(event)  # validates payload; raises PermanentMessageError on bad data
        record = record_from_order(order)
        document = record_to_document(record, occurred_at=event.occurred_at)

        await self._read_store.upsert_order(document)
        self._logger.info(
            "read_store_projected",
            extra={
                "event_id": str(event.event_id),
                "order_id": str(order.id),
                "customer_id": str(order.customer_id),
                "total_cents": order.total_cents,
                "line_count": len(order.lines),
            },
        )

    # --- order.status.changed -----------------------------------------------

    async def _project_status_changed(self, event: DomainEvent) -> None:
        payload = event.payload
        order_id = str(payload.get("order_id", ""))
        status = str(payload.get("status", ""))
        if not order_id or not status:
            raise PermanentMessageError(
                "order.status.changed payload must contain order_id and status"
            )
        await self._read_store.update_status(order_id, status)
        self._logger.info(
            "read_store_status_synced",
            extra={
                "event_id": str(event.event_id),
                "order_id": order_id,
                "new_status": status,
            },
        )