"""
Consumer-group handler that persists `order.created` events into the orders
write model (PostgreSQL in production) and maintains the CQRS read projection.

Runs inside scripts/consume_orders.py (its own process/container), NOT inside
the API process. Because delivery is at-least-once, this handler is
**idempotent**: if the order already exists it logs `event_duplicate_skipped`
and returns success so the broker can safely ack.

Write/read (CQRS Week 7): the handler writes the normalized aggregate through
`OrderWriteRepository` AND upserts the denormalized read model
(`orders_read_orders`) through `OrderReadRepository` — both in the SAME
transaction, so the projection never lags for the create path. Confirm/cancel
command handlers keep the projection's `status` in sync for later transitions.
"""
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.contexts.orders.events import order_from_created_event
from src.contexts.orders.repositories.order_read_repository import OrderReadRepository
from src.contexts.orders.repositories.order_write_repository import OrderWriteRepository
from src.core.logging_config import get_logger


class PersistOrderCreatedHandler:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._logger = get_logger("orders.persistence_handler")

    async def handle(self, event) -> None:
        order = order_from_created_event(event)  # raises PermanentMessageError on bad payloads

        log_common: dict[str, Any] = {
            "event_id": str(event.event_id),
            "order_id": str(order.id),
            "correlation_id": event.correlation_id,
        }
        async with self._session_factory() as session:
            write_repo = OrderWriteRepository(session)
            read_repo = OrderReadRepository(session)
            existing = await write_repo.get_by_id(order.id)
            if existing is not None:
                # At-least-once delivery => duplicates are expected, not errors.
                self._logger.info("event_duplicate_skipped", extra=log_common)
                return
            try:
                await write_repo.add(order)
                # Maintain the read projection transactionally with the write.
                await read_repo.upsert_from(order, created_at=event.occurred_at)
                await session.commit()
            except IntegrityError:
                # Week 11: the check-then-act above is not atomic, so two
                # concurrent redeliveries can both observe "missing" and race on
                # the primary key. The losing INSERT raises IntegrityError, which
                # is not a PermanentMessageError — the consumer therefore counted
                # it as transient, burned all three retries and dead-lettered an
                # order that had actually been persisted correctly. Verify the row
                # really is there and treat it as the duplicate it is.
                await session.rollback()
                if await write_repo.get_by_id(order.id) is not None:
                    self._logger.info(
                        "event_duplicate_skipped",
                        extra={**log_common, "reason": "concurrent_duplicate"},
                    )
                    return
                raise

        self._logger.info(
            "order_persisted",
            extra={
                **log_common,
                "total_cents": order.total_cents,
                "line_count": len(order.lines),
            },
        )