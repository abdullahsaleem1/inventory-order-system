"""
Inventory worker handler (Week 6 — Async Processing, Phase 2).

Consumes `order.created` events and asynchronously deducts stock for every
line item. Runs inside scripts/worker.py — its own process/container, NOT the
API process.

Error classification (drives the consumer's retry + DLQ behaviour):
  * PermanentMessageError  — malformed payload, unknown product: retrying will
    never succeed, so the message goes straight to the DLQ.
  * Any other exception (e.g. InsufficientStockError, transient DB error) —
    treated as transient by the consumer and retried with exponential backoff,
    then dead-lettered once retries are exhausted. Insufficient stock is
    retryable because a restock may make the reservation succeed later.

Idempotency: AMQP delivery is at-least-once, so the same event may arrive
several times. The handler records a per-order row in `inventory_reservation_log`
inside the SAME transaction that mutates stock; a later redelivery sees the row
and skips — so a crash between stock-write and ack can never double-deduct.
"""
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.contexts.inventory.domain.product import Product
from src.contexts.inventory.events import StockDeductionIntent, deduction_intent_from_created_event
from src.contexts.inventory.repositories.inventory_reservation_repository import (
    InventoryReservationRepository,
)
from src.contexts.inventory.repositories.product_repository import ProductRepository
from src.shared.messaging.consumer import PermanentMessageError

logger = logging.getLogger("inventory.worker_handler")


class DeductInventoryHandler:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def handle(self, event) -> None:
        intent: StockDeductionIntent = deduction_intent_from_created_event(event)

        async with self._session_factory() as session:
            product_repo = ProductRepository(session)
            reservation_repo = InventoryReservationRepository(session)

            if await reservation_repo.exists(intent.order_id):
                # At-least-once delivery => duplicates are expected, not errors.
                logger.info(
                    "stock_deduction_duplicate_skipped",
                    extra={"event_id": str(intent.event_id), "order_id": str(intent.order_id)},
                )
                return

            try:
                for line in intent.lines:
                    product = await product_repo.get_by_id(line.product_id)
                    if product is None:
                        # Retrying won't conjure a product into existence.
                        raise PermanentMessageError(
                            f"product {line.product_id} not found for order {intent.order_id}"
                        )
                    self._reserve(product, line.quantity)
                    await product_repo.update(product)
                await reservation_repo.add(intent.order_id, intent.event_id)
                await session.commit()
            except PermanentMessageError:
                await session.rollback()
                raise
            except Exception:
                # Transient (e.g. insufficient stock) — rollback any partial
                # per-line mutations and let the consumer retry with backoff.
                await session.rollback()
                raise

        logger.info(
            "stock_deducted",
            extra={
                "event_id": str(intent.event_id),
                "order_id": str(intent.order_id),
                "line_count": len(intent.lines),
            },
        )

    @staticmethod
    def _reserve(product: Product, quantity: int) -> None:
        """Deduct stock, letting InsufficientStockError propagate as transient."""
        product.reserve_stock(quantity)
