"""
Inventory bounded context — infrastructure layer.
SQLAlchemy ORM models live here, separate from the pure domain model in
domain/product.py. The repository layer is responsible for translating
between the two.
"""
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Uuid, func
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from src.shared.infrastructure.database import Base


class ProductModel(Base):
    __tablename__ = "inventory_products"

    id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sku: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    price_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    quantity_on_hand: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class InventoryReservationLogModel(Base):
    """Idempotency log (Week 6) — one row per order whose stock was deducted.

    At-least-once AMQP delivery means the same `order.created` may be
    delivered more than once. Before deducting stock the worker looks up this
    table by `order_id`; if a row already exists the deduction is a no-op.
    The unique constraint makes the guard race-safe across worker replicas.
    """

    __tablename__ = "inventory_reservation_log"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), unique=True, index=True, nullable=False)
    event_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="RESERVED")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

