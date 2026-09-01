"""
Orders bounded context — read-optimized projection (CQRS Week 7).

`orders_read_orders` is the **read model** for order queries. It is fully
denormalized for read traffic:

* one row per order, no join needed to render the API response (lines are
  stored inline as JSON),
* `customer_id` is indexed because "all orders for customer X" is a hot
  read-side query,
* `line_count` is materialized so pagination/count queries avoid loading the
  items payload.

It is maintained projectively: the `orders.order-created.persistence`
consumer populates it in the same transaction that writes the write model, and
the confirm/cancel command handlers keep its `status` in sync. `created_at`/
`updated_at` mirror the aggregate's lifecycle for display and sorting.
"""
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, JSON, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from src.shared.infrastructure.database import Base


class OrderReadModel(Base):
    __tablename__ = "orders_read_orders"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    customer_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    total_cents: Mapped[int] = mapped_column(Integer, nullable=False)
    line_count: Mapped[int] = mapped_column(Integer, nullable=False)
    items: Mapped[list] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)