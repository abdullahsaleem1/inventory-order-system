"""CQRS read model + write-table read-index removal

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-01

CQRS Week 7 — introducing a read-optimized projection and a write-optimized write table:

1. `orders_read_orders` is the denormalized read model for order queries:
   one row per order with a customer_id index (the hot read-side query), the
   per-line items stored inline as JSON, and materialized counters so reads
   never need to join or aggregate.

2. The `customer_id` index on the write table (`orders_orders`) is removed.
   By-customer lookups now go through the read side; keeping an index that the
   write path never uses just slows down every INSERT by-maintenance. This is
   the "remove unnecessary read indexes from the write DB" optimization.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Read-model projection (orders context).
    op.create_table(
        "orders_read_orders",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("customer_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("total_cents", sa.Integer(), nullable=False),
        sa.Column("line_count", sa.Integer(), nullable=False),
        sa.Column("items", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_orders_read_orders_customer_id", "orders_read_orders", ["customer_id"]
    )

    # Write-optimization: drop the read-side index on the write table.
    op.drop_index("ix_orders_orders_customer_id", table_name="orders_orders")


def downgrade() -> None:
    # Restore the customer lookup index on the write table.
    op.create_index("ix_orders_orders_customer_id", "orders_orders", ["customer_id"])
    op.drop_table("orders_read_orders")