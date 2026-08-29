"""add inventory reservation log (worker idempotency)

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-29

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "inventory_reservation_log",
        sa.Column("id", sa.Uuid(as_uuid=True), primary_key=True),
        sa.Column("order_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.Uuid(as_uuid=True), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="RESERVED"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_inventory_reservation_log_order_id",
        "inventory_reservation_log",
        ["order_id"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_inventory_reservation_log_order_id", table_name="inventory_reservation_log")
    op.drop_table("inventory_reservation_log")
