"""add refresh tokens and access-token blocklist

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-15

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "identity_refresh_tokens",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("token_hash", sa.String(length=64), nullable=False),
        sa.Column("token_family", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_identity_refresh_tokens_token_hash",
        "identity_refresh_tokens",
        ["token_hash"],
        unique=True,
    )
    op.create_index(
        "ix_identity_refresh_tokens_user_id",
        "identity_refresh_tokens",
        ["user_id"],
    )

    op.create_table(
        "identity_blacklisted_tokens",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("jti", sa.String(length=36), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("blacklisted_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "ix_identity_blacklisted_tokens_jti",
        "identity_blacklisted_tokens",
        ["jti"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_table("identity_blacklisted_tokens")
    op.drop_table("identity_refresh_tokens")
