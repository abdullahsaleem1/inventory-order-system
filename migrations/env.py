"""
Alembic environment configuration.

Uses the app's own async engine/settings (src.core.config) rather than a
separate hardcoded URL, so migrations always target whatever DATABASE_URL
is currently configured (local, Docker, CI, etc).

Imports every bounded context's ORM models so `Base.metadata` is fully
populated for autogenerate — each context still owns its own models file,
this is just where they get registered for migration purposes.
"""
import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from src.core.config import get_settings
from src.shared.infrastructure.database import Base

# Import all ORM models from every bounded context so they register with
# Base.metadata. Required for autogenerate to detect them.
from src.contexts.inventory.infrastructure import models as inventory_models  # noqa: F401
from src.contexts.orders.infrastructure import models as orders_models  # noqa: F401
from src.contexts.orders.infrastructure import read_models as order_read_models  # noqa: F401
from src.contexts.identity.infrastructure import models as identity_models  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

# Override the ini file's placeholder URL with the app's real configured URL.
settings = get_settings()
config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())