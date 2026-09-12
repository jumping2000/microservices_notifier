import asyncio
import os

import app.models.outbox  # noqa: F401
import app.models.processed_event  # noqa: F401
import app.models.route  # noqa: F401  register the table on the metadata
from alembic import context
from app.models.base import Base
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

config = context.config
if not config.get_main_option("sqlalchemy.url", None):
    config.set_main_option("sqlalchemy.url", os.environ["DATABASE_URL"])

target_metadata = Base.metadata


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    context.configure(url=config.get_main_option("sqlalchemy.url"), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(run_async_migrations())
