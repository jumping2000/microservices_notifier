"""Fixtures backed by real Postgres and Redis containers.

Container startup is session-scoped because it is slow; engines and clients are
function-scoped because pytest-asyncio runs each test in its own event loop and
a connection created in one loop cannot be reused in another.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator

import pytest
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as container:
        yield container.get_connection_url()


@pytest.fixture
async def engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest.fixture
async def make_schema(engine: AsyncEngine) -> AsyncIterator[Callable]:
    """Create a MetaData's tables and hand back a session factory.

    Each test brings its own DeclarativeBase, so the harness stays agnostic
    about which service is under test.
    """
    created = []

    async def _make(metadata) -> async_sessionmaker:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)
        created.append(metadata)
        return async_sessionmaker(engine, expire_on_commit=False)

    yield _make

    for metadata in reversed(created):
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer("redis:7-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
async def redis_client(redis_url: str) -> AsyncIterator[aioredis.Redis]:
    client = aioredis.from_url(redis_url)
    await client.flushall()
    yield client
    await client.aclose()
