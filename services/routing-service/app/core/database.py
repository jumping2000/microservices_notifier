"""Engine and session lifecycle.

The engine lives on `app.state` rather than at module scope so tests can inject
their own, and so the process does not open connections at import time.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request


class Database:
    def __init__(self, url: str) -> None:
        self._engine = create_async_engine(url, pool_pre_ping=True)
        self.session_factory = async_sessionmaker(self._engine, expire_on_commit=False)

    async def ping(self) -> bool:
        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return True

    async def dispose(self) -> None:
        await self._engine.dispose()


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.db.session_factory() as session:
        yield session
