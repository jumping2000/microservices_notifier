"""Fixtures for tests that run against a live docker compose stack.

Bring the stack up first:

    docker compose up --build -d --wait
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import httpx
import pytest

NOTIFICATION_URL = os.environ.get("NOTIFICATION_URL", "http://localhost:8001")
CONFIGURATION_URL = os.environ.get("CONFIGURATION_URL", "http://localhost:8003")
TERMINAL = {"COMPLETED", "FAILED"}


@pytest.fixture
async def notifications() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=NOTIFICATION_URL, timeout=10.0) as client:
        yield client


@pytest.fixture
async def configuration() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=CONFIGURATION_URL, timeout=10.0) as client:
        yield client


@pytest.fixture
async def email_enabled(configuration: httpx.AsyncClient) -> AsyncIterator[None]:
    """Leave the email channel enabled however the test ends."""
    await configuration.put("/channels/email", json={"enabled": True})
    yield
    await configuration.put("/channels/email", json={"enabled": True})


@pytest.fixture
def settle():
    """Poll the read model until the status is terminal.

    Exposed as a fixture rather than a module-level helper so the tests need no
    import from conftest, which is not reliably importable.
    """

    async def _settle(
        client: httpx.AsyncClient, notification_id: str, timeout_s: float = 30.0
    ) -> dict:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        seen: list[str] = []
        while loop.time() < deadline:
            body = (await client.get(f"/notifications/{notification_id}")).json()
            if not seen or seen[-1] != body["status"]:
                seen.append(body["status"])
            if body["status"] in TERMINAL:
                body["_observed"] = seen
                return body
            await asyncio.sleep(0.25)
        raise AssertionError(
            f"notification {notification_id} did not settle in {timeout_s}s; saw {seen}"
        )

    return _settle
