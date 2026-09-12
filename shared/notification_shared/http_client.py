"""HTTPX wrapper for the few remaining synchronous service calls.

404 and 5xx are mapped to different exception types on purpose: a missing
channel is a permanent routing failure, an unreachable Configuration Service
is transient. See spec correction 3.18.
"""

from __future__ import annotations

from typing import Any

import httpx

from notification_shared.context import get_correlation_id
from notification_shared.exceptions import NotFoundError, ServiceUnavailableError
from notification_shared.middleware import CORRELATION_ID_HEADER


class ServiceClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport
        )

    async def get(self, path: str) -> dict[str, Any]:
        return await self._send("GET", path)

    async def put(self, path: str, json: dict[str, Any]) -> dict[str, Any]:
        return await self._send("PUT", path, json=json)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _send(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        headers = {}
        correlation_id = get_correlation_id()
        if correlation_id:
            headers[CORRELATION_ID_HEADER] = correlation_id

        try:
            response = await self._client.request(method, path, headers=headers, **kwargs)
        except httpx.TimeoutException as exc:
            raise ServiceUnavailableError(f"{method} {path} timed out") from exc
        except httpx.TransportError as exc:
            raise ServiceUnavailableError(f"{method} {path} failed: {exc}") from exc

        if response.status_code == 404:
            raise NotFoundError(f"{method} {path} returned 404")
        if response.status_code >= 500:
            raise ServiceUnavailableError(
                f"{method} {path} returned {response.status_code}"
            )
        return response.json()
