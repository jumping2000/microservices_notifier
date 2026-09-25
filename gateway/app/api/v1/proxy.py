"""Thin pass-through proxy (slice 2 spec 5.3).

Explicit routes rather than a catch-all, so the Gateway's Swagger lists exactly
what it exposes. No validation, no payload transformation, no knowledge of
channels: status, body and content-type come back exactly as the service sent
them.
"""

from __future__ import annotations

import httpx
from fastapi import APIRouter
from notification_shared.exceptions import BadGatewayError, GatewayTimeoutError
from notification_shared.middleware import CORRELATION_ID_HEADER
from starlette.requests import Request
from starlette.responses import Response

router = APIRouter(prefix="/api/v1", tags=["proxy"])

# Hop-by-hop headers describe this connection, not the request; the correlation
# header is re-added once below so it can never be sent twice.
_NOT_FORWARDED = {
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "keep-alive",
    "upgrade",
    "te",
    "trailer",
    "proxy-authorization",
    "proxy-authenticate",
    CORRELATION_ID_HEADER.lower(),
}


async def _forward(request: Request, base_url: str, path: str) -> Response:
    client: httpx.AsyncClient = request.app.state.http
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _NOT_FORWARDED}
    headers[CORRELATION_ID_HEADER] = request.state.correlation_id
    try:
        upstream = await client.request(
            request.method,
            f"{base_url}{path}",
            params=request.query_params.multi_items(),
            content=await request.body(),
            headers=headers,
        )
    except httpx.TimeoutException:
        raise GatewayTimeoutError(f"{request.method} {path} timed out") from None
    except httpx.TransportError:
        raise BadGatewayError(f"{request.method} {path}: service unreachable") from None
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type"),
    )


def _notifications(request: Request) -> str:
    return request.app.state.settings.notification_service_url


def _configuration(request: Request) -> str:
    return request.app.state.settings.configuration_service_url


@router.post("/notifications", summary="Submit a notification")
async def create_notification(request: Request) -> Response:
    return await _forward(request, _notifications(request), "/notifications")


@router.get("/notifications/{notification_id}", summary="Read notification status")
async def get_notification(notification_id: str, request: Request) -> Response:
    return await _forward(request, _notifications(request), f"/notifications/{notification_id}")


@router.get("/notifications", summary="List notifications")
async def list_notifications(request: Request) -> Response:
    return await _forward(request, _notifications(request), "/notifications")


@router.get("/channels", summary="List channels")
async def list_channels(request: Request) -> Response:
    return await _forward(request, _configuration(request), "/channels")


@router.put("/channels/{name}", summary="Enable or disable a channel")
async def set_channel(name: str, request: Request) -> Response:
    return await _forward(request, _configuration(request), f"/channels/{name}")
