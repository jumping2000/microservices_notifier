import json
from contextlib import asynccontextmanager

import httpx
import pytest
from app.core.config import Settings
from app.main import create_app

NOTIFICATION = "http://notification-service:8000"
CONFIGURATION = "http://configuration-service:8000"


class Downstream:
    """MockTransport handler standing in for both downstream services."""

    def __init__(self, respond=None, error: Exception | None = None) -> None:
        self.calls: list[httpx.Request] = []
        self.respond = respond
        self.error = error

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.error:
            raise self.error
        if self.respond:
            return self.respond(request)
        return httpx.Response(200, json={"ok": True})


@asynccontextmanager
async def gateway(downstream: Downstream):
    app = create_app(Settings(_env_file=None), transport=httpx.MockTransport(downstream))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway"
        ) as client:
            yield client


@pytest.mark.parametrize(
    ("method", "path", "body", "expected_url"),
    [
        ("POST", "/api/v1/notifications", {"channel": "email"}, f"{NOTIFICATION}/notifications"),
        ("GET", "/api/v1/notifications/abc", None, f"{NOTIFICATION}/notifications/abc"),
        ("GET", "/api/v1/notifications", None, f"{NOTIFICATION}/notifications"),
        ("GET", "/api/v1/channels", None, f"{CONFIGURATION}/channels"),
        ("PUT", "/api/v1/channels/email", {"enabled": False}, f"{CONFIGURATION}/channels/email"),
    ],
)
async def test_each_route_forwards_to_its_service(method, path, body, expected_url):
    downstream = Downstream()
    async with gateway(downstream) as client:
        response = await client.request(method, path, json=body)

    assert response.status_code == 200
    (request,) = downstream.calls
    assert request.method == method
    assert str(request.url) == expected_url
    if body is not None:
        assert json.loads(request.content) == body
        assert request.headers["content-type"] == "application/json"


async def test_repeated_query_parameters_are_forwarded_unchanged():
    """Review Focus 5."""
    downstream = Downstream()
    async with gateway(downstream) as client:
        await client.get("/api/v1/notifications?status=FAILED&status=CREATED&limit=5")

    assert downstream.calls[0].url.params.multi_items() == [
        ("status", "FAILED"),
        ("status", "CREATED"),
        ("limit", "5"),
    ]


async def test_the_client_host_header_is_not_forwarded():
    downstream = Downstream()
    async with gateway(downstream) as client:
        await client.get("/api/v1/channels")
    assert downstream.calls[0].headers["host"] == "configuration-service:8000"


async def test_a_correlation_id_is_generated_when_absent_and_sent_downstream():
    downstream = Downstream()
    async with gateway(downstream) as client:
        response = await client.get("/api/v1/channels")

    generated = response.headers["X-Correlation-ID"]
    assert generated
    assert downstream.calls[0].headers.get_list("x-correlation-id") == [generated]


async def test_a_client_correlation_id_is_propagated_exactly_once():
    downstream = Downstream()
    async with gateway(downstream) as client:
        response = await client.get("/api/v1/channels", headers={"X-Correlation-ID": "corr-gw"})

    assert response.headers["X-Correlation-ID"] == "corr-gw"
    assert downstream.calls[0].headers.get_list("x-correlation-id") == ["corr-gw"]


@pytest.mark.parametrize(
    ("status", "content_type", "content"),
    [
        (404, "application/json", b'{"error":{"code":"NOT_FOUND","message":"x"}}'),
        (422, "application/json", b'{"error":{"code":"VALIDATION_ERROR","message":"x"}}'),
        (500, "text/plain; charset=utf-8", b"Internal Server Error"),
    ],
)
async def test_downstream_errors_pass_through_unchanged(status, content_type, content):
    """Review Focus 5: including a non-JSON body."""
    downstream = Downstream(
        respond=lambda _r: httpx.Response(
            status, content=content, headers={"content-type": content_type}
        )
    )
    async with gateway(downstream) as client:
        response = await client.get("/api/v1/notifications/abc")

    assert response.status_code == status
    assert response.content == content
    assert response.headers["content-type"] == content_type


async def test_a_downstream_timeout_is_a_504():
    async with gateway(Downstream(error=httpx.ReadTimeout("slow"))) as client:
        response = await client.get("/api/v1/notifications")

    assert response.status_code == 504
    assert response.json()["error"]["code"] == "GATEWAY_TIMEOUT"


async def test_an_unreachable_downstream_is_a_502():
    async with gateway(Downstream(error=httpx.ConnectError("refused"))) as client:
        response = await client.post("/api/v1/notifications", json={"channel": "email"})

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "BAD_GATEWAY"


async def test_health_reports_only_the_gateway():
    downstream = Downstream()
    async with gateway(downstream) as client:
        response = await client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "UP", "service": "gateway"}
    assert downstream.calls == []
