import httpx
import pytest
from notification_shared.context import set_correlation_id
from notification_shared.exceptions import NotFoundError, ServiceUnavailableError
from notification_shared.http_client import ServiceClient
from notification_shared.middleware import CORRELATION_ID_HEADER


def _client(handler) -> ServiceClient:
    return ServiceClient(
        base_url="http://configuration-service:8000",
        transport=httpx.MockTransport(handler),
    )


async def test_a_200_returns_the_decoded_body():
    client = _client(lambda _r: httpx.Response(200, json={"name": "email", "enabled": True}))
    try:
        assert await client.get("/channels/email") == {"name": "email", "enabled": True}
    finally:
        await client.aclose()


async def test_a_404_raises_not_found():
    client = _client(lambda _r: httpx.Response(404, json={}))
    try:
        with pytest.raises(NotFoundError):
            await client.get("/channels/sms")
    finally:
        await client.aclose()


async def test_a_500_raises_service_unavailable():
    client = _client(lambda _r: httpx.Response(500, text="boom"))
    try:
        with pytest.raises(ServiceUnavailableError):
            await client.get("/channels/email")
    finally:
        await client.aclose()


async def test_a_422_raises_service_unavailable():
    """A non-404 4xx must not be handed back as if it were a successful body.

    Without this, `{"detail": [...]}` would be returned to `_decide`, which
    does `state["enabled"]` on it and raises an unhandled `KeyError` several
    frames away — safe only by accident.
    """
    client = _client(lambda _r: httpx.Response(422, json={"detail": ["bad payload"]}))
    try:
        with pytest.raises(ServiceUnavailableError):
            await client.get("/channels/email")
    finally:
        await client.aclose()


async def test_a_403_raises_service_unavailable():
    client = _client(lambda _r: httpx.Response(403, json={}))
    try:
        with pytest.raises(ServiceUnavailableError):
            await client.get("/channels/email")
    finally:
        await client.aclose()


async def test_a_timeout_raises_service_unavailable():
    def handler(request):
        raise httpx.ReadTimeout("too slow", request=request)

    client = _client(handler)
    try:
        with pytest.raises(ServiceUnavailableError):
            await client.get("/channels/email")
    finally:
        await client.aclose()


async def test_a_connect_error_raises_service_unavailable():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    client = _client(handler)
    try:
        with pytest.raises(ServiceUnavailableError):
            await client.get("/channels/email")
    finally:
        await client.aclose()


async def test_not_found_and_unavailable_stay_distinguishable():
    """Spec 3.18 — Routing Service branches on exactly this difference."""
    not_found = _client(lambda _r: httpx.Response(404, json={}))
    unavailable = _client(lambda _r: httpx.Response(503, text=""))
    try:
        with pytest.raises(NotFoundError):
            await not_found.get("/channels/sms")
        with pytest.raises(ServiceUnavailableError):
            await unavailable.get("/channels/email")
    finally:
        await not_found.aclose()
        await unavailable.aclose()


async def test_the_correlation_id_is_forwarded_downstream():
    seen = {}

    def handler(request):
        seen["header"] = request.headers.get(CORRELATION_ID_HEADER)
        return httpx.Response(200, json={})

    set_correlation_id("corr-forwarded")
    client = _client(handler)
    try:
        await client.get("/channels/email")
    finally:
        await client.aclose()
        set_correlation_id(None)
    assert seen["header"] == "corr-forwarded"


async def test_put_sends_the_json_body():
    seen = {}

    def handler(request):
        seen["body"] = request.content
        return httpx.Response(200, json={"name": "email", "enabled": False})

    client = _client(handler)
    try:
        result = await client.put("/channels/email", json={"enabled": False})
    finally:
        await client.aclose()
    assert b"false" in seen["body"]
    assert result["enabled"] is False
