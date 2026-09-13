import httpx
from fastapi import FastAPI, Request
from notification_shared.context import get_correlation_id
from notification_shared.middleware import CORRELATION_ID_HEADER, CorrelationIDMiddleware


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(CorrelationIDMiddleware)

    @app.get("/probe")
    async def probe(request: Request) -> dict:
        # The `Request` annotation is required: without it FastAPI treats
        # `request` as a mandatory query parameter instead of injecting the
        # request object, and every assertion below fails with a 422.
        return {
            "from_state": request.state.correlation_id,
            "from_context": get_correlation_id(),
        }

    return app


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=_app()), base_url="http://test")


async def test_an_absent_header_is_generated():
    async with _client() as client:
        response = await client.get("/probe")
    assert response.status_code == 200
    assert len(response.headers[CORRELATION_ID_HEADER]) == 36


async def test_a_supplied_header_is_preserved_and_echoed():
    async with _client() as client:
        response = await client.get("/probe", headers={CORRELATION_ID_HEADER: "corr-supplied"})
    assert response.headers[CORRELATION_ID_HEADER] == "corr-supplied"
    assert response.json()["from_state"] == "corr-supplied"


async def test_the_context_var_is_set_for_the_duration_of_the_request():
    async with _client() as client:
        response = await client.get("/probe", headers={CORRELATION_ID_HEADER: "corr-ctx"})
    assert response.json()["from_context"] == "corr-ctx"


async def test_two_requests_get_different_generated_ids():
    async with _client() as client:
        first = (await client.get("/probe")).headers[CORRELATION_ID_HEADER]
        second = (await client.get("/probe")).headers[CORRELATION_ID_HEADER]
    assert first != second
