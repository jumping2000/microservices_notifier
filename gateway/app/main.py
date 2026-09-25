"""API Gateway.

The single client entry point: a thin FastAPI reverse proxy over
notification-service and configuration-service (slice 2 spec section 5). No
database, no Redis, no business logic.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from app.api.v1.router import router
from app.core.config import Settings
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from notification_shared.exceptions import ServiceError
from notification_shared.logging import configure_logging
from notification_shared.middleware import CorrelationIDMiddleware
from starlette.requests import Request


def create_app(
    settings: Settings | None = None, transport: httpx.AsyncBaseTransport | None = None
) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.http = httpx.AsyncClient(
            timeout=settings.gateway_timeout_seconds, transport=transport
        )
        yield
        await app.state.http.aclose()

    app = FastAPI(title="API Gateway", version=settings.service_version, lifespan=lifespan)
    app.add_middleware(CorrelationIDMiddleware)
    app.include_router(router)

    @app.exception_handler(ServiceError)
    async def handle_service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code, content=exc.to_response().model_dump(mode="json")
        )

    return app
