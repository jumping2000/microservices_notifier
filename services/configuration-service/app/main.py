"""Configuration Service.

Runtime channel state, read by Routing Service once per event. Publishes no
events: configuration is read-model state, not a domain event stream.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from app.api.v1.router import router
from app.core.config import Settings
from app.core.database import Database
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from notification_shared.exceptions import ServiceError
from notification_shared.logging import configure_logging
from notification_shared.middleware import CorrelationIDMiddleware
from starlette.requests import Request


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.db = Database(settings.database_url)
        yield
        await app.state.db.dispose()

    app = FastAPI(
        title="Configuration Service",
        version=settings.service_version,
        lifespan=lifespan,
    )
    app.add_middleware(CorrelationIDMiddleware)
    app.include_router(router)

    @app.exception_handler(ServiceError)
    async def handle_service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code, content=exc.to_response().model_dump(mode="json")
        )

    return app
