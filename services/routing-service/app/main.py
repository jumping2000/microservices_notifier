"""Routing Service.

Decides where a notification goes. No REST endpoints for domain operations:
its whole job runs in background consumers, reacting to notification.created.
"""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from app.api.v1.router import router
from app.core.config import Settings
from app.core.database import Database
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.workers.notification_consumer import NotificationCreatedConsumer
from app.workers.results_consumer import ResultsConsumer
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from notification_shared.events import ConsumerGroup, Stream
from notification_shared.exceptions import ServiceError, ValidationFailedError
from notification_shared.http_client import ServiceClient
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.logging import configure_logging
from notification_shared.middleware import CorrelationIDMiddleware
from notification_shared.outbox import OutboxRepository
from notification_shared.publisher import OutboxPublisher
from notification_shared.recovery import PendingRecoverer
from notification_shared.streams import RedisStreamPublisher
from starlette.requests import Request


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.db = Database(settings.database_url)
        app.state.redis = aioredis.from_url(settings.redis_url)
        await app.state.redis.ping()
        app.state.config_client = ServiceClient(
            settings.configuration_service_url, timeout=settings.http_timeout_seconds
        )

        consumer = NotificationCreatedConsumer(
            session_factory=app.state.db.session_factory,
            redis=app.state.redis,
            consumer_name=socket.gethostname(),
            configuration_client=app.state.config_client,
            poll_interval_ms=settings.consumer_poll_interval_ms,
        )
        await consumer.ensure_groups()

        results = ResultsConsumer(
            session_factory=app.state.db.session_factory,
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-results",
            poll_interval_ms=settings.consumer_poll_interval_ms,
        )
        await results.ensure_groups()

        publisher = OutboxPublisher(
            session_factory=app.state.db.session_factory,
            repository=OutboxRepository(Outbox),
            publisher=RedisStreamPublisher(app.state.redis),
            poll_interval_ms=settings.outbox_poll_interval_ms,
            batch_size=settings.outbox_batch_size,
        )

        recoverer = PendingRecoverer(
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-recovery",
            session_factory=app.state.db.session_factory,
            idempotency=IdempotencyRepository(ProcessedEvent),
            pending_timeout_ms=settings.pending_timeout_ms,
            max_retries=settings.pending_max_retries,
            poll_interval_ms=settings.recovery_poll_interval_ms,
        )
        recoverer.register(Stream.NOTIFICATION_CREATED, ConsumerGroup.ROUTING, consumer)
        for stream in (Stream.DELIVERY_COMPLETED, Stream.DELIVERY_FAILED):
            recoverer.register(stream, ConsumerGroup.ROUTING_RESULTS, results)

        tasks = [
            asyncio.create_task(consumer.run_forever(), name="notification-consumer"),
            asyncio.create_task(results.run_forever(), name="results-consumer"),
            asyncio.create_task(publisher.run_forever(), name="outbox-publisher"),
            asyncio.create_task(recoverer.run_forever(), name="pending-recoverer"),
        ]

        yield

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await app.state.config_client.aclose()
        await app.state.redis.aclose()
        await app.state.db.dispose()

    app = FastAPI(title="Routing Service", version=settings.service_version, lifespan=lifespan)
    app.add_middleware(CorrelationIDMiddleware)
    app.include_router(router)

    @app.exception_handler(ServiceError)
    async def handle_service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code, content=exc.to_response().model_dump(mode="json")
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # Spec 6.5: validation failures use the common error model, not
        # FastAPI's default {"detail": [...]}.
        error = ValidationFailedError(str(exc.errors()))
        return JSONResponse(
            status_code=error.status_code,
            content=error.to_response().model_dump(mode="json"),
        )

    return app
