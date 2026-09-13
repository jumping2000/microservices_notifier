"""Consumes delivery results and closes the route.

Skips RoutingFailed: Routing Service published that event itself, having
already marked the route FAILED in the same transaction. Consuming it back
would be a loop in the topology. See spec correction 3.3.
"""

from __future__ import annotations

import asyncio
import logging

from app.models.processed_event import ProcessedEvent
from app.models.route import RouteStatus
from app.repositories.route import RouteRepository
from notification_shared.context import set_correlation_id
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.streams import RedisStreamConsumer

logger = logging.getLogger(__name__)

STREAMS = (Stream.DELIVERY_COMPLETED, Stream.DELIVERY_FAILED)
GROUP = ConsumerGroup.ROUTING_RESULTS
HANDLED = (EventType.DELIVERY_COMPLETED, EventType.DELIVERY_FAILED)


class ResultsConsumer:
    def __init__(
        self, *, session_factory, redis, consumer_name: str, poll_interval_ms: int = 500
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._routes = RouteRepository()
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms

    async def ensure_groups(self) -> None:
        for stream in STREAMS:
            await self._consumer.ensure_group(stream, GROUP)

    async def consume_once(self) -> int:
        acked = 0
        for stream in STREAMS:
            messages = await self._consumer.read(stream, GROUP, block_ms=self._poll_interval_ms)
            for message in messages:
                if await self._handle(message.envelope):
                    await self._consumer.ack(stream, GROUP, message.message_id)
                    acked += 1
        return acked

    async def run_forever(self) -> None:
        while True:
            try:
                await self.consume_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("routing results consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        if envelope.event_type not in HANDLED:
            logger.debug("our own routing failure, acking and skipping", extra=log_fields)
            return True

        if envelope.event_type is EventType.DELIVERY_COMPLETED:
            new_status = RouteStatus.COMPLETED.value
            fail_reason = None
        else:
            new_status = RouteStatus.FAILED.value
            fail_reason = envelope.payload.get("reason")

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                return True

            updated = await self._routes.set_status(
                session, envelope.aggregate_id, new_status, fail_reason
            )
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        if updated:
            logger.info("route reached %s", new_status, extra=log_fields)
        else:
            logger.debug("no route for this notification, acking", extra=log_fields)
        return True
