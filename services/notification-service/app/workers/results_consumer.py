"""Consumes delivery.completed and delivery.failed and closes the notification.

Unlike Routing Service's results consumer, this one *does* handle
RoutingFailed: a routing failure must fail the notification. See spec 3.3.
"""

from __future__ import annotations

import asyncio
import logging

from app.models.notification import NotificationStatus
from app.models.processed_event import ProcessedEvent
from app.repositories.notification import NotificationRepository
from notification_shared.context import set_correlation_id
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.streams import RedisStreamConsumer

logger = logging.getLogger(__name__)

STREAMS = (Stream.DELIVERY_COMPLETED, Stream.DELIVERY_FAILED)
GROUP = ConsumerGroup.NOTIFICATION_RESULTS
NON_TERMINAL = [NotificationStatus.CREATED.value, NotificationStatus.PROCESSING.value]


class ResultsConsumer:
    def __init__(
        self, *, session_factory, redis, consumer_name: str, poll_interval_ms: int = 500
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._notifications = NotificationRepository()
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms

    async def ensure_groups(self) -> None:
        for stream in STREAMS:
            await self._consumer.ensure_group(stream, GROUP)

    async def consume_once(self) -> int:
        acked = 0
        for stream in STREAMS:
            messages = await self._consumer.read(
                stream, GROUP, block_ms=self._poll_interval_ms
            )
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
                logger.exception("results consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        if envelope.event_type is EventType.DELIVERY_COMPLETED:
            new_status = NotificationStatus.COMPLETED.value
            fail_reason = None
        else:
            # DeliveryFailed and RoutingFailed both land here, both carry a reason.
            new_status = NotificationStatus.FAILED.value
            fail_reason = envelope.payload.get("reason")

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                return True

            updated = await self._notifications.advance_status(
                session,
                envelope.aggregate_id,
                new_status,
                allowed_from=NON_TERMINAL,
                fail_reason=fail_reason,
            )
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        if updated:
            logger.info("notification reached %s", new_status, extra=log_fields)
        else:
            logger.debug("notification already terminal or absent, acking", extra=log_fields)
        return True
