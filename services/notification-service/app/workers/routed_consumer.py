"""Consumes notification.routed and marks the notification PROCESSING.

This is the only writer of PROCESSING. Without it the status the README
documents would be unreachable and slice 2's watchdog would never fire.
See spec correction 3.1.
"""

from __future__ import annotations

import asyncio
import logging

from app.models.notification import NotificationStatus
from app.models.processed_event import ProcessedEvent
from app.repositories.notification import NotificationRepository
from notification_shared.context import set_correlation_id
from notification_shared.events import ConsumerGroup, EventEnvelope, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.streams import RedisStreamConsumer
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

STREAM = Stream.NOTIFICATION_ROUTED
GROUP = ConsumerGroup.NOTIFICATION_ROUTED


class RoutedConsumer:
    def __init__(
        self, *, session_factory, redis, consumer_name: str, poll_interval_ms: int = 500
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._notifications = NotificationRepository()
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms

    async def ensure_groups(self) -> None:
        await self._consumer.ensure_group(STREAM, GROUP)

    async def consume_once(self) -> int:
        messages = await self._consumer.read(STREAM, GROUP, block_ms=self._poll_interval_ms)
        acked = 0
        for message in messages:
            if await self.handle(message.envelope):
                await self._consumer.ack(STREAM, GROUP, message.message_id)
                acked += 1
        return acked

    async def run_forever(self) -> None:
        while True:
            try:
                await self.consume_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("routed consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                return True

            updated = await self._notifications.advance_status(
                session,
                envelope.aggregate_id,
                NotificationStatus.PROCESSING.value,
                allowed_from=[NotificationStatus.CREATED.value],
            )
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        if updated:
            logger.info("notification is processing", extra=log_fields)
        else:
            # Already past CREATED: the delivery result arrived first, or this
            # notification is gone. Either way the event is fully handled.
            logger.debug("no CREATED row to advance, acking", extra=log_fields)
        return True

    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """PendingRecoverer hook after PENDING_MAX_RETRIES failed attempts.

        Nothing to write or publish: this service owns the notification, so
        there is no one to tell. A notification left PROCESSING is closed by
        the watchdog (slice 2 spec 2.6).
        """
