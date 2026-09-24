"""Consumes notification.routed and delivers email.

Delivery is simulated. Failure is deterministic rather than random so the
behaviour can be tested and demonstrated: a recipient containing "fail" fails.
See spec correction 3.8.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import UTC, datetime
from uuid import uuid4

from app.models.email_delivery import DeliveryStatus, EmailDelivery
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.repositories.email_delivery import EmailDeliveryRepository
from notification_shared.context import set_correlation_id
from notification_shared.events import (
    Channel,
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.outbox import OutboxRepository
from notification_shared.recovery import MAX_RETRIES_EXCEEDED
from notification_shared.streams import RedisStreamConsumer
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

STREAM = Stream.NOTIFICATION_ROUTED
GROUP = ConsumerGroup.EMAIL
FAILURE_MARKER = "fail"


class RoutedConsumer:
    def __init__(
        self,
        *,
        session_factory,
        redis,
        consumer_name: str,
        poll_interval_ms: int = 500,
        delivery_latency_ms_max: int = 500,
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._deliveries = EmailDeliveryRepository()
        self._outbox = OutboxRepository(Outbox)
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms
        self._latency_ms_max = delivery_latency_ms_max

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
                logger.exception("email consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        if envelope.payload.get("channel") != Channel.EMAIL:
            # Both delivery services read this stream; each handles its own
            # channel. Acking without recording anything is correct: this
            # event was never ours to process.
            logger.debug("not an email event, acking and skipping", extra=log_fields)
            return True

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                logger.debug("event already processed, acking", extra=log_fields)
                return True

        recipient = envelope.payload["recipient"]
        fail_reason = await self._deliver(
            recipient, envelope.payload.get("subject"), envelope.payload["body"]
        )
        delivered_at = datetime.now(UTC)

        async with self._session_factory() as session:
            await self._record_outcome(session, envelope, fail_reason, sent_at=delivered_at)
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        logger.info(
            "email delivered" if fail_reason is None else "email delivery failed",
            extra=log_fields,
        )
        return True

    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """PendingRecoverer hook after PENDING_MAX_RETRIES failed attempts.

        Records a FAILED delivery and publishes DeliveryFailed, so the
        notification closes as FAILED instead of waiting for the watchdog
        (ADR 0024). No send here: give_up must not fail for the reason handle
        did.
        """
        await self._record_outcome(session, envelope, MAX_RETRIES_EXCEEDED, sent_at=None)

    async def _record_outcome(
        self,
        session: AsyncSession,
        envelope: EventEnvelope,
        fail_reason: str | None,
        sent_at: datetime | None,
    ) -> None:
        """The delivery row, inserted already terminal (ADR 0022), and its
        outbox event. The caller owns the transaction."""
        recipient = envelope.payload["recipient"]
        delivery = EmailDelivery(
            id=uuid4(),
            notification_id=envelope.aggregate_id,
            recipient=recipient,
            status=(
                DeliveryStatus.DELIVERED.value
                if fail_reason is None
                else DeliveryStatus.FAILED.value
            ),
            fail_reason=fail_reason,
            sent_at=sent_at,
        )
        await self._deliveries.add(session, delivery)
        await session.flush()

        if fail_reason is None:
            await self._outbox.save(
                session,
                Stream.DELIVERY_COMPLETED,
                EventEnvelope.new(
                    event_type=EventType.DELIVERY_COMPLETED,
                    aggregate_id=envelope.aggregate_id,
                    payload={
                        "channel": Channel.EMAIL.value,
                        "delivery_id": str(delivery.id),
                        "recipient": recipient,
                        "delivered_at": sent_at.isoformat(),
                    },
                    correlation_id=envelope.correlation_id,
                ),
            )
        else:
            await self._outbox.save(
                session,
                Stream.DELIVERY_FAILED,
                EventEnvelope.new(
                    event_type=EventType.DELIVERY_FAILED,
                    aggregate_id=envelope.aggregate_id,
                    payload={
                        "channel": Channel.EMAIL.value,
                        "delivery_id": str(delivery.id),
                        "reason": fail_reason,
                    },
                    correlation_id=envelope.correlation_id,
                ),
            )

    async def _deliver(self, recipient: str, subject: str | None, body: str) -> str | None:
        """Simulated delivery. Returns None on success, or a failure reason."""
        logger.info("sending email to %s", recipient)
        if self._latency_ms_max:
            await asyncio.sleep(random.uniform(0, self._latency_ms_max) / 1000)
        if FAILURE_MARKER in recipient.lower():
            return "simulated_failure"
        return None
