"""Consumes notification.routed and delivers email.

Before any send, the shared rules apply in order (slice 2 spec section 3): a
recipient containing "fail" fails deterministically, and a reserved recipient
is always simulated. Everything else goes to the configured sender, simulated
or SMTP (ADR 0029, ADR 0030).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from uuid import uuid4

from app.models.email_delivery import DeliveryStatus, EmailDelivery
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.repositories.email_delivery import EmailDeliveryRepository
from app.senders import SMTP_REJECTED, EmailRejectedError, EmailSender, SimulatedSender
from notification_shared.context import set_correlation_id
from notification_shared.delivery import (
    SIMULATED_FAILURE,
    is_failure_recipient,
    is_reserved_recipient,
)
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


class RoutedConsumer:
    def __init__(
        self,
        *,
        session_factory,
        redis,
        consumer_name: str,
        poll_interval_ms: int = 500,
        delivery_latency_ms_max: int = 500,
        sender: EmailSender | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._deliveries = EmailDeliveryRepository()
        self._outbox = OutboxRepository(Outbox)
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms
        # Reserved recipients always use the simulated sender, whatever is configured.
        self._simulated = SimulatedSender(delivery_latency_ms_max)
        self._sender = sender or self._simulated

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
        """Returns None on success, or a permanent failure reason.

        Raises EmailUnavailableError for a transient failure: nothing is
        written, the message stays pending, PendingRecoverer retries it.
        """
        if is_failure_recipient(recipient):
            return SIMULATED_FAILURE
        reserved = is_reserved_recipient(Channel.EMAIL, recipient)
        sender = self._simulated if reserved else self._sender
        try:
            await sender.send(recipient, subject, body)
        except EmailRejectedError as exc:
            logger.warning("email rejected by the server: %s", exc)
            return SMTP_REJECTED
        return None
