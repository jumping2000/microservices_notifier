"""Consumes notification.created and makes the routing decision.

The Configuration Service call happens outside the database transaction: a
synchronous REST call must not hold a transaction open. The decision it returns
then drives a single transaction containing the route row, the outbox row and
the idempotency mark.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.models.route import Route, RouteStatus
from app.repositories.route import RouteRepository
from notification_shared.context import set_correlation_id
from notification_shared.events import (
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.exceptions import NotFoundError, ServiceUnavailableError
from notification_shared.http_client import ServiceClient
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.outbox import OutboxRepository
from notification_shared.recovery import MAX_RETRIES_EXCEEDED
from notification_shared.streams import RedisStreamConsumer
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

STREAM = Stream.NOTIFICATION_CREATED
GROUP = ConsumerGroup.ROUTING


class NotificationCreatedConsumer:
    def __init__(
        self,
        *,
        session_factory,
        redis,
        consumer_name: str,
        configuration_client: ServiceClient,
        poll_interval_ms: int = 500,
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._config = configuration_client
        self._routes = RouteRepository()
        self._outbox = OutboxRepository(Outbox)
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms

    async def ensure_groups(self) -> None:
        await self._consumer.ensure_group(STREAM, GROUP)

    async def consume_once(self) -> int:
        """Read one batch. Returns how many messages were acked."""
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
                logger.exception("routing consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def handle(self, envelope: EventEnvelope) -> bool:
        """Returns True when the message should be acked."""
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                logger.debug("event already processed, acking", extra=log_fields)
                return True

        channel = envelope.payload["channel"]
        try:
            fail_reason = await self._decide(channel)
        except ServiceUnavailableError as exc:
            # Transient. Do not ack: the message stays pending and
            # PendingRecoverer retries it after PENDING_TIMEOUT_MS, giving up
            # after PENDING_MAX_RETRIES (slice 2 spec 2.3). An outage must
            # never be recorded as an ordinary routing failure.
            logger.warning(
                "configuration service unavailable, leaving message pending: %s",
                exc,
                extra=log_fields,
            )
            return False

        async with self._session_factory() as session:
            await self._record_decision(session, envelope, fail_reason)
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        logger.info(
            "routed notification" if fail_reason is None else "routing failed",
            extra=log_fields,
        )
        return True

    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """PendingRecoverer hook after PENDING_MAX_RETRIES failed attempts.

        Records the route as FAILED and publishes RoutingFailed, so the
        notification goes CREATED -> FAILED instead of staying CREATED forever
        (ADR 0024). No Configuration Service call: give_up must not fail for
        the reason handle did.
        """
        await self._record_decision(session, envelope, MAX_RETRIES_EXCEEDED)

    async def _record_decision(
        self, session: AsyncSession, envelope: EventEnvelope, fail_reason: str | None
    ) -> None:
        """The route row and its outbox event. The caller owns the transaction."""
        # give_up must not fail for the reason handle did (spec 2.6): a
        # malformed payload missing its channel is the poison message the
        # retry cap protects against (docs/patterns.md). The success branch
        # below is unreachable with a missing channel, because handle's
        # _decide already required it.
        channel = envelope.payload.get("channel", "unknown")
        route = Route(
            id=uuid4(),
            notification_id=envelope.aggregate_id,
            channel=channel,
            status=(
                RouteStatus.PROCESSING.value if fail_reason is None else RouteStatus.FAILED.value
            ),
            fail_reason=fail_reason,
        )
        await self._routes.add(session, route)
        await session.flush()

        if fail_reason is None:
            await self._outbox.save(
                session,
                Stream.NOTIFICATION_ROUTED,
                EventEnvelope.new(
                    event_type=EventType.NOTIFICATION_ROUTED,
                    aggregate_id=envelope.aggregate_id,
                    payload={
                        # Forwarded from NotificationCreated: the delivery
                        # services cannot read this notification from
                        # another service's database. See spec 3.20.
                        "channel": channel,
                        "recipient": envelope.payload["recipient"],
                        "subject": envelope.payload.get("subject"),
                        "body": envelope.payload["body"],
                        "route_id": str(route.id),
                    },
                    correlation_id=envelope.correlation_id,
                ),
            )
        else:
            await self._outbox.save(
                session,
                Stream.DELIVERY_FAILED,
                EventEnvelope.new(
                    event_type=EventType.ROUTING_FAILED,
                    aggregate_id=envelope.aggregate_id,
                    payload={"channel": channel, "route_id": str(route.id), "reason": fail_reason},
                    correlation_id=envelope.correlation_id,
                ),
            )

    async def _decide(self, channel: str) -> str | None:
        """None means routable; a string is the RoutingFailed reason.

        A 404 is permanent (the channel does not exist). A timeout or 5xx
        raises ServiceUnavailableError and is handled by the caller as
        transient. See spec 3.18.
        """
        try:
            state = await self._config.get(f"/channels/{channel}")
        except NotFoundError:
            return "unknown_channel"
        return None if state["enabled"] else "channel_disabled"
