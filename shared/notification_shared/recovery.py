"""Recovery of stranded stream entries.

A consumer that crashes, or whose handler fails, leaves its entry pending in
the group. Consumer names are container hostnames and change on restart, so
those entries can only be found by idle time and claimed (slice 2 spec 2.1).

Shared, like OutboxPublisher, because the mechanics are identical everywhere.
The consumers themselves stay explicit: the recoverer only calls their public
`handle` — the same code as the normal read path — and, on the last attempt,
their `give_up` (ADR 0024, ADR 0025).

Only the recoverer counts attempts. A failure on the normal read path just
leaves the entry pending.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from notification_shared.context import set_correlation_id
from notification_shared.events import EventEnvelope
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.streams import ClaimedMessage, RedisStreamConsumer

logger = logging.getLogger(__name__)

MAX_RETRIES_EXCEEDED = "max_retries_exceeded"


class RecoverableConsumer(Protocol):
    async def handle(self, envelope: EventEnvelope) -> bool: ...

    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None: ...


def should_give_up(fail_count: int, max_retries: int) -> bool:
    return fail_count >= max_retries


class PendingRecoverer:
    def __init__(
        self,
        *,
        redis,
        consumer_name: str,
        session_factory,
        idempotency: IdempotencyRepository,
        pending_timeout_ms: int = 30000,
        max_retries: int = 3,
        poll_interval_ms: int = 5000,
        batch_size: int = 10,
    ) -> None:
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._session_factory = session_factory
        self._idempotency = idempotency
        self._pending_timeout_ms = pending_timeout_ms
        self._max_retries = max_retries
        self._poll_interval_ms = poll_interval_ms
        self._batch_size = batch_size
        self._registrations: list[tuple[str, str, RecoverableConsumer]] = []

    def register(self, stream: str, group: str, consumer: RecoverableConsumer) -> None:
        self._registrations.append((stream, group, consumer))

    async def recover_once(self) -> int:
        """One sweep over every registration. Returns how many entries were acked."""
        acked = 0
        for stream, group, consumer in self._registrations:
            pending = await self._consumer.get_pending(
                stream, group, self._pending_timeout_ms, self._batch_size
            )
            if not pending:
                continue
            claimed = await self._consumer.claim(
                stream, group, self._pending_timeout_ms, [entry.message_id for entry in pending]
            )
            for message in claimed:
                if await self._recover(stream, group, consumer, message):
                    await self._consumer.ack(stream, group, message.message_id)
                    acked += 1
        set_correlation_id(None)
        return acked

    async def run_forever(self) -> None:
        while True:
            try:
                await self.recover_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("recovery iteration failed")
            await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _recover(
        self, stream: str, group: str, consumer: RecoverableConsumer, message: ClaimedMessage
    ) -> bool:
        """Returns True when the entry should be acked."""
        if message.envelope is None:
            logger.error(
                "unparseable stream entry %s, acking and discarding",
                message.message_id,
                extra={"stream": stream, "consumer_group": group},
            )
            return True

        envelope = message.envelope
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": group,
            "stream": stream,
        }

        try:
            if await consumer.handle(envelope):
                logger.info("recovered pending entry", extra=log_fields)
                return True
        except Exception:
            logger.exception("recovery attempt failed", extra=log_fields)

        async with self._session_factory() as session:
            fail_count = await self._idempotency.increment_fail_count(
                session, envelope.event_id, group
            )
            await session.commit()

        if fail_count is None:
            # The ledger row is already terminal (PROCESSED or
            # FAILED_PERMANENT): some other attempt already finished this
            # event. Ack without reopening it as FAILING.
            logger.info("event already terminal in the ledger, acking", extra=log_fields)
            return True

        if not should_give_up(fail_count, self._max_retries):
            logger.warning(
                "attempt %d of %d failed, leaving entry pending",
                fail_count,
                self._max_retries,
                extra=log_fields,
            )
            return False

        try:
            async with self._session_factory() as session:
                await consumer.give_up(session, envelope)
                await self._idempotency.mark_failed_permanent(session, envelope.event_id, group)
                await session.commit()
        except Exception:
            logger.exception("give_up failed, entry stays pending", extra=log_fields)
            return False

        logger.error("gave up after %d attempts", fail_count, extra=log_fields)
        return True
