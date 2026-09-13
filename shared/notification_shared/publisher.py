"""The outbox publisher loop.

Identical in every service, so it lives here rather than being copied four
times. Consumer handlers are not shared: they differ per service and reading
them is the point.
"""

from __future__ import annotations

import asyncio
import logging

from notification_shared.context import set_correlation_id
from notification_shared.events import EventEnvelope
from notification_shared.outbox import OutboxRepository
from notification_shared.streams import RedisStreamPublisher

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(
        self,
        *,
        session_factory,
        repository: OutboxRepository,
        publisher: RedisStreamPublisher,
        poll_interval_ms: int = 500,
        batch_size: int = 100,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._publisher = publisher
        self._poll_interval_ms = poll_interval_ms
        self._batch_size = batch_size

    async def publish_once(self) -> int:
        """Publish one batch. Returns the number of rows published.

        If the process dies after XADD but before the mark commits, the row
        stays pending and is published again next iteration. That duplicate is
        expected: consumers are idempotent.
        """
        async with self._session_factory() as session:
            rows = await self._repository.get_pending(session, limit=self._batch_size)
            if not rows:
                return 0

            published_ids = []
            try:
                for row in rows:
                    envelope = EventEnvelope.model_validate(row.payload)
                    set_correlation_id(envelope.correlation_id)
                    await self._publisher.publish(row.stream, envelope)
                    logger.debug(
                        "published event to stream",
                        extra={
                            "event_id": envelope.event_id,
                            "event_type": envelope.event_type,
                            "stream": row.stream,
                        },
                    )
                    published_ids.append(row.id)
            finally:
                # Otherwise a failure logged after this loop (including
                # run_forever's own "iteration failed") is stamped with the
                # last successfully-published row's correlation id, blaming
                # an unrelated event for the failure. See spec 15.
                set_correlation_id(None)

            await self._repository.mark_published(session, published_ids)
            await session.commit()
            return len(published_ids)

    async def run_forever(self) -> None:
        while True:
            try:
                count = await self.publish_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox publisher iteration failed")
                count = 0
            if count == 0:
                await asyncio.sleep(self._poll_interval_ms / 1000)
