"""Stale-processing watchdog.

A notification stays PROCESSING when its delivery result never arrives — for
example because this service's own results consumer gave up. This sweep fails
it after PROCESSING_TIMEOUT_MINUTES. It publishes no event, so Routing
Service's route stays PROCESSING, and a result arriving later does not reopen
the notification (ADR 0028).
"""

from __future__ import annotations

import asyncio
import logging

from app.repositories.notification import NotificationRepository

logger = logging.getLogger(__name__)


class Watchdog:
    def __init__(
        self,
        *,
        session_factory,
        processing_timeout_minutes: int = 5,
        interval_seconds: int = 60,
    ) -> None:
        self._session_factory = session_factory
        self._notifications = NotificationRepository()
        self._timeout_minutes = processing_timeout_minutes
        self._interval_seconds = interval_seconds

    async def sweep_once(self) -> int:
        async with self._session_factory() as session:
            failed = await self._notifications.fail_stale_processing(session, self._timeout_minutes)
            await session.commit()
        if failed:
            logger.warning(
                "failed %d notification(s) stuck in PROCESSING for over %d minutes",
                failed,
                self._timeout_minutes,
            )
        return failed

    async def run_forever(self) -> None:
        while True:
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watchdog sweep failed")
            await asyncio.sleep(self._interval_seconds)
