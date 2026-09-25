"""Telegram senders.

The consumer applies the shared rules first — failure marker, then reserved
`sim-` chat ids (slice 2 spec section 3) — and only then calls one of these.
The notification's recipient is the chat id (ADR 0026).
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Protocol

logger = logging.getLogger(__name__)

TELEGRAM_REJECTED = "telegram_rejected"


class TelegramRejectedError(Exception):
    """Permanent: the Bot API refused this chat or message (400/403)."""


class TelegramUnavailableError(Exception):
    """Transient: worth retrying. The message stays pending for recovery."""


class TelegramSender(Protocol):
    mode: str

    async def send(self, chat_id: str, text: str) -> None: ...

    async def aclose(self) -> None: ...


class SimulatedSender:
    mode = "simulated"

    def __init__(self, latency_ms_max: int = 500) -> None:
        self._latency_ms_max = latency_ms_max

    async def send(self, chat_id: str, text: str) -> None:
        logger.info("simulating telegram message to %s", chat_id)
        if self._latency_ms_max:
            await asyncio.sleep(random.uniform(0, self._latency_ms_max) / 1000)

    async def aclose(self) -> None:
        return None


def build_text(subject: str | None, body: str) -> str:
    return f"{subject}\n\n{body}" if subject else body


def build_sender(settings, transport=None) -> TelegramSender:
    """Task 9 adds the Bot API sender; until then delivery is always simulated."""
    return SimulatedSender(settings.delivery_latency_ms_max)


def delivery_mode(settings) -> str:
    return SimulatedSender.mode
