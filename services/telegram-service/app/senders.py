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

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_REJECTED = "telegram_rejected"
TELEGRAM_API_BASE_URL = "https://api.telegram.org"


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


def classify_response(status_code: int, ok: bool) -> str:
    """Spec 4.5: 400/403 are permanent; 429, 5xx and anything unexpected are
    transient and left to PendingRecoverer."""
    if status_code == 200 and ok:
        return "delivered"
    if status_code in (400, 403):
        return "rejected"
    return "transient"


def silence_client_logs() -> None:
    """httpx logs every request URL at INFO, and the URL carries the token."""
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)


class BotApiSender:
    mode = "bot_api"

    def __init__(
        self,
        token: str,
        *,
        timeout: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        silence_client_logs()
        self._path = f"/bot{token}/sendMessage"
        self._client = httpx.AsyncClient(
            base_url=TELEGRAM_API_BASE_URL, timeout=timeout, transport=transport
        )

    async def send(self, chat_id: str, text: str) -> None:
        logger.info("sending telegram message to %s via the Bot API", chat_id)
        # `from None` throughout: a chained httpx exception can carry the
        # request URL, and with it the token, into a logged traceback.
        try:
            response = await self._client.post(self._path, json={"chat_id": chat_id, "text": text})
        except httpx.TimeoutException:
            raise TelegramUnavailableError("Bot API timed out") from None
        except httpx.TransportError as exc:
            raise TelegramUnavailableError(f"Bot API unreachable ({type(exc).__name__})") from None

        try:
            ok = bool(response.json().get("ok"))
        except ValueError:
            ok = False
        outcome = classify_response(response.status_code, ok)
        if outcome == "rejected":
            raise TelegramRejectedError(f"Bot API refused the message ({response.status_code})")
        if outcome == "transient":
            raise TelegramUnavailableError(f"Bot API returned {response.status_code}")

    async def aclose(self) -> None:
        await self._client.aclose()


def build_sender(settings, transport=None) -> TelegramSender:
    if not settings.telegram_bot_token:
        return SimulatedSender(settings.delivery_latency_ms_max)
    return BotApiSender(
        settings.telegram_bot_token, timeout=settings.http_timeout_seconds, transport=transport
    )


def delivery_mode(settings) -> str:
    """The configured mode. `sim-` chat ids are simulated regardless."""
    return BotApiSender.mode if settings.telegram_bot_token else SimulatedSender.mode
