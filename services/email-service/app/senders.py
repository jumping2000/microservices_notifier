"""Email senders.

The consumer applies the shared rules first — failure marker, then reserved
recipients (slice 2 spec section 3) — and only then calls one of these. Which
sender is configured is decided once at startup from SMTP_HOST (ADR 0029).
"""

from __future__ import annotations

import asyncio
import logging
import random
from email.message import EmailMessage
from typing import Protocol

import aiosmtplib

logger = logging.getLogger(__name__)

DEFAULT_SUBJECT = "Notification"
SMTP_REJECTED = "smtp_rejected"


class EmailRejectedError(Exception):
    """Permanent: the server refused this message or recipient."""


class EmailUnavailableError(Exception):
    """Transient: worth retrying. The message stays pending for recovery."""


class EmailSender(Protocol):
    mode: str

    async def send(self, recipient: str, subject: str | None, body: str) -> None: ...


class SimulatedSender:
    mode = "simulated"

    def __init__(self, latency_ms_max: int = 500) -> None:
        self._latency_ms_max = latency_ms_max

    async def send(self, recipient: str, subject: str | None, body: str) -> None:
        logger.info("simulating email to %s", recipient)
        if self._latency_ms_max:
            await asyncio.sleep(random.uniform(0, self._latency_ms_max) / 1000)


class SmtpSender:
    mode = "smtp"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        sender_address: str,
        username: str | None = None,
        password: str | None = None,
        security: str = "starttls",
        timeout: float = 5.0,
    ) -> None:
        # aiosmtplib logs protocol lines at DEBUG, AUTH included.
        logging.getLogger("aiosmtplib").setLevel(logging.WARNING)
        self._host = host
        self._port = port
        self._from = sender_address
        self._username = username or None
        self._password = password or None
        self._security = security
        self._timeout = timeout

    async def send(self, recipient: str, subject: str | None, body: str) -> None:
        message = EmailMessage()
        message["From"] = self._from
        message["To"] = recipient
        flat_subject = " ".join((subject or DEFAULT_SUBJECT).splitlines())
        message["Subject"] = flat_subject
        message.set_content(body)
        logger.info("sending email to %s via SMTP", recipient)
        try:
            await aiosmtplib.send(
                message,
                hostname=self._host,
                port=self._port,
                username=self._username,
                password=self._password,
                use_tls=self._security == "ssl",
                start_tls=self._security == "starttls",
                timeout=self._timeout,
            )
        except aiosmtplib.SMTPRecipientsRefused as exc:
            codes = [refused.code for refused in exc.recipients]
            if codes and all(code >= 500 for code in codes):
                raise EmailRejectedError(f"recipient refused {codes}") from None
            raise EmailUnavailableError(f"recipient deferred {codes}") from None
        except aiosmtplib.SMTPAuthenticationError as exc:
            # A configuration problem, not a recipient problem: transient, so
            # fixed credentials let the notification through (spec 6.2).
            logger.error(
                "SMTP authentication failed (%s); check SMTP_USERNAME and SMTP_PASSWORD", exc.code
            )
            raise EmailUnavailableError("SMTP authentication failed") from None
        except aiosmtplib.SMTPResponseException as exc:
            if exc.code >= 500:
                raise EmailRejectedError(f"SMTP {exc.code}") from None
            raise EmailUnavailableError(f"SMTP {exc.code}") from None
        except (aiosmtplib.SMTPException, OSError, TimeoutError) as exc:
            raise EmailUnavailableError(f"SMTP unreachable ({type(exc).__name__})") from None


def build_sender(settings) -> EmailSender:
    if not settings.smtp_host:
        return SimulatedSender(settings.delivery_latency_ms_max)
    return SmtpSender(
        host=settings.smtp_host,
        port=settings.smtp_port,
        sender_address=settings.smtp_from,
        username=settings.smtp_username,
        password=settings.smtp_password,
        security=settings.smtp_security,
        timeout=settings.http_timeout_seconds,
    )


def delivery_mode(settings) -> str:
    """The configured mode. Reserved recipients are simulated regardless."""
    return SmtpSender.mode if settings.smtp_host else SimulatedSender.mode
