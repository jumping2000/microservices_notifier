"""Delivery rules shared by every delivery service.

Both rules run before any network call, in this order, so email-service and
telegram-service cannot drift apart:

1. A recipient containing "fail" fails deterministically (slice 1 spec 3.8).
2. A reserved recipient is always delivered by the simulated sender, even when
   real delivery is configured (slice 2 spec section 3, ADR 0030). That is what
   lets e2e tests and load tests run on a stack with real credentials.
"""

from __future__ import annotations

from notification_shared.events import Channel

FAILURE_MARKER = "fail"
SIMULATED_FAILURE = "simulated_failure"

# RFC 2606: names reserved for documentation and testing. Nothing sent to them
# can reach a real inbox, so they are safe to use against real senders.
RESERVED_EMAIL_DOMAINS = ("example.com", "example.org", "example.net")
RESERVED_TOP_LEVEL_DOMAINS = ("example", "invalid", "test")
RESERVED_CHAT_PREFIX = "sim-"


def is_failure_recipient(recipient: str) -> bool:
    return FAILURE_MARKER in recipient.lower()


def is_reserved_recipient(channel: str, recipient: str) -> bool:
    if channel == Channel.TELEGRAM:
        return recipient.startswith(RESERVED_CHAT_PREFIX)
    if channel == Channel.EMAIL:
        return _is_reserved_email(recipient)
    return False


def _is_reserved_email(recipient: str) -> bool:
    _, at, domain = recipient.rpartition("@")
    if not at:
        return False
    domain = domain.strip().rstrip(".").lower()
    if domain.rsplit(".", 1)[-1] in RESERVED_TOP_LEVEL_DOMAINS:
        return True
    return any(
        domain == reserved or domain.endswith(f".{reserved}") for reserved in RESERVED_EMAIL_DOMAINS
    )
