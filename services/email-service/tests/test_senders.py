import base64
import logging
import socket
from email import message_from_bytes, policy

import httpx
import pytest
from aiosmtpd.controller import Controller
from aiosmtpd.smtp import AuthResult, LoginPassword
from app.core.config import Settings
from app.main import create_app
from app.senders import (
    DEFAULT_SUBJECT,
    EmailRejectedError,
    EmailUnavailableError,
    SimulatedSender,
    SmtpSender,
    build_sender,
    delivery_mode,
)
from pydantic import ValidationError

USER = "mailer"
PASSWORD = "s3cret-Pa55word"
DB_URL = "postgresql+asyncpg://u:p@localhost/db"


class RecordingHandler:
    """In-process SMTP server behaviour: `reject…` gets 550, `busy…` gets 451."""

    def __init__(self) -> None:
        self.messages = []

    async def handle_RCPT(self, server, session, envelope, address, rcpt_options):  # noqa: N802
        if address.startswith("reject"):
            return "550 5.1.1 No such user"
        if address.startswith("busy"):
            return "451 4.3.0 Try again later"
        envelope.rcpt_tos.append(address)
        return "250 OK"

    async def handle_DATA(self, server, session, envelope):  # noqa: N802
        raw = envelope.original_content or envelope.content
        self.messages.append(message_from_bytes(raw, policy=policy.default))
        return "250 Message accepted"


def _authenticator(server, session, envelope, mechanism, auth_data):
    ok = (
        isinstance(auth_data, LoginPassword)
        and auth_data.login == USER.encode()
        and auth_data.password == PASSWORD.encode()
    )
    return AuthResult(success=ok, handled=False)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def smtp_server():
    # The fake server logs every received line at DEBUG, AUTH included. Those
    # are the test double's logs, not ours; silence them so the password test
    # measures only the code under test.
    logging.getLogger("mail.log").setLevel(logging.WARNING)
    handler = RecordingHandler()
    controller = Controller(
        handler,
        hostname="127.0.0.1",
        port=_free_port(),
        authenticator=_authenticator,
        auth_require_tls=False,
    )
    controller.start()
    yield controller, handler
    controller.stop()


def _sender(controller, **overrides) -> SmtpSender:
    kwargs = {
        "host": controller.hostname,
        "port": controller.port,
        "sender_address": "platform@example.com",
        "security": "none",
        "timeout": 5.0,
    }
    kwargs.update(overrides)
    return SmtpSender(**kwargs)


async def test_an_accepted_message_reaches_the_server(smtp_server):
    controller, handler = smtp_server
    await _sender(controller).send("someone@real-domain.org", "Welcome", "Hello!")

    (message,) = handler.messages
    assert message["From"] == "platform@example.com"
    assert message["To"] == "someone@real-domain.org"
    assert message["Subject"] == "Welcome"
    assert message.get_content().strip() == "Hello!"


async def test_a_non_ascii_subject_and_body_arrive_intact(smtp_server):
    """Review Focus 3."""
    controller, handler = smtp_server
    await _sender(controller).send("someone@real-domain.org", "Città ✓", "Ciao, città! 🎉")

    (message,) = handler.messages
    assert message["Subject"] == "Città ✓"
    assert message.get_content().strip() == "Ciao, città! 🎉"


async def test_a_missing_subject_uses_the_default(smtp_server):
    controller, handler = smtp_server
    await _sender(controller).send("someone@real-domain.org", None, "Hello!")
    assert handler.messages[0]["Subject"] == DEFAULT_SUBJECT == "Notification"


async def test_a_5xx_refusal_is_permanent(smtp_server):
    controller, _ = smtp_server
    with pytest.raises(EmailRejectedError):
        await _sender(controller).send("reject@real-domain.org", "s", "b")


async def test_a_4xx_refusal_is_transient(smtp_server):
    controller, _ = smtp_server
    with pytest.raises(EmailUnavailableError):
        await _sender(controller).send("busy@real-domain.org", "s", "b")


async def test_valid_credentials_authenticate(smtp_server):
    controller, handler = smtp_server
    await _sender(controller, username=USER, password=PASSWORD).send("a@real-domain.org", "s", "b")
    assert len(handler.messages) == 1


async def test_an_authentication_failure_is_transient_and_logged(smtp_server, caplog):
    controller, handler = smtp_server
    caplog.set_level(logging.ERROR)
    with pytest.raises(EmailUnavailableError):
        await _sender(controller, username=USER, password="wrong").send(
            "a@real-domain.org", "s", "b"
        )
    assert handler.messages == []
    assert "authentication failed" in caplog.text


async def test_an_unreachable_server_is_transient():
    sender = SmtpSender(
        host="127.0.0.1",
        port=_free_port(),
        sender_address="p@example.com",
        security="none",
        timeout=2.0,
    )
    with pytest.raises(EmailUnavailableError):
        await sender.send("a@real-domain.org", "s", "b")


async def test_the_password_never_reaches_the_logs(smtp_server, caplog):
    controller, _ = smtp_server
    caplog.set_level(logging.DEBUG)
    await _sender(controller, username=USER, password=PASSWORD).send("a@real-domain.org", "s", "b")
    with pytest.raises(EmailUnavailableError):
        await _sender(controller, username=USER, password=PASSWORD + "x").send(
            "a@real-domain.org", "s", "b"
        )
    with pytest.raises(EmailRejectedError):
        await _sender(controller, username=USER, password=PASSWORD).send(
            "reject@real-domain.org", "s", "b"
        )

    plain_blob = base64.b64encode(f"\0{USER}\0{PASSWORD}".encode()).decode()
    assert PASSWORD not in caplog.text
    assert plain_blob not in caplog.text


def _settings(**overrides) -> Settings:
    return Settings(database_url=DB_URL, _env_file=None, **overrides)


def test_no_smtp_host_means_simulated():
    settings = _settings()
    assert isinstance(build_sender(settings), SimulatedSender)
    assert delivery_mode(settings) == "simulated"


def test_an_smtp_host_means_smtp():
    settings = _settings(smtp_host="smtp.real-domain.org", smtp_from="p@real-domain.org")
    assert isinstance(build_sender(settings), SmtpSender)
    assert delivery_mode(settings) == "smtp"


def test_an_smtp_host_without_a_from_address_fails_at_startup():
    with pytest.raises(ValidationError, match="SMTP_FROM"):
        _settings(smtp_host="smtp.real-domain.org")


def test_smtp_security_only_accepts_the_three_modes():
    with pytest.raises(ValidationError):
        _settings(smtp_security="tls")


async def test_version_reports_the_delivery_mode():
    settings = _settings(smtp_host="smtp.real-domain.org", smtp_from="p@real-domain.org")
    app = create_app(settings)
    app.state.settings = settings
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        body = (await client.get("/version")).json()
    assert body == {"service": "email-service", "version": "1.0.0", "delivery_mode": "smtp"}
