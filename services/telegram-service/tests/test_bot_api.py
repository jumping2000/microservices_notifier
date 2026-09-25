import json
import logging
from uuid import uuid4

import httpx
import pytest
from app.core.config import Settings
from app.models.base import Base
from app.models.telegram_delivery import DeliveryStatus, TelegramDelivery
from app.senders import (
    BotApiSender,
    TelegramRejectedError,
    TelegramUnavailableError,
    build_sender,
    classify_response,
    delivery_mode,
)
from app.workers.routed_consumer import RoutedConsumer
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import func, select

TOKEN = "123456:SECRET-token-value"
DB_URL = "postgresql+asyncpg://u:p@localhost/db"


class FakeBotApi:
    """httpx.MockTransport handler that records every call."""

    def __init__(self, status: int = 200, ok: bool = True, error: Exception | None = None):
        self.status, self.ok, self.error = status, ok, error
        self.calls: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.error:
            raise self.error
        return httpx.Response(self.status, json={"ok": self.ok, "description": "test"})


def _sender(api: FakeBotApi) -> BotApiSender:
    return BotApiSender(TOKEN, timeout=1.0, transport=httpx.MockTransport(api))


@pytest.mark.parametrize(
    ("status", "ok", "expected"),
    [
        (200, True, "delivered"),
        (200, False, "transient"),
        (400, False, "rejected"),
        (403, False, "rejected"),
        (429, False, "transient"),
        (500, False, "transient"),
        (502, False, "transient"),
        (418, False, "transient"),
    ],
)
def test_classify_response(status, ok, expected):
    assert classify_response(status, ok) == expected


async def test_a_successful_send_posts_to_send_message():
    api = FakeBotApi()
    await _sender(api).send("-1001234567890", "Hello")

    (request,) = api.calls
    assert request.method == "POST"
    assert request.url.host == "api.telegram.org"
    assert request.url.path == f"/bot{TOKEN}/sendMessage"
    # A negative group chat id is passed through as the same string.
    assert json.loads(request.content) == {"chat_id": "-1001234567890", "text": "Hello"}


@pytest.mark.parametrize("status", [400, 403])
async def test_a_refusal_is_permanent(status):
    with pytest.raises(TelegramRejectedError):
        await _sender(FakeBotApi(status=status, ok=False)).send("42", "Hi")


@pytest.mark.parametrize(
    "api",
    [
        FakeBotApi(status=429, ok=False),
        FakeBotApi(status=500, ok=False),
        FakeBotApi(error=httpx.ReadTimeout("timed out")),
        FakeBotApi(error=httpx.ConnectError("refused")),
    ],
)
async def test_rate_limits_server_errors_and_network_failures_are_transient(api):
    with pytest.raises(TelegramUnavailableError):
        await _sender(api).send("42", "Hi")


async def test_the_token_never_reaches_logs_or_exception_text(caplog):
    caplog.set_level(logging.DEBUG)
    await _sender(FakeBotApi()).send("42", "Hi")
    for api in (FakeBotApi(status=400, ok=False), FakeBotApi(error=httpx.ReadTimeout("t"))):
        with pytest.raises(Exception) as excinfo:
            await _sender(api).send("42", "Hi")
        assert TOKEN not in str(excinfo.value)
        assert excinfo.value.__cause__ is None
    assert TOKEN not in caplog.text


def test_constructing_a_sender_silences_httpx_and_httpcore_logs():
    logging.getLogger("httpx").setLevel(logging.NOTSET)
    logging.getLogger("httpcore").setLevel(logging.NOTSET)

    _sender(FakeBotApi())

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING


def test_a_token_means_bot_api():
    settings = Settings(database_url=DB_URL, telegram_bot_token=TOKEN, _env_file=None)
    assert isinstance(build_sender(settings), BotApiSender)
    assert delivery_mode(settings) == "bot_api"


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


def _consumer(sessions, redis_client, api: FakeBotApi) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions,
        redis=redis_client,
        consumer_name="telegram-bot",
        poll_interval_ms=10,
        delivery_latency_ms_max=0,
        sender=_sender(api),
    )


async def _publish_routed(redis_client, chat_id: str) -> None:
    envelope = EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={
            "channel": "telegram",
            "recipient": chat_id,
            "subject": None,
            "body": "Hi",
            "route_id": str(uuid4()),
        },
        correlation_id="corr-bot",
    )
    await RedisStreamPublisher(redis_client).publish(Stream.NOTIFICATION_ROUTED, envelope)


@pytest.mark.integration
async def test_a_sim_chat_id_never_reaches_the_bot_api(sessions, redis_client):
    api = FakeBotApi()
    consumer = _consumer(sessions, redis_client, api)
    await consumer.ensure_groups()
    await _publish_routed(redis_client, "sim-e2e")

    assert await consumer.consume_once() == 1
    assert api.calls == []


@pytest.mark.integration
async def test_a_real_chat_id_is_sent_through_the_bot_api(sessions, redis_client):
    api = FakeBotApi()
    consumer = _consumer(sessions, redis_client, api)
    await consumer.ensure_groups()
    await _publish_routed(redis_client, "123456789")

    assert await consumer.consume_once() == 1
    assert len(api.calls) == 1
    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
    assert delivery.status == DeliveryStatus.DELIVERED


@pytest.mark.integration
async def test_a_refused_chat_fails_with_telegram_rejected(sessions, redis_client):
    consumer = _consumer(sessions, redis_client, FakeBotApi(status=400, ok=False))
    await consumer.ensure_groups()
    await _publish_routed(redis_client, "123456789")

    await consumer.consume_once()
    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
    assert (delivery.status, delivery.fail_reason) == (DeliveryStatus.FAILED, "telegram_rejected")


@pytest.mark.integration
async def test_a_transient_failure_writes_nothing_and_stays_pending(sessions, redis_client):
    consumer = _consumer(sessions, redis_client, FakeBotApi(status=500, ok=False))
    await consumer.ensure_groups()
    await _publish_routed(redis_client, "123456789")

    with pytest.raises(TelegramUnavailableError):
        await consumer.consume_once()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(TelegramDelivery)) == 0
    pending = await redis_client.xpending(
        str(Stream.NOTIFICATION_ROUTED), str(ConsumerGroup.TELEGRAM)
    )
    assert pending["pending"] == 1
