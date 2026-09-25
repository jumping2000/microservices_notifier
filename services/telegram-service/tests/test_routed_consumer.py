from uuid import uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from app.core.config import Settings
from app.main import create_app
from app.models.base import Base
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.models.telegram_delivery import DeliveryStatus, TelegramDelivery
from app.senders import SimulatedSender, build_sender, build_text, delivery_mode
from app.workers.routed_consumer import RoutedConsumer
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.integration

ALEMBIC_DIR = "services/telegram-service"
DB_URL = "postgresql+asyncpg://u:p@localhost/db"


def _routed_event(
    channel: str = "telegram", recipient: str = "sim-e2e", subject="Hi"
) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={
            "channel": channel,
            "recipient": recipient,
            "subject": subject,
            "body": "Hello there",
            "route_id": str(uuid4()),
        },
        correlation_id="corr-telegram",
    )


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def consumer(sessions, redis_client) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions,
        redis=redis_client,
        consumer_name="telegram-test",
        poll_interval_ms=10,
        delivery_latency_ms_max=0,
    )


async def _publish(redis_client, envelope: EventEnvelope) -> None:
    await RedisStreamPublisher(redis_client).publish(Stream.NOTIFICATION_ROUTED, envelope)


async def _count(sessions, model) -> int:
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(model))


def test_build_text_joins_subject_and_body():
    assert build_text("Welcome", "Hello") == "Welcome\n\nHello"
    assert build_text(None, "Hello") == "Hello"
    assert build_text("", "Hello") == "Hello"


async def test_a_delivery_records_delivered_and_publishes_completed(
    consumer, sessions, redis_client
):
    await consumer.ensure_groups()
    event = _routed_event()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
        assert delivery.notification_id == event.aggregate_id
        assert delivery.chat_id == "sim-e2e"
        assert delivery.status == DeliveryStatus.DELIVERED
        assert delivery.sent_at is not None

        published = EventEnvelope.model_validate(
            (await session.scalars(select(Outbox))).one().payload
        )
        assert published.event_type is EventType.DELIVERY_COMPLETED
        assert published.payload["channel"] == "telegram"
        assert published.payload["recipient"] == "sim-e2e"
        assert published.payload["delivery_id"] == str(delivery.id)
        assert published.correlation_id == "corr-telegram"


async def test_a_chat_id_containing_fail_fails_deterministically(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="sim-e2e-FAIL"))

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
        assert (delivery.status, delivery.fail_reason) == (
            DeliveryStatus.FAILED,
            "simulated_failure",
        )
        published = EventEnvelope.model_validate(
            (await session.scalars(select(Outbox))).one().payload
        )
        assert published.event_type is EventType.DELIVERY_FAILED
        assert published.payload["reason"] == "simulated_failure"


async def test_an_email_event_is_acked_skipped_and_not_recorded(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(channel="email", recipient="john@example.com"))

    assert await consumer.consume_once() == 1

    assert await _count(sessions, TelegramDelivery) == 0
    assert await _count(sessions, ProcessedEvent) == 0
    pending = await redis_client.xpending(
        str(Stream.NOTIFICATION_ROUTED), str(ConsumerGroup.TELEGRAM)
    )
    assert pending["pending"] == 0


async def test_a_replayed_event_delivers_once(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    event = _routed_event()
    await _publish(redis_client, event)
    await consumer.consume_once()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1
    assert await _count(sessions, TelegramDelivery) == 1
    assert await _count(sessions, Outbox) == 1


async def test_give_up_records_a_failed_delivery_and_publishes_delivery_failed(consumer, sessions):
    event = _routed_event()
    async with sessions() as session:
        await consumer.give_up(session, event)
        await session.commit()

    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
        assert (delivery.status, delivery.fail_reason) == (
            DeliveryStatus.FAILED,
            "max_retries_exceeded",
        )
        published = EventEnvelope.model_validate(
            (await session.scalars(select(Outbox))).one().payload
        )
        assert published.payload["reason"] == "max_retries_exceeded"


async def test_give_up_on_a_payload_missing_the_recipient_still_records_a_failed_delivery(
    consumer, sessions
):
    """spec 2.6: give_up cannot fail for the reason handle did. A malformed
    payload — missing recipient (chat id) — is the poison message the retry
    cap protects against (docs/patterns.md)."""
    event = EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={
            "channel": "telegram",
            "subject": "Hi",
            "body": "Hello there",
            "route_id": str(uuid4()),
        },
        correlation_id="corr-telegram",
    )

    async with sessions() as session:
        await consumer.give_up(session, event)
        await session.commit()

    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
        assert delivery.notification_id == event.aggregate_id
        assert delivery.chat_id == ""
        assert (delivery.status, delivery.fail_reason) == (
            DeliveryStatus.FAILED,
            "max_retries_exceeded",
        )
        published = EventEnvelope.model_validate(
            (await session.scalars(select(Outbox))).one().payload
        )
        assert published.payload["reason"] == "max_retries_exceeded"


async def test_consume_once_on_an_empty_stream_returns_zero(consumer):
    await consumer.ensure_groups()
    assert await consumer.consume_once() == 0


async def test_the_notification_id_unique_constraint_blocks_a_second_delivery(sessions):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            TelegramDelivery(notification_id=notification_id, chat_id="sim-a", status="DELIVERED")
        )
        await session.commit()
    with pytest.raises(IntegrityError):
        async with sessions() as session:
            session.add(
                TelegramDelivery(
                    notification_id=notification_id, chat_id="sim-a", status="DELIVERED"
                )
            )
            await session.commit()


def test_no_token_means_simulated():
    settings = Settings(database_url=DB_URL, _env_file=None)
    assert isinstance(build_sender(settings), SimulatedSender)
    assert delivery_mode(settings) == "simulated"


async def test_version_reports_the_delivery_mode():
    settings = Settings(database_url=DB_URL, _env_file=None)
    app = create_app(settings)
    app.state.settings = settings
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        body = (await client.get("/version")).json()
    assert body == {"service": "telegram-service", "version": "1.0.0", "delivery_mode": "simulated"}


def test_alembic_upgrade_head_matches_the_models(postgres_url):
    """Synchronous on purpose: alembic's env.py calls asyncio.run internally."""
    import asyncio

    from alembic.autogenerate import compare_metadata
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy.ext.asyncio import create_async_engine

    config = Config(f"{ALEMBIC_DIR}/alembic.ini")
    config.set_main_option("script_location", f"{ALEMBIC_DIR}/alembic")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.upgrade(config, "head")
    try:

        def _compare(sync_connection):
            return compare_metadata(MigrationContext.configure(sync_connection), Base.metadata)

        async def _diff():
            engine = create_async_engine(postgres_url)
            async with engine.connect() as conn:
                result = await conn.run_sync(_compare)
            await engine.dispose()
            return result

        assert asyncio.run(_diff()) == []
    finally:
        command.downgrade(config, "base")
