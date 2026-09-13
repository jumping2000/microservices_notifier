from uuid import uuid4

import pytest
from app.models.base import Base
from app.models.email_delivery import DeliveryStatus, EmailDelivery
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.workers.routed_consumer import RoutedConsumer
from notification_shared.events import (
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import func, select

pytestmark = pytest.mark.integration


def _routed_event(channel: str = "email", recipient: str = "john@example.com") -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={
            "channel": channel,
            "recipient": recipient,
            "subject": "Welcome",
            "body": "Hello John!",
            "route_id": str(uuid4()),
        },
        correlation_id="corr-email",
    )


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def consumer(sessions, redis_client) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions,
        redis=redis_client,
        consumer_name="email-test",
        poll_interval_ms=10,
        delivery_latency_ms_max=0,
    )


async def _publish(redis_client, envelope: EventEnvelope) -> None:
    await RedisStreamPublisher(redis_client).publish(Stream.NOTIFICATION_ROUTED, envelope)


async def test_a_successful_delivery_records_delivered_and_publishes_completed(
    consumer, sessions, redis_client
):
    await consumer.ensure_groups()
    event = _routed_event()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        delivery = (await session.scalars(select(EmailDelivery))).one()
        assert delivery.notification_id == event.aggregate_id
        assert delivery.recipient == "john@example.com"
        assert delivery.status == DeliveryStatus.DELIVERED
        assert delivery.sent_at is not None
        assert delivery.fail_reason is None

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "delivery.completed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.DELIVERY_COMPLETED
        assert published.aggregate_id == event.aggregate_id
        assert published.correlation_id == "corr-email"
        assert published.payload["channel"] == "email"
        assert published.payload["delivery_id"] == str(delivery.id)
        assert published.payload["recipient"] == "john@example.com"
        assert "delivered_at" in published.payload


async def test_a_recipient_containing_fail_records_failed_and_publishes_delivery_failed(
    consumer, sessions, redis_client
):
    """Spec 3.8 — the deterministic failure rule."""
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="fail@example.com"))

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        delivery = (await session.scalars(select(EmailDelivery))).one()
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.fail_reason == "simulated_failure"

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "delivery.failed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.DELIVERY_FAILED
        assert published.payload["reason"] == "simulated_failure"


async def test_the_fail_rule_is_case_insensitive(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="FAILURE@example.com"))
    await consumer.consume_once()

    async with sessions() as session:
        assert (await session.scalars(select(EmailDelivery))).one().status == DeliveryStatus.FAILED


async def test_a_telegram_event_is_acked_and_skipped(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(channel="telegram"))

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0

    pending = await redis_client.xpending(str(Stream.NOTIFICATION_ROUTED), str(ConsumerGroup.EMAIL))
    assert pending["pending"] == 0


async def test_a_skipped_event_is_not_recorded_as_processed(consumer, sessions, redis_client):
    """Filtering is not processing: no row is written for another channel's event."""
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(channel="telegram"))
    await consumer.consume_once()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0


async def test_a_replayed_event_delivers_once(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    event = _routed_event()
    await _publish(redis_client, event)
    await consumer.consume_once()

    await _publish(redis_client, event)
    assert await consumer.consume_once() == 1

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(EmailDelivery)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_a_mixed_batch_delivers_only_the_email_events(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(channel="telegram"))
    await _publish(redis_client, _routed_event(recipient="a@example.com"))
    await _publish(redis_client, _routed_event(channel="telegram"))

    assert await consumer.consume_once() == 3

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(EmailDelivery)) == 1


async def test_consume_once_on_an_empty_stream_returns_zero(consumer):
    await consumer.ensure_groups()
    assert await consumer.consume_once() == 0


async def test_the_notification_id_unique_constraint_blocks_a_second_delivery(sessions):
    from sqlalchemy.exc import IntegrityError

    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            EmailDelivery(
                notification_id=notification_id,
                recipient="a@b.com",
                status=DeliveryStatus.SENDING.value,
            )
        )
        await session.commit()

    with pytest.raises(IntegrityError):
        async with sessions() as session:
            session.add(
                EmailDelivery(
                    notification_id=notification_id,
                    recipient="a@b.com",
                    status=DeliveryStatus.SENDING.value,
                )
            )
            await session.commit()
