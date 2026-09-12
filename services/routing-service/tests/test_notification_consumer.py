from uuid import uuid4

import httpx
import pytest
from app.models.base import Base
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.models.route import Route, RouteStatus
from app.workers.notification_consumer import NotificationCreatedConsumer
from notification_shared.events import (
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.http_client import ServiceClient
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import func, select

pytestmark = pytest.mark.integration


def _created_event(channel: str = "email") -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={
            "channel": channel,
            "recipient": "john@example.com",
            "subject": "Welcome",
            "body": "Hello John!",
        },
        correlation_id="corr-route",
    )


def _config_client(handler) -> ServiceClient:
    return ServiceClient(
        base_url="http://configuration-service:8000",
        transport=httpx.MockTransport(handler),
    )


def _enabled(_request):
    return httpx.Response(200, json={"name": "email", "enabled": True})


def _disabled(_request):
    return httpx.Response(200, json={"name": "email", "enabled": False})


def _absent(_request):
    return httpx.Response(404, json={})


def _unavailable(_request):
    return httpx.Response(503, text="")


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def make_consumer(sessions, redis_client):
    created = []

    def _make(handler) -> NotificationCreatedConsumer:
        consumer = NotificationCreatedConsumer(
            session_factory=sessions,
            redis=redis_client,
            consumer_name="routing-test",
            configuration_client=_config_client(handler),
            poll_interval_ms=10,
        )
        created.append(consumer)
        return consumer

    yield _make


async def _publish(redis_client, envelope: EventEnvelope) -> None:
    await RedisStreamPublisher(redis_client).publish(Stream.NOTIFICATION_CREATED, envelope)


async def test_an_enabled_channel_produces_a_processing_route_and_a_routed_event(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_enabled)
    await consumer.ensure_groups()
    event = _created_event()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        route = (await session.scalars(select(Route))).one()
        assert route.notification_id == event.aggregate_id
        assert route.status == RouteStatus.PROCESSING
        assert route.fail_reason is None

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "notification.routed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.NOTIFICATION_ROUTED
        assert published.aggregate_id == event.aggregate_id
        assert published.correlation_id == "corr-route"
        assert published.payload == {
            "channel": "email",
            "recipient": "john@example.com",
            "subject": "Welcome",
            "body": "Hello John!",
            "route_id": str(route.id),
        }


async def test_a_disabled_channel_produces_a_failed_route_and_routing_failed(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_disabled)
    await consumer.ensure_groups()
    event = _created_event()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        route = (await session.scalars(select(Route))).one()
        assert route.status == RouteStatus.FAILED
        assert route.fail_reason == "channel_disabled"

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "delivery.failed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.ROUTING_FAILED
        assert published.payload["reason"] == "channel_disabled"
        assert published.payload["route_id"] == str(route.id)


async def test_an_unknown_channel_fails_with_unknown_channel(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_absent)
    await consumer.ensure_groups()
    await _publish(redis_client, _created_event("email"))

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        assert (await session.scalars(select(Route))).one().fail_reason == "unknown_channel"
        outbox_row = (await session.scalars(select(Outbox))).one()
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.payload["reason"] == "unknown_channel"


async def test_configuration_unavailable_writes_nothing_and_leaves_the_message_pending(
    make_consumer, sessions, redis_client
):
    """Spec 3.18 — an outage must not become a permanent routing failure."""
    consumer = make_consumer(_unavailable)
    await consumer.ensure_groups()
    await _publish(redis_client, _created_event())

    assert await consumer.consume_once() == 0

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Route)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0

    pending = await redis_client.xpending(
        str(Stream.NOTIFICATION_CREATED), str(ConsumerGroup.ROUTING)
    )
    assert pending["pending"] == 1


async def test_a_configuration_timeout_behaves_the_same_as_a_5xx(
    make_consumer, sessions, redis_client
):
    def timeout(request):
        raise httpx.ReadTimeout("too slow", request=request)

    consumer = make_consumer(timeout)
    await consumer.ensure_groups()
    await _publish(redis_client, _created_event())

    assert await consumer.consume_once() == 0
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Route)) == 0


async def test_an_outage_leaves_the_message_pending_for_recovery(
    make_consumer, sessions, redis_client
):
    """Nothing is acked during an outage, so the message survives as pending.

    Slice 1 has no XCLAIM worker, so the guarantee this test pins is narrow
    and deliberately so: the message is pending, not lost. Slice 2's recovery
    worker is what actually re-routes it.
    """
    down = make_consumer(_unavailable)
    await down.ensure_groups()
    await _publish(redis_client, _created_event())
    assert await down.consume_once() == 0

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Route)) == 0

    # The message is pending, not lost. Slice 2's XCLAIM worker is what picks
    # it up; asserting the pending count is the slice 1 guarantee.
    pending = await redis_client.xpending(
        str(Stream.NOTIFICATION_CREATED), str(ConsumerGroup.ROUTING)
    )
    assert pending["pending"] == 1


async def test_a_replayed_event_is_acked_without_writing_a_second_route(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_enabled)
    await consumer.ensure_groups()
    event = _created_event()
    await _publish(redis_client, event)
    await consumer.consume_once()

    await _publish(redis_client, event)
    assert await consumer.consume_once() == 1

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Route)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_the_route_is_marked_processed_for_its_own_group_only(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_enabled)
    await consumer.ensure_groups()
    await _publish(redis_client, _created_event())
    await consumer.consume_once()

    async with sessions() as session:
        row = (await session.scalars(select(ProcessedEvent))).one()
        assert row.consumer_group == ConsumerGroup.ROUTING
        assert row.status == "PROCESSED"


async def test_consume_once_on_an_empty_stream_returns_zero(make_consumer):
    consumer = make_consumer(_enabled)
    await consumer.ensure_groups()
    assert await consumer.consume_once() == 0


async def test_the_notification_id_unique_constraint_blocks_a_second_route(sessions):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            Route(
                notification_id=notification_id,
                channel="email",
                status=RouteStatus.PROCESSING.value,
            )
        )
        await session.commit()

    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        async with sessions() as session:
            session.add(
                Route(
                    notification_id=notification_id,
                    channel="email",
                    status=RouteStatus.PROCESSING.value,
                )
            )
            await session.commit()
