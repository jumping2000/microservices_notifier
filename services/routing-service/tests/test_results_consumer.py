from datetime import UTC, datetime
from uuid import uuid4

import pytest
from app.models.base import Base
from app.models.processed_event import ProcessedEvent
from app.models.route import Route, RouteStatus
from app.workers.results_consumer import ResultsConsumer
from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import func, select

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def consumer(sessions, redis_client) -> ResultsConsumer:
    return ResultsConsumer(
        session_factory=sessions,
        redis=redis_client,
        consumer_name="routing-results-test",
        poll_interval_ms=10,
    )


async def _seed_route(sessions, status: str = RouteStatus.PROCESSING.value):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(Route(notification_id=notification_id, channel="email", status=status))
        await session.commit()
    return notification_id


async def _route(sessions, notification_id):
    async with sessions() as session:
        return (
            await session.execute(
                select(Route.status, Route.fail_reason).where(
                    Route.notification_id == notification_id
                )
            )
        ).one()


def _completed(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=notification_id,
        payload={
            "channel": "email",
            "delivery_id": str(uuid4()),
            "recipient": "a@b.com",
            "delivered_at": datetime.now(UTC).isoformat(),
        },
        correlation_id="corr-rr",
    )


def _delivery_failed(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.DELIVERY_FAILED,
        aggregate_id=notification_id,
        payload={
            "channel": "email",
            "delivery_id": str(uuid4()),
            "reason": "simulated_failure",
        },
        correlation_id="corr-rr",
    )


def _routing_failed(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.ROUTING_FAILED,
        aggregate_id=notification_id,
        payload={
            "channel": "email",
            "route_id": str(uuid4()),
            "reason": "channel_disabled",
        },
        correlation_id="corr-rr",
    )


async def test_delivery_completed_marks_the_route_completed(consumer, sessions, redis_client):
    notification_id = await _seed_route(sessions)
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_COMPLETED, _completed(notification_id)
    )

    assert await consumer.consume_once() == 1
    row = await _route(sessions, notification_id)
    assert row.status == RouteStatus.COMPLETED
    assert row.fail_reason is None


async def test_delivery_failed_marks_the_route_failed_with_the_reason(
    consumer, sessions, redis_client
):
    notification_id = await _seed_route(sessions)
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _delivery_failed(notification_id)
    )

    assert await consumer.consume_once() == 1
    row = await _route(sessions, notification_id)
    assert row.status == RouteStatus.FAILED
    assert row.fail_reason == "simulated_failure"


async def test_routing_failed_is_acked_and_skipped(consumer, sessions, redis_client):
    """Spec 3.3 — Routing Service must not consume its own failure event.

    The route was already marked FAILED in the transaction that published this
    event. Re-handling it would be a loop.
    """
    notification_id = await _seed_route(sessions, RouteStatus.FAILED.value)
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _routing_failed(notification_id)
    )

    assert await consumer.consume_once() == 1

    row = await _route(sessions, notification_id)
    assert row.status == RouteStatus.FAILED
    assert row.fail_reason is None  # untouched: the seed set no reason

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0


async def test_a_completed_route_is_not_reopened_by_a_later_failure(
    consumer, sessions, redis_client
):
    """F7 — `set_status` guards its UPDATE the same way `advance_status` does.

    Mirrors
    `test_status_transitions.py::test_a_completed_notification_is_not_reopened_by_a_later_failure`.
    A route that already reached a terminal state must never be reopened by
    an out-of-order result event; the guard is a `WHERE status = ...` clause,
    not application-level idempotency, since idempotency only protects against
    duplicates of the *same* event, not ordering between distinct ones.
    """
    notification_id = await _seed_route(sessions, RouteStatus.COMPLETED.value)
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _delivery_failed(notification_id)
    )

    assert await consumer.consume_once() == 1
    row = await _route(sessions, notification_id)
    assert row.status == RouteStatus.COMPLETED
    assert row.fail_reason is None


async def test_an_event_for_an_unknown_route_is_acked(consumer, redis_client):
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(Stream.DELIVERY_COMPLETED, _completed(uuid4()))
    assert await consumer.consume_once() == 1


async def test_a_replayed_result_updates_once(consumer, sessions, redis_client):
    notification_id = await _seed_route(sessions)
    await consumer.ensure_groups()
    event = _completed(notification_id)
    publisher = RedisStreamPublisher(redis_client)

    await publisher.publish(Stream.DELIVERY_COMPLETED, event)
    await consumer.consume_once()
    await publisher.publish(Stream.DELIVERY_COMPLETED, event)
    assert await consumer.consume_once() == 1

    assert (await _route(sessions, notification_id)).status == RouteStatus.COMPLETED
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1


async def test_both_result_streams_are_read(consumer, sessions, redis_client):
    first = await _seed_route(sessions)
    second = await _seed_route(sessions)
    await consumer.ensure_groups()

    publisher = RedisStreamPublisher(redis_client)
    await publisher.publish(Stream.DELIVERY_COMPLETED, _completed(first))
    await publisher.publish(Stream.DELIVERY_FAILED, _delivery_failed(second))

    assert await consumer.consume_once() == 2
    assert (await _route(sessions, first)).status == RouteStatus.COMPLETED
    assert (await _route(sessions, second)).status == RouteStatus.FAILED


async def test_consume_once_on_empty_streams_returns_zero(consumer):
    await consumer.ensure_groups()
    assert await consumer.consume_once() == 0
