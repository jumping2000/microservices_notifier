from datetime import UTC, datetime
from uuid import uuid4

import pytest
from app.models.base import Base
from app.models.notification import Notification, NotificationStatus
from app.workers.results_consumer import ResultsConsumer
from app.workers.routed_consumer import RoutedConsumer
from notification_shared.events import (
    Channel,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import select

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def routed(sessions, redis_client) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions, redis=redis_client,
        consumer_name="notif-routed-test", poll_interval_ms=10,
    )


@pytest.fixture
def results(sessions, redis_client) -> ResultsConsumer:
    return ResultsConsumer(
        session_factory=sessions, redis=redis_client,
        consumer_name="notif-results-test", poll_interval_ms=10,
    )


async def _seed_notification(sessions, status: str = NotificationStatus.CREATED.value):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            Notification(
                id=notification_id, channel=Channel.EMAIL.value,
                recipient="john@example.com", subject=None, body="Hello",
                status=status,
            )
        )
        await session.commit()
    return notification_id


async def _status(sessions, notification_id):
    async with sessions() as session:
        row = (
            await session.execute(
                select(Notification.status, Notification.fail_reason).where(
                    Notification.id == notification_id
                )
            )
        ).one()
        return row.status, row.fail_reason


def _routed_event(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "recipient": "john@example.com",
            "subject": None, "body": "Hello", "route_id": str(uuid4()),
        },
        correlation_id="corr-transition",
    )


def _completed_event(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "delivery_id": str(uuid4()),
            "recipient": "john@example.com",
            "delivered_at": datetime.now(UTC).isoformat(),
        },
        correlation_id="corr-transition",
    )


def _delivery_failed_event(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.DELIVERY_FAILED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "delivery_id": str(uuid4()),
            "reason": "simulated_failure",
        },
        correlation_id="corr-transition",
    )


def _routing_failed_event(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.ROUTING_FAILED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "route_id": str(uuid4()),
            "reason": "channel_disabled",
        },
        correlation_id="corr-transition",
    )


async def test_the_routed_event_moves_created_to_processing(routed, sessions, redis_client):
    notification_id = await _seed_notification(sessions)
    await routed.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.NOTIFICATION_ROUTED, _routed_event(notification_id)
    )

    assert await routed.consume_once() == 1
    assert await _status(sessions, notification_id) == (NotificationStatus.PROCESSING, None)


async def test_the_completed_event_moves_processing_to_completed(
    results, sessions, redis_client
):
    notification_id = await _seed_notification(sessions, NotificationStatus.PROCESSING.value)
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_COMPLETED, _completed_event(notification_id)
    )

    assert await results.consume_once() == 1
    assert await _status(sessions, notification_id) == (NotificationStatus.COMPLETED, None)


async def test_a_delivery_failure_records_the_reason(results, sessions, redis_client):
    notification_id = await _seed_notification(sessions, NotificationStatus.PROCESSING.value)
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _delivery_failed_event(notification_id)
    )

    assert await results.consume_once() == 1
    assert await _status(sessions, notification_id) == (
        NotificationStatus.FAILED,
        "simulated_failure",
    )


async def test_routing_failed_moves_created_straight_to_failed(
    results, sessions, redis_client
):
    """No notification.routed is published when routing fails, so PROCESSING is skipped."""
    notification_id = await _seed_notification(sessions)
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _routing_failed_event(notification_id)
    )

    assert await results.consume_once() == 1
    assert await _status(sessions, notification_id) == (
        NotificationStatus.FAILED,
        "channel_disabled",
    )


async def test_a_result_arriving_before_the_routed_event_is_not_overwritten(
    routed, results, sessions, redis_client
):
    """Spec 3.16 — the exact race the guarded transitions exist to prevent.

    Without the `status = 'CREATED'` guard on the routed handler, this test
    ends at PROCESSING: a finished notification reopened by a late event.
    """
    notification_id = await _seed_notification(sessions)
    await routed.ensure_groups()
    await results.ensure_groups()

    publisher = RedisStreamPublisher(redis_client)
    await publisher.publish(Stream.NOTIFICATION_ROUTED, _routed_event(notification_id))
    await publisher.publish(Stream.DELIVERY_COMPLETED, _completed_event(notification_id))

    # Results first, out of order.
    assert await results.consume_once() == 1
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.COMPLETED

    # The routed event arrives late and must change nothing.
    assert await routed.consume_once() == 1
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.COMPLETED


async def test_a_completed_notification_is_not_reopened_by_a_later_failure(
    results, sessions, redis_client
):
    notification_id = await _seed_notification(sessions, NotificationStatus.COMPLETED.value)
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _delivery_failed_event(notification_id)
    )

    assert await results.consume_once() == 1
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.COMPLETED


async def test_an_event_for_an_unknown_notification_is_acked(
    results, sessions, redis_client
):
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_COMPLETED, _completed_event(uuid4())
    )
    assert await results.consume_once() == 1


async def test_a_replayed_routed_event_is_acked_once_and_changes_nothing_twice(
    routed, sessions, redis_client
):
    notification_id = await _seed_notification(sessions)
    await routed.ensure_groups()
    event = _routed_event(notification_id)
    publisher = RedisStreamPublisher(redis_client)

    await publisher.publish(Stream.NOTIFICATION_ROUTED, event)
    await routed.consume_once()
    await publisher.publish(Stream.NOTIFICATION_ROUTED, event)
    assert await routed.consume_once() == 1

    assert (await _status(sessions, notification_id))[0] == NotificationStatus.PROCESSING


async def test_the_results_consumer_reads_both_result_streams(
    results, sessions, redis_client
):
    first = await _seed_notification(sessions, NotificationStatus.PROCESSING.value)
    second = await _seed_notification(sessions, NotificationStatus.PROCESSING.value)
    await results.ensure_groups()

    publisher = RedisStreamPublisher(redis_client)
    await publisher.publish(Stream.DELIVERY_COMPLETED, _completed_event(first))
    await publisher.publish(Stream.DELIVERY_FAILED, _delivery_failed_event(second))

    assert await results.consume_once() == 2
    assert (await _status(sessions, first))[0] == NotificationStatus.COMPLETED
    assert (await _status(sessions, second))[0] == NotificationStatus.FAILED


async def test_the_full_ordered_sequence_ends_completed(
    routed, results, sessions, redis_client
):
    notification_id = await _seed_notification(sessions)
    await routed.ensure_groups()
    await results.ensure_groups()
    publisher = RedisStreamPublisher(redis_client)

    await publisher.publish(Stream.NOTIFICATION_ROUTED, _routed_event(notification_id))
    await routed.consume_once()
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.PROCESSING

    await publisher.publish(Stream.DELIVERY_COMPLETED, _completed_event(notification_id))
    await results.consume_once()
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.COMPLETED
