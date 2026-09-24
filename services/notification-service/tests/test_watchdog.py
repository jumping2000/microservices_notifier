from datetime import timedelta
from uuid import uuid4

import pytest
from app.models.base import Base
from app.models.notification import PROCESSING_TIMEOUT, Notification, NotificationStatus
from app.workers.results_consumer import ResultsConsumer
from app.workers.watchdog import Watchdog
from notification_shared.events import EventEnvelope, EventType
from sqlalchemy import func, select, update

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


async def _seed(sessions, status: str, minutes_ago: int):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            Notification(
                id=notification_id,
                channel="email",
                recipient="john@example.com",
                subject=None,
                body="Hello",
                status=status,
            )
        )
        await session.flush()
        # An explicit value wins over the column's onupdate=now().
        await session.execute(
            update(Notification)
            .where(Notification.id == notification_id)
            .values(updated_at=func.now() - timedelta(minutes=minutes_ago))
        )
        await session.commit()
    return notification_id


async def _state(sessions, notification_id):
    async with sessions() as session:
        return (
            await session.execute(
                select(Notification.status, Notification.fail_reason).where(
                    Notification.id == notification_id
                )
            )
        ).one()


def _watchdog(sessions) -> Watchdog:
    return Watchdog(session_factory=sessions, processing_timeout_minutes=5, interval_seconds=1)


async def test_a_stale_processing_notification_is_failed(sessions):
    notification_id = await _seed(sessions, NotificationStatus.PROCESSING.value, minutes_ago=10)

    assert await _watchdog(sessions).sweep_once() == 1

    assert await _state(sessions, notification_id) == ("FAILED", PROCESSING_TIMEOUT)


async def test_a_recent_processing_notification_is_left_alone(sessions):
    notification_id = await _seed(sessions, NotificationStatus.PROCESSING.value, minutes_ago=1)

    assert await _watchdog(sessions).sweep_once() == 0

    assert await _state(sessions, notification_id) == ("PROCESSING", None)


@pytest.mark.parametrize("status", ["CREATED", "COMPLETED", "FAILED"])
async def test_other_statuses_are_never_touched(sessions, status):
    notification_id = await _seed(sessions, status, minutes_ago=60)

    assert await _watchdog(sessions).sweep_once() == 0

    assert (await _state(sessions, notification_id))[0] == status


async def test_a_late_delivery_result_does_not_reopen_it(sessions, redis_client):
    """ADR 0028: terminal states never reopen, even after the watchdog."""
    notification_id = await _seed(sessions, NotificationStatus.PROCESSING.value, minutes_ago=10)
    await _watchdog(sessions).sweep_once()

    results = ResultsConsumer(
        session_factory=sessions, redis=redis_client, consumer_name="late", poll_interval_ms=10
    )
    late = EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=notification_id,
        payload={"channel": "email", "delivery_id": str(uuid4())},
        correlation_id="corr-late",
    )
    assert await results.handle(late) is True

    assert await _state(sessions, notification_id) == ("FAILED", PROCESSING_TIMEOUT)
