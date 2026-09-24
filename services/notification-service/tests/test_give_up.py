from uuid import uuid4

import pytest
from app.models.base import Base
from app.workers.results_consumer import ResultsConsumer
from app.workers.routed_consumer import RoutedConsumer
from notification_shared.events import EventEnvelope, EventType

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.mark.parametrize("consumer_cls", [RoutedConsumer, ResultsConsumer])
async def test_notification_consumers_give_up_without_writing(sessions, redis_client, consumer_cls):
    """Spec 2.6: this service owns the notification, so there is no one to tell."""
    consumer = consumer_cls(
        session_factory=sessions, redis=redis_client, consumer_name="x", poll_interval_ms=10
    )
    envelope = EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-give-up",
    )
    async with sessions() as session:
        await consumer.give_up(session, envelope)
        assert not session.new
        assert not session.dirty
