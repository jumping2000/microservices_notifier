import asyncio
from uuid import UUID, uuid4

import pytest
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.idempotency import IdempotencyRepository, ProcessedStatus
from notification_shared.models import ProcessedEventMixin
from notification_shared.recovery import PendingRecoverer
from notification_shared.streams import RedisStreamConsumer, RedisStreamPublisher
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

pytestmark = pytest.mark.integration

STREAM = Stream.NOTIFICATION_ROUTED
GROUP = ConsumerGroup.EMAIL
IDLE_MS = 20


class Base(DeclarativeBase):
    pass


class ProcessedEvent(Base, ProcessedEventMixin):
    __tablename__ = "processed_events"


class GiveUpMarker(Base):
    """Written by FakeConsumer.give_up, to prove it shares the transaction."""

    __tablename__ = "give_up_markers"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)


class FakeConsumer:
    """Scripted outcomes: True acks, False or an exception fails the attempt."""

    def __init__(self, outcomes=(), give_up_error: Exception | None = None) -> None:
        self.outcomes = list(outcomes)
        self.give_up_error = give_up_error
        self.handled: list[UUID] = []
        self.given_up: list[UUID] = []

    async def handle(self, envelope: EventEnvelope) -> bool:
        self.handled.append(envelope.event_id)
        outcome = self.outcomes.pop(0) if self.outcomes else True
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def give_up(self, session, envelope: EventEnvelope) -> None:
        session.add(GiveUpMarker(event_id=envelope.event_id))
        await session.flush()
        if self.give_up_error:
            raise self.give_up_error
        self.given_up.append(envelope.event_id)


def _envelope() -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "recipient": "john@example.com", "body": "Hi"},
        correlation_id="corr-recovery",
    )


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


def _recoverer(redis_client, sessions, *, pending_timeout_ms=IDLE_MS, max_retries=3):
    return PendingRecoverer(
        redis=redis_client,
        consumer_name="recovery",
        session_factory=sessions,
        idempotency=IdempotencyRepository(ProcessedEvent),
        pending_timeout_ms=pending_timeout_ms,
        max_retries=max_retries,
        poll_interval_ms=10,
    )


async def _strand(redis_client, stream=STREAM, group=GROUP) -> EventEnvelope:
    """A consumer that then "dies": it read the message and never acked it."""
    envelope = _envelope()
    dead = RedisStreamConsumer(redis_client, consumer_name="dead")
    await dead.ensure_group(stream, group)
    await RedisStreamPublisher(redis_client).publish(stream, envelope)
    await dead.read(stream, group, block_ms=100)
    await asyncio.sleep(IDLE_MS * 2 / 1000)
    return envelope


async def _pending_count(redis_client, stream=STREAM, group=GROUP) -> int:
    return (await redis_client.xpending(str(stream), str(group)))["pending"]


async def _ledger(sessions, event_id):
    async with sessions() as session:
        return (
            await session.execute(
                select(ProcessedEvent.status, ProcessedEvent.fail_count).where(
                    ProcessedEvent.event_id == event_id
                )
            )
        ).one_or_none()


async def _markers(sessions) -> int:
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(GiveUpMarker))


async def test_a_stranded_message_is_claimed_and_completed(redis_client, sessions):
    envelope = await _strand(redis_client)
    consumer = FakeConsumer([True])
    recoverer = _recoverer(redis_client, sessions)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 1

    assert consumer.handled == [envelope.event_id]
    assert await _pending_count(redis_client) == 0


async def test_a_message_that_is_not_idle_yet_is_left_alone(redis_client, sessions):
    await _strand(redis_client)
    consumer = FakeConsumer([True])
    recoverer = _recoverer(redis_client, sessions, pending_timeout_ms=60_000)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 0
    assert consumer.handled == []
    assert await _pending_count(redis_client) == 1


async def test_each_failed_attempt_increments_the_count(redis_client, sessions):
    envelope = await _strand(redis_client)
    consumer = FakeConsumer([False, RuntimeError("boom")])
    recoverer = _recoverer(redis_client, sessions)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 0
    assert await _ledger(sessions, envelope.event_id) == (ProcessedStatus.FAILING, 1)

    await asyncio.sleep(IDLE_MS * 2 / 1000)
    assert await recoverer.recover_once() == 0
    assert await _ledger(sessions, envelope.event_id) == (ProcessedStatus.FAILING, 2)
    assert await _pending_count(redis_client) == 1


async def test_the_last_failed_attempt_gives_up_and_acks(redis_client, sessions):
    envelope = await _strand(redis_client)
    consumer = FakeConsumer([False, False, False])
    recoverer = _recoverer(redis_client, sessions, max_retries=3)
    recoverer.register(STREAM, GROUP, consumer)

    acked = 0
    for _ in range(3):
        acked += await recoverer.recover_once()
        await asyncio.sleep(IDLE_MS * 2 / 1000)

    assert acked == 1
    assert consumer.given_up == [envelope.event_id]
    assert await _ledger(sessions, envelope.event_id) == (ProcessedStatus.FAILED_PERMANENT, 3)
    assert await _markers(sessions) == 1
    assert await _pending_count(redis_client) == 0


async def test_a_give_up_that_raises_rolls_back_and_stays_pending(redis_client, sessions):
    envelope = await _strand(redis_client)
    consumer = FakeConsumer([False], give_up_error=RuntimeError("outbox down"))
    recoverer = _recoverer(redis_client, sessions, max_retries=1)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 0

    assert await _ledger(sessions, envelope.event_id) == (ProcessedStatus.FAILING, 1)
    assert await _markers(sessions) == 0
    assert await _pending_count(redis_client) == 1


async def test_an_unparseable_entry_is_acked_and_discarded(redis_client, sessions):
    dead = RedisStreamConsumer(redis_client, consumer_name="dead")
    await dead.ensure_group(STREAM, GROUP)
    await redis_client.xadd(str(STREAM), {"garbage": "x"})
    with pytest.raises(ValueError):
        await dead.read(STREAM, GROUP, block_ms=100)
    await asyncio.sleep(IDLE_MS * 2 / 1000)

    consumer = FakeConsumer()
    recoverer = _recoverer(redis_client, sessions)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 1
    assert consumer.handled == []
    assert await _pending_count(redis_client) == 0


async def test_every_registration_is_recovered(redis_client, sessions):
    """Results consumers read two streams and are registered once per stream."""
    group = ConsumerGroup.NOTIFICATION_RESULTS
    first = await _strand(redis_client, Stream.DELIVERY_COMPLETED, group)
    second = await _strand(redis_client, Stream.DELIVERY_FAILED, group)
    consumer = FakeConsumer([True, True])
    recoverer = _recoverer(redis_client, sessions)
    recoverer.register(Stream.DELIVERY_COMPLETED, group, consumer)
    recoverer.register(Stream.DELIVERY_FAILED, group, consumer)

    assert await recoverer.recover_once() == 2
    assert sorted(consumer.handled) == sorted([first.event_id, second.event_id])
