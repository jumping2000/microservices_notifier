from uuid import uuid4

import pytest
from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.models import OutboxMixin
from notification_shared.outbox import OutboxRepository
from notification_shared.publisher import OutboxPublisher
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy.orm import DeclarativeBase

pytestmark = pytest.mark.integration


class Base(DeclarativeBase):
    pass


class Outbox(Base, OutboxMixin):
    __tablename__ = "outbox"


def _envelope() -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-pub",
    )


@pytest.fixture
async def harness(make_schema, redis_client):
    sessions = await make_schema(Base.metadata)
    repository = OutboxRepository(Outbox)
    worker = OutboxPublisher(
        session_factory=sessions,
        repository=repository,
        publisher=RedisStreamPublisher(redis_client),
    )
    return sessions, repository, worker


async def _seed(sessions, repository, count: int = 1) -> None:
    async with sessions() as session:
        for _ in range(count):
            await repository.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.commit()


async def test_publish_once_on_an_empty_outbox_publishes_nothing(harness, redis_client):
    _sessions, _repo, worker = harness
    assert await worker.publish_once() == 0
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 0


async def test_publish_once_moves_pending_rows_to_the_stream(harness, redis_client):
    sessions, repository, worker = harness
    await _seed(sessions, repository, count=3)

    assert await worker.publish_once() == 3
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 3


async def test_published_rows_are_marked_and_not_republished(harness, redis_client):
    sessions, repository, worker = harness
    await _seed(sessions, repository, count=2)

    await worker.publish_once()
    assert await worker.publish_once() == 0
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 2

    async with sessions() as session:
        assert await repository.get_pending(session) == []


async def test_a_crash_between_xadd_and_the_mark_causes_a_duplicate(
    harness, redis_client, monkeypatch
):
    """The at-least-once proof, spec 10.2.

    The event reaches Redis, the mark never lands, and the next iteration
    publishes it again. Duplicates are a fact of this design; consumer
    idempotency is what makes them harmless.
    """
    sessions, repository, worker = harness
    await _seed(sessions, repository, count=1)

    async def boom(*_args, **_kwargs):
        raise RuntimeError("crash after XADD, before the mark")

    monkeypatch.setattr(repository, "mark_published", boom)
    with pytest.raises(RuntimeError):
        await worker.publish_once()

    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 1
    async with sessions() as session:
        assert len(await repository.get_pending(session)) == 1

    monkeypatch.undo()
    assert await worker.publish_once() == 1
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 2

    entries = await redis_client.xrange(str(Stream.NOTIFICATION_CREATED))
    envelopes = [EventEnvelope.from_redis(fields) for _id, fields in entries]
    assert envelopes[0].event_id == envelopes[1].event_id


async def test_batch_size_caps_one_iteration(harness, redis_client):
    sessions, repository, _worker = harness
    await _seed(sessions, repository, count=5)
    worker = OutboxPublisher(
        session_factory=sessions,
        repository=repository,
        publisher=RedisStreamPublisher(redis_client),
        batch_size=2,
    )
    assert await worker.publish_once() == 2


async def test_rows_are_published_to_the_stream_named_on_each_row(harness, redis_client):
    sessions, repository, worker = harness
    async with sessions() as session:
        await repository.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await repository.save(session, Stream.DELIVERY_COMPLETED, _envelope())
        await session.commit()

    assert await worker.publish_once() == 2
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 1
    assert await redis_client.xlen(str(Stream.DELIVERY_COMPLETED)) == 1
