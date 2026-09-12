from uuid import uuid4

import pytest
from notification_shared.events import ConsumerGroup
from notification_shared.idempotency import IdempotencyRepository, ProcessedStatus
from notification_shared.models import ProcessedEventMixin
from sqlalchemy import func, select
from sqlalchemy.orm import DeclarativeBase

pytestmark = pytest.mark.integration

GROUP = ConsumerGroup.ROUTING


class Base(DeclarativeBase):
    pass


class ProcessedEvent(Base, ProcessedEventMixin):
    __tablename__ = "processed_events"


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


async def test_an_unseen_event_is_not_processed(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    async with sessions() as session:
        assert await repo.is_processed(session, uuid4(), GROUP) is False


async def test_mark_processed_then_is_processed(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.mark_processed(session, event_id, GROUP)
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, GROUP) is True


async def test_a_failing_row_is_not_treated_as_processed(sessions):
    """Spec 3.4 — the bug the status column exists to prevent."""
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        assert await repo.increment_fail_count(session, event_id, GROUP) == 1
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, GROUP) is False
        status = await session.scalar(
            select(ProcessedEvent.status).where(ProcessedEvent.event_id == event_id)
        )
        assert status == ProcessedStatus.FAILING


async def test_increment_accumulates_and_returns_the_new_count(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        assert await repo.increment_fail_count(session, event_id, GROUP) == 1
        assert await repo.increment_fail_count(session, event_id, GROUP) == 2
        assert await repo.increment_fail_count(session, event_id, GROUP) == 3
        await session.commit()


async def test_mark_processed_overwrites_a_failing_row(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.increment_fail_count(session, event_id, GROUP)
        await repo.mark_processed(session, event_id, GROUP)
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, GROUP) is True


async def test_mark_processed_twice_does_not_violate_the_unique_constraint(sessions):
    """Asserting "did not raise" would not prove the upsert worked.

    Without ON CONFLICT the second call raises; with it, exactly one row must
    remain. Count the rows.
    """
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.mark_processed(session, event_id, GROUP)
        await repo.mark_processed(session, event_id, GROUP)
        await session.commit()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1
        assert await repo.is_processed(session, event_id, GROUP) is True


async def test_the_same_event_is_tracked_per_consumer_group(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.mark_processed(session, event_id, ConsumerGroup.ROUTING)
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, ConsumerGroup.ROUTING) is True
        assert await repo.is_processed(session, event_id, ConsumerGroup.EMAIL) is False
