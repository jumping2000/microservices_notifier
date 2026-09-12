from uuid import uuid4

import pytest
from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.models import OutboxMixin
from notification_shared.outbox import OutboxRepository
from sqlalchemy import func, select
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
        payload={"channel": "email", "recipient": "a@b.com"},
        correlation_id="corr-outbox",
    )


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


async def test_save_enlists_in_the_callers_transaction(sessions):
    repo = OutboxRepository(Outbox)
    async with sessions() as session:
        await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.commit()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_a_rolled_back_transaction_leaves_no_outbox_row(sessions):
    repo = OutboxRepository(Outbox)
    async with sessions() as session:
        await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.rollback()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_get_pending_returns_only_unpublished_rows_oldest_first(sessions):
    """One transaction per row on purpose.

    `created_at` defaults to `func.now()`, which in Postgres is the
    *transaction* start time — three rows saved in one transaction would share
    a timestamp and the ORDER BY could not be asserted at all.
    """
    repo = OutboxRepository(Outbox)
    saved_ids = []
    for _ in range(3):
        async with sessions() as session:
            await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
            await session.commit()
        async with sessions() as session:
            newest = (await repo.get_pending(session))[-1]
            saved_ids.append(newest.id)

    async with sessions() as session:
        pending = await repo.get_pending(session)
        assert [row.id for row in pending] == saved_ids, "not returned oldest-first"
        await repo.mark_published(session, [pending[0].id])
        await session.commit()

    async with sessions() as session:
        remaining = await repo.get_pending(session)
        assert [row.id for row in remaining] == saved_ids[1:]
        assert all(row.published is False for row in remaining)


async def test_get_pending_respects_the_limit(sessions):
    repo = OutboxRepository(Outbox)
    async with sessions() as session:
        for _ in range(5):
            await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.commit()

    async with sessions() as session:
        assert len(await repo.get_pending(session, limit=2)) == 2


async def test_mark_published_with_an_empty_list_is_a_no_op(sessions):
    """Must distinguish a guarded no-op from an unguarded one.

    Asserting only that the call does not raise proves nothing: an empty
    `IN ()` compiles to an always-false predicate, so an unguarded UPDATE
    would also pass. Seed a row and prove it was left alone.
    """
    repo = OutboxRepository(Outbox)
    async with sessions() as session:
        await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.commit()

    async with sessions() as session:
        await repo.mark_published(session, [])
        await session.commit()

    async with sessions() as session:
        assert len(await repo.get_pending(session)) == 1


async def test_the_stored_payload_round_trips_back_into_an_envelope(sessions):
    repo = OutboxRepository(Outbox)
    original = _envelope()
    async with sessions() as session:
        await repo.save(session, Stream.NOTIFICATION_CREATED, original)
        await session.commit()

    async with sessions() as session:
        row = (await repo.get_pending(session))[0]
        assert row.stream == "notification.created"
        assert EventEnvelope.model_validate(row.payload) == original
