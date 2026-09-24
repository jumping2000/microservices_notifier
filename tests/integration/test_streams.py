import asyncio
from uuid import uuid4

import pytest
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.streams import RedisStreamConsumer, RedisStreamPublisher

pytestmark = pytest.mark.integration

STREAM = Stream.NOTIFICATION_CREATED
GROUP = ConsumerGroup.ROUTING


def _envelope(correlation_id: str = "corr-stream") -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "recipient": "a@b.com"},
        correlation_id=correlation_id,
    )


async def test_publish_returns_a_string_message_id(redis_client):
    message_id = await RedisStreamPublisher(redis_client).publish(STREAM, _envelope())
    assert isinstance(message_id, str)
    assert "-" in message_id


async def test_ensure_group_creates_the_stream_when_it_does_not_exist(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    assert await redis_client.exists(str(STREAM)) == 1


async def test_ensure_group_is_idempotent(redis_client):
    """Two calls must leave exactly one group, not merely avoid raising.

    A BUSYGROUP error that was swallowed too broadly, or a second group
    created under the same name, would both pass a no-assert test.
    """
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    await consumer.ensure_group(STREAM, GROUP)

    groups = await redis_client.xinfo_groups(str(STREAM))
    assert len(groups) == 1
    name = groups[0]["name"]
    assert (name.decode() if isinstance(name, bytes) else name) == str(GROUP)


async def test_a_group_created_at_offset_zero_sees_earlier_messages(redis_client):
    """Spec 3.2 — the first-boot race. With '$' this test would read nothing."""
    published = _envelope()
    await RedisStreamPublisher(redis_client).publish(STREAM, published)

    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)

    messages = await consumer.read(STREAM, GROUP, block_ms=100)
    assert [m.envelope for m in messages] == [published]


async def test_read_returns_the_envelope_intact(redis_client):
    published = _envelope("corr-intact")
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    await RedisStreamPublisher(redis_client).publish(STREAM, published)

    message = (await consumer.read(STREAM, GROUP, block_ms=100))[0]
    assert message.envelope == published
    assert isinstance(message.message_id, str)


async def test_an_acked_message_is_not_redelivered(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    await RedisStreamPublisher(redis_client).publish(STREAM, _envelope())

    first = await consumer.read(STREAM, GROUP, block_ms=100)
    assert len(first) == 1
    await consumer.ack(STREAM, GROUP, first[0].message_id)

    assert await consumer.read(STREAM, GROUP, block_ms=100) == []
    pending = await redis_client.xpending(str(STREAM), str(GROUP))
    assert pending["pending"] == 0


async def test_an_unacked_message_stays_pending(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    await RedisStreamPublisher(redis_client).publish(STREAM, _envelope())

    await consumer.read(STREAM, GROUP, block_ms=100)
    pending = await redis_client.xpending(str(STREAM), str(GROUP))
    assert pending["pending"] == 1


async def test_two_groups_on_one_stream_each_receive_every_message(redis_client):
    """delivery.failed is read independently by routing and notification."""
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, ConsumerGroup.ROUTING)
    await consumer.ensure_group(STREAM, ConsumerGroup.NOTIFICATION_RESULTS)
    published = _envelope()
    await RedisStreamPublisher(redis_client).publish(STREAM, published)

    for group in (ConsumerGroup.ROUTING, ConsumerGroup.NOTIFICATION_RESULTS):
        messages = await consumer.read(STREAM, group, block_ms=100)
        assert [m.envelope for m in messages] == [published]


async def test_read_on_an_empty_stream_returns_an_empty_list(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    assert await consumer.read(STREAM, GROUP, block_ms=50) == []


async def test_count_caps_the_batch_size(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    publisher = RedisStreamPublisher(redis_client)
    for _ in range(5):
        await publisher.publish(STREAM, _envelope())

    assert len(await consumer.read(STREAM, GROUP, count=2, block_ms=100)) == 2


async def _strand(redis_client, envelope: EventEnvelope | None = None) -> RedisStreamConsumer:
    """Publish one message and let a consumer named "dead" read it without acking."""
    dead = RedisStreamConsumer(redis_client, consumer_name="dead")
    await dead.ensure_group(STREAM, GROUP)
    await RedisStreamPublisher(redis_client).publish(STREAM, envelope or _envelope())
    await dead.read(STREAM, GROUP, block_ms=100)
    return dead


async def test_get_pending_skips_entries_that_are_not_idle_long_enough(redis_client):
    dead = await _strand(redis_client)
    assert await dead.get_pending(STREAM, GROUP, min_idle_ms=60_000) == []


async def test_get_pending_reports_idle_entries_with_their_owner(redis_client):
    dead = await _strand(redis_client)
    await asyncio.sleep(0.05)

    pending = await dead.get_pending(STREAM, GROUP, min_idle_ms=20)

    assert len(pending) == 1
    assert pending[0].consumer == "dead"
    assert pending[0].times_delivered == 1
    assert pending[0].idle_ms >= 20
    assert isinstance(pending[0].message_id, str)


async def test_claim_moves_an_idle_entry_to_the_claiming_consumer(redis_client):
    """Spec 2.1: consumer names change on restart, so recovery must claim."""
    published = _envelope("corr-claim")
    dead = await _strand(redis_client, published)
    await asyncio.sleep(0.05)
    message_id = (await dead.get_pending(STREAM, GROUP, min_idle_ms=20))[0].message_id

    alive = RedisStreamConsumer(redis_client, consumer_name="alive")
    claimed = await alive.claim(STREAM, GROUP, min_idle_ms=20, message_ids=[message_id])

    assert claimed == [(message_id, published)]
    owner = await redis_client.xpending_range(str(STREAM), str(GROUP), "-", "+", 10)
    assert owner[0]["consumer"] in (b"alive", "alive")


async def test_claim_does_not_steal_an_entry_that_is_not_idle_enough(redis_client):
    dead = await _strand(redis_client)
    message_id = (await dead.get_pending(STREAM, GROUP, min_idle_ms=0))[0].message_id

    alive = RedisStreamConsumer(redis_client, consumer_name="alive")
    assert await alive.claim(STREAM, GROUP, min_idle_ms=60_000, message_ids=[message_id]) == []


async def test_claim_returns_none_for_an_unparseable_entry(redis_client):
    dead = RedisStreamConsumer(redis_client, consumer_name="dead")
    await dead.ensure_group(STREAM, GROUP)
    await redis_client.xadd(str(STREAM), {"garbage": "not an envelope"})
    with pytest.raises(ValueError):
        await dead.read(STREAM, GROUP, block_ms=100)
    await asyncio.sleep(0.05)
    message_id = (await dead.get_pending(STREAM, GROUP, min_idle_ms=20))[0].message_id

    claimed = await RedisStreamConsumer(redis_client, "alive").claim(
        STREAM, GROUP, min_idle_ms=20, message_ids=[message_id]
    )

    assert claimed == [(message_id, None)]


async def test_claim_skips_an_entry_deleted_from_the_stream(redis_client):
    dead = await _strand(redis_client)
    await asyncio.sleep(0.05)
    message_id = (await dead.get_pending(STREAM, GROUP, min_idle_ms=20))[0].message_id
    await redis_client.xdel(str(STREAM), message_id)

    claimed = await RedisStreamConsumer(redis_client, "alive").claim(
        STREAM, GROUP, min_idle_ms=20, message_ids=[message_id]
    )

    assert claimed == []


async def test_claim_with_no_ids_does_not_call_redis(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="alive")
    assert await consumer.claim(STREAM, GROUP, min_idle_ms=0, message_ids=[]) == []
