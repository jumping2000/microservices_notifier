"""Redis Streams access.

Groups are always created at offset 0 so that an event published before its
consumer started is still delivered. Replay is safe because every consumer
checks the idempotency ledger. See spec correction 3.2.

`get_pending` and `claim` exist for `PendingRecoverer`: consumer names are
container hostnames and change on restart, so a crashed consumer's pending
entries can only be recovered by claiming them by idle time. Slice 2 spec 2.1.
"""

from __future__ import annotations

from typing import Any, NamedTuple

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from notification_shared.events import EventEnvelope


class StreamMessage(NamedTuple):
    message_id: str
    envelope: EventEnvelope


class PendingEntry(NamedTuple):
    message_id: str
    consumer: str
    idle_ms: int
    times_delivered: int


class ClaimedMessage(NamedTuple):
    message_id: str
    # None when the entry cannot be parsed; the recoverer acks and discards it.
    envelope: EventEnvelope | None


def _as_str(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class RedisStreamPublisher:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def publish(self, stream: str, envelope: EventEnvelope) -> str:
        message_id = await self._redis.xadd(str(stream), envelope.to_redis())
        return _as_str(message_id)


class RedisStreamConsumer:
    def __init__(self, redis: Redis, consumer_name: str) -> None:
        self._redis = redis
        self._consumer_name = consumer_name

    async def ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(
                name=str(stream), groupname=str(group), id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read(
        self, stream: str, group: str, count: int = 10, block_ms: int = 1000
    ) -> list[StreamMessage]:
        response = await self._redis.xreadgroup(
            groupname=str(group),
            consumername=self._consumer_name,
            streams={str(stream): ">"},
            count=count,
            block=block_ms,
        )
        messages: list[StreamMessage] = []
        for _stream_name, entries in response or []:
            for message_id, fields in entries:
                messages.append(
                    StreamMessage(_as_str(message_id), EventEnvelope.from_redis(fields))
                )
        return messages

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        await self._redis.xack(str(stream), str(group), message_id)

    async def get_pending(
        self, stream: str, group: str, min_idle_ms: int, count: int = 10
    ) -> list[PendingEntry]:
        """XPENDING with an IDLE filter. Slice 1 spec correction 3.5."""
        rows = await self._redis.xpending_range(
            str(stream), str(group), min="-", max="+", count=count, idle=min_idle_ms
        )
        return [
            PendingEntry(
                message_id=_as_str(row["message_id"]),
                consumer=_as_str(row["consumer"]),
                idle_ms=int(row["time_since_delivered"]),
                times_delivered=int(row["times_delivered"]),
            )
            for row in rows
        ]

    async def claim(
        self, stream: str, group: str, min_idle_ms: int, message_ids: list[str]
    ) -> list[ClaimedMessage]:
        """XCLAIM to this consumer. The same min_idle_ms as get_pending means an
        entry another consumer picked up in the meantime is not stolen."""
        if not message_ids:
            return []
        entries = await self._redis.xclaim(
            str(stream),
            str(group),
            self._consumer_name,
            min_idle_time=min_idle_ms,
            message_ids=message_ids,
        )
        claimed: list[ClaimedMessage] = []
        for message_id, fields in entries:
            if message_id is None or not fields:
                # Deleted from the stream since it was delivered.
                continue
            try:
                envelope = EventEnvelope.from_redis(fields)
            except ValueError:
                envelope = None
            claimed.append(ClaimedMessage(_as_str(message_id), envelope))
        return claimed
