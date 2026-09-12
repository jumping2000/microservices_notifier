"""Redis Streams access.

Groups are always created at offset 0 so that an event published before its
consumer started is still delivered. Replay is safe because every consumer
checks the idempotency ledger. See spec correction 3.2.
"""

from __future__ import annotations

from typing import Any, NamedTuple

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from notification_shared.events import EventEnvelope


class StreamMessage(NamedTuple):
    message_id: str
    envelope: EventEnvelope


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
