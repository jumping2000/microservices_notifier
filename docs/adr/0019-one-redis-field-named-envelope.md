# 0019: One Redis field named `envelope`

## Status

Accepted

## Context

Redis Streams entries are field-value maps; a design could flatten an event's fields directly into
that map (`channel`, `recipient`, `event_type`, ... as separate stream fields), which would let
`redis-cli XRANGE` show individual fields at a glance but would require every producer and consumer
to agree on a flat field set per event type, and would need a schema change in Redis-facing code
every time a payload field is added.

## Decision

Each `XADD` writes a single field named `envelope`, whose value is the JSON-serialized
`EventEnvelope` (`EventEnvelope.to_redis()` / `.from_redis()` in
`shared/notification_shared/events.py`). Consumers parse that one field. No multi-field
flattening.

## Consequences

Inspecting a stream entry with `redis-cli XRANGE` now shows one opaque JSON blob per entry rather
than legible individual fields — reading it requires piping through something that can parse JSON,
which `docs/operations.md` (slice 3) will need to show. In exchange, adding or changing a payload
field is purely a Pydantic model change with no corresponding change to how entries are written to
or read from Redis: `RedisStreamConsumer.read` and `RedisStreamPublisher.publish` never need to
know what a payload contains.
