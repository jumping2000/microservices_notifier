# 0002: Consumer groups start at offset zero

## Status

Accepted

## Context

`XGROUP CREATE <stream> <group> $ MKSTREAM` consumes only messages published after the group is
created. Under `docker compose up`, all services start concurrently, so an event published before
its consumer had created its own group was lost permanently. The Outbox Pattern guarantees an
event is *published*, not that anyone *hears* it — using `$` silently defeated the very guarantee
the platform exists to demonstrate.

## Decision

Every consumer group is created with `XGROUP CREATE <stream> <group> 0 MKSTREAM`
(`RedisStreamConsumer.ensure_group` in `shared/notification_shared/streams.py`). Starting at `0`
means a newly created group reads the stream's entire history. Idempotent consumers make that
replay safe.

## Consequences

Because a group at offset `0` replays everything Redis still holds, Postgres and Redis volumes
must be reset together. If Postgres volumes are wiped (say, `docker compose down -v` was run only
partially, or a single database container's volume was dropped by hand) while Redis's AOF file
survives, every consumer replays the full stream history against empty `processed_events` tables —
duplicate routing, duplicate delivery attempts, and duplicate status writes, all masquerading as
first-time processing. This couples the two stores' lifecycles: `README.md` and this project's
reset instructions must state that volumes are reset together, via `docker compose down -v`, never
independently.
