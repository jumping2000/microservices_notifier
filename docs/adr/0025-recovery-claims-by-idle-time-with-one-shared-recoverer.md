# 0025: Recovery claims by idle time, with one shared recoverer

## Status

Accepted

## Context

Every consumer registers with Redis under a name of `socket.gethostname()`. In Docker that name is
the container id, which is different every time the container restarts. A consumer that crashes
mid-handle leaves its entries pending under a consumer name that no longer exists; a restarted
consumer, reading under its own new name, has no way to ask Redis for "my predecessor's unfinished
work" — `XREADGROUP ... 0` only replays a name's *own* history, and that name is gone. Recovery
therefore cannot be "re-read what I left pending"; it has to find idle entries regardless of which
consumer name owns them and take them over.

## Decision

`RedisStreamConsumer` (`shared/notification_shared/streams.py`) gains two methods for this:
`get_pending(stream, group, min_idle_ms, count)`, which wraps `XPENDING <stream> <group> IDLE
<min_idle_ms> - + <count>`, and `claim(stream, group, min_idle_ms, message_ids)`, which wraps
`XCLAIM` with the same `min_idle_ms` so an entry another consumer picked up in the meantime is not
stolen out from under it. `claim` returns a `ClaimedMessage` with `envelope=None` when the entry's
JSON cannot be parsed; an entry Redis reports as nil (deleted from the stream since delivery) is
skipped.

A single `PendingRecoverer` (`shared/notification_shared/recovery.py`) drives this, one instance per
service, wired in `lifespan` next to `OutboxPublisher` — the mechanics are identical everywhere, so
they are shared exactly the way `OutboxPublisher` is, while the per-service consumers a reader is
meant to study stay explicit (slice 1 ledger, ruling F4). Each service calls `register(stream,
group, consumer)` once per `(stream, group)` a consumer reads — a consumer reading two streams,
like both results consumers, registers once per stream. `recover_once()` sweeps every registration:
`get_pending`, `claim`, then for each claimed message calls the consumer's public `handle` — the
same method the normal read path calls, made public rather than duplicated. A failure there
increments `processed_events.fail_count` via
`IdempotencyRepository.increment_fail_count`, committed in its own transaction; **only the recoverer
counts attempts** — a failure on the normal read path just leaves the entry pending, uncounted.
`fail_count` is never reset, including by `mark_processed` overwriting a `FAILING` row (slice 1
ledger, Task 6 deferred minor, now deliberate): the count is the history of how many attempts an
event needed, readable with `psql`.

## Consequences

Recovery latency is at least `PENDING_TIMEOUT_MS` (default `30000`): an entry is only eligible once
it has been idle that long, so the fastest a stranded message is noticed is one poll interval after
that timeout. Consumer names of dead containers accumulate in each group's `XINFO CONSUMERS`
listing — Redis does not clean these up on its own, and nothing in this platform does either; it is
harmless, just visible clutter for anyone inspecting the group. An entry whose envelope cannot be
parsed is acked and discarded at ERROR — it has no `event_id`, so it cannot be counted in
`processed_events`, and leaving it pending forever would block nothing (recovery moves past it) but
would never resolve either. The per-service consumers keep their explicit `read`/`handle`/`ack`
loops for the normal path; only the recovery path is generic, which is the whole point of drawing
the line here rather than folding recovery into each consumer's own loop.

Proof: `tests/integration/test_recovery.py` covers the claim (`test_a_stranded_message_is_claimed_and_completed`),
the idle guard (`test_a_message_that_is_not_idle_yet_is_left_alone`), the counting
(`test_each_failed_attempt_increments_the_count`), give-up
(`test_the_last_failed_attempt_gives_up_and_acks`), a raising `give_up`
(`test_a_give_up_that_raises_rolls_back_and_stays_pending`), an unparseable entry
(`test_an_unparseable_entry_is_acked_and_discarded`), and one sweep driving several registrations
(`test_every_registration_is_recovered`).
