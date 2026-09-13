# 0023: The idempotency check may precede the write transaction

## Status

Accepted

## Context

Spec §5.4 said every consumer handler runs one transaction containing the idempotency check, the
domain state change, any outbox row, and the `mark_processed` write. `docs/patterns.md` repeated
this as an absolute: "Every consumer handler in every service calls `is_processed` before doing
any work and `mark_processed` inside the same commit as the state change."

Two of the six consumer handlers do not do this, and cannot:

- `services/routing-service/app/workers/notification_consumer.py`'s `_handle` calls
  `is_processed` in a first session, closes it, then calls Configuration Service over HTTP
  (`_decide`), and only afterwards opens a second session for the route row, the outbox row and
  `mark_processed`.
- `services/email-service/app/workers/routed_consumer.py`'s `_handle` calls `is_processed` in a
  first session, closes it, then runs the simulated delivery (`_deliver`, including its latency
  sleep), and only afterwards opens a second session for the `email_delivery` row, the outbox row
  and `mark_processed`.

Both do slow, synchronous I/O — a REST call, a sleep standing in for real delivery latency — that
must not run inside an open database transaction: holding a transaction across an HTTP round trip
or an arbitrary delay ties up a connection and, under load, a connection pool, for no reason
related to the database work itself. The alternative — hold the transaction open across the slow
call — is the actual bug the deviation avoids. Notification Service's two consumers
(`routed_consumer.py`, `results_consumer.py`) have no such slow step between the check and the
decision, so they keep all four operations in one transaction, exactly as originally specified.

## Decision

Narrow spec §5.4: the state change, the outbox row and `mark_processed` are always atomic in one
transaction. The idempotency check is part of that same transaction only when the handler has
nothing slower than a local write to do first; when the handler must perform slow I/O before it
can decide what the state change even is, `is_processed` runs in its own earlier, short
transaction instead. `docs/patterns.md`'s "Idempotent consumers" section is corrected to name
which consumers keep all four together and which two split the check out, and why.

## Consequences

This is a real cost, not just a documentation nit, and it matters most for what slice 2 adds.
Today, in slice 1, the topology invariant (one `notification.created` in, at most one
`routes`/`email_delivery` row out, per notification) means a split check is safe in practice: only
one handler instance is ever working a given event at a time. Once slice 2 adds `XCLAIM` recovery,
a message whose original consumer is slow (mid-HTTP-call, mid-delivery-sleep) can be claimed by
another consumer and processed concurrently. Both instances can call `is_processed` in their own
transaction before either commits, both see "not yet processed", and both can proceed to write —
the advisory check does not prevent this once the check and the write are no longer atomic with
each other. What actually stops the duplicate is `UNIQUE (notification_id)` on `routes` and
`email_delivery` (correction 3.6): the second insert fails with `IntegrityError` rather than
silently producing two rows. Slice 2 must not rediscover this by tracing a race; it should build
its `XCLAIM` recovery already knowing the unique constraint, not the idempotency check, is the
backstop for these two consumers.
