# 0016: Status transitions are guarded in SQL

## Status

Accepted

## Context

ADR 0001 introduces a race: `notification-service-routed` (reading `notification.routed`) and
`notification-service-results` (reading `delivery.completed`/`delivery.failed`) are independent
consumer groups with no ordering guarantee between them. With fast delivery, `delivery.completed`
can be consumed *before* `notification.routed` — the row is still `CREATED` when the completion
event arrives. An unguarded, read-then-write handler could set the notification to `COMPLETED`,
and then the late `notification.routed` event would overwrite it back to `PROCESSING`: a finished
notification silently reopened, and a visibly wrong state for a polling client.

## Decision

Status transitions are monotone and guarded in SQL, not in Python, expressed as conditional
`UPDATE ... WHERE id = :id AND status IN (...)` statements
(`NotificationRepository.advance_status` in
`services/notification-service/app/repositories/notification.py`):

| Handler | Conditional update |
|---|---|
| routed consumer | `SET status='PROCESSING' WHERE id=:id AND status='CREATED'` |
| results consumer | `SET status=:status WHERE id=:id AND status IN ('CREATED','PROCESSING')` |
| stale watchdog (slice 2) | `SET status='FAILED' WHERE status='PROCESSING' AND updated_at < :cutoff` |

Legal transitions: `CREATED → PROCESSING`, `CREATED → COMPLETED`, `CREATED → FAILED`,
`PROCESSING → COMPLETED`, `PROCESSING → FAILED`. Terminal states never reopen. An out-of-order
handler updates zero rows (`rowcount == 0`) and still `XACK`s.

`CREATED → COMPLETED` is in that list for a reason worth stating explicitly, because it looks like
a gap in the state machine and is not. In the exact race this ADR exists to handle, the delivery
result is consumed *before* `notification.routed`, so the row is still `CREATED` when the
completion arrives. If the results handler's guard admitted only `PROCESSING`, that update would
match zero rows — and because the handler still acks and still marks the event processed (the
transaction commits regardless of `rowcount`), the event would never be redelivered, and the
notification would sit at `CREATED` forever even though delivery genuinely succeeded. The guard
therefore admits both non-terminal states (`CREATED` and `PROCESSING`) for both terminal
destinations (`COMPLETED` and `FAILED`).

## Consequences

The guard lives in the repository as a Core `update()` statement rather than an ORM
load-mutate-save, which is a deliberate exception to this project's layering rule that repositories
generally wrap ORM operations — the guard must be evaluated atomically by Postgres in the `WHERE`
clause, not read and re-checked in Python, or the race reopens. `advance_status` returns the number
of rows updated specifically so callers can log "no-op, already terminal or out of order" instead
of mistaking a guarded no-op for an error. The proof this works is
`services/notification-service/tests/test_status_transitions.py::test_a_result_arriving_before_the_routed_event_is_not_overwritten`,
which publishes both events, consumes the results event first out of order, and asserts the
routed event arriving late changes nothing.
