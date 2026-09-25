# 0028: The watchdog publishes no event

## Status

Accepted

## Context

A notification reaches `PROCESSING` when `notification-service-routed` consumes
`NotificationRouted`, and only leaves it when a delivery result arrives on
`notification-service-results`. If that result never arrives — the delivery consumer crashed and
recovery eventually gave up silently before slice 2 (ADR 0024 closes that specific case now), or any
other reason the result stream simply never carries the event — the notification sits `PROCESSING`
forever with nothing watching it. Correction 3.16 of the slice 1 spec described the fix as a guarded
SQL sweep; slice 1 did not build it.

## Decision

A new worker, `services/notification-service/app/workers/watchdog.py::Watchdog`, runs every
`WATCHDOG_INTERVAL_SECONDS` and calls `NotificationRepository.fail_stale_processing`
(`services/notification-service/app/repositories/notification.py`):

```sql
UPDATE notifications
SET status = 'FAILED', fail_reason = 'processing_timeout'
WHERE status = 'PROCESSING'
  AND updated_at < now() - make_interval(mins => :processing_timeout_minutes)
```

This is the same guarded-`WHERE` shape as every other status transition in the platform (ADR 0016):
only `PROCESSING` rows older than `PROCESSING_TIMEOUT_MINUTES` match, so a row already terminal is
never touched, and `now()` is the database's own clock so container clock skew cannot affect the
cutoff. The watchdog publishes nothing — no outbox row, no stream event. It only ever touches
`notifications`; Routing Service's `routes` row for the same notification is untouched.

## Consequences

Routing Service's `routes` row stays `PROCESSING` after the watchdog fails the notification —
nothing tells Routing Service the notification closed, because the watchdog does not publish, and
Routing Service's own `give_up` for its results consumer already leaves the route `PROCESSING` for
the same reason (ADR 0024). This is a known, accepted inconsistency between the two services' views
of the same notification, not a bug to fix later.

More consequential: terminal states never reopen (ADR 0016), so a delivery result that arrives
*after* the watchdog has already failed the notification does not reopen it. The notification reads
`FAILED` / `processing_timeout` even though the message was, in fact, delivered — the watchdog's
guard only admits `PROCESSING` rows, so once it has written `FAILED` the later completion event's
guard (`status IN ('CREATED', 'PROCESSING')`) matches zero rows and changes nothing, exactly as
designed for every other out-of-order arrival. This is a real, user-visible cost of the watchdog
existing at all, worth stating plainly rather than discovering by surprise: the watchdog trades "a
notification can sit `PROCESSING` forever" for "a notification can read `FAILED` after actually
succeeding," and the platform accepts that trade because the first failure mode is silent and
unbounded while the second is bounded by `PROCESSING_TIMEOUT_MINUTES` and, at least, visibly
`FAILED` rather than invisibly stuck.

Proof: `services/notification-service/tests/test_watchdog.py::test_a_late_delivery_result_does_not_reopen_it`
fails a notification via the watchdog, then delivers a `DeliveryCompleted` for the same notification
and asserts the status is still `FAILED`. `test_a_stale_processing_notification_is_failed` and
`test_a_recent_processing_notification_is_left_alone` prove the timeout guard itself;
`test_other_statuses_are_never_touched` proves terminal rows are untouched.
