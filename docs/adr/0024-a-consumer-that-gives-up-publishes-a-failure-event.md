# 0024: A consumer that gives up publishes a failure event

## Status

Accepted

## Context

Slice 1 spec section 15 left max-retry handling as "discard" — an event that keeps failing is
eventually dropped and nothing more is said about it. Discarding alone is not enough: it leaves the
notification wherever it was when the last attempt failed. A routing consumer that gives up on
`notification.created` leaves the notification `CREATED` forever, because routing never happened
and no one is coming to change it. A delivery consumer that gives up on `notification.routed`
leaves it `PROCESSING` forever, because the result it was waiting for will never arrive. Slice 2's
stale-processing watchdog (ADR 0028) closes `PROCESSING` rows after a timeout, but it only ever
sees `PROCESSING` — a notification stuck `CREATED` has no safety net at all.

## Decision

After `PENDING_MAX_RETRIES` failed recovery attempts, `PendingRecoverer._recover`
(`shared/notification_shared/recovery.py`) calls the consumer's `give_up(session, envelope)`. Each
`give_up` writes its own failure record and a failure event through the outbox, in the same
transaction as `IdempotencyRepository.mark_failed_permanent` — `RoutingFailed` on `delivery.failed`
from `services/routing-service/app/workers/notification_consumer.py`, `DeliveryFailed` on
`delivery.failed` from `services/email-service/app/workers/routed_consumer.py` and
`services/telegram-service/app/workers/routed_consumer.py` — always with reason
`max_retries_exceeded`. Only once that transaction commits does the recoverer `XACK` the entry.

Two consumers own the notification directly and have no one downstream to tell:
`services/notification-service/app/workers/routed_consumer.py`'s and `results_consumer.py`'s
`give_up` methods are empty (a notification left `PROCESSING` is the watchdog's job), and so is
`services/routing-service/app/workers/results_consumer.py`'s (the route stays `PROCESSING`, a
documented limitation — see "Known limitations").

## Consequences

A persistent outage now ends the notification `FAILED` with a precise reason
(`max_retries_exceeded`) within roughly two minutes at the default settings, instead of leaving it
stuck indefinitely. `give_up` must never call an external system — a Configuration Service request,
an SMTP send, a Bot API call — because it runs after every ordinary retry has already failed for
whatever reason `handle` failed for; if `give_up` itself raised for the same reason, the platform
would have no way to ever close the notification, and `PendingRecoverer._recover` treats a raising
`give_up` as "stays pending, try again next cycle" specifically because that failure mode must stay
recoverable. Every consumer therefore owns a second public method beyond `handle`, which is more
surface for a reader to hold in their head, but it is where the responsibility actually belongs: the
consumer that knows how to write its own failure row is also the one that knows how to write it
after giving up.

If `give_up` raises, `_recover` always calls `consumer.handle(envelope)` once more on the next
cycle before re-checking the retry count and giving up again — `handle` runs unconditionally at the
top of `_recover` regardless of how high `fail_count` already is. Spec 2.3's "the count is already
at the limit, so it goes straight to give-up" describes the retry-count check, not an extra `handle`
skip; the implementation still runs `handle` every cycle.

Proof: `services/routing-service/tests/test_notification_consumer.py::test_a_persistent_outage_ends_in_routing_failed_after_max_retries`
drives a consumer through `PENDING_MAX_RETRIES` failed attempts and asserts the route ends `FAILED`
with a `RoutingFailed` outbox row carrying `max_retries_exceeded`.
