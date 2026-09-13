# 0017: Event payload schemas defined

## Status

Accepted

## Context

An earlier design names five event types — `NotificationCreated`, `NotificationRouted`,
`RoutingFailed`, `DeliveryCompleted`, `DeliveryFailed` — but never defines their payloads. Every
consumer's behaviour depends on what fields it can expect to find in `envelope.payload`, and
without a defined schema each service would have to guess or inspect another service's source.

## Decision

Each event type's payload is defined explicitly (see `docs/event-flows.md` for the full schemas,
copied from the specification). `subject` is nullable throughout; `aggregate_id` on every envelope
is always the `notification_id`, so the whole saga correlates on one identifier regardless of which
service or stream is involved.

## Consequences

The payload schemas are enforced only by convention and by each producer/consumer pair's own code
and tests — `EventEnvelope.payload` is typed as `dict[str, Any]` in
`shared/notification_shared/events.py`, not as a per-event-type Pydantic model, so a producer that
drifts from the documented shape is not caught by the type system. This is consistent with the
platform's stated limitation of no schema registry (see `README.md`'s known limitations): payload
correctness is tested, not enforced.
