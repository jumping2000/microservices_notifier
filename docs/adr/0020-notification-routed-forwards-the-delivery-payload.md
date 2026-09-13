# 0020: `NotificationRouted` forwards the delivery payload

## Status

Accepted

## Context

Email and Telegram services need `recipient`, `subject`, and `body` to actually deliver a
notification, but they cannot read Notification Service's database (database-per-service; see
`docs/architecture.md`) and cannot call its REST API for a domain read (services communicate
cross-service only through Redis Streams, plus the one sanctioned Routing → Configuration REST
call). Routing Service itself does not persist the notification body — it never needed to before
this.

## Decision

`NotificationRouted` carries the delivery fields (`recipient`, `subject`, `body`) forward from
`NotificationCreated`, unchanged, through Routing Service
(`services/routing-service/app/workers/notification_consumer.py`). This is a deliberate,
consciously accepted exception to "events are small and focused": the only alternatives were a
forbidden cross-service read or a shared database, both of which the architecture already rules
out.

## Consequences

This knowingly breaks the "events are small" principle stated elsewhere in this project's own
design: `NotificationRouted` is not a small fact about routing, it duplicates the entire delivery
payload of `NotificationCreated` plus a `route_id`. Every future producer of a routed-style event
must decide the same trade-off again — forward the data or accept a cross-service read — and this
ADR exists so that trade-off is visible rather than accidental. It also means Email Service's
`email_delivery` table stores a `recipient` that is a copy of data Notification Service already
persisted, not a reference to it; the two copies can never drift because Routing Service passes the
value through unmodified, but a future change to how recipients are represented would need to
change in two places.
