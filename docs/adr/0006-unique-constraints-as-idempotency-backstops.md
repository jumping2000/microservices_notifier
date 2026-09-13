# 0006: Unique constraints as idempotency backstops

## Status

Accepted

## Context

The idempotency ledger (`processed_events`) is the primary defence against duplicate processing,
but it is an ordinary table maintained by application code, not a database-enforced invariant on
the domain tables themselves. A bug in a handler — a missing idempotency check, a wrong consumer
group name — could still let a notification produce two routes or two delivery attempts if nothing
in the domain schema itself forbids it.

## Decision

`UNIQUE (notification_id)` is declared on `routes` (`services/routing-service/app/models/route.py`)
and `email_delivery` (`services/email-service/app/models/email_delivery.py`), and is specified for
`telegram_delivery` in slice 2. One notification produces exactly one route and one delivery
attempt per channel, enforced by Postgres regardless of what the application code does.

## Consequences

A genuine double-processing bug that reaches the database now raises an `IntegrityError` instead
of silently inserting a duplicate row — a loud failure in place of a silent one, which is the
point, but it also means such a bug surfaces as an unhandled exception in a consumer loop rather
than a clean rejection, and the event is left unacked until that is fixed. The constraint also
means the domain tables can never be extended to hold more than one attempt per channel per
notification (for example, a deliberate retry-with-new-row design) without a migration to relax it.
