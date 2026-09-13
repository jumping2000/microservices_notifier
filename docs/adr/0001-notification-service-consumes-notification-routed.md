# 0001: Notification Service consumes `notification.routed`

## Status

Accepted

## Context

The original design promised a client observes `CREATED → PROCESSING → COMPLETED/FAILED`, and its
stale-processing watchdog was to query `status = 'PROCESSING'`. No service ever wrote `PROCESSING`
to the `notifications` table — Routing Service marked only its own `routes` row. The watchdog
could never fire and the documented polling sequence was unreachable.

## Decision

Notification Service gains a consumer group, `notification-service-routed`, on the
`notification.routed` stream. Its handler (`RoutedConsumer` in
`services/notification-service/app/workers/routed_consumer.py`) sets
`notifications.status = 'PROCESSING'`.

## Consequences

Notification Service now runs a third background worker in slice 1 (outbox publisher, routed
consumer, results consumer) instead of one, adding a consumer group whose only job is a single
column write. It also introduces the ordering race between this consumer and
`notification-service-results` reading `delivery.completed`/`delivery.failed` concurrently — see
ADR 0016 for how that race is resolved. Without this correction, `PROCESSING` would be dead code:
declared in the schema and the API response model, never reachable in practice.
