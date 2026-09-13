# 0003: Routing Service must not consume its own `RoutingFailed`

## Status

Accepted

## Context

Routing Service publishes `RoutingFailed` to `delivery.failed` when a channel is disabled or
unknown. Routing Service also subscribes to `delivery.failed` (to close out its `routes` row on
delivery outcomes), so without a filter it would consume the very event it just produced — a loop
in the topology, and a route that already recorded `FAILED` would be processed again for no
reason.

## Decision

The `routing-service-results` consumer (`services/routing-service/app/workers/results_consumer.py`)
handles only `DeliveryCompleted` and `DeliveryFailed`. It `XACK`s and skips `RoutingFailed`,
mirroring the channel-filter pattern already used by the delivery services to ignore events meant
for the other channel. `RoutingFailed` stays published on `delivery.failed` so Notification Service
still receives it and can move the notification to `FAILED`.

## Consequences

The results consumer must inspect `event_type` before doing any work, adding a branch that exists
solely to recognize "this is mine, I already handled it." A future event type added to
`delivery.failed` must be explicitly added to the handled set or it will silently be skipped here
too — the filter is an allow-list (`DELIVERY_COMPLETED`, `DELIVERY_FAILED`), not a deny-list of
`RoutingFailed` alone.
