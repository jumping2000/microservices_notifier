# 0018: Configuration Service unavailability is transient

## Status

Accepted

## Context

An earlier design does not say what Routing Service should do when its HTTPX call to Configuration
Service times out or returns a 5xx response. Treating that the same as "channel disabled" or
"channel unknown" would record a permanent routing failure for what may be a temporary outage.

## Decision

Routing Service distinguishes by response, in `NotificationCreatedConsumer._decide`
(`services/routing-service/app/workers/notification_consumer.py`):

| Configuration Service response | Routing Service behaviour |
|---|---|
| `200` with `enabled: true` | publish `NotificationRouted` |
| `200` with `enabled: false` | publish `RoutingFailed`, reason `channel_disabled` |
| `404` (channel not in the table) | publish `RoutingFailed`, reason `unknown_channel` |
| timeout or `5xx` | transient: log WARNING, do **not** `XACK`, write no outbox row, leave the message pending for `XCLAIM` recovery (slice 2) |

This relies on `NotFoundError` and `ServiceUnavailableError` being sibling exception types, neither
a subclass of the other (`shared/notification_shared/exceptions.py`), so the handler can tell them
apart with a single `except`.

## Consequences

A Configuration Service outage now leaves the message unacked and pending in the
`notification.created` stream's `routing-service` group rather than resolved one way or the other.
In slice 1, with no recovery worker (`XPENDING`/`XCLAIM` is slice 2), that message simply stays
pending indefinitely — the notification is stuck at `CREATED` until slice 2's recovery lands. This
is the correct choice for correctness (an outage must never be recorded as a permanent failure) but
it means slice 1 has no way to unstick such a notification short of manually re-publishing or
restarting the consumer to reprocess pending entries.
