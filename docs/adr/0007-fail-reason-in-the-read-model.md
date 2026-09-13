# 0007: `fail_reason` in the read model

## Status

Accepted

## Context

An earlier design stores `fail_reason` on the `notifications` table but omits it from the
`GET /notifications/{id}` response model, so a client that polls to `FAILED` learns that delivery
failed but never learns why.

## Decision

`fail_reason` is included in `NotificationRead`
(`services/notification-service/app/schemas/notification.py`), `null` unless status is `FAILED`.

## Consequences

The response schema now carries a field that is meaningful only in one of four states, so every
consumer of the API must treat `fail_reason: null` as "not failed (yet)" rather than "no reason
given." The value is whatever string the failing handler wrote — `channel_disabled`,
`unknown_channel`, or `simulated_failure` — so the client-facing contract is coupled to those
internal reason strings; renaming one is a response-body change, not just an internal refactor.
