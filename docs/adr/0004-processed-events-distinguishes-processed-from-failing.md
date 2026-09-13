# 0004: `processed_events` distinguishes processed from failing

## Status

Accepted

## Context

An earlier design used one table with `UNIQUE (event_id, consumer_group)` and a `fail_count`, with
no status column. A single failed attempt inserts a row for that `(event_id, consumer_group)` pair,
after which a naive `is_processed()` check finds a row and reports a never-successfully-processed
event as done — the event is dropped rather than retried.

## Decision

`processed_events` (materialized from `ProcessedEventMixin` in
`shared/notification_shared/models.py`) carries a `status` column with values `PROCESSED`,
`FAILING`, `FAILED_PERMANENT`. `IdempotencyRepository.is_processed()`
(`shared/notification_shared/idempotency.py`) returns `True` only for `PROCESSED` and
`FAILED_PERMANENT`; a `FAILING` row is not treated as processed.

## Consequences

Every idempotency check now depends on a status value, not merely row existence, so any future
consumer that queries `processed_events` directly (rather than through `IdempotencyRepository`)
must remember to filter on `status` or reintroduce this exact bug. `FAILED_PERMANENT` and the
retry counting that produces it (`increment_fail_count`) are not exercised anywhere in slice 1 —
they exist only so that this repository is not rewritten and re-reviewed when slice 2's max-retry
handling arrives.
