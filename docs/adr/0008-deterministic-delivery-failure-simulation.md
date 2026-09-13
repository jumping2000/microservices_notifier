# 0008: Deterministic delivery failure simulation

## Status

Accepted

## Context

An earlier design says a delivery may end `FAILED` "on simulated error" without specifying when.
Non-deterministic failure — a random failure rate — cannot be tested reliably or demonstrated on
demand: a test or a manual walkthrough would need to retry until the random condition happened to
fire.

## Decision

A delivery fails when the recipient (Email) or chat_id (Telegram) contains the literal substring
`fail`. Implemented as `FAILURE_MARKER = "fail"` checked against the lower-cased recipient in
`services/email-service/app/workers/routed_consumer.py`. No random failure rate.

## Consequences

Failure is now a property of the input data rather than of runtime chance, which means a normal
recipient address that happens to contain "fail" (for example, `raphael@example.com` does not, but
a hypothetical `abigfailure@example.com` would) is deterministically treated as a failing delivery
even though nothing about it is actually invalid. This is an accepted, documented quirk of the
simulation, not a real-world validation rule — it must never be confused with actual recipient
validation.
