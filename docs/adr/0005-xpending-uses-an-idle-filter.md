# 0005: `XPENDING` uses an idle filter

## Status

Accepted

## Context

An earlier design specified `XPENDING <stream> <group> - + 30` to "find messages delivered more
than `PENDING_TIMEOUT_MS` ago". That command filters nothing by time: the trailing `30` in that
form is a result count, not a millisecond threshold. As specified, recovery would inspect an
arbitrary 30 pending entries regardless of age rather than the stale ones.

## Decision

Recovery must use `XPENDING <stream> <group> IDLE <PENDING_TIMEOUT_MS> - + <count>`, which filters
by idle time before applying the range and count. This is slice 2 work — `RedisStreamConsumer` in
slice 1 deliberately has no `get_pending()` or `claim()` method.

## Consequences

This ADR records a decision with no slice 1 code behind it yet: `get_pending()` and `claim()` are
out of scope until slice 2's recovery worker lands, so there is nothing in this repository today
that exercises the `IDLE` form. The risk it heads off is real regardless of timing: had slice 2
been built against the un-corrected command, recovery would have claimed effectively random
pending entries instead of ones actually stuck past `PENDING_TIMEOUT_MS`.
