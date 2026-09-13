# 0022: The delivery row is inserted in its terminal state

## Status

Accepted

## Context

Spec §6.1 step 5 and the `email_delivery` comment in §7 described a two-step write: insert the
row as `SENDING`, simulate delivery, then update it to `DELIVERED` or `FAILED`. The code in
`services/email-service/app/workers/routed_consumer.py::RoutedConsumer._handle` never does this.
It calls `_deliver` first — the simulated send, including the latency sleep — and only then
inserts a single `email_delivery` row, already carrying its terminal status
(`DELIVERED`/`FAILED`). `DeliveryStatus.SENDING` is never written by production code; it appears
only as a fixture value in
`services/email-service/tests/test_routed_consumer.py::test_the_notification_id_unique_constraint_blocks_a_second_delivery`.

Three documents claimed a state the table never holds. That is corrected by this ADR rather than
by changing the code, because the two ways to make the code match the old text are both worse:

- Inserting `SENDING` and then updating it to terminal inside the same transaction is invisible
  to every observer — no other transaction can see the row between the two writes — so it is
  ceremony with no behavioural effect, only the appearance of a state machine.
- Splitting the insert and the update across two transactions would mean the idempotency check,
  the state change, the outbox row and `mark_processed` no longer land in one transaction, which
  breaches spec §5.4's transactional rule for consumers.

## Decision

Keep the code as it is: `_handle` performs the simulated delivery, then inserts `email_delivery`
once, already in its terminal state, in the same transaction as the outbox row and
`mark_processed`. Correct spec §6.1 step 5, the `email_delivery` comment in spec §7, and
`services/email-service/README.md` to describe this instead of a `SENDING → DELIVERED` update
that never happens.

## Consequences

`DeliveryStatus.SENDING` remains in the enum as an unused member — no production code path ever
writes it. A reader of `app/models/email_delivery.py` alone may reasonably expect a `SENDING` row
to exist somewhere (mid-delivery, say), and it never does; this ADR is the record of why the
member is defined but silent, so that expectation gets corrected by documentation rather than by
surprise.
