# 0030: Reserved recipients are always simulated

## Status

Accepted

## Context

Once email-service can send real SMTP mail and telegram-service can send real Bot API messages
(ADR 0029, ADR 0026), every automated path that submits a notification becomes a way to send a real
message the moment credentials sit in a developer's `.env` — the e2e suite, the README's own `curl`
examples, and the playground's load test all submit notifications, and none of them should be able
to leave the platform for a stranger's inbox or chat just because someone configured real
credentials for manual testing.

## Decision

`shared/notification_shared/delivery.py::is_reserved_recipient` recognizes recipients that RFC 2606
reserves for documentation and testing and can never reach a real destination: for email, the
domains `example.com`, `example.org`, `example.net`, or any domain under the top-level domains
`.example`, `.invalid`, `.test`; for Telegram, a chat id starting with `sim-`. The shared rules
(`is_failure_recipient` then `is_reserved_recipient`) run in every delivery consumer — email's
`RoutedConsumer._deliver` and telegram's `RoutedConsumer._deliver` — in that order, before any
network call: the `fail` marker is checked first, then reserved status, and only a recipient that is
neither uses whatever sender is actually configured. A reserved recipient always resolves to the
`SimulatedSender`, even when `SMTP_HOST` or `TELEGRAM_BOT_TOKEN` is set. The load test engine
(`tools/playground/loadtest.py::plan_notifications`) generates only reserved recipients
(`load-{i}@example.com`, `sim-load-{i}`, with `-fail` inserted for the failing share) and raises
rather than accepting a caller-supplied recipient, so a load test cannot send a real message by
construction, not merely by convention.

## Consequences

Every automated path — `tests/e2e`, the load test, and every README example — is safe to run
against a stack with real credentials configured, because every recipient they use is reserved.
This is what makes it possible to keep real credentials in a developer's own `.env` for manual
playground testing without also having to remember to unset them before running the test suite. The
cost lands on a human: a user who types a reserved-looking address expecting to prove real delivery
works — testing with their own address under `example.com` by habit, say — gets a silent simulation
instead of the real send they expected. The playground states this rule next to the send form
(`RESERVED_HINT` in `tools/playground/app.py`) specifically so that surprise does not happen
silently. `GET /version`'s `delivery_mode` reports the *configured* sender, not what any individual
recipient will actually get — a service configured for `smtp` still simulates a reserved recipient,
so `delivery_mode` alone cannot tell an operator whether a specific message went out for real.

Proof: `tests/unit/test_delivery.py` covers the reserved domains and TLDs, near-misses such as
`example.com.evil.org` and `notexample.com`, and reserved chat ids, at the shared-rule level.
`services/email-service/tests/test_routed_consumer.py::test_a_reserved_recipient_never_reaches_the_configured_sender`
and `services/telegram-service/tests/test_bot_api.py::test_a_sim_chat_id_never_reaches_the_bot_api`
prove it end to end: a reserved recipient never reaches the real sender even when one is configured.
