# 0026: Telegram's recipient is the chat id

## Status

Accepted

## Context

The slice 1 spec said real Telegram Bot API delivery would need two environment variables:
`TELEGRAM_BOT_TOKEN` to authenticate the bot, and `TELEGRAM_CHAT_ID` to say where to send. But every
notification the platform accepts already carries a `recipient` field, the same field email
delivery uses as the destination address. A fixed `TELEGRAM_CHAT_ID` would mean every Telegram
notification in a given deployment goes to the same chat regardless of what the caller asked for —
useful for a single-operator demo, but inconsistent with how the email channel already works, and
with what a `recipient` field is for.

## Decision

The notification's `recipient` **is** the Telegram chat id. `TELEGRAM_CHAT_ID` is removed from
configuration entirely; `TELEGRAM_BOT_TOKEN` alone switches real delivery on
(`services/telegram-service/app/senders.py::build_sender`). In
`services/telegram-service/app/workers/routed_consumer.py::RoutedConsumer.handle`, `chat_id =
envelope.payload["recipient"]` — the same field name and the same forwarding path
(`NotificationRouted` carries `recipient` forward from `NotificationCreated`, per ADR 0020) that
email delivery already uses.

## Consequences

Both channels now have exactly one recipient concept: one notification, one destination address,
carried in the same field regardless of which channel it is routed to. The shared delivery rules in
`shared/notification_shared/delivery.py` apply to chat ids the same way they apply to email
addresses — the `fail` substring marker fails deterministically, and a `sim-`-prefixed chat id is
always simulated (ADR 0030) — because both rules operate on "the recipient", not on a
channel-specific concept. Telegram's own outcome classification
(`services/telegram-service/app/senders.py::classify_response`) treats a `400` or `403` response as
permanent (`telegram_rejected`) — the Bot API rejected this specific chat id or message — while
`429`, `5xx`, and network failures are transient and left to `PendingRecoverer`. Anyone who wants a
Telegram notification to reach a specific chat now configures that chat id as the `recipient` at
submission time, the same way they would configure an email address.
