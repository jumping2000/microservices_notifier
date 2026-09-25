# telegram-service

Delivers Telegram notifications. Simulated by default; real delivery through the Telegram Bot API
when `TELEGRAM_BOT_TOKEN` is configured (ADR 0026). It exposes **no domain REST endpoints**: its
entire job runs in background workers reacting to `notification.routed`.

## Tables

| Table | Purpose |
|---|---|
| `telegram_delivery` | One row per notification, inserted once already in its terminal state (ADR 0022): `notification_id` (unique), `chat_id`, `status` (`DELIVERED`\|`FAILED`), `fail_reason`, `sent_at` |
| `outbox` | Events awaiting publication (`OutboxMixin`) |
| `processed_events` | Idempotency ledger (`ProcessedEventMixin`) |

## Streams consumed

| Stream | Consumer group | Behaviour |
|---|---|---|
| `notification.routed` | `telegram-service` | Filters `payload.channel == "telegram"` (acks and skips otherwise); delivers, then writes `telegram_delivery` already in its terminal state and publishes `DeliveryCompleted` or `DeliveryFailed` |

## Streams published

| Stream | Event type |
|---|---|
| `delivery.completed` | `DeliveryCompleted` |
| `delivery.failed` | `DeliveryFailed` |

The notification's `recipient` is the chat id (ADR 0026) — there is no separate `TELEGRAM_CHAT_ID`.
Delivery fails deterministically, before any network call, when the chat id contains the substring
`fail` (case insensitive), and a chat id starting with `sim-` is always simulated regardless of
configuration (ADR 0030) — see "Delivery" below for the full rule order.

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | — (required) | `postgresql+asyncpg://...` |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `SERVICE_NAME` | `telegram-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | |
| `OUTBOX_BATCH_SIZE` | `100` | |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | |
| `PENDING_TIMEOUT_MS` | `30000` | How long an entry must be idle before recovery claims it |
| `PENDING_MAX_RETRIES` | `3` | Recovery attempts before `give_up` |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | Recovery sweep interval |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Bot API request timeout |
| `DELIVERY_LATENCY_MS_MAX` | `500` | Upper bound of the simulated random delivery delay |
| `TELEGRAM_BOT_TOKEN` | unset | Empty means simulated delivery; set switches on the Bot API |

## Delivery

Checked in this order, before any network call (`shared/notification_shared/delivery.py`, shared
with email-service so the two cannot drift — ADR 0030):

1. A chat id containing `fail` (case insensitive) → `DeliveryFailed`, reason `simulated_failure`.
2. A chat id starting with `sim-` → the simulated sender, even when `TELEGRAM_BOT_TOKEN` is set.
3. Otherwise → the configured sender: simulated when `TELEGRAM_BOT_TOKEN` is unset or empty, the
   Bot API when it is set.

Bot API outcomes (spec section 4.5), after the rules above:

| Response | Outcome |
|---|---|
| `200` with `ok: true` | `DeliveryCompleted` |
| `400`, `403` | permanent → `DeliveryFailed`, reason `telegram_rejected` |
| `429`, `5xx`, timeout, connection error, any other status | transient → `handle` raises; nothing is written or acked; `PendingRecoverer` retries and eventually gives up with `max_retries_exceeded` |

The token is part of the request URL, and httpx logs request URLs at `INFO`; `silence_client_logs()`
sets the `httpx` and `httpcore` loggers to `WARNING` so the token never reaches logs.

`GET /version` reports `delivery_mode`: `simulated` or `bot_api`. It describes the *configured*
sender — a `sim-` chat id is still simulated even when `delivery_mode` reports `bot_api`.

## HTTP endpoints

**None for domain operations** — there is no `POST /send` (correction 3.21). Only:

| Endpoint | Notes |
|---|---|
| `GET /health` | Checks Postgres and Redis; `503` if either is down |
| `GET /version` | Reports `delivery_mode` |
| `GET /docs`, `/redoc` | |

## Workers

Three background tasks started in the FastAPI `lifespan`: the `notification.routed` consumer
(`telegram-service`), the outbox publisher, and the pending recoverer.
