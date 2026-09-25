# email-service

Delivers email notifications. Simulated by default; real delivery through generic SMTP when
`SMTP_HOST` is configured (ADR 0029). It exposes **no domain REST endpoints**: its entire job runs
in background workers reacting to `notification.routed`.

## Tables

| Table | Purpose |
|---|---|
| `email_delivery` | One row per notification, inserted once already in its terminal state: `notification_id` (unique), `recipient`, `status` (`DELIVERED`\|`FAILED`; `SENDING` is a defined but unwritten enum member — see ADR 0022), `fail_reason`, `sent_at` |
| `outbox` | Events awaiting publication (`OutboxMixin`) |
| `processed_events` | Idempotency ledger (`ProcessedEventMixin`) |

## Streams consumed

| Stream | Consumer group | Behaviour |
|---|---|---|
| `notification.routed` | `email-service` | Filters `payload.channel == "email"` (acks and skips otherwise); simulates delivery, then writes `email_delivery` already in its terminal state and publishes `DeliveryCompleted` or `DeliveryFailed` |

## Streams published

| Stream | Event type |
|---|---|
| `delivery.completed` | `DeliveryCompleted` |
| `delivery.failed` | `DeliveryFailed` |

There is no random failure rate — see "Delivery" below for the exact rule order.

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | — (required) | `postgresql+asyncpg://...` |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `SERVICE_NAME` | `email-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | |
| `OUTBOX_BATCH_SIZE` | `100` | |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | |
| `PENDING_TIMEOUT_MS` | `30000` | How long an entry must be idle before recovery claims it |
| `PENDING_MAX_RETRIES` | `3` | Recovery attempts before `give_up` |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | Recovery sweep interval |
| `DELIVERY_LATENCY_MS_MAX` | `500` | Upper bound of the simulated random delivery delay |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | SMTP connect timeout, reused rather than a separate variable |
| `SMTP_HOST` | unset | Empty means simulated delivery; set switches on `SmtpSender` |
| `SMTP_PORT` | `587` | |
| `SMTP_USERNAME` | unset | Unset means no authentication |
| `SMTP_PASSWORD` | unset | Never logged |
| `SMTP_FROM` | unset | Required when `SMTP_HOST` is set — startup fails otherwise |
| `SMTP_SECURITY` | `starttls` | `starttls` (587), `ssl` (465), or `none` (a local server such as Mailpit) |

## Delivery

Checked in this order, before any network call (`shared/notification_shared/delivery.py`, shared
with telegram-service so the two cannot drift — ADR 0030):

1. A recipient containing `fail` (case insensitive) → `DeliveryFailed`, reason `simulated_failure`.
2. A reserved recipient (`example.com`/`.org`/`.net`, or any domain under `.example`/`.invalid`/
   `.test`) → the simulated sender, even when `SMTP_HOST` is set.
3. Otherwise → the configured sender: simulated when `SMTP_HOST` is unset or empty, SMTP when it is
   set.

SMTP outcomes (spec section 6.2), after the rules above:

| Condition | Outcome |
|---|---|
| Message accepted | `DeliveryCompleted` |
| Recipient refused, or any other `5xx` reply | permanent → `DeliveryFailed`, reason `smtp_rejected` |
| `4xx` reply, timeout, connection error | transient → `handle` raises; `PendingRecoverer` retries |
| Authentication failure (`535`) | transient, logged at ERROR: a configuration problem, not a recipient problem |

`GET /version` reports `delivery_mode`: `simulated` or `smtp`. It describes the *configured*
sender — a reserved recipient is still simulated even when `delivery_mode` reports `smtp`.

## HTTP endpoints

**None for domain operations** — the debug `POST /send` endpoint from the original design was
dropped (ADR 0021). Only:

| Endpoint | Notes |
|---|---|
| `GET /health` | Checks Postgres and Redis; `503` if either is down |
| `GET /version` | Reports `delivery_mode` |
| `GET /docs`, `/redoc` | |

## Workers

Three background tasks started in the FastAPI `lifespan`: the `notification.routed` consumer
(`email-service`), the outbox publisher, and the pending recoverer.
