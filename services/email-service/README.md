# email-service

Delivers email notifications. Delivery is simulated (no real SMTP). It exposes **no domain REST
endpoints**: its entire job runs in background consumers reacting to `notification.routed`.

## Tables

| Table | Purpose |
|---|---|
| `email_delivery` | One row per notification: `notification_id` (unique), `recipient`, `status` (`SENDING`\|`DELIVERED`\|`FAILED`), `fail_reason`, `sent_at` |
| `outbox` | Events awaiting publication (`OutboxMixin`) |
| `processed_events` | Idempotency ledger (`ProcessedEventMixin`) |

## Streams consumed

| Stream | Consumer group | Behaviour |
|---|---|---|
| `notification.routed` | `email-service` | Filters `payload.channel == "email"` (acks and skips otherwise); simulates delivery; writes `email_delivery` and publishes `DeliveryCompleted` or `DeliveryFailed` |

## Streams published

| Stream | Event type |
|---|---|
| `delivery.completed` | `DeliveryCompleted` |
| `delivery.failed` | `DeliveryFailed` |

Delivery fails, deterministically, when `recipient` contains the substring `fail` (case
insensitive) — see ADR 0008. There is no random failure rate.

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
| `DELIVERY_LATENCY_MS_MAX` | `500` | Upper bound of the simulated random delivery delay |

## HTTP endpoints

**None for domain operations** — the debug `POST /send` endpoint from the original design was
dropped (ADR 0021). Only:

| Endpoint | Notes |
|---|---|
| `GET /health` | Checks Postgres and Redis; `503` if either is down |
| `GET /version`, `/docs`, `/redoc` | |

## Workers

Two background tasks started in the FastAPI `lifespan`: the `notification.routed` consumer
(`email-service`) and the outbox publisher.
