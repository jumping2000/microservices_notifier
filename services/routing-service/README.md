# routing-service

Decides which channel a notification is routed to. It exposes **no domain REST endpoints**: its
entire job runs in background consumers reacting to `notification.created`, and it makes one
synchronous REST call outward, to Configuration Service, per notification it routes.

## Tables

| Table | Purpose |
|---|---|
| `routes` | One row per notification: `notification_id` (unique), `channel`, `status` (`PROCESSING`\|`COMPLETED`\|`FAILED`), `fail_reason` |
| `outbox` | Events awaiting publication (`OutboxMixin`) |
| `processed_events` | Idempotency ledger (`ProcessedEventMixin`) |

## Streams consumed

| Stream | Consumer group | Behaviour |
|---|---|---|
| `notification.created` | `routing-service` | Calls Configuration Service, writes `routes`, publishes `NotificationRouted` or `RoutingFailed` |
| `delivery.completed` | `routing-service-results` | `SET routes.status='COMPLETED'` |
| `delivery.failed` | `routing-service-results` | `SET routes.status='FAILED', fail_reason=...` for `DeliveryFailed`; skips its own `RoutingFailed` (see ADR 0003) |

## Streams published

| Stream | Event type |
|---|---|
| `notification.routed` | `NotificationRouted` |
| `delivery.failed` | `RoutingFailed` |

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | — (required) | `postgresql+asyncpg://...` |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `CONFIGURATION_SERVICE_URL` | `http://configuration-service:8000` | |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout for the Configuration Service call |
| `SERVICE_NAME` | `routing-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | |
| `OUTBOX_BATCH_SIZE` | `100` | |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | |

## HTTP endpoints

**None for domain operations.** Only:

| Endpoint | Notes |
|---|---|
| `GET /health` | Checks Postgres and Redis; `503` if either is down |
| `GET /version`, `/docs`, `/redoc` | |

## Workers

Three background tasks started in the FastAPI `lifespan`: the `notification.created` consumer
(`routing-service`), the results consumer (`routing-service-results`), and the outbox publisher.

A Configuration Service timeout or `5xx` is treated as **transient**: the message is left pending
(no `XACK`, no outbox row) for slice 2's `XCLAIM` recovery rather than being recorded as a routing
failure. See ADR 0018.
