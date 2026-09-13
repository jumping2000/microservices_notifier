# notification-service

Owns the notification aggregate: what was requested, and its current status. It is the only
client-facing entry point in slice 1 and the only service whose read model the client ever polls.

## Tables

| Table | Purpose |
|---|---|
| `notifications` | The aggregate: `channel`, `recipient`, `subject`, `body`, `status` (`CREATED`\|`PROCESSING`\|`COMPLETED`\|`FAILED`), `fail_reason` |
| `outbox` | Events awaiting publication (`OutboxMixin`) |
| `processed_events` | Idempotency ledger (`ProcessedEventMixin`) |

## Streams consumed

| Stream | Consumer group | Behaviour |
|---|---|---|
| `notification.routed` | `notification-service-routed` | `SET status='PROCESSING' WHERE status='CREATED'` |
| `delivery.completed` | `notification-service-results` | `SET status='COMPLETED' WHERE status IN ('CREATED','PROCESSING')` |
| `delivery.failed` | `notification-service-results` | `SET status='FAILED', fail_reason=... WHERE status IN ('CREATED','PROCESSING')` (handles both `DeliveryFailed` and `RoutingFailed`) |

## Streams published

| Stream | Event type |
|---|---|
| `notification.created` | `NotificationCreated` |

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | — (required) | `postgresql+asyncpg://...` |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `SERVICE_NAME` | `notification-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | Outbox publisher poll interval |
| `OUTBOX_BATCH_SIZE` | `100` | Outbox publisher batch size |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | Poll interval for both consumers (`routed`, `results`) |

## HTTP endpoints

| Endpoint | Notes |
|---|---|
| `POST /notifications` | `202 Accepted`, body `{notification_id, status}` |
| `GET /notifications/{id}` | Read model: `notification_id`, `channel`, `status`, `fail_reason`, `created_at`, `updated_at`; `404` if unknown |
| `GET /notifications` | `?limit=20&offset=0&status=&channel=` |
| `GET /health` | Checks Postgres and Redis; `503` if either is down |
| `GET /version`, `/docs`, `/redoc` | |

## Workers

Three background tasks started in the FastAPI `lifespan`: the outbox publisher, the routed
consumer (`notification-service-routed`), and the results consumer
(`notification-service-results`).
