# Prompt – Universal Notification Platform (Microservices Demo)

## Objective

Design and implement a complete but simple **Universal Notification Platform** using a
**Microservices Architecture** with an **event-driven choreography** backbone.

The goal is educational: demonstrate how a real enterprise microservice architecture works,
including asynchronous event flows, the Outbox Pattern, Redis Streams, and choreography-based
sagas, while keeping the codebase easy to understand.

The project must be fully runnable on a local machine using Docker Compose.

---

# Technology Stack

Use the following technologies.

- Python 3.14 - 3.13
- FastAPI (used for every service, **including the API Gateway**)
- SQLAlchemy 2.x (async engine with `asyncpg` driver)
- Alembic (one migration history per service, applied automatically on container startup via
  `alembic upgrade head` as the container entrypoint before Uvicorn starts)
- PostgreSQL 16 (one database per microservice)
- Redis 7 with AOF persistence enabled (used exclusively as the event streaming broker via
  Redis Streams — no Pub/Sub, no Redis as a cache)
- `redis-py` 5.x (async client, `redis.asyncio`)
- Docker
- Docker Compose v2
- Pydantic v2
- `pydantic-settings` (for environment-based configuration)
- HTTPX (for the remaining synchronous REST calls: Gateway → downstream services,
  Routing Service → Configuration Service)
- Uvicorn

Do NOT use Kubernetes.

Do NOT use Kafka or RabbitMQ. Redis Streams is the chosen broker — see "Event Architecture" below.

Do NOT use a dedicated API Gateway product (Kong, Traefik, Nginx, Envoy). The Gateway is a
FastAPI application, like every other service.

The **primary communication channel between domain services is Redis Streams** (events).
REST via HTTPX is used only for:
- Client → API Gateway → Notification Service (write path)
- Client → API Gateway → Notification Service (read/polling path)
- Client → API Gateway → Configuration Service (channel management)
- Routing Service → Configuration Service (channel state read, per-request, no caching)

No service may call another service's REST API for domain operations (routing, delivery).
Those interactions must flow through Redis Streams.

---

# Architecture

```
                             Client
                                │
                                │  REST
                       +--------+--------+
                       |    API Gateway  |
                       |    (FastAPI)    |
                       +--------+--------+
                                │  REST
               ┌────────────────┼────────────────┐
               │                │                │
               │ REST           │ REST           │ REST
               ▼                ▼                ▼
    Notification Service   (polling only)  Configuration Service
               │                                 ▲
               │ XADD                            │ REST (sync read)
               ▼                                 │
    ┌─── Redis Streams ───────────────┐          │
    │                                 │          │
    │  notification.created           │   Routing Service
    │  notification.routed  ◄─────────┼──── XADD ┘
    │  delivery.completed   ◄─────────┼──── XADD (Email / Telegram)
    │  delivery.failed      ◄─────────┼──── XADD (Email / Telegram)
    │                                 │
    └─────────────────────────────────┘
               ▲
               │ XREADGROUP
               ├── Routing Service      (consumes notification.created)
               ├── Email Service        (consumes notification.routed, filter channel=email)
               ├── Telegram Service     (consumes notification.routed, filter channel=telegram)
               └── Notification Service (consumes delivery.completed, delivery.failed)
```

Every service owns its own PostgreSQL database.
No service may directly access another service's database.

---

# Event Architecture

## Design Principles (from event-sourcing-architect skill)

- **Events are immutable facts**: once published to a stream they are never modified or deleted.
- **Events are small and focused**: one event type per domain fact, no fat payloads.
- **Version from day one**: every event carries `event_version: 1` in its envelope.
- **Idempotent consumers**: every consumer checks whether an `event_id` has already been
  processed before acting, using a `processed_events` table with a unique constraint on
  `(event_id, consumer_group)`. If already processed → `XACK` without re-processing.
- **Outbox Pattern**: every XADD is preceded by writing the event to a local `outbox` table
  inside the same Postgres transaction as the state change. A background publisher (asyncio
  task running in the FastAPI `lifespan`) reads pending outbox rows and publishes them to
  Redis Streams, then marks them as published. This guarantees atomicity between the DB write
  and the event publication even in the presence of crashes.
- **Correlation ID propagation**: every event envelope carries the `correlation_id` from the
  originating HTTP request, so the full saga can be traced across services via logs.

## Standard Event Envelope (Pydantic schema, in shared library)

```python
class EventEnvelope(BaseModel):
    event_id: UUID          # Unique ID of this event instance
    event_type: str         # e.g. "NotificationCreated"
    event_version: int = 1  # Schema version — always 1 for this demo
    occurred_at: datetime
    correlation_id: str
    aggregate_id: UUID      # Always the notification_id
    payload: dict           # Event-specific data
```

## Stream Topology

```
notification.created
  Publisher:  Notification Service (outbox publisher)
  Consumer:   Routing Service       (consumer group: routing-service)

notification.routed
  Publisher:  Routing Service (outbox publisher)
  Consumers:  Email Service    (consumer group: email-service)
              Telegram Service (consumer group: telegram-service)
  Note: both consumer groups read the same stream independently.
        Each service filters on payload.channel in the handler.

delivery.completed
  Publishers: Email Service, Telegram Service (outbox publisher)
  Consumer:   Notification Service (consumer group: notification-service)

delivery.failed
  Publishers: Email Service, Telegram Service (outbox publisher)
  Consumers:  Notification Service (consumer group: notification-service)
              Routing Service      (consumer group: routing-service-delivery-result)
```

## Consumer Group Initialisation

Each service must create its consumer group(s) at startup (inside the FastAPI `lifespan`,
before starting the consumer task) using `XGROUP CREATE <stream> <group> $ MKSTREAM`.
Use the `MKSTREAM` flag so the stream is created if it does not yet exist.
Use `$` as the start offset so that only new messages published after startup are consumed
(not historical ones from a previous run).

## Pending Message Recovery (XPENDING / XCLAIM)

Each consumer loop must, on every iteration:
1. First call `XPENDING <stream> <group> - + 30` to find messages delivered more than
   `PENDING_TIMEOUT_MS` ago (default: 30 000 ms, configurable via env var) that have not
   been acknowledged.
2. For each such message, call `XCLAIM` to take ownership and re-process it.
3. After successful processing, call `XACK`.

This implements at-least-once delivery recovery without a separate dead-letter mechanism.
For the demo, a message that fails processing 3 consecutive times (tracked via a counter in
the `processed_events` table) must be logged at ERROR level and `XACK`-ed to prevent
indefinite blocking.

## Step Timeout / Stale Processing Detection

The Notification Service must run a second background task (alongside the outbox publisher
and the delivery-result consumer) that:
- Queries `notifications` where `status = 'PROCESSING'` and
  `updated_at < now() - interval 'PROCESSING_TIMEOUT_MINUTES minutes'`
  (default: 5, configurable via env var).
- Marks them as `FAILED` with a reason of `"processing_timeout"`.
- Logs at WARNING level.

This prevents notifications from being stuck in `PROCESSING` forever if a consumer crashes
after reading from the stream but before publishing its outbox.

---

# Microservices

---

## 1. API Gateway

The Gateway is a standard FastAPI application acting as a thin reverse proxy.
For each incoming request it uses an async `httpx.AsyncClient` to forward the call to the
appropriate downstream service, then returns the downstream response (status code, body,
relevant headers) unchanged to the client.

Responsibilities

- Single entry point for the client
- Thin pass-through proxy: request in → forward → response out, no payload transformation
- Static routing table via env-configured base URLs
- Forward all request headers to downstream services
- Generate `X-Correlation-ID` if absent; always forward it downstream
- Enforce a configurable request timeout (default 10 s); return `504 Gateway Timeout`
  with the common error model on timeout
- No business logic: no content validation, no routing decisions, no knowledge of channels
  or stream state

Routes exposed to the client

```
POST   /api/v1/notifications          → Notification Service POST /notifications
GET    /api/v1/notifications/{id}     → Notification Service GET  /notifications/{id}
GET    /api/v1/notifications          → Notification Service GET  /notifications
GET    /api/v1/channels               → Configuration Service GET /channels
PUT    /api/v1/channels/{name}        → Configuration Service PUT /channels/{name}
GET    /api/v1/health                 → Gateway own health check (does not aggregate)
```

Note: the Gateway health check only reports the Gateway's own status, not that of downstream
services. Each service's own `/health` is accessible directly on its internal port for
Docker Compose healthchecks.

---

## 2. Notification Service

Responsibilities

- Receive notification requests from the Gateway (REST)
- Validate input (channel is a valid `Channel` enum value; recipient and body are non-empty)
- Persist the notification with status `CREATED`
- Write a `NotificationCreated` event to the local `outbox` table in the **same transaction**
- Return `202 Accepted` with `notification_id` and `status: CREATED` immediately
- (Background) Outbox publisher: publish pending outbox events to `notification.created` stream
- (Background) Delivery-result consumer: consume `delivery.completed` and `delivery.failed`
  streams, update the notification status to `COMPLETED` or `FAILED` accordingly
- (Background) Stale processing detector: time out notifications stuck in `PROCESSING`

Client interaction model: after `POST /notifications` returns `202`, the client polls
`GET /notifications/{id}` to observe the state advance from `CREATED → PROCESSING →
COMPLETED / FAILED`. The README must document this explicitly with a polling example.

Database tables

```
notifications
  id           UUID PRIMARY KEY
  channel      VARCHAR NOT NULL
  recipient    VARCHAR NOT NULL
  subject      VARCHAR
  body         TEXT NOT NULL
  status       VARCHAR NOT NULL  (CREATED / PROCESSING / COMPLETED / FAILED)
  fail_reason  VARCHAR           (populated on FAILED)
  created_at   TIMESTAMP NOT NULL
  updated_at   TIMESTAMP NOT NULL

outbox
  id           UUID PRIMARY KEY
  stream       VARCHAR NOT NULL   (e.g. "notification.created")
  payload      JSONB NOT NULL     (serialised EventEnvelope)
  published    BOOLEAN NOT NULL DEFAULT FALSE
  created_at   TIMESTAMP NOT NULL

processed_events
  id           UUID PRIMARY KEY
  event_id     UUID NOT NULL
  consumer_group VARCHAR NOT NULL
  fail_count   INTEGER NOT NULL DEFAULT 0
  processed_at TIMESTAMP NOT NULL
  UNIQUE (event_id, consumer_group)
```

Endpoints

```
POST   /notifications
GET    /notifications/{id}
GET    /notifications?limit=20&offset=0&status=&channel=
GET    /health      (checks Postgres connectivity with SELECT 1)
GET    /version
```

HTTP responses

```
POST /notifications  → 202 Accepted
  { "notification_id": "...", "status": "CREATED" }

GET /notifications/{id} → 200 OK
  { "notification_id": "...", "channel": "...", "status": "...", "created_at": "...", "updated_at": "..." }
```

Possible status values: `CREATED`, `PROCESSING`, `COMPLETED`, `FAILED`

---

## 3. Routing Service

Responsibilities

- (Background) Consumer of `notification.created` stream (consumer group: `routing-service`)
- For each consumed event:
  1. Mark the local `routes` record as `PROCESSING`
  2. Call Configuration Service via REST (synchronous, with HTTPX timeout) to check if the
     channel is enabled
  3. If disabled: publish `RoutingFailed` to `delivery.failed` stream (via outbox);
     mark `routes` record as `FAILED`
  4. If enabled: publish `NotificationRouted` to `notification.routed` stream (via outbox);
     mark `routes` record as `PROCESSING` (final status comes from delivery result)
- (Background) Consumer of `delivery.failed` stream (consumer group: `routing-service-delivery-result`)
  to update `routes` record to `FAILED` when a delivery service reports failure
- (Background) Consumer of `delivery.completed` stream (consumer group: `routing-service-delivery-result`)
  to update `routes` record to `COMPLETED`
- (Background) Outbox publisher: same pattern as Notification Service

The Routing Service no longer exposes a `POST /route` REST endpoint for domain operations.
The old REST endpoint is replaced by the stream consumer.

Database tables

```
routes
  id               UUID PRIMARY KEY
  notification_id  UUID NOT NULL
  channel          VARCHAR NOT NULL
  status           VARCHAR NOT NULL  (PROCESSING / COMPLETED / FAILED)
  fail_reason      VARCHAR
  created_at       TIMESTAMP NOT NULL
  updated_at       TIMESTAMP NOT NULL

outbox            (same schema as Notification Service outbox)
processed_events  (same schema as Notification Service processed_events)
```

Endpoints

```
GET /health    (checks Postgres and Redis connectivity)
GET /version
```

Note: the Routing Service has no public REST endpoints for domain operations. Its entire
domain logic runs as background consumers. The `/health` endpoint is the only HTTP surface.

---

## 4. Configuration Service

Responsibilities: unchanged from v2. Provides runtime channel configuration via REST.
Does **not** publish events — configuration is treated as read-model state, not a domain event stream.

Database

```
channels
  name     VARCHAR PRIMARY KEY
  enabled  BOOLEAN NOT NULL
```

Initial seed data (applied via Alembic data migration or a startup seed script):

```
email    = true
telegram = true
```

Endpoints

```
GET  /channels
GET  /channels/{name}
PUT  /channels/{name}     body: { "enabled": true|false }
GET  /health              (checks Postgres connectivity)
GET  /version
```

Changing `enabled` to `false` takes effect immediately on the next routing decision
(Routing Service reads Configuration Service per-event, no local cache).

---

## 5. Email Service

Responsibilities

- (Background) Consumer of `notification.routed` stream (consumer group: `email-service`)
- Filter: process only events where `payload.channel == "email"`; `XACK` and skip others
- For each matching event:
  1. Check idempotency (`processed_events`)
  2. Insert a row into `email_delivery` with status `SENDING`
  3. Simulate delivery (log "Sending email to {recipient}...", sleep 0–500 ms, log "Delivered")
  4. Update `email_delivery` status to `DELIVERED` (or `FAILED` on simulated error)
  5. Publish `DeliveryCompleted` or `DeliveryFailed` to the appropriate stream (via outbox)
  6. `XACK`
- (Background) Outbox publisher

The `POST /send` REST endpoint from v2 is **retained** as an optional manual trigger for
Swagger testing. It must be clearly marked in the OpenAPI description as
"debug endpoint — not used in normal event-driven flow". It does not bypass idempotency checks.

Database tables

```
email_delivery
  id               UUID PRIMARY KEY
  notification_id  UUID NOT NULL
  recipient        VARCHAR NOT NULL
  status           VARCHAR NOT NULL  (SENDING / DELIVERED / FAILED)
  fail_reason      VARCHAR
  sent_at          TIMESTAMP

outbox            (same schema)
processed_events  (same schema)
```

Endpoints

```
POST /send      (debug only — see note above)
GET  /health    (checks Postgres and Redis connectivity)
GET  /version
```

---

## 6. Telegram Service

Responsibilities: mirrors Email Service exactly, with `channel == "telegram"` and
consumer group `telegram-service`.

If `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` env vars are set, call the real Telegram
Bot API (`https://api.telegram.org/bot{token}/sendMessage`). Otherwise simulate delivery.

Database tables

```
telegram_delivery
  id               UUID PRIMARY KEY
  notification_id  UUID NOT NULL
  chat_id          VARCHAR NOT NULL
  status           VARCHAR NOT NULL  (SENDING / DELIVERED / FAILED)
  fail_reason      VARCHAR
  sent_at          TIMESTAMP

outbox            (same schema)
processed_events  (same schema)
```

Endpoints

```
POST /send      (debug only)
GET  /health    (checks Postgres and Redis connectivity)
GET  /version
```

---

# Request and Event Flow

## Write path (client → notification created)

```
Client
  POST /api/v1/notifications
  ↓
API Gateway (adds X-Correlation-ID if absent, forwards)
  ↓
Notification Service
  ├── validate input
  ├── INSERT notifications (status=CREATED)
  ├── INSERT outbox (stream=notification.created, payload=NotificationCreated{...})
  └── COMMIT  ← single transaction
  ↓
  202 Accepted { notification_id, status: CREATED }
  ↓
Client receives response immediately
```

## Async event chain (after 202 is returned)

```
[Notification Service — outbox publisher task]
  XADD notification.created ← reads outbox row, publishes, marks published=true

[Routing Service — consumer task]
  XREADGROUP notification.created routing-service
  ├── check idempotency
  ├── INSERT routes (status=PROCESSING)
  ├── GET /channels/{channel} → Configuration Service  (REST, sync)
  ├── if disabled:
  │     INSERT outbox (stream=delivery.failed, payload=RoutingFailed)
  │     UPDATE routes (status=FAILED)
  ├── if enabled:
  │     INSERT outbox (stream=notification.routed, payload=NotificationRouted{channel,...})
  └── XACK

[Routing Service — outbox publisher task]
  XADD notification.routed  OR  XADD delivery.failed

[Email Service or Telegram Service — consumer task]
  XREADGROUP notification.routed email-service / telegram-service
  ├── filter payload.channel
  ├── check idempotency
  ├── INSERT email_delivery / telegram_delivery (status=SENDING)
  ├── simulate or real delivery
  ├── UPDATE delivery record (status=DELIVERED or FAILED)
  ├── INSERT outbox (stream=delivery.completed or delivery.failed)
  └── XACK

[Email/Telegram Service — outbox publisher task]
  XADD delivery.completed  OR  XADD delivery.failed

[Notification Service — delivery-result consumer task]
  XREADGROUP delivery.completed / delivery.failed  notification-service
  ├── check idempotency
  ├── UPDATE notifications (status=COMPLETED or FAILED, fail_reason=...)
  └── XACK
```

## Read path (client polling)

```
Client
  GET /api/v1/notifications/{id}  (polls until status != CREATED and != PROCESSING)
  ↓
API Gateway → Notification Service GET /notifications/{id}
  ↓
  200 OK { notification_id, status, channel, created_at, updated_at }
```

---

# REST API Examples

## POST — create notification

```http
POST /api/v1/notifications
Content-Type: application/json
X-Correlation-ID: 550e8400-e29b-41d4-a716-446655440000

{
    "channel": "email",
    "recipient": "john@example.com",
    "subject": "Welcome",
    "body": "Hello John!"
}
```

Response `202 Accepted`

```json
{
    "notification_id": "a1b2c3d4-...",
    "status": "CREATED"
}
```

## GET — poll status

```http
GET /api/v1/notifications/a1b2c3d4-...
```

Response while processing

```json
{
    "notification_id": "a1b2c3d4-...",
    "channel": "email",
    "status": "PROCESSING",
    "created_at": "2025-01-01T10:00:00Z",
    "updated_at": "2025-01-01T10:00:01Z"
}
```

Response when done

```json
{
    "notification_id": "a1b2c3d4-...",
    "channel": "email",
    "status": "COMPLETED",
    "created_at": "2025-01-01T10:00:00Z",
    "updated_at": "2025-01-01T10:00:02Z"
}
```

---

# Folder Structure

```
notification-platform/
│
├── gateway/
│   ├── app/
│   │   ├── main.py
│   │   ├── core/
│   │   │   ├── config.py
│   │   │   └── proxy.py
│   │   └── api/
│   │       └── v1/
│   ├── Dockerfile
│   └── pyproject.toml
│
├── services/
│   ├── notification-service/
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── core/
│   │   │   ├── api/
│   │   │   ├── models/
│   │   │   ├── repositories/
│   │   │   ├── services/
│   │   │   └── workers/        ← outbox publisher, delivery-result consumer, stale detector
│   │   ├── alembic/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   ├── routing-service/
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── core/
│   │   │   ├── api/
│   │   │   ├── models/
│   │   │   ├── repositories/
│   │   │   ├── services/
│   │   │   └── workers/        ← notification consumer, delivery-result consumer, outbox publisher
│   │   ├── alembic/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   ├── configuration-service/
│   ├── email-service/
│   └── telegram-service/
│
├── shared/
│   └── notification_shared/
│       ├── __init__.py
│       ├── config.py           ← BaseSettings loader
│       ├── logging.py          ← structured log config
│       ├── middleware.py       ← CorrelationID middleware
│       ├── http_client.py      ← HTTPX wrapper with timeout + error translation
│       ├── events.py           ← EventEnvelope schema + Channel enum
│       ├── models.py           ← SQLAlchemy Base + TimestampMixin
│       ├── outbox.py           ← OutboxRepository
│       ├── streams.py          ← RedisStreamPublisher + RedisStreamConsumer
│       ├── idempotency.py      ← IdempotencyRepository (processed_events)
│       └── exceptions.py       ← common error codes + ErrorResponse model
│
├── docker/
│   └── redis/
│       └── redis.conf          ← AOF enabled, maxmemory-policy noeviction
│
├── docs/
│   ├── architecture.md
│   └── event-flows.md
│
├── docker-compose.yml
└── README.md
```

---

# Shared Library — `notification_shared`

Create a shared Python package installed as a local dependency in every service's
`pyproject.toml` via `pip install -e ../../shared`.

The package must provide the following modules:

## `events.py`

```python
from enum import Enum
from pydantic import BaseModel, UUID4
from datetime import datetime

class Channel(str, Enum):
    EMAIL = "email"
    TELEGRAM = "telegram"

class EventEnvelope(BaseModel):
    event_id: UUID4
    event_type: str
    event_version: int = 1
    occurred_at: datetime
    correlation_id: str
    aggregate_id: UUID4        # notification_id
    payload: dict
```

Event types to implement (string constants in the shared library):

```
NotificationCreated
NotificationRouted
RoutingFailed
DeliveryCompleted
DeliveryFailed
```

## `outbox.py`

`OutboxRepository` with:
- `save(session, stream: str, envelope: EventEnvelope) → None` — inserts outbox row inside
  the caller's transaction
- `get_pending(session, limit: int = 100) → list[OutboxRow]` — returns unpublished rows
- `mark_published(session, ids: list[UUID]) → None`

## `streams.py`

`RedisStreamPublisher` — async wrapper around `redis.asyncio`:
- `publish(stream: str, envelope: EventEnvelope) → None` — calls `XADD`

`RedisStreamConsumer` — async wrapper:
- `ensure_group(stream: str, group: str) → None` — `XGROUP CREATE MKSTREAM`
- `read(stream: str, group: str, consumer: str, count: int, block_ms: int) → list[Message]`
  — calls `XREADGROUP`
- `ack(stream: str, group: str, message_id: str) → None` — calls `XACK`
- `get_pending(stream: str, group: str, min_idle_ms: int) → list[PendingMessage]`
- `claim(stream: str, group: str, consumer: str, min_idle_ms: int,
  message_ids: list[str]) → list[Message]`

## `idempotency.py`

`IdempotencyRepository`:
- `is_processed(session, event_id: UUID, consumer_group: str) → bool`
- `mark_processed(session, event_id: UUID, consumer_group: str) → None`
- `increment_fail_count(session, event_id: UUID, consumer_group: str) → int`
  — returns new fail count

## `middleware.py`

`CorrelationIDMiddleware` (Starlette `BaseHTTPMiddleware`):
- Reads `X-Correlation-ID` from incoming request; generates a UUID4 if absent
- Stores it in `request.state.correlation_id`
- Adds it to the response headers

## `http_client.py`

`ServiceClient` — thin wrapper around `httpx.AsyncClient`:
- Constructor takes `base_url: str` and `timeout: float = 5.0`
- `get(path: str, **kwargs) → dict` — raises `ServiceUnavailableError` on timeout or 5xx
- `put(path: str, **kwargs) → dict`

## `exceptions.py`

Common error codes:

```python
class ErrorCode(str, Enum):
    VALIDATION_ERROR          = "VALIDATION_ERROR"
    NOT_FOUND                 = "NOT_FOUND"
    CHANNEL_DISABLED          = "CHANNEL_DISABLED"
    ROUTING_UNAVAILABLE       = "ROUTING_UNAVAILABLE"
    CONFIGURATION_UNAVAILABLE = "CONFIGURATION_UNAVAILABLE"
    DELIVERY_UNAVAILABLE      = "DELIVERY_UNAVAILABLE"
    STREAM_ERROR              = "STREAM_ERROR"
```

Common error response model (FastAPI exception handler returns this):

```json
{
    "error": {
        "code": "CHANNEL_DISABLED",
        "message": "Channel telegram is currently disabled"
    }
}
```

## `models.py`

`Base` (SQLAlchemy `DeclarativeBase`) with a `TimestampMixin` that adds `created_at` and
`updated_at` with server-side defaults and `onupdate`.

---

# Background Workers — Implementation Pattern

Each service that participates in the event flow must start its background tasks in the
FastAPI `lifespan` context manager, not as per-request `BackgroundTasks`:

```python
from contextlib import asynccontextmanager
import asyncio

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await db_engine.connect()
    await redis_client.ping()
    await consumer.ensure_group(stream="notification.created", group="routing-service")

    tasks = [
        asyncio.create_task(outbox_publisher_loop()),
        asyncio.create_task(notification_consumer_loop()),
    ]

    yield

    # Shutdown
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await db_engine.dispose()
    await redis_client.aclose()

app = FastAPI(lifespan=lifespan)
```

Each worker loop must:
- Handle exceptions internally (log at ERROR level, do not crash the loop)
- Implement a configurable `CONSUMER_POLL_INTERVAL_MS` sleep between iterations
  (default 500 ms) to avoid busy-waiting
- Log each processed event at DEBUG level with `event_id`, `event_type`, `correlation_id`

---

# Docker Compose

Services and containers:

```
gateway
notification-service     notification-db
routing-service          routing-db
configuration-service    configuration-db
email-service            email-db
telegram-service         telegram-db
redis                    (shared by all event-driven services)
```

Redis configuration (`docker/redis/redis.conf`):
```
appendonly yes
appendfsync everysec
maxmemory-policy noeviction
```

Each application service must have:

- **Dockerfile**: multi-stage build (build stage installs deps → runtime stage copies only
  the venv and app code); non-root user (`RUN adduser --disabled-password appuser`);
  base image `python:3.13-slim`; `.dockerignore` excludes `__pycache__`, `.venv`, `.git`,
  `*.pyc`, `alembic/versions/*.py` (migrations are in the repo, not excluded, but cache is)
- **Healthcheck**: calls `GET /health` with `curl -f` or `wget`; interval 10s, timeout 5s,
  retries 5, start_period 30s
- **Restart policy**: `restart: on-failure`
- **Named network**: `notification-net`
- **Named volume**: one per Postgres container
- **`depends_on`**: with `condition: service_healthy` for its own DB and for `redis`
  (event-driven services only)

Environment variables (documented per service, all overridable):

```yaml
# Example for notification-service
DATABASE_URL: postgresql+asyncpg://user:pass@notification-db:5432/notificationdb
REDIS_URL: redis://redis:6379/0
SERVICE_NAME: notification-service
SERVICE_VERSION: 1.0.0
LOG_LEVEL: INFO
PROCESSING_TIMEOUT_MINUTES: 5
CONSUMER_POLL_INTERVAL_MS: 500
PENDING_TIMEOUT_MS: 30000
PENDING_MAX_RETRIES: 3
```

---

# Internal Service Structure (per service, from fastapi-templates skill)

Each service must follow this layout:

```
app/
├── main.py             ← FastAPI app + lifespan
├── core/
│   ├── config.py       ← pydantic-settings Settings class
│   └── database.py     ← async engine + get_db dependency
├── api/
│   └── v1/
│       ├── router.py
│       └── endpoints/
├── models/             ← SQLAlchemy ORM models
├── schemas/            ← Pydantic request/response schemas
├── repositories/       ← data access only, no business logic
├── services/           ← business logic, calls repositories
└── workers/            ← background asyncio tasks (consumers, publisher)
```

Rules (from `fastapi-templates` playbook):
- Route handlers call `service` layer only — no direct DB or Redis access in endpoints
- `service` layer calls `repository` layer — no SQLAlchemy queries in services
- `repository` layer is the only place with SQLAlchemy `select/insert/update`
- Dependencies injected via `Depends(get_db)`, never instantiated inline in handlers

---

# OpenAPI Documentation

Every service exposes Swagger UI at `/docs` and ReDoc at `/redoc`.

The API Gateway does **not** merge or aggregate downstream Swagger specs.

Every endpoint must have:
- `summary` and `description` in the route decorator
- `response_model` declared on all `GET` endpoints
- `status_code` declared explicitly (e.g. `status_code=202` on `POST /notifications`)
- Debug-only endpoints (e.g. `POST /send`) must include in their description:
  `"⚠️ Debug endpoint — not part of the normal event-driven flow."`

---

# Logging

Every log line must be structured JSON (not plain text) and include:

```json
{
    "timestamp": "2025-01-01T10:00:00.123Z",
    "level": "INFO",
    "service": "notification-service",
    "correlation_id": "550e8400-...",
    "event_id": "a1b2c3d4-...",
    "event_type": "NotificationCreated",
    "notification_id": "...",
    "message": "Event published to stream notification.created"
}
```

Use Python's `logging` module with a custom `JSONFormatter`. The shared library's
`logging.py` provides `configure_logging(service_name: str, log_level: str)` which
sets up the root logger with the JSON formatter and outputs to stdout.

---

# Error Handling

## HTTP errors (REST endpoints)

Use the common error model. Register a FastAPI exception handler for `ServiceError`:

```json
{
    "error": {
        "code": "CHANNEL_DISABLED",
        "message": "Channel telegram is currently disabled"
    }
}
```

## Event processing errors (consumer loops)

- Transient errors (DB unavailable, Redis timeout): log at WARNING, do not `XACK`,
  let the message become pending for XPENDING/XCLAIM recovery
- Permanent errors (invalid payload, schema mismatch): log at ERROR, `XACK` to prevent
  blocking, record in `processed_events` with `fail_count = PENDING_MAX_RETRIES`
- After `PENDING_MAX_RETRIES` consecutive failures for the same `event_id`:
  log at CRITICAL, `XACK`, mark in `processed_events` as permanently failed

---

# Health Endpoints

```
GET /health
```

Response for a healthy service:

```json
{
    "status": "UP",
    "service": "notification-service",
    "checks": {
        "database": "UP",
        "redis": "UP"
    }
}
```

Response when a dependency is down (HTTP 503):

```json
{
    "status": "DOWN",
    "service": "notification-service",
    "checks": {
        "database": "UP",
        "redis": "DOWN"
    }
}
```

Configuration Service has no Redis check. API Gateway returns only its own process status
(no downstream checks).

---

# Version Endpoint

```
GET /version
```

```json
{
    "service": "notification-service",
    "version": "1.0.0"
}
```

---

# Design Principles

- Single Responsibility Principle
- Database per Service — no shared databases, no cross-service DB queries
- Stateless Services — all state in Postgres or Redis, no in-process state
- Event-driven for domain operations — REST only for client-facing read/write and
  Configuration Service (stable read-model)
- Outbox Pattern — atomic event publication with the DB transaction
- Idempotent consumers — at-least-once delivery is safe because every consumer checks
  `processed_events` before acting
- Independent deployment
- Configuration externalized through environment variables (Twelve-Factor App)
- OpenAPI-first — Pydantic schemas drive the OpenAPI spec
- Clean Architecture — strict layer separation (api → service → repository)
- Dependency Injection via FastAPI `Depends`
- SOLID principles throughout

---

# Nice-to-have (optional)

If time permits:

- Simple API Key authentication on the Gateway (`X-API-Key` header, validated against
  an env var `GATEWAY_API_KEY`)
- Prometheus `/metrics` endpoint per service (expose Redis stream lag as a gauge:
  `stream_pending_messages{stream, group}`)
- A minimal integration test (`pytest` + `httpx` + `docker-compose up`) that:
  1. POST /api/v1/notifications
  2. Polls GET /api/v1/notifications/{id} until status is COMPLETED or timeout
  3. Asserts status == COMPLETED

---

# Expected Result

The final project must demonstrate:

- API Gateway (FastAPI reverse proxy)
- Microservices with independent databases
- **Event-driven choreography via Redis Streams**
- **Outbox Pattern** (atomic event publication)
- **Idempotent consumers** with `processed_events`
- **Choreography-based saga** (no central orchestrator)
- **Stale processing detection** (background timeout watchdog)
- Shared library (`notification_shared`) used by all services
- Clean Architecture (api → service → repository → worker)
- Dockerized deployment with health checks and proper startup ordering
- Complete OpenAPI documentation
- Structured JSON logging with correlation ID propagation across the event chain

The code must prioritize readability, modularity, and educational value over advanced features.

---

# README Requirements

The README must include:

1. **Architecture Overview** — diagram and prose explanation of the event flow
2. **Quick Start** — `docker compose up` and a curl example with polling instructions
3. **Stream Topology** — table of streams, publishers, and consumer groups
4. **Polling Guide** — explicit example showing how a client observes state transitions
5. **Configuration** — table of all env vars per service with defaults
6. **Known Limitations** — section explicitly listing deliberate simplifications:
   - No schema registry (events versioned by convention, not enforced)
   - No dead-letter queue (max-retry via `processed_events.fail_count`, then discard)
   - No distributed tracing backend (correlation ID in logs only, no OpenTelemetry spans)
   - No circuit breaker on Configuration Service REST call from Routing Service
   - No horizontal scaling of consumers (single instance per service, single consumer per group)
   - At-least-once delivery (not exactly-once) — idempotency mitigates duplicate effects
   - Simulated delivery for Email (and optionally Telegram) — no real SMTP
