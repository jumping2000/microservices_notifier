# Universal Notification Platform — Design Specification

**Date:** 2026-09-12
**Status:** Approved for planning
**Supersedes:** `prompt_microservice_notifier_v3.md` (v3 remains the source of the original
requirements; this document records the corrections and decisions that make it implementable)

---

## 1. Goal

Build a runnable, educational microservices platform that demonstrates event-driven
choreography: the Outbox Pattern, idempotent consumers, at-least-once delivery with recovery,
and a choreography-based saga with no central orchestrator. Redis Streams is the only broker.
The whole platform runs locally via Docker Compose.

Readability and educational value take priority over feature breadth. Every pattern in the
system must be traceable to a documented reason and a test that proves it works.

---

## 2. Scope and decomposition

The platform is six services and is too large for a single implementation plan. It is built in
three slices. Each slice ends with working code, passing tests at both tiers, and the
documentation for what that slice built.

**The first implementation plan covers slice 1 only.** Slices 2 and 3 are described here so the
boundaries are deliberate, and each gets its own plan later.

### Slice 1 — the saga, both outcomes

Shared library, Notification Service, Configuration Service, Routing Service, Email Service,
Redis, four Postgres databases, Docker Compose, uv workspace, `.vscode/` configuration.

Behaviour delivered:

- Happy path: `CREATED → PROCESSING → COMPLETED`
- Channel disabled: `CREATED → FAILED` with `fail_reason = channel_disabled`
- Delivery failure: `CREATED → PROCESSING → FAILED` with the reason from the delivery service
- Outbox Pattern and idempotent consumers, fully implemented
- Guarded monotonic status transitions

Deliberately excluded from slice 1: `XPENDING`/`XCLAIM` recovery, max-retry handling, the
stale-processing watchdog, Telegram Service, API Gateway. Until slice 2 lands, a notification
whose consumer crashes mid-flight stays in its current state indefinitely. This is a known
and accepted gap for slice 1.

### Slice 2 — resilience and breadth

Telegram Service with its own database (real Bot API call when `TELEGRAM_BOT_TOKEN` and
`TELEGRAM_CHAT_ID` are set, simulated otherwise), API Gateway, `XPENDING IDLE`/`XCLAIM`
recovery, max-retry with `FAILED_PERMANENT`, stale-processing watchdog.

### Slice 3 — polish

`docs/operations.md`, final documentation review, and optional extras: API key on the Gateway,
Prometheus `/metrics` per service exposing `stream_pending_messages{stream, group}`.

---

## 3. Corrections to v3

Each correction below becomes an ADR under `docs/adr/`.

### 3.1 Notification status ownership

**Problem.** v3 promises the client observes `CREATED → PROCESSING → COMPLETED/FAILED`, and its
stale-processing watchdog queries `status = 'PROCESSING'`. No service ever wrote `PROCESSING` to
the `notifications` table — Routing Service marked only its own `routes` row. The watchdog could
never fire and the documented polling sequence was unreachable.

**Decision.** Notification Service gains a consumer group `notification-service-routed` on the
`notification.routed` stream. Its handler sets `notifications.status = 'PROCESSING'`.

### 3.2 Consumer group start offset

**Problem.** `XGROUP CREATE <stream> <group> $ MKSTREAM` consumes only messages published after
group creation. Under `docker compose up` all services start concurrently, so an event published
before its consumer created its group was lost permanently. The Outbox Pattern guarantees an
event is *published*, not that anyone *hears* it — so this silently defeated the guarantee the
platform exists to demonstrate.

**Decision.** Use `0` as the start offset. Idempotent consumers make replay safe.

**Consequence.** If Postgres volumes are wiped while Redis AOF data survives, consumers replay
the full stream history against empty `processed_events` tables. `README.md` and
`docs/operations.md` must state that volumes are reset together, via `docker compose down -v`.

### 3.3 Routing Service must not consume its own failure events

**Problem.** Routing Service publishes `RoutingFailed` to `delivery.failed` and also subscribes
to `delivery.failed`, so it consumed its own event — a loop in the topology.

**Decision.** The `routing-service-results` consumer handles only `DeliveryCompleted` and
`DeliveryFailed`. It `XACK`s and skips `RoutingFailed`, mirroring the channel-filter pattern
already used by the delivery services. `RoutingFailed` stays on `delivery.failed` so Notification
Service still receives it.

### 3.4 `processed_events` must distinguish processed from failing

**Problem.** v3 used one table with `UNIQUE (event_id, consumer_group)` and a `fail_count`.
A single failed attempt inserts a row, after which `is_processed()` reports a never-processed
event as done — the event is dropped.

**Decision.** Add a `status` column with values `PROCESSED`, `FAILING`, `FAILED_PERMANENT`.
`is_processed()` returns true only for `PROCESSED` and `FAILED_PERMANENT`.

### 3.5 `XPENDING` needs an idle filter

**Problem.** v3 specified `XPENDING <stream> <group> - + 30` to "find messages delivered more
than `PENDING_TIMEOUT_MS` ago". That command filters nothing by time; `30` is a result count.

**Decision.** Use `XPENDING <stream> <group> IDLE <PENDING_TIMEOUT_MS> - + <count>`. (Slice 2.)

### 3.6 Unique constraints as idempotency backstops

**Decision.** `UNIQUE (notification_id)` on `routes`, `email_delivery`, and `telegram_delivery`.
One notification produces exactly one route and one delivery attempt per channel. The constraint
turns a silent double-processing bug into a loud one.

### 3.7 `fail_reason` in the read model

**Problem.** v3 stores `fail_reason` but omits it from the `GET /notifications/{id}` response, so
a client polling to `FAILED` learns nothing about why.

**Decision.** Include `fail_reason` in the response model; `null` unless status is `FAILED`.

### 3.8 Deterministic delivery failure simulation

**Problem.** v3 says a delivery may end `FAILED` "on simulated error" without specifying when.
Non-deterministic failure cannot be tested or demonstrated reliably.

**Decision.** A delivery fails when the recipient (Email) or chat_id (Telegram) contains the
literal substring `fail`. No random failure rate.

### 3.9 Configuration seed via Alembic data migration

**Problem.** v3 offered either an Alembic data migration or a startup seed script.

**Decision.** Alembic data migration. It is deterministic, runs before Uvicorn starts, has no
startup race, and is covered by the `alembic upgrade head` integration test.

### 3.10 uv workspace replaces `pip install -e`

**Problem.** v3 specified `pip install -e ../../shared`. Project rules (`CLAUDE.md`) mandate `uv`
exclusively and forbid `pip`.

**Decision.** A uv workspace with a single root `uv.lock`. See section 8.

### 3.11 Python 3.14 everywhere

**Problem.** v3's stack section said "Python 3.14 - 3.13" while its Dockerfile specified
`python:3.13-slim`.

**Decision.** Python 3.14 uniformly: `requires-python = ">=3.14"`, `.python-version` containing
`3.14`, and `python:3.14-slim` base images.

**Risk.** `asyncpg` is Cython-based and historically lags new CPython releases. Before writing
service code, verify a 3.14 wheel exists for the pinned `asyncpg` version. If it does not, the
fallback is `psycopg[binary]` in async mode, which changes only the driver portion of
`DATABASE_URL` (`postgresql+psycopg://`). This is the first verification step of slice 1.

### 3.12 Per-service declarative Base, shared mixins

**Problem.** A single `Base` exported from the shared library works per-service at runtime and
for Alembic autogenerate, because each service process imports only its own models. It breaks in
the **test suite**: with one root `.venv`, a test importing models from two services registers
both on the same registry, and `Base.metadata` then contains every service's tables. Schema
creation in an integration test is no longer scoped to one service.

**Decision.** The shared library exports abstract mixins only — `TimestampMixin`, `OutboxMixin`,
`ProcessedEventMixin`, none carrying `__tablename__`. Each service declares its own
`DeclarativeBase` and materializes its tables from the mixins. `OutboxRepository` and
`IdempotencyRepository` receive the model class via their constructor.

### 3.13 Git repository

The project directory was not a git repository. Initialized on 2026-09-12.

### 3.14 `.vscode/` workspace configuration

**Decision.** Commit `extensions.json`, `settings.json`, `launch.json`, and `tasks.json`.
See section 8.4.

### 3.15 Extended documentation, written per slice

**Decision.** The documentation set in section 11 is authored incrementally: each slice ends with
the documentation for what that slice built. Documentation written only at the end of a
six-service build describes the system as imagined rather than as built.

### 3.16 Guarded monotonic status transitions

**Problem.** Correction 3.1 introduces a race. `notification-service-routed` and
`notification-service-results` are independent consumer groups with no ordering guarantee between
them. With fast delivery, `delivery.completed` can be consumed before `notification.routed`,
leaving the notification at `COMPLETED` and then overwriting it back to `PROCESSING` — a visibly
wrong state for a polling client.

**Decision.** Status transitions are monotone and guarded in SQL, not in Python:

| Handler | Conditional update |
|---|---|
| routed consumer | `SET status='PROCESSING' WHERE id=:id AND status='CREATED'` |
| results consumer | `SET status=:status WHERE id=:id AND status IN ('CREATED','PROCESSING')` |
| stale watchdog (slice 2) | `SET status='FAILED' WHERE status='PROCESSING' AND updated_at < :cutoff` |

Legal transitions: `CREATED → PROCESSING`, `CREATED → COMPLETED`, `CREATED → FAILED`,
`PROCESSING → COMPLETED`, `PROCESSING → FAILED`. Terminal states never reopen. An out-of-order
handler updates zero rows and still `XACK`s.

`CREATED → COMPLETED` is in that list for a reason worth stating, because it looks like a gap in
the state machine and is not. In the very race this correction exists to handle, the delivery
result is consumed *before* `notification.routed`, so the row is still `CREATED` when the
completion arrives. If the results handler's guard admitted only `PROCESSING`, that update would
match zero rows — and because the handler still acks and still marks the event processed, the
event would never be redelivered and the notification would sit at `CREATED` forever. The
guard therefore admits both non-terminal states for both terminal destinations.

### 3.17 Event payload schemas

**Problem.** v3 names five event types but never defines their payloads. Every consumer's
behaviour depends on them.

**Decision.** Defined in section 5.3.

### 3.18 Configuration Service unavailability is transient

**Problem.** v3 does not say what Routing Service does when its HTTPX call to Configuration
Service times out or returns 5xx.

**Decision.** Distinguish by response:

| Configuration Service response | Routing Service behaviour |
|---|---|
| `200` with `enabled: true` | publish `NotificationRouted` |
| `200` with `enabled: false` | publish `RoutingFailed`, reason `channel_disabled` |
| `404` (channel not in the table) | publish `RoutingFailed`, reason `unknown_channel` |
| timeout or `5xx` | transient: log WARNING, do **not** `XACK`, write no outbox row, leave the message pending for `XCLAIM` recovery (slice 2) |

A configuration outage must never be recorded as a permanent routing failure. Note that in slice
1, with no recovery worker, a transient failure here leaves the notification at `CREATED` until
slice 2 lands — consistent with the slice 1 gap stated in section 2.

### 3.19 Redis message format

**Decision.** Each `XADD` writes a single field: `envelope`, whose value is the JSON-serialized
`EventEnvelope`. Consumers parse that one field. No multi-field flattening.

### 3.20 Payload forwarding by Routing Service

**Problem.** Email and Telegram services need `recipient`, `subject`, and `body` to deliver, but
they cannot read Notification Service's database and cannot call its REST API for domain
operations. Routing Service does not persist the body.

**Decision.** `NotificationRouted` carries the delivery fields forward from
`NotificationCreated`. This is a deliberate exception to "events are small and focused": the
alternative is either a forbidden cross-service read or a shared database. Recorded as an ADR so
the trade-off is visible.

### 3.21 The debug `POST /send` endpoint is dropped

**Problem.** v3 retains `POST /send` on Email and Telegram Services as a manual Swagger trigger,
marked "debug endpoint — not part of the normal event-driven flow", and requires it to respect
idempotency checks.

**Decision.** Drop it. It is a second entry path into delivery that no test and no documented
flow uses, and honouring idempotency on a request that carries no `event_id` is ill-defined.
Manual triggering is better served by the end-to-end tests and by the `redis-cli` and `curl`
recipes in `docs/operations.md`. Delivery services therefore expose only `/health`, `/version`,
`/docs`, and `/redoc`.

---

## 4. Architecture

```
                              Client
                                 │ REST
                         ┌───────┴───────┐
                         │  API Gateway  │  (slice 2)
                         └───────┬───────┘
              ┌──────────────────┼──────────────────┐
              ▼ REST             ▼ REST             ▼ REST
   Notification Service                    Configuration Service
              │                                      ▲
              │                                      │ REST (sync, per event)
              │                              Routing Service
              │                                      │
        ┌─────┴──────────────── Redis Streams ────────┴─────┐
        │  notification.created                             │
        │  notification.routed                              │
        │  delivery.completed                               │
        │  delivery.failed                                  │
        └───────────────────────────────────────────────────┘
                     ▲                        ▲
              Email Service            Telegram Service (slice 2)
```

Every service owns its own Postgres database. No service reads another service's database. REST
is used only for the client-facing read/write path and for Routing Service → Configuration
Service. All domain interactions between services flow through Redis Streams.

---

## 5. Event architecture

### 5.1 Envelope

```python
class EventEnvelope(BaseModel):
    event_id: UUID4
    event_type: str
    event_version: int = 1
    occurred_at: datetime
    correlation_id: str
    aggregate_id: UUID4   # always the notification_id
    payload: dict
```

Serialized to a single Redis field named `envelope` (correction 3.19).

### 5.2 Stream topology

| Stream | Publishers | Consumer group | Behaviour |
|---|---|---|---|
| `notification.created` | notification-svc | `routing-service` | routes the notification |
| `notification.routed` | routing-svc | `email-service` | filters `payload.channel == "email"` |
| | | `telegram-service` | slice 2; filters `"telegram"` |
| | | `notification-service-routed` | sets `PROCESSING` |
| `delivery.completed` | email-svc, telegram-svc | `notification-service-results` | sets `COMPLETED` |
| | | `routing-service-results` | sets `routes.COMPLETED` |
| `delivery.failed` | routing-svc, email-svc, telegram-svc | `notification-service-results` | sets `FAILED` + reason |
| | | `routing-service-results` | skips `RoutingFailed` (3.3) |

All groups are created with `XGROUP CREATE <stream> <group> 0 MKSTREAM` inside the FastAPI
`lifespan`, before any consumer task starts.

### 5.3 Event types and payloads

`NotificationCreated` → `notification.created`

```json
{ "channel": "email", "recipient": "a@b.com", "subject": "Welcome", "body": "Hello" }
```

`NotificationRouted` → `notification.routed`

```json
{ "channel": "email", "recipient": "a@b.com", "subject": "Welcome", "body": "Hello",
  "route_id": "uuid" }
```

`RoutingFailed` → `delivery.failed`

```json
{ "channel": "email", "route_id": "uuid", "reason": "channel_disabled" }
```

`reason` is one of `channel_disabled`, `unknown_channel`.

`DeliveryCompleted` → `delivery.completed`

```json
{ "channel": "email", "delivery_id": "uuid", "recipient": "a@b.com",
  "delivered_at": "2026-09-12T10:00:02Z" }
```

`DeliveryFailed` → `delivery.failed`

```json
{ "channel": "email", "delivery_id": "uuid", "reason": "simulated_failure" }
```

`subject` is nullable throughout. `aggregate_id` on every envelope is the `notification_id`.

### 5.4 Transactional rule for consumers

The domain state change, any outbox row, and the `mark_processed` write are always atomic: one
database transaction, committed together, with `XACK` happening only after that commit. This is
the point at which the Outbox Pattern is demonstrated, and the integration tests in section 10
assert it directly.

The idempotency check (`is_processed`) is part of that same transaction only when the handler has
nothing slower than a local write to do between the check and the decision. When a handler must
perform slow I/O — a synchronous REST call, a delivery latency — before it can decide what the
state change even is, that I/O must not run inside an open transaction, so `is_processed` runs in
its own earlier, short transaction instead. Routing Service's `notification_consumer.py` (the
Configuration Service call) and Email Service's `routed_consumer.py` (the simulated delivery
latency) both do this; Notification Service's two consumers do not need to, and keep all four
together. In every case the real backstop against a duplicate state change is not the advisory
`is_processed` check but the `UNIQUE (notification_id)` constraint from correction 3.6 — see ADR
0023.

---

## 6. Saga flows

### 6.1 Happy path

1. `POST /notifications` → Notification Service: `INSERT notifications (CREATED)` +
   `INSERT outbox (notification.created)` in one transaction → `202 Accepted`.
2. Notification Service outbox publisher: `XADD notification.created`, mark row published.
3. Routing Service `routing-service`: idempotency check → `INSERT routes (PROCESSING)` →
   `GET /channels/{channel}` → enabled → `INSERT outbox (notification.routed)` →
   `mark_processed` → commit → `XACK`.
4. Routing Service outbox publisher: `XADD notification.routed`.
5. Concurrently, no ordering guaranteed:
   - `notification-service-routed`: `SET status='PROCESSING' WHERE status='CREATED'` → `XACK`.
   - `email-service`: filter `channel == "email"` → idempotency check → simulate delivery →
     `INSERT email_delivery` in its terminal state (`DELIVERED`) →
     `INSERT outbox (delivery.completed)` → `mark_processed` → commit → `XACK`. The row is
     inserted once, already terminal — see ADR 0022 for why.
6. Email Service outbox publisher: `XADD delivery.completed`.
7. `notification-service-results`: `SET status='COMPLETED' WHERE status IN ('CREATED','PROCESSING')`
   → `XACK`. `routing-service-results`: `SET routes.status='COMPLETED'` → `XACK`.

### 6.2 Channel disabled

Steps 1–2 as above. At step 3 Configuration Service returns `enabled: false`, so Routing Service
writes, in one transaction, `INSERT routes (FAILED, fail_reason='channel_disabled')` +
`INSERT outbox (delivery.failed, RoutingFailed)`, then `XACK`s. `notification.routed` is never
published, so the notification goes `CREATED → FAILED` directly, with
`fail_reason = channel_disabled`.

### 6.3 Delivery failure

Steps 1–5 as in the happy path, but the recipient contains `fail`, so Email Service writes
`email_delivery (FAILED)` + `outbox (delivery.failed, DeliveryFailed)`. Notification Service
moves `PROCESSING → FAILED` with the payload reason; Routing Service moves `routes → FAILED`.

### 6.4 Client read path

`GET /notifications/{id}` returns `notification_id`, `channel`, `status`, `fail_reason`,
`created_at`, `updated_at`. The client polls until `status` is `COMPLETED` or `FAILED`.

### 6.5 REST endpoint inventory

| Service | Endpoint | Notes |
|---|---|---|
| notification-service | `POST /notifications` | `202 Accepted`, body `{notification_id, status}` |
| | `GET /notifications/{id}` | read model of 6.4; `404` if unknown |
| | `GET /notifications` | `?limit=20&offset=0&status=&channel=` |
| configuration-service | `GET /channels` | |
| | `GET /channels/{name}` | `404` if absent — see 3.18 |
| | `PUT /channels/{name}` | body `{enabled: bool}`; effective on the next routing decision |
| routing-service | none | domain logic is entirely in consumers |
| email-service | none | see 3.21 |
| all services | `GET /health`, `GET /version` | plus `/docs` and `/redoc` |
| gateway (slice 2) | `/api/v1/*` | thin proxy over the routes above |

Validation on `POST /notifications`: `channel` must be a valid `Channel` enum value; `recipient`
and `body` must be non-empty. Failures return the common error model with
`code = VALIDATION_ERROR`. Routing Service reads channel state per event with no local cache, so
a `PUT /channels/{name}` takes effect on the next routing decision.

---

## 7. Database schemas

Domain tables use UUID primary keys and the shared `TimestampMixin` (`created_at`, `updated_at`
with server defaults and `onupdate`). Two deliberate exceptions: `channels` is keyed by its name
and carries no timestamps, and `processed_events` carries only `processed_at`. One Alembic
history per service, applied by the container entrypoint before Uvicorn starts.

### Shared table shapes, materialized per service

```
outbox
  id          UUID PK
  stream      VARCHAR   NOT NULL
  payload     JSONB     NOT NULL    -- serialized EventEnvelope
  published   BOOLEAN   NOT NULL DEFAULT FALSE
  created_at  TIMESTAMP NOT NULL
  INDEX (published, created_at)

processed_events
  id             UUID PK
  event_id       UUID      NOT NULL
  consumer_group VARCHAR   NOT NULL
  status         VARCHAR   NOT NULL   -- PROCESSED | FAILING | FAILED_PERMANENT
  fail_count     INTEGER   NOT NULL DEFAULT 0
  processed_at   TIMESTAMP NOT NULL
  UNIQUE (event_id, consumer_group)
```

### notification-service

```
notifications
  id, channel, recipient, subject (nullable), body,
  status       VARCHAR NOT NULL   -- CREATED | PROCESSING | COMPLETED | FAILED
  fail_reason  VARCHAR (nullable)
  created_at, updated_at
  INDEX (status, created_at)
+ outbox, processed_events
```

### routing-service

```
routes
  id, notification_id UNIQUE NOT NULL, channel,
  status      VARCHAR NOT NULL   -- PROCESSING | COMPLETED | FAILED
  fail_reason VARCHAR (nullable)
  created_at, updated_at
+ outbox, processed_events
```

### configuration-service

```
channels
  name    VARCHAR PK
  enabled BOOLEAN NOT NULL
```

Seeded by Alembic data migration: `email = true`, `telegram = true`. No outbox, no
`processed_events`, no Redis.

### email-service

```
email_delivery
  id, notification_id UNIQUE NOT NULL, recipient,
  status      VARCHAR NOT NULL   -- DELIVERED | FAILED (inserted already terminal; SENDING is
                                  -- a defined but unwritten enum member, see ADR 0022)
  fail_reason VARCHAR (nullable)
  sent_at     TIMESTAMP (nullable)
  created_at, updated_at
+ outbox, processed_events
```

### telegram-service (slice 2)

As email-service, with `chat_id` in place of `recipient`.

---

## 8. Packaging and toolchain

### 8.1 uv workspace

Root `pyproject.toml`:

```toml
[tool.uv.workspace]
members = ["gateway", "services/*", "shared"]
```

One `uv.lock` at the root. `.python-version` contains `3.14`. Each member declares
`notification-shared` as a dependency and resolves it with
`[tool.uv.sources] notification-shared = { workspace = true }`.

Local setup: `uv venv` then `uv sync --all-packages` at the root produces a **single root
`.venv`** containing every service and the shared library. That is the interpreter VSCode uses
and the environment that runs cross-service tests.

### 8.2 Docker

The workspace requires the build context to be the repository root, so that `shared/` and the
root `uv.lock` are reachable:

```yaml
notification-service:
  build:
    context: .
    dockerfile: services/notification-service/Dockerfile
```

Multi-stage Dockerfile per service:

```dockerfile
FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY shared/ ./shared/
COPY services/notification-service/ ./services/notification-service/
RUN uv sync --frozen --no-dev --no-editable --package notification-service

FROM python:3.14-slim AS runtime
RUN adduser --disabled-password appuser
WORKDIR /app
COPY --from=builder --chown=appuser /app/.venv /app/.venv
COPY --chown=appuser services/notification-service/ /app/
USER appuser
ENTRYPOINT ["/app/entrypoint.sh"]
```

`--package` installs only that member's dependencies rather than all six. `--no-editable` bakes
`notification_shared` into `site-packages`, so the runtime image does not carry `shared/`
sources.

`entrypoint.sh` per service:

```sh
#!/bin/sh
set -e
/app/.venv/bin/alembic upgrade head
exec /app/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Root `.dockerignore` excludes `.venv`, `.git`, `__pycache__`, `*.pyc`, `.vscode`, `docs`.

### 8.3 Compose

All application containers listen internally on port 8000. Host port mapping:

| Container | Host port |
|---|---|
| gateway (slice 2) | 8000 |
| notification-service | 8001 |
| routing-service | 8002 |
| configuration-service | 8003 |
| email-service | 8004 |
| telegram-service (slice 2) | 8005 |
| notification-db | 5433 |
| routing-db | 5434 |
| configuration-db | 5435 |
| email-db | 5436 |
| telegram-db (slice 2) | 5437 |
| redis | 6379 |

Dependencies are published on localhost so a service can be run under the VSCode debugger
against containerized Postgres and Redis.

Each service: healthcheck on `GET /health` (interval 10s, timeout 5s, retries 5, start_period
30s), `restart: on-failure`, network `notification-net`, named volume per Postgres,
`depends_on` with `condition: service_healthy` for its own database and for Redis.

`docker/redis/redis.conf`:

```
appendonly yes
appendfsync everysec
maxmemory-policy noeviction
```

### 8.4 `.vscode/`

- **`extensions.json`** — `ms-python.python`, `ms-python.vscode-pylance`, `ms-python.debugpy`,
  `charliermarsh.ruff`, `ms-azuretools.vscode-docker`, `tamasfe.even-better-toml`,
  `redhat.vscode-yaml`, `bierner.markdown-mermaid`.
- **`settings.json`** — interpreter `${workspaceFolder}/.venv/Scripts/python.exe`; Ruff as
  formatter with format-on-save and import organization; pytest enabled at the root; explorer
  and search exclusions for `__pycache__`, `.venv`, `*.egg-info`.
- **`launch.json`** — one Uvicorn configuration per slice-1 service (notification, routing,
  configuration, email), each setting `DATABASE_URL` to the matching localhost port, plus a
  "Debug Tests" configuration.
- **`tasks.json`** — `compose up --build`, `compose down -v`, compose infrastructure only,
  `uv sync --all-packages`, `pytest tests/unit`, `pytest tests/integration`.

### 8.5 Lint and format

Ruff for linting and formatting, configured in the root `pyproject.toml`. Type checking is left
to Pylance in the editor; no separate type-checker in the build.

---

## 9. Shared library — `notification_shared`

| Module | Contents |
|---|---|
| `config.py` | `BaseSettings` loader (pydantic-settings) |
| `logging.py` | `configure_logging(service_name, log_level)` and `JSONFormatter` |
| `context.py` | `ContextVar` holding the current `correlation_id` |
| `middleware.py` | `CorrelationIDMiddleware` — reads or generates `X-Correlation-ID`, sets the ContextVar, echoes the header |
| `http_client.py` | `ServiceClient` — HTTPX wrapper with timeout and `ServiceUnavailableError` translation |
| `events.py` | `EventEnvelope`, `Channel` enum, event type constants, stream name constants |
| `models.py` | `TimestampMixin`, `OutboxMixin`, `ProcessedEventMixin` — abstract, no `__tablename__` |
| `outbox.py` | `OutboxRepository(model_cls)` — `save`, `get_pending`, `mark_published` |
| `streams.py` | `RedisStreamPublisher` (`publish`), `RedisStreamConsumer` (`ensure_group`, `read`, `ack`, `get_pending`, `claim`) |
| `idempotency.py` | `IdempotencyRepository(model_cls)` — `is_processed`, `mark_processed`, `increment_fail_count` |
| `exceptions.py` | `ErrorCode` enum, `ErrorResponse` model, `ServiceError` |

`context.py` exists because the middleware covers only the HTTP path, while workers derive
`correlation_id` from the event envelope. With a ContextVar, `JSONFormatter` reads it in both
contexts and no log call has to pass it explicitly.

---

## 10. Testing

Two tiers plus end-to-end. Run from the repository root against the single `.venv`.

### 10.1 Unit — `tests/unit/`, no I/O

- `EventEnvelope` round-trip; `event_version` defaults to 1
- Unknown channel is rejected by validation
- Channel filter predicate
- Legal status transitions as a pure function, mirroring the SQL `WHERE` clauses of 3.16
- `JSONFormatter` emits the required keys and reads `correlation_id` from the ContextVar
- Deterministic failure rule: the `fail` substring is detected in recipient and chat_id

### 10.2 Integration — `tests/integration/`, real Postgres and Redis via testcontainers

- A rolled-back transaction leaves no outbox row and publishes nothing
- The outbox publisher moves pending rows to the stream and marks them published
- **Crash between `XADD` and the published mark** → on restart the event is republished →
  consumer idempotency absorbs the duplicate. This is the at-least-once proof.
- The same `event_id` processed twice produces one state change
- A `FAILING` row is not treated as processed (correction 3.4)
- `XREADGROUP` → `XACK` → no redelivery
- **The race from 3.16**: `delivery.completed` consumed before `notification.routed` leaves the
  final status `COMPLETED`, not overwritten to `PROCESSING`
- Channel disabled → `RoutingFailed` published and `routes` set to `FAILED`
- Configuration Service unavailable → no `XACK`, no outbox row, message left pending (3.18)
- `routing-service-results` skips `RoutingFailed`
- `alembic upgrade head` on an empty database, per service
- `UNIQUE (notification_id)` blocks a duplicate insert on `routes`

### 10.3 End-to-end — `tests/e2e/`, docker compose

- `POST` → poll → `COMPLETED`
- Disable a channel via Configuration Service → `POST` → poll → `FAILED` with
  `fail_reason = channel_disabled`
- Recipient containing `fail` → poll → `FAILED`

Slice 2 adds: `XCLAIM` recovers a message whose consumer was killed mid-flight; the watchdog
marks a stalled `PROCESSING` notification `FAILED` after the timeout; the Gateway propagates
`X-Correlation-ID` and returns `504` on downstream timeout.

---

## 11. Documentation deliverables

| File | Contents |
|---|---|
| `README.md` | Architecture overview, quick start, stream topology table, polling guide, env var table, known limitations, local setup with uv |
| `docs/architecture.md` | Services and boundaries, why choreography over orchestration, database-per-service, layering rules |
| `docs/event-flows.md` | Every stream, the envelope, each event type's payload schema, Mermaid sequence diagrams of all three outcomes |
| `docs/patterns.md` | Outbox, idempotent consumers, at-least-once, `XPENDING`/`XCLAIM` recovery, guarded transitions — each with the file it lives in and the exact failure it prevents |
| `docs/operations.md` | Running, resetting, inspecting: `redis-cli` commands for streams, groups and pending entries; `psql` queries against `outbox` and `processed_events`; how to trigger each failure path; troubleshooting |
| `docs/local-development.md` | uv workspace, root `.venv`, VSCode debugging, running both test tiers, infrastructure-only compose |
| `docs/adr/NNNN-*.md` | One ADR per decision in section 3: context, decision, consequences |
| `services/*/README.md` | Per service: responsibilities, tables, streams consumed and published, env vars |

Diagrams are written in Mermaid so they render on GitHub and in VSCode.

Written per slice (correction 3.15): slice 1 produces `architecture.md`, `event-flows.md`,
`patterns.md`, `local-development.md`, the per-service READMEs, the ADRs for decisions taken,
and the README quick start. Slice 3 adds `operations.md` and the final review.

---

## 12. Configuration

Per-service environment variables, all overridable in `docker-compose.yml`:

| Variable | Default | Applies to |
|---|---|---|
| `DATABASE_URL` | — | all except gateway |
| `REDIS_URL` | `redis://redis:6379/0` | all event-driven services |
| `SERVICE_NAME` | per service | all |
| `SERVICE_VERSION` | `1.0.0` | all |
| `LOG_LEVEL` | `INFO` | all |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | event-driven services |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | services with an outbox |
| `OUTBOX_BATCH_SIZE` | `100` | services with an outbox |
| `PENDING_TIMEOUT_MS` | `30000` | slice 2 |
| `PENDING_MAX_RETRIES` | `3` | slice 2 |
| `PROCESSING_TIMEOUT_MINUTES` | `5` | notification-service, slice 2 |
| `CONFIGURATION_SERVICE_URL` | — | routing-service |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | routing-service, gateway |
| `GATEWAY_TIMEOUT_SECONDS` | `10.0` | gateway, slice 2 |
| `TELEGRAM_BOT_TOKEN` | unset | telegram-service, slice 2 |
| `TELEGRAM_CHAT_ID` | unset | telegram-service, slice 2 |

---

## 13. Service internals

Per service, unchanged from v3:

```
app/
├── main.py          FastAPI app + lifespan
├── core/            config.py, database.py
├── api/v1/          router.py, endpoints/
├── models/          SQLAlchemy models (own Base)
├── schemas/         Pydantic request/response
├── repositories/    data access only
├── services/        business logic
└── workers/         background asyncio tasks
```

Rules: route handlers call the service layer only; the service layer calls repositories; only
repositories contain SQLAlchemy `select`/`insert`/`update`; dependencies are injected with
`Depends`. Background tasks are started in the `lifespan` context manager, never as per-request
`BackgroundTasks`. Each worker loop handles its own exceptions, logs at ERROR without dying, and
sleeps its configured poll interval between iterations.

Worker counts in slice 1: notification-service 3 (outbox publisher, routed consumer, results
consumer), routing-service 3 (outbox publisher, created consumer, results consumer),
email-service 2 (outbox publisher, routed consumer), configuration-service 0.

Every service exposes `GET /health` (checking Postgres, plus Redis where applicable, returning
503 when a dependency is down), `GET /version`, Swagger at `/docs`, and ReDoc at `/redoc`.
Logging is structured JSON on stdout including `service`, `correlation_id`, and, in worker
contexts, `event_id` and `event_type`.

---

## 14. Verification gates

A slice is complete only when all of the following hold:

1. `uv run pytest tests/unit` passes
2. `uv run pytest tests/integration` passes
3. `docker compose up --build` brings every service of the slice to `healthy`
4. The slice's end-to-end tests pass
5. The slice's documentation exists and matches the code

---

## 15. Known limitations

Carried over from v3 unchanged, to be restated in `README.md`:

- No schema registry — events are versioned by convention, not enforced
- No dead-letter queue — max-retry via `processed_events`, then discard
- No distributed tracing backend — correlation ID in logs only, no OpenTelemetry spans
- No circuit breaker on the Routing → Configuration REST call
- No horizontal scaling of consumers — one instance per service, one consumer per group
- At-least-once delivery, not exactly-once; idempotency mitigates duplicate effects
- Simulated delivery for Email; Telegram is real only when its environment variables are set
- No real SMTP

Added by this specification:

- Slice 1 has no recovery and no watchdog: a notification whose consumer crashes mid-flight
  stays in its current state until slice 2 lands
- Postgres and Redis state are coupled — volumes must be reset together (correction 3.2)

---

## 16. Open risks

| Risk | Handling |
|---|---|
| `asyncpg` may lack a Python 3.14 wheel | Verify first in slice 1; fall back to `psycopg[binary]` async, changing only the `DATABASE_URL` driver |
| Python 3.14 deferred annotation evaluation (PEP 649) | Pin SQLAlchemy and Pydantic versions known to support 3.14; the `alembic upgrade head` integration test surfaces breakage early |
| testcontainers startup cost on Windows | Session-scoped container fixtures with per-test schema reset; the unit tier stays the fast inner loop |
