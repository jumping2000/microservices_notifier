# Universal Notification Platform — Slice 2 Design Specification

**Date:** 2026-09-24
**Status:** Approved in conversation, pending written review
**Builds on:** `docs/superpowers/specs/2026-09-12-notification-platform-design.md` (the slice 1
spec). Everything that spec decides still holds unless a section below says otherwise. Where the
two disagree, this document wins for slice 2 work.

---

## 1. Goal and scope

Slice 2 closes the one gap slice 1 accepted — a notification whose consumer fails mid-flight
stays where it is forever — and adds the two missing components.

One spec, one plan, three parts:

| Part | Contents |
|---|---|
| **A. Resilience** | `XPENDING IDLE` / `XCLAIM` recovery, max-retry with `FAILED_PERMANENT`, a failure event on give-up, the stale-processing watchdog |
| **B. Telegram Service** | Its own database; simulated delivery by default, real Bot API delivery when a token is configured |
| **C. API Gateway** | FastAPI thin reverse proxy on `/api/v1/*` |

The platform's priorities are unchanged: educational readability first, every pattern traceable to
a documented reason and a test that proves it.

### Decisions taken during design

| # | Question | Decision |
|---|---|---|
| D1 | What happens to the notification when a consumer exhausts its retries? | The consumer that gives up publishes a failure event with reason `max_retries_exceeded`. The watchdog stays as the safety net for `PROCESSING`. |
| D2 | Which chat does real Telegram delivery target? | The notification's `recipient` is the `chat_id`. `TELEGRAM_CHAT_ID` is dropped; `TELEGRAM_BOT_TOKEN` alone switches real delivery on. |
| D3 | How is recovery structured? | One shared `PendingRecoverer` per service, like `OutboxPublisher`. Consumers keep their explicit read/handle/ack loops and expose only `handle` and `give_up`. |
| D4 | What does the Gateway return when a downstream service is unreachable? | `502` with `ErrorCode.BAD_GATEWAY`. The slice 1 spec only defined `504` for timeouts. |

Each becomes an ADR (section 9).

---

## 2. Part A — Resilience

### 2.1 Why recovery must claim, not re-read

Every consumer is named `socket.gethostname()`, which in Docker is the container ID and changes on
every restart. After a crash, the pending entries belong to a consumer name that no longer exists.
Re-reading the consumer's own pending list (`XREADGROUP ... 0`) would therefore find nothing:
recovery has to find idle entries regardless of owner and `XCLAIM` them.

### 2.2 `RedisStreamConsumer` additions

Two methods the slice 1 spec listed in section 9 but slice 1 deliberately did not build:

- `get_pending(stream, group, min_idle_ms, count) -> list[PendingEntry]`
  — `XPENDING <stream> <group> IDLE <min_idle_ms> - + <count>` (correction 3.5 of the slice 1
  spec). `PendingEntry` is a `NamedTuple(message_id, consumer, idle_ms, times_delivered)`.
- `claim(stream, group, min_idle_ms, message_ids) -> list[ClaimedMessage]`
  — `XCLAIM` with the same `min_idle_ms`, so a message another consumer picked up in the meantime
  is not stolen. `ClaimedMessage` is a `NamedTuple(message_id, envelope: EventEnvelope | None)`;
  `envelope` is `None` when the entry cannot be parsed (section 2.5). An entry that was deleted
  from the stream is returned by Redis as nil and is skipped.

### 2.3 `PendingRecoverer`

New module `shared/notification_shared/recovery.py`. It is shared for the same reason
`OutboxPublisher` is: the mechanics are identical in every service, and the per-service
consumers — the code a reader is meant to study — stay explicit (slice 1 ledger, ruling F4).

A consumer the recoverer can drive satisfies:

```python
class RecoverableConsumer(Protocol):
    async def handle(self, envelope: EventEnvelope) -> bool: ...
    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None: ...
```

`handle` is today's `_handle`, made public and otherwise unchanged. Its contract is the existing
one: returns `True` when the message should be acked, `False` or raises when it should not.

Construction and wiring, in each service's `lifespan`:

```python
recoverer = PendingRecoverer(
    redis=..., consumer_name=socket.gethostname(),
    session_factory=..., idempotency=IdempotencyRepository(ProcessedEvent),
    pending_timeout_ms=settings.pending_timeout_ms,
    max_retries=settings.pending_max_retries,
    poll_interval_ms=settings.recovery_poll_interval_ms,
)
recoverer.register(Stream.NOTIFICATION_ROUTED, ConsumerGroup.EMAIL, routed_consumer)
```

A consumer reading two streams (both results consumers) is registered once per stream.

`recover_once()`, for each registered `(stream, group, consumer)`:

1. `get_pending(stream, group, pending_timeout_ms, count=10)`.
2. `claim(...)` those ids to this consumer.
3. For each claimed message:
   - `envelope is None` → log ERROR with the message id, `XACK`. Nothing to count against.
   - `await consumer.handle(envelope)` returns `True` → `XACK`.
   - `handle` returns `False` or raises → in its own transaction,
     `count = increment_fail_count(event_id, group)`, commit, log WARNING with the count.
     - If `count >= max_retries` → in one transaction: `consumer.give_up(session, envelope)`,
       `mark_failed_permanent(event_id, group)`, commit; then `XACK`; log ERROR.
     - Otherwise leave the entry pending. It becomes eligible again after another
       `pending_timeout_ms` of idleness.

`run_forever()` calls `recover_once()` every `recovery_poll_interval_ms`, catching and logging
exceptions without dying, like every other worker loop.

**Only the recoverer counts attempts.** The normal read path is unchanged: a failure there leaves
the message pending and nothing more. With the defaults, a message gets one normal attempt plus
three recovery attempts, roughly two minutes, before the consumer gives up.

If `give_up` itself raises, the transaction rolls back, nothing is acked, and the next cycle tries
again (the count is already at the limit, so it goes straight to give-up).

### 2.4 `IdempotencyRepository` changes

- New `mark_failed_permanent(session, event_id, consumer_group)` — the same upsert shape as
  `mark_processed`, setting `status = FAILED_PERMANENT`. `is_processed` already treats that status
  as terminal, so a replay of a given-up event is skipped.
- `mark_processed` keeps **not** resetting `fail_count` when it overwrites a `FAILING` row. This is
  now deliberate (slice 1 ledger, Task 6 deferred minor): the count is the history of how many
  attempts an event needed before it succeeded, readable with `psql`.

### 2.5 Unparseable entries

An entry without an `envelope` field, or whose JSON fails validation, has no `event_id`, so it
cannot be counted in `processed_events`. The recoverer logs it at ERROR and acks it. On the normal
read path such an entry makes `read()` raise for the whole batch; every message of that batch is
already in the pending list, so the recoverer later claims them one by one, handles the good ones
and discards the bad one.

### 2.6 `give_up` per consumer

All give-ups use reason `max_retries_exceeded`.

| Service / consumer | `give_up` writes |
|---|---|
| routing `notification.created` | `routes` row `FAILED` + outbox `RoutingFailed` on `delivery.failed` → notification `CREATED → FAILED` |
| email `notification.routed` | `email_delivery` row `FAILED` + outbox `DeliveryFailed` → notification `PROCESSING → FAILED` |
| telegram `notification.routed` | `telegram_delivery` row `FAILED` + outbox `DeliveryFailed` → notification `PROCESSING → FAILED` |
| notification `notification-service-routed` | nothing — no one to tell; the results event still closes the notification |
| notification `notification-service-results` | nothing — the notification stays `PROCESSING` and the watchdog closes it |
| routing `routing-service-results` | nothing — the route stays `PROCESSING` (documented limitation) |

`give_up` must not call an external system: it is a local write plus an outbox row, so it cannot
fail for the reason `handle` did.

A give-up for a delivery consumer whose channel filter would skip the event cannot happen: the
filter makes `handle` return `True` before any I/O.

### 2.7 Stale-processing watchdog

New worker in notification-service, `app/workers/watchdog.py`, running every
`WATCHDOG_INTERVAL_SECONDS`. It calls a new repository method:

```sql
UPDATE notifications
SET status = 'FAILED', fail_reason = 'processing_timeout'
WHERE status = 'PROCESSING'
  AND updated_at < now() - make_interval(mins => :processing_timeout_minutes)
```

This is the guarded update from correction 3.16 of the slice 1 spec, with `now()` taken from the
database so container clock skew cannot matter. It logs how many rows it failed.

Documented limitations (README "Known limitations"):

- The watchdog publishes no event, so Routing Service's `routes` row stays `PROCESSING`.
- A delivery result arriving after the timeout does not reopen the notification: terminal states
  never reopen. The notification reads `FAILED` even though the message was delivered.

### 2.8 Consumer changes in existing services

Mechanical, the same in each of the five slice 1 consumers:

- `_handle` → `handle`.
- Add `give_up` as in section 2.6.
- `main.py` builds one `PendingRecoverer`, registers every consumer, and starts its task.
- Routing Service's transient branch comment ("stays pending for slice 2's XCLAIM recovery") is
  updated to name the recoverer.

---

## 3. Part B — Telegram Service

### 3.1 Layout

`services/telegram-service/`, following email-service file by file:

```
app/
├── main.py                      lifespan: routed consumer, outbox publisher, recoverer
├── core/config.py, database.py
├── api/v1/                      /health, /version only (no POST /send — correction 3.21)
├── models/                      base, outbox, processed_event, telegram_delivery
├── repositories/telegram_delivery.py   add(), get_by_notification()
├── senders.py                   TelegramSender protocol, SimulatedSender, BotApiSender
└── workers/routed_consumer.py
alembic/ + alembic.ini, Dockerfile, entrypoint.sh, README.md, tests/
```

Database `telegram-db` (host port 5437); service host port 8005.

### 3.2 Table

```
telegram_delivery
  id, notification_id UNIQUE NOT NULL, chat_id,
  status      VARCHAR NOT NULL   -- DELIVERED | FAILED, inserted already terminal (ADR 0022)
  fail_reason VARCHAR (nullable)
  sent_at     TIMESTAMP (nullable)
  created_at, updated_at
+ outbox, processed_events
```

### 3.3 Consumer

Group `telegram-service` on `notification.routed`. Acks and skips events whose
`payload.channel != "telegram"`. Otherwise the same shape as email's `RoutedConsumer`: idempotency
check in a short transaction, delivery outside any transaction, then one transaction with the
delivery row, the outbox row and `mark_processed` (correction 3.6 of slice 1, ADR 0023).

`chat_id` is `payload["recipient"]`. Message text is `f"{subject}\n\n{body}"`, or `body` alone
when `subject` is null.

### 3.4 Senders

```python
class TelegramSender(Protocol):
    async def send(self, chat_id: str, text: str) -> None: ...
```

`send` returns on success, raises `TelegramRejectedError` for a permanent failure and
`TelegramUnavailableError` for a transient one.

- **`SimulatedSender`** — used when `TELEGRAM_BOT_TOKEN` is unset or empty. Sleeps a random
  `0..DELIVERY_LATENCY_MS_MAX` ms, like email. Never raises.
- **`BotApiSender`** — used when the token is set. `POST
  https://api.telegram.org/bot<token>/sendMessage` with JSON `{chat_id, text}`, through an
  `httpx.AsyncClient` with timeout `HTTP_TIMEOUT_SECONDS`. The client accepts an injected
  transport so tests use `httpx.MockTransport`.

Which sender is used is decided once in `lifespan` and logged (without the token).

### 3.5 Failure rules

Checked in this order:

1. `chat_id` contains `fail` (case-insensitive) → `DeliveryFailed`, reason `simulated_failure`.
   Applies in **both** modes and is checked **before** any network call, so end-to-end tests are
   deterministic whether or not a token is configured (correction 3.8 of slice 1).
2. Bot API response:

| Response | Outcome |
|---|---|
| `200` with `ok: true` | `DeliveryCompleted` |
| `400`, `403` | permanent → `DeliveryFailed`, reason `telegram_rejected` |
| `429`, `5xx`, timeout, connection error | transient → `handle` raises, nothing written, nothing acked; the recoverer retries and eventually gives up with `max_retries_exceeded` |

Any other status is treated as transient.

### 3.6 Token hygiene

The token is part of the request URL, and httpx logs request URLs at INFO. The service sets the
`httpx` and `httpcore` loggers to WARNING in `lifespan`, and a test asserts the token never
appears in captured log output for a successful send, a rejected send and a timeout.

### 3.7 Other changes

- `TELEGRAM_CHAT_ID` is removed from every document that lists it.
- `tools/playground/app.py` drops its "telegram stays PROCESSING" warning.

---

## 4. Part C — API Gateway

### 4.1 Layout

`gateway/` at the repository root, added to the workspace:

```toml
[tool.uv.workspace]
members = ["gateway", "services/*", "shared"]
```

```
gateway/
├── app/
│   ├── main.py          lifespan owns one httpx.AsyncClient
│   ├── core/config.py
│   └── api/v1/          proxy.py (forwarding routes), system.py (/api/v1/health)
├── Dockerfile, entrypoint.sh (no alembic step), README.md, pyproject.toml
└── tests/
```

No database, no Redis. Host port 8000.

### 4.2 Routes

| Gateway | Forwards to |
|---|---|
| `POST /api/v1/notifications` | notification-service `POST /notifications` |
| `GET /api/v1/notifications/{id}` | notification-service `GET /notifications/{id}` |
| `GET /api/v1/notifications` | notification-service `GET /notifications` |
| `GET /api/v1/channels` | configuration-service `GET /channels` |
| `PUT /api/v1/channels/{name}` | configuration-service `PUT /channels/{name}` |
| `GET /api/v1/health` | the Gateway's own status only; does not aggregate downstream health |

Base URLs come from `NOTIFICATION_SERVICE_URL` and `CONFIGURATION_SERVICE_URL`. The routing table
is static: explicit routes, not a catch-all path, so the Gateway's Swagger lists exactly what it
exposes.

### 4.3 Forwarding

- Method, query string, body and request headers are forwarded, minus hop-by-hop headers (`host`,
  `content-length`, `connection`, `transfer-encoding`).
- `CorrelationIDMiddleware` from the shared library generates `X-Correlation-ID` when absent; the
  Gateway always sends it downstream.
- The downstream status, body and `content-type` are returned unchanged, including downstream
  `404` and `422` responses. No payload transformation, no validation, no knowledge of channels.

### 4.4 Downstream failures

| Condition | Response |
|---|---|
| no response within `GATEWAY_TIMEOUT_SECONDS` | `504`, `ErrorCode.GATEWAY_TIMEOUT` |
| connection refused / DNS failure | `502`, `ErrorCode.BAD_GATEWAY` (D4) |

Both use the common error model. Both codes are added to the shared `ErrorCode` enum.

### 4.5 Deliberately not in the Gateway

API key (slice 3), CORS (the Gateway is where it would go, if a browser client ever needs it),
retries, caching, response aggregation.

---

## 5. Configuration

New or changed variables. Everything else in section 12 of the slice 1 spec is unchanged.

| Variable | Default | Applies to |
|---|---|---|
| `PENDING_TIMEOUT_MS` | `30000` | routing, email, telegram, notification |
| `PENDING_MAX_RETRIES` | `3` | routing, email, telegram, notification |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | routing, email, telegram, notification — new |
| `PROCESSING_TIMEOUT_MINUTES` | `5` | notification |
| `WATCHDOG_INTERVAL_SECONDS` | `60` | notification — new |
| `GATEWAY_TIMEOUT_SECONDS` | `10.0` | gateway |
| `NOTIFICATION_SERVICE_URL` | — | gateway — new |
| `CONFIGURATION_SERVICE_URL` | — | routing, gateway |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | routing, telegram |
| `DELIVERY_LATENCY_MS_MAX` | `500` | email, telegram |
| `TELEGRAM_BOT_TOKEN` | unset | telegram — unset or empty means simulated |
| `TELEGRAM_CHAT_ID` | — | **removed** (D2) |

The three recovery settings live in each consumer service's own `Settings`, not in
`BaseServiceSettings`, because configuration-service and the gateway have no consumers.

---

## 6. Compose

- Three new containers: `gateway`, `telegram-service`, `telegram-db`; twelve in total.
- `telegram-service` gets `TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}`, so a token in a local,
  git-ignored `.env` next to `docker-compose.yml` enables real delivery and nothing else changes.
  `.env` is already in `.gitignore`.
- `gateway` `depends_on` notification-service and configuration-service `service_healthy`; its
  healthcheck is `GET /api/v1/health`.
- `telegram-service` sets `DELIVERY_LATENCY_MS_MAX: "2000"` like email, for the same e2e reason.
- Service defaults for the recovery and watchdog variables are used; compose does not override
  them.

---

## 7. Testing

Same tiers and invocation rules as slice 1 (`docs/local-development.md`): repository-root tiers
plus one pytest process per service.

### 7.1 Unit — `tests/unit/`

- Give-up decision: `count >= max_retries` boundary.
- Telegram response classification: `200`, `400`, `403`, `429`, `500`, unknown status.
- Telegram text building with and without `subject`.
- Sender selection: empty token → simulated, token → Bot API.
- `fail` marker detection on `chat_id`.

### 7.2 Integration — `tests/integration/`, testcontainers

`PendingRecoverer`, against real Redis and Postgres, with a small `pending_timeout_ms`:

- A message read by a consumer name that then "dies" is claimed and completed by another name.
- A message that is not yet idle is left alone.
- Each failed attempt increments `fail_count` by one.
- At `max_retries`, `give_up` runs, the row is `FAILED_PERMANENT`, the message is acked.
- `give_up` raising leaves the message pending and the row `FAILING`.
- An unparseable entry is acked and logged.
- A replay of a `FAILED_PERMANENT` event is skipped by `is_processed`.

`IdempotencyRepository.mark_failed_permanent`; `mark_processed` over a `FAILING` row keeps
`fail_count`.

### 7.3 Service suites

- **notification-service** — watchdog: stale `PROCESSING` → `FAILED` / `processing_timeout`;
  recent `PROCESSING` untouched; `COMPLETED` untouched.
- **routing-service** — `give_up` writes a `FAILED` route and a `RoutingFailed` with
  `max_retries_exceeded`, in one transaction.
- **email-service** — `give_up` writes a `FAILED` delivery and a `DeliveryFailed` with
  `max_retries_exceeded`.
- **telegram-service** — the email suite's equivalents (channel filter, `fail` rule, replay,
  unique constraint, `alembic upgrade head`); `BotApiSender` against `httpx.MockTransport` for
  `200`, `400`, `429`, timeout; a transient failure leaves nothing written and the message
  unacked; the token never appears in logs; `give_up`.
- **gateway** — each route forwards method, path, query and body; `X-Correlation-ID` generated
  when absent and propagated when present; downstream `404`/`422` passed through unchanged;
  timeout → `504`; connection refused → `502`; `/api/v1/health` does not call downstream.

### 7.4 End-to-end — `tests/e2e/`, full stack

- Telegram (simulated): happy path `CREATED → PROCESSING → COMPLETED`; `fail` chat id →
  `FAILED` / `simulated_failure`.
- The three slice 1 outcomes re-run **through the Gateway**, plus correlation id propagation.
- Recovery for real: stop configuration-service, submit a notification, observe it stay `CREATED`,
  start configuration-service, observe `COMPLETED` within 60 s.
- `GATEWAY_URL` joins the existing env-overridable URLs.

Give-up (≈2 min) and the watchdog (5 min) are proven at the integration and service tiers only;
exercising them end-to-end would make the suite too slow.

---

## 8. Parked items carried in

- `docs/patterns.md` names only one of the two guarded-transition implementations (slice 1 ledger,
  parked). Fixed as part of this slice's documentation pass.
- The e2e `PROCESSING` observation flakiness (slice 1 ledger, parked) stays parked: it needs a
  decision between a latency floor and a shorter poll interval, and nothing in slice 2 changes it.

---

## 9. Documentation deliverables

New ADRs, continuing from 0023:

| ADR | Decision |
|---|---|
| 0024 | A consumer that gives up publishes a failure event (D1) |
| 0025 | Recovery claims by idle time because consumer names change on restart; one shared recoverer (D3, section 2.1) |
| 0026 | Telegram `recipient` is the `chat_id`; `TELEGRAM_CHAT_ID` dropped (D2) |
| 0027 | Gateway returns `502` for an unreachable downstream (D4) |
| 0028 | The watchdog publishes no event, and terminal states never reopen after it |

Updated: `README.md` (quick start through the Gateway, stream topology with the live
`telegram-service` group, env var table, known limitations), `docs/architecture.md`,
`docs/event-flows.md` (a give-up sequence diagram and the telegram path),
`docs/patterns.md` (recovery, max-retry, watchdog; both guarded transitions),
`docs/local-development.md` (new test invocations, gateway and telegram debug configurations),
`services/telegram-service/README.md`, `gateway/README.md`, `.vscode/launch.json` and
`.vscode/tasks.json`.

---

## 10. Verification gates

Slice 2 is complete only when:

1. `uv run pytest tests/unit` and `uv run pytest tests/integration` pass.
2. Every service suite passes, including telegram-service and gateway.
3. `docker compose up --build -d --wait` brings all twelve containers to `healthy`.
4. `uv run pytest tests/e2e` passes.
5. `uv run ruff check .` and `uv run ruff format --check .` are clean.
6. The documentation in section 9 exists and matches the code.

---

## 11. Known limitations after slice 2

Carried from slice 1 and still true: no schema registry, no dead-letter queue (a given-up event is
recorded in `processed_events` and discarded from the stream), no tracing backend, no circuit
breaker on Routing → Configuration, one consumer per group, at-least-once delivery, no real SMTP.

New:

- Recovery latency: a transient failure is retried only after `PENDING_TIMEOUT_MS` of idleness;
  give-up takes about two minutes with the defaults.
- The watchdog leaves Routing Service's `routes` row at `PROCESSING` and can mark a notification
  `FAILED` whose delivery later succeeds (ADR 0028).
- Consumer names of dead containers accumulate in each group's `XINFO CONSUMERS` listing. Harmless;
  not cleaned up.
- Real Telegram delivery is not exercised by any automated test; it is covered by the
  `MockTransport` suite and by manual use with a token.

---

## 12. Out of scope

API key on the Gateway, Prometheus metrics, `docs/operations.md` (all slice 3); CORS; horizontal
scaling of consumers; routing the playground dashboard through the Gateway.

---

## 13. Open risks

| Risk | Handling |
|---|---|
| `XCLAIM` of an entry deleted from the stream returns nil | `claim` skips nil entries; covered by an integration test if redis-py surfaces them |
| A slow `handle` (> `PENDING_TIMEOUT_MS`) could be claimed while still running | Email and Telegram latencies are bounded well below 30 s; idempotency and `UNIQUE (notification_id)` absorb a duplicate if it ever happens |
| Telegram Bot API rate limits (`429`) during manual testing | Classified transient; recovery retries with the pending timeout as natural back-off |
