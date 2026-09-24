# Universal Notification Platform — Slice 2 Design Specification

**Date:** 2026-09-24
**Status:** Approved in conversation, pending written review
**Builds on:** `docs/superpowers/specs/2026-09-12-notification-platform-design.md` (the slice 1
spec). Everything that spec decides still holds unless a section below says otherwise. Where the
two disagree, this document wins for slice 2 work.

---

## 1. Goal and scope

Slice 2 closes the one gap slice 1 accepted — a notification whose consumer fails mid-flight
stays where it is forever — adds the two missing components, makes real delivery possible on both
channels, and turns the playground into the tool for exercising the whole platform by hand.

One spec, one plan, five parts:

| Part | Contents |
|---|---|
| **A. Resilience** | `XPENDING IDLE` / `XCLAIM` recovery, max-retry with `FAILED_PERMANENT`, a failure event on give-up, the stale-processing watchdog |
| **B. Telegram Service** | Its own database; simulated delivery by default, real Bot API delivery when a token is configured |
| **C. API Gateway** | FastAPI thin reverse proxy on `/api/v1/*` |
| **D. Real email** | An SMTP sender in email-service, used when SMTP is configured; reserved recipients always simulated on both channels |
| **E. Playground** | The Streamlit dashboard reworked as the platform's manual test console, plus a simulated load test page |

The platform's priorities are unchanged: educational readability first, every pattern traceable to
a documented reason and a test that proves it.

### Decisions taken during design

| # | Question | Decision |
|---|---|---|
| D1 | What happens to the notification when a consumer exhausts its retries? | The consumer that gives up publishes a failure event with reason `max_retries_exceeded`. The watchdog stays as the safety net for `PROCESSING`. |
| D2 | Which chat does real Telegram delivery target? | The notification's `recipient` is the `chat_id`. `TELEGRAM_CHAT_ID` is dropped; `TELEGRAM_BOT_TOKEN` alone switches real delivery on. |
| D3 | How is recovery structured? | One shared `PendingRecoverer` per service, like `OutboxPublisher`. Consumers keep their explicit read/handle/ack loops and expose only `handle` and `give_up`. |
| D4 | What does the Gateway return when a downstream service is unreachable? | `502` with `ErrorCode.BAD_GATEWAY`. The slice 1 spec only defined `504` for timeouts. |
| D5 | How does email-service send real email? | Generic SMTP through `aiosmtplib`, switched on by `SMTP_HOST`. No provider-specific API. |
| D6 | How can a load test run safely when real credentials are configured? | Reserved recipients — RFC 2606 email domains and `sim-` chat ids — are always delivered in simulated mode, whatever the configuration. The load test only ever generates reserved recipients. |
| D7 | Where does the load test live? | A page of the playground, with its engine in a separate module free of Streamlit so it can be tested. |

Each becomes an ADR (section 11).

---

## 2. Part A — Resilience

### 2.1 Why recovery must claim, not re-read

Every consumer is named `socket.gethostname()`, which in Docker is the container ID and changes on
every restart. After a crash, the pending entries belong to a consumer name that no longer exists.
Re-reading the consumer's own pending list (`XREADGROUP ... 0`) would therefore find nothing:
recovery has to find idle entries regardless of owner and `XCLAIM` them.

### 2.2 `RedisStreamConsumer` additions

Two methods the slice 1 spec listed in its section 9 but slice 1 deliberately did not build:

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

## 3. Delivery rules shared by both channels

These rules apply identically in email-service (Part D) and telegram-service (Part B). They live in
a new shared module, `shared/notification_shared/delivery.py`, so the two services cannot drift.

Checked in this order, before any network call:

1. **Failure marker** — the recipient contains `fail` (case-insensitive) → `DeliveryFailed`,
   reason `simulated_failure`. Correction 3.8 of the slice 1 spec, now in shared code instead of
   email's consumer.
2. **Reserved recipient** (D6) → delivered by the simulated sender, even when real delivery is
   configured:
   - email: domain `example.com`, `example.org`, `example.net`, or any domain under the TLDs
     `.example`, `.invalid`, `.test` (RFC 2606);
   - telegram: a `chat_id` starting with `sim-`.
3. Otherwise → the configured sender, real or simulated.

Consequences: every README example, every e2e test and every load test recipient is reserved, so
they all stay simulated and deterministic on a stack with real credentials in `.env`.

Each delivery service reports its configured mode in `GET /version`:

```json
{"service": "email-service", "version": "1.0.0", "delivery_mode": "smtp"}
```

`delivery_mode` is `simulated`, `smtp` (email) or `bot_api` (telegram). It describes the
configured sender; reserved recipients are simulated regardless.

---

## 4. Part B — Telegram Service

### 4.1 Layout

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

### 4.2 Table

```
telegram_delivery
  id, notification_id UNIQUE NOT NULL, chat_id,
  status      VARCHAR NOT NULL   -- DELIVERED | FAILED, inserted already terminal (ADR 0022)
  fail_reason VARCHAR (nullable)
  sent_at     TIMESTAMP (nullable)
  created_at, updated_at
+ outbox, processed_events
```

### 4.3 Consumer

Group `telegram-service` on `notification.routed`. Acks and skips events whose
`payload.channel != "telegram"`. Otherwise the same shape as email's `RoutedConsumer`: idempotency
check in a short transaction, delivery outside any transaction, then one transaction with the
delivery row, the outbox row and `mark_processed` (ADR 0023).

`chat_id` is `payload["recipient"]`. Message text is `f"{subject}\n\n{body}"`, or `body` alone
when `subject` is null.

### 4.4 Senders

```python
class TelegramSender(Protocol):
    async def send(self, chat_id: str, text: str) -> None: ...
```

`send` returns on success, raises `TelegramRejectedError` for a permanent failure and
`TelegramUnavailableError` for a transient one.

- **`SimulatedSender`** — the configured sender when `TELEGRAM_BOT_TOKEN` is unset or empty, and
  always used for reserved chat ids (section 3). Sleeps a random `0..DELIVERY_LATENCY_MS_MAX` ms,
  like email. Never raises.
- **`BotApiSender`** — the configured sender when the token is set. `POST
  https://api.telegram.org/bot<token>/sendMessage` with JSON `{chat_id, text}`, through an
  `httpx.AsyncClient` with timeout `HTTP_TIMEOUT_SECONDS`. The client accepts an injected
  transport so tests use `httpx.MockTransport`.

Which sender is configured is decided once in `lifespan` and logged (without the token).

### 4.5 Bot API outcomes

After the shared rules of section 3:

| Response | Outcome |
|---|---|
| `200` with `ok: true` | `DeliveryCompleted` |
| `400`, `403` | permanent → `DeliveryFailed`, reason `telegram_rejected` |
| `429`, `5xx`, timeout, connection error | transient → `handle` raises, nothing written, nothing acked; the recoverer retries and eventually gives up with `max_retries_exceeded` |

Any other status is treated as transient.

### 4.6 Token hygiene

The token is part of the request URL, and httpx logs request URLs at INFO. The service sets the
`httpx` and `httpcore` loggers to WARNING in `lifespan`, and a test asserts the token never
appears in captured log output for a successful send, a rejected send and a timeout.

### 4.7 Other changes

`TELEGRAM_CHAT_ID` is removed from every document that lists it.

---

## 5. Part C — API Gateway

### 5.1 Layout

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

### 5.2 Routes

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

### 5.3 Forwarding

- Method, query string, body and request headers are forwarded, minus hop-by-hop headers (`host`,
  `content-length`, `connection`, `transfer-encoding`).
- `CorrelationIDMiddleware` from the shared library generates `X-Correlation-ID` when absent; the
  Gateway always sends it downstream.
- The downstream status, body and `content-type` are returned unchanged, including downstream
  `404` and `422` responses. No payload transformation, no validation, no knowledge of channels.

### 5.4 Downstream failures

| Condition | Response |
|---|---|
| no response within `GATEWAY_TIMEOUT_SECONDS` | `504`, `ErrorCode.GATEWAY_TIMEOUT` |
| connection refused / DNS failure | `502`, `ErrorCode.BAD_GATEWAY` (D4) |

Both use the common error model. Both codes are added to the shared `ErrorCode` enum.

### 5.5 Deliberately not in the Gateway

API key (slice 3), CORS (the Gateway is where it would go, if a browser client ever needs it),
retries, caching, response aggregation.

---

## 6. Part D — Real email

### 6.1 Senders

New `services/email-service/app/senders.py`, the same shape as Telegram's:

```python
class EmailSender(Protocol):
    async def send(self, recipient: str, subject: str | None, body: str) -> None: ...
```

`send` returns on success, raises `EmailRejectedError` for a permanent failure and
`EmailUnavailableError` for a transient one.

- **`SimulatedSender`** — today's `_deliver` latency, moved here. The configured sender when
  `SMTP_HOST` is unset or empty, and always used for reserved recipients (section 3).
- **`SmtpSender`** — the configured sender when `SMTP_HOST` is set. Sends a plain-text message
  through `aiosmtplib` from `SMTP_FROM`, authenticating with `SMTP_USERNAME` / `SMTP_PASSWORD` when
  a username is set, with transport security per `SMTP_SECURITY`:
  - `starttls` (default) — plain connect, then STARTTLS; the usual port 587;
  - `ssl` — implicit TLS; the usual port 465;
  - `none` — no TLS, for a local SMTP server such as Mailpit.

  The subject line is the notification's `subject`, or `Notification` when it is null. The
  connection timeout is `HTTP_TIMEOUT_SECONDS`, reused rather than adding a separate variable.

`SMTP_FROM` is required when `SMTP_HOST` is set: settings validation fails at startup otherwise,
so a half-configured service does not reach `healthy`.

`RoutedConsumer._deliver` is replaced by a call to the shared rules (section 3) followed by the
chosen sender. The consumer's transaction shape is unchanged.

### 6.2 SMTP outcomes

| Condition | Outcome |
|---|---|
| message accepted | `DeliveryCompleted` |
| recipient refused, or any other `5xx` reply | permanent → `DeliveryFailed`, reason `smtp_rejected` |
| `4xx` reply, timeout, connection error | transient → `handle` raises; the recoverer retries |
| authentication failure (`535`) | **transient**, logged at ERROR: it is a configuration problem, not a recipient problem. Fixing the credentials within the retry window lets the notification through; otherwise it ends `max_retries_exceeded` |

### 6.3 Secret hygiene

`SMTP_PASSWORD` is never logged, and `aiosmtplib`'s own logger is set to WARNING. A test asserts
the password never appears in captured log output for a successful send, a rejected send and an
authentication failure.

### 6.4 Dependencies

`aiosmtplib` is added to email-service's dependencies. `aiosmtpd` is added to the root `dev`
group, for an in-process fake SMTP server in the email-service tests.

---

## 7. Part E — Playground

### 7.1 Purpose and layout

`tools/playground/` becomes the platform's manual test console: exercise every flow by hand,
send real messages when credentials are configured, and run a simulated load test. It stays
outside the workspace members, with its dependencies in the root `playground` dependency group.

```
tools/playground/
├── app.py                 "Console" page (the existing dashboard, extended)
├── pages/2_Load_test.py   "Load test" page
├── loadtest.py            load test engine: async httpx, no Streamlit import
└── tests/test_loadtest.py
```

Run command unchanged:

```bash
uv run --group playground streamlit run tools/playground/app.py
```

### 7.2 Console page

- **Through the Gateway.** Notification and channel calls go to `GATEWAY_URL` (default
  `http://localhost:8000`). Health stays per service, because the Gateway does not aggregate it:
  six services are listed — gateway, notification, routing, configuration, email, telegram — each
  with its own URL variable, as `tests/e2e/conftest.py` does.
- **Delivery mode badges.** Email and telegram show the `delivery_mode` read from `/version`:
  SIMULATED, or REAL (`smtp` / `bot_api`).
- **Real-send confirmation.** When the selected channel's mode is REAL and the recipient is not
  reserved (section 3), the form shows "this will send a real message" and requires an explicit
  confirmation checkbox before the Send button submits.
- **Scenarios.** The three slice 1 scenarios, plus Telegram happy path and Telegram `fail`. All
  use reserved recipients, so they stay simulated.
- **Unchanged:** the saga timeline, the notification table, the channel toggles. The "telegram
  stays PROCESSING" warning is removed.

### 7.3 Load test engine — `loadtest.py`

Input (`LoadTestConfig`): `total` (1–2000), `concurrency` (1–50), `telegram_ratio` (0–1),
`fail_ratio` (0–1), `settle_timeout_s` (default 120), `base_url` (the Gateway).

**Recipients are generated, never supplied.** Notification `i` goes to `load-{i}@example.com` or
`sim-load-{i}`, with `-fail` inserted for the failing share. Every generated recipient is reserved
(section 3), so a load test cannot send a real message even on a stack with real credentials. The
engine rejects a config it cannot satisfy this way rather than accepting recipients from the
caller.

Two phases:

1. **Submit** — `POST /api/v1/notifications` for every notification, bounded by an
   `asyncio.Semaphore(concurrency)`. Records, per notification, the submit start time, the POST
   latency, and the response status. Non-`202` responses and transport errors are counted, not
   raised.
2. **Settle** — polls `GET /api/v1/notifications/{id}` for every accepted id, every 250 ms, with
   the same concurrency bound, until each is terminal or `settle_timeout_s` elapses. Records the
   time each first reached a terminal status.

A progress callback (`done, total, phase`) lets the page drive a progress bar without the engine
importing Streamlit.

Result (`LoadTestReport`):

- accepted per second over the submit phase; submit errors by status;
- POST latency p50 / p95 / max;
- end-to-end latency (POST start → first terminal observation) p50 / p95 / max — resolution is the
  250 ms poll interval, stated on the page;
- counts by outcome: `COMPLETED`, `FAILED` by `fail_reason`, still non-terminal at timeout;
- completions per second over time, for a chart;
- **correctness verdict** — each notification is expected to end `FAILED` / `simulated_failure`
  when its recipient carries `-fail`, and `COMPLETED` otherwise. The report lists how many matched,
  how many ended in the wrong state, and how many never settled. A load test that loses or
  misroutes a notification fails visibly, not just slowly.

### 7.4 Load test page

Form for the config, a Run button, a progress bar per phase, then the report as metrics, a table
and a chart. A note on the page states what the numbers measure: in compose each consumer group
has one consumer and the simulated delivery sleeps up to 2 s, so throughput reflects those
deliberate limits rather than the code's ceiling.

---

## 8. Configuration

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
| `HTTP_TIMEOUT_SECONDS` | `5.0` | routing, telegram, email (SMTP connect timeout) |
| `DELIVERY_LATENCY_MS_MAX` | `500` | email, telegram (simulated sender) |
| `TELEGRAM_BOT_TOKEN` | unset | telegram — unset or empty means simulated |
| `TELEGRAM_CHAT_ID` | — | **removed** (D2) |
| `SMTP_HOST` | unset | email — new; unset or empty means simulated |
| `SMTP_PORT` | `587` | email — new |
| `SMTP_USERNAME` | unset | email — new; unset means no authentication |
| `SMTP_PASSWORD` | unset | email — new |
| `SMTP_FROM` | unset | email — new; required when `SMTP_HOST` is set |
| `SMTP_SECURITY` | `starttls` | email — new; `starttls`, `ssl` or `none` |
| `GATEWAY_URL` | `http://localhost:8000` | playground, e2e tests — new |
| `TELEGRAM_URL` | `http://localhost:8005` | playground, e2e tests — new |

The three recovery settings live in each consumer service's own `Settings`, not in
`BaseServiceSettings`, because configuration-service and the gateway have no consumers.

---

## 9. Compose

- Three new containers: `gateway`, `telegram-service`, `telegram-db`; twelve in total.
- Secrets come from a local `.env` next to `docker-compose.yml` (already in `.gitignore`), passed
  with empty defaults so an absent `.env` means simulated delivery:
  - `telegram-service`: `TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}`;
  - `email-service`: `SMTP_HOST: ${SMTP_HOST:-}`, `SMTP_PORT: ${SMTP_PORT:-587}`,
    `SMTP_USERNAME`, `SMTP_PASSWORD`, `SMTP_FROM` with `:-` empty defaults,
    `SMTP_SECURITY: ${SMTP_SECURITY:-starttls}`.
- A committed `.env.example` lists every secret variable with an empty value and a comment.
- `gateway` `depends_on` notification-service and configuration-service `service_healthy`; its
  healthcheck is `GET /api/v1/health`.
- `telegram-service` sets `DELIVERY_LATENCY_MS_MAX: "2000"` like email, for the same e2e reason.
- Service defaults for the recovery and watchdog variables are used; compose does not override
  them.

---

## 10. Testing

Same tiers and invocation rules as slice 1 (`docs/local-development.md`): repository-root tiers
plus one pytest process per service, plus the playground suite.

### 10.1 Unit — `tests/unit/`

- Give-up decision: `count >= max_retries` boundary.
- Shared delivery rules: failure marker; reserved email domains and TLDs (and near-misses such as
  `example.com.evil.org`, `notexample.com`); reserved `sim-` chat ids; rule order.
- Telegram response classification: `200`, `400`, `403`, `429`, `500`, unknown status.
- Telegram text building with and without `subject`.
- Sender selection: empty token / empty `SMTP_HOST` → simulated; set → real.

### 10.2 Integration — `tests/integration/`, testcontainers

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

### 10.3 Service suites

- **notification-service** — watchdog: stale `PROCESSING` → `FAILED` / `processing_timeout`;
  recent `PROCESSING` untouched; `COMPLETED` untouched.
- **routing-service** — `give_up` writes a `FAILED` route and a `RoutingFailed` with
  `max_retries_exceeded`, in one transaction.
- **email-service** — `give_up`; `SmtpSender` against an in-process `aiosmtpd` server: accepted,
  `5xx` refusal → `smtp_rejected`, `4xx` → transient, authentication failure → transient; a
  reserved recipient never reaches the SMTP server even with `SMTP_HOST` set; the password never
  appears in logs; `/version` reports `delivery_mode`; startup fails with `SMTP_HOST` set and
  `SMTP_FROM` missing.
- **telegram-service** — the email suite's equivalents (channel filter, `fail` rule, replay,
  unique constraint, `alembic upgrade head`); `BotApiSender` against `httpx.MockTransport` for
  `200`, `400`, `429`, timeout; a transient failure leaves nothing written and the message
  unacked; a `sim-` chat id never reaches the Bot API with a token set; the token never appears in
  logs; `/version` reports `delivery_mode`; `give_up`.
- **gateway** — each route forwards method, path, query and body; `X-Correlation-ID` generated
  when absent and propagated when present; downstream `404`/`422` passed through unchanged;
  timeout → `504`; connection refused → `502`; `/api/v1/health` does not call downstream.

### 10.4 Playground — `tools/playground/tests/`

Run with `PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests`.
Against `httpx.MockTransport`: every generated recipient is reserved; the `fail` share matches
`fail_ratio`; percentiles; the correctness verdict counts matched, wrong-state and unsettled
notifications; a `5xx` during submit is counted, not raised; `settle_timeout_s` bounds the run.

### 10.5 End-to-end — `tests/e2e/`, full stack

- Telegram (simulated): happy path `CREATED → PROCESSING → COMPLETED`; `fail` chat id →
  `FAILED` / `simulated_failure`.
- The three slice 1 outcomes re-run **through the Gateway**, plus correlation id propagation.
- Recovery for real: stop configuration-service, submit a notification, observe it stay `CREATED`,
  start configuration-service, observe `COMPLETED` within 60 s.
- `GATEWAY_URL` and `TELEGRAM_URL` join the existing env-overridable URLs.

Every e2e recipient is reserved (section 3), so the suite stays deterministic and sends nothing
real on a stack with credentials in `.env`. The slice 1 tests already use `@example.com`; the new
Telegram tests use `sim-e2e` and `sim-e2e-fail`.

Give-up (about 2 min) and the watchdog (5 min) are proven at the integration and service tiers
only; exercising them end-to-end would make the suite too slow.

Real SMTP and real Bot API delivery are not exercised by any automated test. They are covered by
the fake-server and `MockTransport` suites, and by manual use through the playground (section
12, gate 7).

---

## 11. Documentation deliverables

New ADRs, continuing from 0023:

| ADR | Decision |
|---|---|
| 0024 | A consumer that gives up publishes a failure event (D1) |
| 0025 | Recovery claims by idle time because consumer names change on restart; one shared recoverer (D3, section 2.1) |
| 0026 | Telegram `recipient` is the `chat_id`; `TELEGRAM_CHAT_ID` dropped (D2) |
| 0027 | Gateway returns `502` for an unreachable downstream (D4) |
| 0028 | The watchdog publishes no event, and terminal states never reopen after it |
| 0029 | Real email through generic SMTP, switched on by configuration (D5) |
| 0030 | Reserved recipients are always simulated (D6) |

Updated: `README.md` (quick start through the Gateway, real delivery setup with `.env`, the
reserved-recipient rule, stream topology with the live `telegram-service` group, env var table,
known limitations), `docs/architecture.md`, `docs/event-flows.md` (a give-up sequence diagram and
the telegram path), `docs/patterns.md` (recovery, max-retry, watchdog; both guarded transitions),
`docs/local-development.md` (new test invocations, the playground's two pages, gateway and
telegram debug configurations), `services/telegram-service/README.md`,
`services/email-service/README.md`, `gateway/README.md`, `.vscode/launch.json` and
`.vscode/tasks.json`.

---

## 12. Verification gates

Slice 2 is complete only when:

1. `uv run pytest tests/unit` and `uv run pytest tests/integration` pass.
2. Every service suite passes, including telegram-service and gateway.
3. The playground suite passes.
4. `docker compose up --build -d --wait` brings all twelve containers to `healthy`, with and
   without a `.env`.
5. `uv run pytest tests/e2e` passes.
6. `uv run ruff check .` and `uv run ruff format --check .` are clean.
7. Manual, through the playground: a load test of 200 notifications ends with a clean correctness
   verdict. Real delivery (one email, one Telegram message to the user's own address and chat) is
   checked by the user when credentials are available; it is not a gate the implementation can
   pass on its own.
8. The documentation in section 11 exists and matches the code.

---

## 13. Parked items carried in

- `docs/patterns.md` names only one of the two guarded-transition implementations (slice 1 ledger,
  parked). Fixed as part of this slice's documentation pass.
- The e2e `PROCESSING` observation flakiness (slice 1 ledger, parked) stays parked: it needs a
  decision between a latency floor and a shorter poll interval, and nothing in slice 2 changes it.

---

## 14. Known limitations after slice 2

Carried from slice 1 and still true: no schema registry, no dead-letter queue (a given-up event is
recorded in `processed_events` and discarded from the stream), no tracing backend, no circuit
breaker on Routing → Configuration, one consumer per group, at-least-once delivery.

No longer true: "No real SMTP".

New:

- Recovery latency: a transient failure is retried only after `PENDING_TIMEOUT_MS` of idleness;
  give-up takes about two minutes with the defaults.
- The watchdog leaves Routing Service's `routes` row at `PROCESSING` and can mark a notification
  `FAILED` whose delivery later succeeds (ADR 0028).
- Consumer names of dead containers accumulate in each group's `XINFO CONSUMERS` listing. Harmless;
  not cleaned up.
- At-least-once delivery now has a visible cost: a crash after a real send but before the commit
  sends the email or Telegram message twice. Idempotency protects the database, not the
  recipient's inbox.
- Real SMTP and Bot API delivery are not exercised by automated tests.
- Load test throughput is bounded by one consumer per group and the simulated delivery latency;
  it measures the platform as configured for teaching, not its ceiling.

---

## 15. Out of scope

API key on the Gateway, Prometheus metrics, `docs/operations.md` (all slice 3); CORS; horizontal
scaling of consumers; HTML email; attachments; a load test against real channels; a command-line
load test runner.

---

## 16. Open risks

| Risk | Handling |
|---|---|
| `XCLAIM` of an entry deleted from the stream returns nil | `claim` skips nil entries; covered by an integration test if redis-py surfaces them |
| A slow `handle` (> `PENDING_TIMEOUT_MS`) could be claimed while still running | Email and Telegram latencies and timeouts are bounded well below 30 s; idempotency and `UNIQUE (notification_id)` absorb a duplicate record — but see the duplicate-send limitation in section 14 |
| Telegram Bot API rate limits (`429`) during manual testing | Classified transient; recovery retries with the pending timeout as natural back-off |
| `aiosmtplib` / `aiosmtpd` support for Python 3.14 | Verify both resolve and import on 3.14 as the first step of Part D, as slice 1 did for `asyncpg` |
| SMTP providers that reject STARTTLS on 587 or require app passwords | `SMTP_SECURITY` covers the transport; app passwords are the user's configuration; documented in the README setup section |
| Streamlit re-running the page while a load test runs | The run happens inside the button's script run; the page states that interacting during a run restarts it |
