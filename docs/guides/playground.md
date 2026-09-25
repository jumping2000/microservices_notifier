**English** | [Italiano](playground.it.md)

# Playground guide

The playground is a Streamlit app for exercising the running platform by hand. It has two pages:
the **Console**, to send notifications and watch them move through the saga, and the **Load
test**, to fire hundreds of simulated notifications and check that every one ends in the right
state.

## Start it

The stack must be running (see [Getting started](getting-started.md)).

```bash
uv sync --all-packages --group playground     # once, or after pulling new code
uv run --group playground streamlit run tools/playground/app.py
```

Open http://localhost:8501. The two pages are in the left navigation: **app** is the Console,
**Load test** is the second page. The playground talks to the Gateway at `GATEWAY_URL` (default
`http://localhost:8000`) and to each service port for health; see
[Configuration](configuration.md#playground-and-end-to-end-tests) to point it elsewhere.

## Console

### Sidebar

- **Services** — one line per service: green when `/health` answers `UP`, with the version and
  the database/Redis checks; red with `unreachable` when the service does not answer. The Gateway
  line shows only the Gateway's own health.
- **Delivery mode** — for email and telegram: 🟢 **SIMULATED**, or 🟠 **REAL (smtp)** /
  **REAL (bot_api)** when real credentials are configured (read from each service's `/version`).
- **Channels** — one toggle per channel. Switching a toggle calls
  `PUT /api/v1/channels/{name}`; it takes effect on the next notification routed.

### Send a notification

Fill **Channel**, **Recipient** (an email address, or a Telegram chat id), **Subject** and **Body**,
then **Send**.

- If the channel is in REAL mode and the recipient is neither reserved nor carrying `fail`, the
  form stops with a warning: tick **I understand this may send a real message** and press **Send**
  again. The box clears after every send, so each real send needs its own confirmation. If the
  playground cannot read a channel's mode, it treats it as real.
- A recipient containing `fail` shows a note: it will end `FAILED` / `simulated_failure` by
  design, and nothing is sent.
- Reserved recipients (`@example.com`, `.test`, `sim-…`, see
  [Configuration](configuration.md#reserved-recipients)) are always simulated, even in REAL mode.

### Scenarios

Five buttons send a prepared notification, all to reserved recipients, so they never send anything
real:

| Button | Expected timeline |
|---|---|
| Email: happy path | `CREATED → PROCESSING → COMPLETED` |
| Email: channel disabled | Disables email, sends, `CREATED → FAILED` (`channel_disabled`), then re-enables email once the notification has settled |
| Email: delivery failure | `CREATED → PROCESSING → FAILED` (`simulated_failure`) |
| Telegram: happy path | `CREATED → PROCESSING → COMPLETED` |
| Telegram: delivery failure | `CREATED → PROCESSING → FAILED` (`simulated_failure`) |

### Saga timeline

After each send, the timeline polls the notification every half second and lists every status it
observed with the elapsed time and the `fail_reason`, until the status is final. If nothing final
arrives within 30 seconds it stops polling and says so: recovery retries a stranded message after
30 s, and the watchdog fails a notification stuck in `PROCESSING` after 5 minutes (see
[Operations](../operations.md#how-recovery-behaves)).

### Notifications table

The 50 most recent notifications, refreshed every two seconds, with filters for status and channel.

## Load test

The load test sends many notifications through the Gateway at once, follows each one to its final
state, and tells you whether every one ended where it should. **It never sends anything real**: it
generates its own recipients, all reserved — `load-N@example.com` for email and `sim-load-N` for
Telegram, with `-fail` added to the failing share — and it accepts no recipient from you.

### Parameters

| Field | Default | Meaning |
|---|---|---|
| Notifications | 200 | How many to send (1–2000) |
| Concurrency | 10 | Requests in flight at once (1–50) |
| Telegram share | 0.3 | Fraction sent to Telegram; the rest go to email |
| Failing share | 0.1 | Fraction whose recipient carries `fail` and must end `FAILED` / `simulated_failure` |
| Settle timeout (s) | 300 | How long to keep polling for final states (10–600) |

A run has two phases, each with its own progress bar: **submit** (all the POSTs) and **settle**
(polling every notification every 250 ms until it is final or the timeout expires). Do not touch the
page during a run: any interaction restarts it.

### Reading the report

- **Verdict** — green when every notification ended in its expected state: `FAILED` /
  `simulated_failure` for the failing share, `COMPLETED` for the rest. Red lists how many ended in
  the wrong state, how many never settled, and how many were rejected at submit.
- **Accepted / s** — how fast the Gateway accepted the POSTs.
- **POST latency** and **End-to-end latency** (p50, p95, max) — end-to-end runs from the POST to the
  first poll that saw a final status, so its resolution is 250 ms.
- **Outcomes** — counts per final state, e.g. `COMPLETED`, `FAILED/simulated_failure`, and
  `UNSETTLED` for notifications still not final at the timeout.
- **Submit errors** — any POST that did not return `202`, by status code or error type.
- **Chart** — completions per second over the run.

### Sizing a run

The compose stack runs **one consumer per channel**, and simulated delivery waits a random 0–2 s per
message, so each channel delivers about one message per second. The numbers measure that deliberate
teaching setup, not the code's ceiling. A reference run with the defaults: 200/200 matched, about
70 accepted/s, end-to-end p50 about 41 s and p95 about 120 s.

- The settle timeout must cover the email backlog: roughly **email share × total × 2 s**. At a
  short timeout the leftovers show as `UNSETTLED` — still queued, not lost.
- Past roughly **450 notifications** the backlog outlives `PROCESSING_TIMEOUT_MINUTES` (5 minutes):
  the watchdog fails still-queued notifications with `processing_timeout`, and the verdict turns
  red. The page warns before such a run and explains the result after it. It is the one-consumer
  stack's limit, not lost or misrouted messages.

## Sending a real message

1. Configure the channel in `.env` (see [Configuration](configuration.md#real-delivery-with-env))
   and recreate the stack.
2. Check the sidebar: the channel shows 🟠 **REAL**.
3. In **Send a notification**, enter **your own** address or chat id, tick the confirmation, and
   send.
4. Follow it in the timeline, and check your inbox or Telegram.

Remember that delivery is at-least-once: if a service crashes, or a timeout hits after the server
has already accepted the message, the same message can be sent twice.
