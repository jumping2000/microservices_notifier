**English** | [Italiano](getting-started.it.md)

# Getting started

From a fresh clone to a delivered notification in about ten minutes. Everything runs locally in
Docker; nothing is sent anywhere real until you add credentials yourself (see
[Configuration](configuration.md)).

## Prerequisites

| Tool | Version | Why |
|---|---|---|
| Docker with Compose v2 | Docker Desktop on Windows/macOS, or Docker Engine on Linux | Runs the twelve containers |
| `uv` | 0.11 or newer | Python environment for the playground and the tests |
| Git | any | To clone the repository |

You do not need a local Python install: `uv` downloads Python 3.14 on first use.

The shell examples below use Bash (Git Bash on Windows). In Windows PowerShell 5.1, `curl` is an
alias of `Invoke-WebRequest` and does not accept these arguments: call `curl.exe` instead, or use
the `Invoke-RestMethod` variant shown after each example.

## 1. Start the stack

```bash
git clone https://github.com/jumping2000/microservices_notifier.git
cd microservices_notifier
docker compose up --build -d --wait
```

The first build takes a few minutes. `--wait` returns only when every healthcheck passes. Check
the result:

```bash
docker compose ps --format "{{.Service}} {{.Status}}"
```

All twelve lines should end in `(healthy)`:

| Container | Host port | Role |
|---|---|---|
| `gateway` | 8000 | Single entry point, `/api/v1/*` |
| `notification-service` | 8001 | Owns notifications and their status |
| `routing-service` | 8002 | Decides whether and where a notification is routed |
| `configuration-service` | 8003 | Channel on/off switches |
| `email-service` | 8004 | Email delivery (simulated, or real SMTP) |
| `telegram-service` | 8005 | Telegram delivery (simulated, or real Bot API) |
| `notification-db` … `telegram-db` | 5433–5437 | One Postgres database per service |
| `redis` | 6379 | Redis Streams, the only message broker |

## 2. Send your first notification

```bash
curl -s -X POST http://localhost:8000/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel": "email", "recipient": "john@example.com", "subject": "Welcome", "body": "Hello John!"}'
```

```json
{"notification_id": "f0dee1d5-2c10-405f-8fa5-731a8e082de4", "status": "CREATED"}
```

PowerShell:

```powershell
$r = Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/notifications `
  -ContentType 'application/json' `
  -Body '{"channel":"email","recipient":"john@example.com","subject":"Welcome","body":"Hello John!"}'
$r.notification_id
```

The `202 Accepted` means the notification is stored and its `NotificationCreated` event is queued.
Delivery happens asynchronously.

## 3. Follow it to the end

Poll the notification with the id you received:

```bash
curl -s http://localhost:8000/api/v1/notifications/f0dee1d5-2c10-405f-8fa5-731a8e082de4
```

PowerShell: `Invoke-RestMethod http://localhost:8000/api/v1/notifications/$($r.notification_id)`

Repeat it a few times. The `status` moves `CREATED → PROCESSING → COMPLETED` within a few seconds.
`john@example.com` is a **reserved** address, so the email is simulated even if you configure real
SMTP later — see [Configuration](configuration.md#reserved-recipients).

Try the two failure outcomes too:

- A recipient containing `fail`, e.g. `fail@example.com`, ends `FAILED` with
  `fail_reason: "simulated_failure"`.
- Disable the channel first, then send: the notification ends `FAILED` with
  `fail_reason: "channel_disabled"` without ever reaching `PROCESSING`.

  ```bash
  curl -s -X PUT http://localhost:8000/api/v1/channels/email -H 'Content-Type: application/json' -d '{"enabled": false}'
  # ...send, poll...
  curl -s -X PUT http://localhost:8000/api/v1/channels/email -H 'Content-Type: application/json' -d '{"enabled": true}'
  ```

The [API reference](api-reference.md) lists every endpoint, field and error. Each service also
serves interactive Swagger UI at `/docs`; the Gateway's is at http://localhost:8000/docs.

## 4. Open the playground

The playground is a Streamlit console for exercising the platform without `curl`:

```bash
uv sync --all-packages --group playground
uv run --group playground streamlit run tools/playground/app.py
```

It opens at http://localhost:8501. The [playground guide](playground.md) walks through both pages.

On Windows, if `uv sync` fails with `Accesso negato` / `Access is denied` on `.venv\Scripts`, close
VS Code (its Python and Ruff language servers keep the environment open) and run it again from an
external terminal.

## 5. Stop and restart

| Goal | Command |
|---|---|
| Stop the containers, keep all data | `docker compose stop` (restart with `docker compose start`) |
| Remove the containers, keep all data | `docker compose down` |
| Wipe **all** data and start from zero | `docker compose down -v` |

Always wipe Postgres and Redis together with `down -v`, never one volume alone: see
[Operations](../operations.md#resetting-safely) for why.

## Next steps

- [Playground guide](playground.md) — the console and the load test
- [Configuration](configuration.md) — every variable, and how to send real email and Telegram messages
- [API reference](api-reference.md) — endpoints, statuses, fail reasons, errors
- [Operations](../operations.md) — inspecting streams and databases, troubleshooting
- [Architecture](../architecture.md) — how the services fit together, and why
