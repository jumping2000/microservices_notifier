# Local Development

## uv workspace

The whole platform is one uv workspace, declared at the repository root:

```toml
[tool.uv.workspace]
members = ["gateway", "services/*", "shared"]
```

`uv sync --all-packages --group playground`, run from the repository root, produces a **single root
`.venv`** containing every service, the Gateway, and the shared library (`notification-shared`)
together. That is the interpreter VS Code is configured to use (`.vscode/settings.json`) and the
environment every test tier runs in.

**If `uv sync` fails with `failed to remove directory ...\.venv\Scripts: Accesso negato (os error
5)`** on Windows, VS Code's Python and Ruff language servers are holding the venv open. Close VS
Code, rerun `uv sync` from an external terminal, then reopen VS Code. Do not delete `.venv` by hand
while VS Code is running.

**`uv pip install` does not work here.** The installed `uv` is 0.11.7, which rejects `uv pip
install` as a legacy pip-compatible interface. Use only:

- `uv add <package>` — add a dependency and update the relevant `pyproject.toml`
- `uv sync` / `uv sync --all-packages` — install/update the locked environment
- `uv run <command>` — run anything (pytest, ruff, alembic, uvicorn) inside the workspace `.venv`

Never `pip`, `venv`, `virtualenv`, `conda`, or `poetry` in this project.

## Running the tests

Eight invocations, run from the repository root (some tests reference repository-relative paths,
such as a service's `alembic.ini`):

```bash
uv run pytest tests/unit
uv run pytest tests/integration
PYTHONPATH=services/configuration-service uv run pytest services/configuration-service/tests
PYTHONPATH=services/notification-service  uv run pytest services/notification-service/tests
PYTHONPATH=services/routing-service       uv run pytest services/routing-service/tests
PYTHONPATH=services/email-service         uv run pytest services/email-service/tests
PYTHONPATH=services/telegram-service      uv run pytest services/telegram-service/tests
PYTHONPATH=gateway                        uv run pytest gateway/tests
```

Plus the playground's own suite, which needs its `playground` dependency group rather than the
default environment:

```bash
PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests
```

**Why each service needs its own process.** Every service defines a module named `app`
(`services/<name>/app/`), and the Gateway does too (`gateway/app/`). If two suites ran in one
pytest process, the second one's `import app...` would resolve to whichever `app` package Python
imported first — so each runs as its own process, with `PYTHONPATH` pointed at that one directory.
The two repository-root tiers (`tests/unit`, `tests/integration`) need no `PYTHONPATH` override
because they only import `notification_shared`, which is on the workspace `.venv`'s path
regardless.

`tests/unit` needs no I/O. `tests/integration` and every `services/*/tests` suite need Docker
running — they start real Postgres and Redis containers via testcontainers. The gateway suite does
not: it has no database and no Redis, so `httpx.MockTransport` is enough to stand in for the
downstream services. `tests/e2e` needs the full stack already up (see below).

`.vscode/tasks.json` wraps these commands (plus lint and the compose lifecycle) as VS Code tasks;
"test: integration (all services)" is the one that loops over the service suites in one task, and
it uses POSIX shell syntax — on Windows it is pinned to `bash.exe` via that task's
`options.shell.executable`.

## Debugging a service from VS Code

`.vscode/launch.json` has one Uvicorn debug configuration per service, each pointed at the matching
**containerized** Postgres and Redis on their published localhost ports rather than the
in-container hostnames used by `docker-compose.yml`. To use one:

1. Start infrastructure only, not the application containers:

   ```bash
   docker compose up -d redis notification-db routing-db configuration-db email-db telegram-db
   ```

   (This is the "compose up (infra only)" task.)

2. **Apply migrations by hand.** A service launched this way runs Uvicorn directly through the
   debugger, so the container `entrypoint.sh` (which normally runs `alembic upgrade head` before
   Uvicorn starts) never runs. Apply migrations yourself first, with `DATABASE_URL` set to the same
   localhost port the launch configuration uses:

   ```bash
   uv run alembic -c services/notification-service/alembic.ini upgrade head
   ```

   (substitute the service name and its own `alembic.ini`; telegram-service applies the same way.
   The Gateway has no database and no migrations.)

3. Launch the service from the Run and Debug panel (e.g. "notification-service"). It listens on
   its host port (`8000` gateway, `8001` notification, `8002` routing, `8003` configuration, `8004`
   email, `8005` telegram) with `--reload` enabled.

**The launch configuration does not run Alembic.** This is the one thing to remember: if a service
fails to start with a "relation does not exist" error under the debugger, migrations were not
applied to that port's database first.

## Playground

`tools/playground/` is the platform's manual test console, two Streamlit pages. Its dependencies
live in their own `playground` dependency group, so the test environment does not carry them:

```bash
docker compose up --build -d --wait
uv run --group playground streamlit run tools/playground/app.py   # http://localhost:8501
```

**Console** (`app.py`) drives the running stack by hand: per-service health and delivery mode
badges, the channel toggles, a send form, one-click buttons for the saga scenarios on both
channels, a live status timeline for the last notification sent, and a filterable notification
table. Notification and channel calls go through the Gateway (`GATEWAY_URL`, default
`http://localhost:8000`); health and `/version` stay per service, because the Gateway does not
aggregate them — it reads the same `NOTIFICATION_URL` / `ROUTING_URL` / `CONFIGURATION_URL` /
`EMAIL_URL` / `TELEGRAM_URL` variables as `tests/e2e`. When the selected channel's configured mode
is real (SMTP or the Bot API) and the recipient is not reserved, the form warns that this will send
a real message and requires an explicit confirmation checkbox before it submits. The scenario
buttons and every README/e2e example use reserved recipients, so they always stay simulated.

**Load test** (`pages/2_Load_test.py`), backed by `loadtest.py` (no Streamlit import, so it is
independently testable): submits a configurable number of notifications through the Gateway at a
bounded concurrency, then polls each to a terminal state, and reports accepted/s, latency
percentiles, and a correctness verdict — how many notifications ended in the state their recipient
implies, how many ended in the wrong state, how many never settled. Every recipient it generates is
reserved (`load-{i}@example.com`, `sim-load-{i}`) — the load test can never send a real message,
even against a stack with real credentials configured, because it never accepts a caller-supplied
recipient. The page's own default settle timeout is 300s (the engine's own `LoadTestConfig` default
is 120s); a large run needs a settle timeout of roughly `email share × total × 2s`, since compose
runs one consumer per group and simulated delivery sleeps up to 2s.

## Resetting

```bash
docker compose down -v
```

resets **Postgres and Redis together** — every named Postgres volume and Redis's AOF-backed data
volume. This is required, not optional, and doing one without the other causes a replay: consumer
groups are created at offset `0` (ADR 0002), so a group created against a wiped Postgres database
but a surviving Redis stream replays that stream's entire history against empty
`processed_events`/domain tables. Every idempotency check starts from zero, and every notification,
route, and delivery ever created since the stream began gets reprocessed. Always reset both stores
in the same `docker compose down -v`, never `docker volume rm` on just one.
