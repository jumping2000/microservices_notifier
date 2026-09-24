# Notification Platform — Slice 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close slice 1's "stuck forever" gap with idle-claim recovery, max-retry give-up and a watchdog; add Telegram Service and the API Gateway; make real SMTP and Telegram delivery possible without endangering tests; and turn the playground into a manual test console with a simulated load test.

**Architecture:** A shared `PendingRecoverer` (like `OutboxPublisher`) claims idle pending entries with `XPENDING IDLE` + `XCLAIM`, re-runs each consumer's public `handle`, counts failures in `processed_events`, and on the last attempt calls the consumer's `give_up`, which writes a failure event through the outbox. Delivery services choose a sender (simulated or real) once at startup; shared rules in `notification_shared/delivery.py` force the failure marker and reserved recipients to stay simulated. The Gateway is a FastAPI reverse proxy with explicit routes over one `httpx.AsyncClient`.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.x async + asyncpg, Alembic, PostgreSQL 16, Redis 7 Streams, redis-py 8.x async, Pydantic v2, pydantic-settings, HTTPX, aiosmtplib (new), aiosmtpd (new, dev only), Streamlit (playground), uv workspace, Docker Compose, pytest + pytest-asyncio + testcontainers, Ruff.

**Spec:** `docs/superpowers/specs/2026-09-24-notification-platform-slice-2-design.md` (binding), on top of `docs/superpowers/specs/2026-09-12-notification-platform-design.md`. Read both before Task 1. `docs/superpowers/slice-1-execution-ledger.md` records the rulings slice 1 took; they still hold.

## Global Constraints

- Python **3.14** everywhere; `requires-python = ">=3.14"`; Docker base `python:3.14-slim`.
- `uv` **exclusively**. Installed uv is **0.11.7**, which rejects `uv pip install`. Use `uv add` (`--package <member>` for a workspace member, `--group dev` / `--group playground` at the root), `uv lock`, `uv sync --all-packages`, `uv run`.
- **Before Task 1:** run `uv sync --all-packages --group playground` from the repository root. If it fails with `failed to remove directory ...\.venv\Scripts: Accesso negato (os error 5)`, VS Code's Python/Ruff language servers hold the venv: close VS Code, rerun from an external terminal, reopen. Do not delete `.venv` by hand while VS Code runs.
- Commands in this plan are **bash** (Git Bash on Windows). Service suites run one process per service: `PYTHONPATH=services/<name> uv run pytest services/<name>/tests -v`. The gateway: `PYTHONPATH=gateway uv run pytest gateway/tests -v`. The playground: `PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests -v`.
- `tests/integration` and every `services/*/tests` suite need **Docker running** (testcontainers).
- **Redis Streams only.** Every `XADD` writes one field, `envelope`. Consumer groups start at offset `0`.
- Consumer transactional rule (slice 1 spec 5.4) is unchanged: the domain write, any outbox row and the idempotency write commit together; `XACK` only after that commit. `give_up` + `mark_failed_permanent` commit together, then `XACK`.
- Layering: route handlers → service layer → repositories; only repositories contain `select`/`insert`/`update`. Background tasks start in `lifespan`.
- Each service keeps its own `DeclarativeBase` (slice 1 spec 3.12). No shared `Base`.
- **Tests that construct a `Settings` object pass `_env_file=None`**, so a developer's `.env` with real credentials can never leak into a test run.
- **Secrets are never logged:** `TELEGRAM_BOT_TOKEN`, `SMTP_PASSWORD`. Exceptions raised from HTTP errors that could carry the Bot API URL are raised `from None`.
- **Every recipient** in `tests/e2e`, in playground scenarios and in the load test is **reserved** (spec section 3): `@example.com`/`.org`/`.net`, TLDs `.example`/`.invalid`/`.test`, or a `sim-` chat id.
- Give-up reason string is exactly `max_retries_exceeded`; watchdog reason `processing_timeout`; Telegram permanent reason `telegram_rejected`; SMTP permanent reason `smtp_rejected`; failure marker reason `simulated_failure`.
- Defaults: `PENDING_TIMEOUT_MS=30000`, `PENDING_MAX_RETRIES=3`, `RECOVERY_POLL_INTERVAL_MS=5000`, `PROCESSING_TIMEOUT_MINUTES=5`, `WATCHDOG_INTERVAL_SECONDS=60`, `GATEWAY_TIMEOUT_SECONDS=10.0`, `SMTP_PORT=587`, `SMTP_SECURITY=starttls`.
- Ruff: before every commit run `uv run ruff check --fix <changed files>` (import order only is a non-deviation, per the slice 1 ledger) and `uv run ruff format <changed files>`; `uv run ruff check .` and `uv run ruff format --check .` must be clean.
- Commit after every task, Conventional Commits, ending with the trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Deviation from spec 10.1, by necessity:** the Telegram response classification, text building and sender-selection tests, and email's sender-selection tests, live in their service suites rather than `tests/unit`, because `tests/unit` cannot import a service's `app` package (one `app` per process — `docs/local-development.md`). They are pure and need no Docker; only the shared rules (`tests/unit/test_delivery.py`) and the give-up policy stay in `tests/unit`.
- Capture the RED run of every test step with `2>&1 | tee /tmp/task-N-red.txt` before writing the implementation (slice 1 ledger, Task 3/4 method rulings). Never delete implementation files to manufacture red.

## Review Focus

Inputs the spec implies but no test would naturally exercise; each line has its pinning test in the owning task.

1. **A real address whose text contains `fail`** (e.g. `failla@gmail.com`, `-100fail` chat id) — the failure marker still wins over real delivery; the playground must say so before sending. Pinned in Task 1 (`test_the_failure_marker_is_checked_on_real_looking_addresses`) and Task 7 (`test_the_failure_marker_precedes_the_real_sender`); surfaced in Task 13's send form.
2. **Reserved email written with odd casing or a trailing dot** (`John@Example.COM.`, `x@mail.example.com`) must still count as reserved, while look-alikes (`x@example.com.evil.org`, `x@notexample.com`) must not. Pinned in Task 1.
3. **Non-ASCII subject or body** (`Città`, emoji) through SMTP must arrive intact. Pinned in Task 7 (`test_a_non_ascii_subject_and_body_arrive_intact`).
4. **Load test against a stack that is down** — every POST fails at transport level; the run must finish with a report of submit errors, not crash the page. Pinned in Task 12 (`test_an_unreachable_gateway_is_reported_not_raised`).
5. **Gateway pass-through of repeated query parameters and a non-JSON downstream error** (`?status=FAILED&limit=5`, a `500 text/plain`) — forwarded and returned unchanged. Pinned in Task 10.

---

## File Structure

```
shared/notification_shared/
  delivery.py            NEW  failure marker + reserved-recipient rules (both channels)
  recovery.py            NEW  PendingRecoverer, RecoverableConsumer, should_give_up, MAX_RETRIES_EXCEEDED
  streams.py             MOD  PendingEntry, ClaimedMessage, get_pending(), claim()
  idempotency.py         MOD  mark_failed_permanent()
  exceptions.py          MOD  GATEWAY_TIMEOUT, BAD_GATEWAY, GatewayTimeoutError, BadGatewayError
  config.py              MOD  ConsumerServiceSettings (recovery settings)

services/notification-service/app/
  workers/routed_consumer.py, results_consumer.py   MOD  handle(), give_up()
  workers/watchdog.py                              NEW  Watchdog
  repositories/notification.py                     MOD  fail_stale_processing()
  core/config.py, main.py                          MOD  recovery + watchdog wiring

services/routing-service/app/
  workers/notification_consumer.py, results_consumer.py   MOD  handle(), give_up()
  core/config.py, main.py                                 MOD  recovery wiring

services/email-service/app/
  senders.py              NEW  EmailSender, SimulatedSender, SmtpSender, build_sender, delivery_mode
  workers/routed_consumer.py  MOD  handle(), give_up(), shared rules, injected sender
  api/v1/endpoints/system.py  MOD  /version reports delivery_mode
  core/config.py, main.py     MOD  SMTP settings + validation, recovery wiring

services/telegram-service/   NEW  mirrors email-service; senders.py has SimulatedSender + BotApiSender

gateway/                     NEW  FastAPI reverse proxy, explicit /api/v1 routes

tools/playground/
  app.py                  REWRITE  Console page through the Gateway
  loadtest.py             NEW      load test engine (no Streamlit import)
  pages/2_Load_test.py    NEW      load test page
  tests/test_loadtest.py  NEW

docker-compose.yml, .env.example, .vscode/launch.json, .vscode/tasks.json   MOD/NEW
tests/unit/test_delivery.py, test_recovery_policy.py                        NEW
tests/integration/test_recovery.py                                          NEW
tests/e2e/conftest.py MOD, tests/e2e/test_slice2.py NEW
docs: README.md, architecture.md, event-flows.md, patterns.md, local-development.md,
      services/*/README.md, gateway/README.md, docs/adr/0024..0030            MOD/NEW
```

---

## Task 1: Shared delivery rules and gateway error codes

**Files:**
- Create: `shared/notification_shared/delivery.py`
- Modify: `shared/notification_shared/exceptions.py`
- Test: `tests/unit/test_delivery.py` (new), `tests/unit/test_exceptions.py` (append)

**Interfaces:**
- Consumes: `notification_shared.events.Channel`.
- Produces:
  - `FAILURE_MARKER = "fail"`, `SIMULATED_FAILURE = "simulated_failure"`, `RESERVED_CHAT_PREFIX = "sim-"`
  - `is_failure_recipient(recipient: str) -> bool`
  - `is_reserved_recipient(channel: str, recipient: str) -> bool`
  - `ErrorCode.GATEWAY_TIMEOUT`, `ErrorCode.BAD_GATEWAY`
  - `GatewayTimeoutError(message: str)` (status 504), `BadGatewayError(message: str)` (status 502), both `ServiceError` subclasses

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_delivery.py`:

```python
import pytest
from notification_shared.delivery import (
    FAILURE_MARKER,
    SIMULATED_FAILURE,
    is_failure_recipient,
    is_reserved_recipient,
)


def test_the_constants_are_the_documented_strings():
    assert FAILURE_MARKER == "fail"
    assert SIMULATED_FAILURE == "simulated_failure"


@pytest.mark.parametrize(
    "recipient", ["fail@example.com", "FAILURE@example.com", "sim-x-fail", "a-Fail-b"]
)
def test_the_failure_marker_is_case_insensitive(recipient):
    assert is_failure_recipient(recipient) is True


def test_the_failure_marker_is_checked_on_real_looking_addresses():
    """Review Focus 1: the marker wins even on an address that looks real.

    This is slice 1 spec 3.8 applied as written. The playground warns about
    it; this test pins that the rule is not quietly narrowed to reserved
    recipients.
    """
    assert is_failure_recipient("failla@gmail.com") is True
    assert is_failure_recipient("-100fail42") is True


@pytest.mark.parametrize("recipient", ["john@example.com", "12345", "sim-ok"])
def test_recipients_without_the_marker_do_not_fail(recipient):
    assert is_failure_recipient(recipient) is False


@pytest.mark.parametrize(
    "recipient",
    [
        "john@example.com",
        "john@example.org",
        "john@example.net",
        "John@Example.COM",
        "john@example.com.",
        "x@mail.example.com",
        "x@anything.test",
        "x@anything.invalid",
        "x@host.example",
    ],
)
def test_reserved_email_recipients(recipient):
    """Review Focus 2: casing, trailing dot and subdomains still count."""
    assert is_reserved_recipient("email", recipient) is True


@pytest.mark.parametrize(
    "recipient",
    [
        "john@gmail.com",
        "x@example.com.evil.org",
        "x@notexample.com",
        "x@example.co",
        "no-at-sign-example.com",
        "x@testing.com",
    ],
)
def test_look_alike_email_recipients_are_not_reserved(recipient):
    assert is_reserved_recipient("email", recipient) is False


@pytest.mark.parametrize("chat_id", ["sim-e2e", "sim-load-7-fail", "sim-"])
def test_reserved_chat_ids(chat_id):
    assert is_reserved_recipient("telegram", chat_id) is True


@pytest.mark.parametrize("chat_id", ["123456789", "-1001234567890", "@mychannel", "SIM-x", "xsim-"])
def test_real_chat_ids_are_not_reserved(chat_id):
    assert is_reserved_recipient("telegram", chat_id) is False


def test_the_rules_are_per_channel():
    assert is_reserved_recipient("telegram", "john@example.com") is False
    assert is_reserved_recipient("email", "sim-e2e") is False
    assert is_reserved_recipient("carrier-pigeon", "john@example.com") is False
```

Append to `tests/unit/test_exceptions.py`:

```python
def test_gateway_errors_carry_their_status_and_code():
    from notification_shared.exceptions import BadGatewayError, GatewayTimeoutError

    timeout = GatewayTimeoutError("GET /notifications timed out")
    assert timeout.status_code == 504
    assert timeout.to_response().model_dump(mode="json") == {
        "error": {"code": "GATEWAY_TIMEOUT", "message": "GET /notifications timed out"}
    }

    bad = BadGatewayError("downstream unreachable")
    assert bad.status_code == 502
    assert bad.to_response().error.code == ErrorCode.BAD_GATEWAY
    assert issubclass(GatewayTimeoutError, ServiceError)
    assert issubclass(BadGatewayError, ServiceError)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_delivery.py tests/unit/test_exceptions.py -v 2>&1 | tee /tmp/task-1-red.txt`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.delivery'` and `ImportError: cannot import name 'BadGatewayError'`.

- [ ] **Step 3: Write the implementation**

Create `shared/notification_shared/delivery.py`:

```python
"""Delivery rules shared by every delivery service.

Both rules run before any network call, in this order, so email-service and
telegram-service cannot drift apart:

1. A recipient containing "fail" fails deterministically (slice 1 spec 3.8).
2. A reserved recipient is always delivered by the simulated sender, even when
   real delivery is configured (slice 2 spec section 3, ADR 0030). That is what
   lets e2e tests and load tests run on a stack with real credentials.
"""

from __future__ import annotations

from notification_shared.events import Channel

FAILURE_MARKER = "fail"
SIMULATED_FAILURE = "simulated_failure"

# RFC 2606: names reserved for documentation and testing. Nothing sent to them
# can reach a real inbox, so they are safe to use against real senders.
RESERVED_EMAIL_DOMAINS = ("example.com", "example.org", "example.net")
RESERVED_TOP_LEVEL_DOMAINS = ("example", "invalid", "test")
RESERVED_CHAT_PREFIX = "sim-"


def is_failure_recipient(recipient: str) -> bool:
    return FAILURE_MARKER in recipient.lower()


def is_reserved_recipient(channel: str, recipient: str) -> bool:
    if channel == Channel.TELEGRAM:
        return recipient.startswith(RESERVED_CHAT_PREFIX)
    if channel == Channel.EMAIL:
        return _is_reserved_email(recipient)
    return False


def _is_reserved_email(recipient: str) -> bool:
    _, at, domain = recipient.rpartition("@")
    if not at:
        return False
    domain = domain.strip().rstrip(".").lower()
    if domain.rsplit(".", 1)[-1] in RESERVED_TOP_LEVEL_DOMAINS:
        return True
    return any(
        domain == reserved or domain.endswith(f".{reserved}") for reserved in RESERVED_EMAIL_DOMAINS
    )
```

In `shared/notification_shared/exceptions.py`, add two members at the end of `ErrorCode`:

```python
    GATEWAY_TIMEOUT = "GATEWAY_TIMEOUT"
    BAD_GATEWAY = "BAD_GATEWAY"
```

and append at the end of the file:

```python
class GatewayTimeoutError(ServiceError):
    status_code = 504

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.GATEWAY_TIMEOUT, message)


class BadGatewayError(ServiceError):
    """A downstream service could not be reached at all. ADR 0027."""

    status_code = 502

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.BAD_GATEWAY, message)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit -v`
Expected: PASS, including every pre-existing unit test.

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix shared tests/unit && uv run ruff format shared tests/unit
git add shared/notification_shared/delivery.py shared/notification_shared/exceptions.py tests/unit/test_delivery.py tests/unit/test_exceptions.py
git commit -m "feat(shared): add delivery rules and gateway error codes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 2: Pending entries, claiming, and FAILED_PERMANENT

**Files:**
- Modify: `shared/notification_shared/streams.py`, `shared/notification_shared/idempotency.py`
- Test: `tests/integration/test_streams.py` (append), `tests/integration/test_idempotency.py` (append)

**Interfaces:**
- Consumes: `EventEnvelope.from_redis` (raises `ValueError`, and pydantic's `ValidationError`, which subclasses `ValueError`).
- Produces:
  - `PendingEntry(NamedTuple)`: `message_id: str`, `consumer: str`, `idle_ms: int`, `times_delivered: int`
  - `ClaimedMessage(NamedTuple)`: `message_id: str`, `envelope: EventEnvelope | None`
  - `RedisStreamConsumer.get_pending(stream: str, group: str, min_idle_ms: int, count: int = 10) -> list[PendingEntry]`
  - `RedisStreamConsumer.claim(stream: str, group: str, min_idle_ms: int, message_ids: list[str]) -> list[ClaimedMessage]`
  - `IdempotencyRepository.mark_failed_permanent(session, event_id: UUID, consumer_group: str) -> None`

- [ ] **Step 1: Write the failing tests**

Append to `tests/integration/test_streams.py` (add `import asyncio` to the imports at the top):

```python
async def _strand(redis_client, envelope: EventEnvelope | None = None) -> RedisStreamConsumer:
    """Publish one message and let a consumer named "dead" read it without acking."""
    dead = RedisStreamConsumer(redis_client, consumer_name="dead")
    await dead.ensure_group(STREAM, GROUP)
    await RedisStreamPublisher(redis_client).publish(STREAM, envelope or _envelope())
    await dead.read(STREAM, GROUP, block_ms=100)
    return dead


async def test_get_pending_skips_entries_that_are_not_idle_long_enough(redis_client):
    dead = await _strand(redis_client)
    assert await dead.get_pending(STREAM, GROUP, min_idle_ms=60_000) == []


async def test_get_pending_reports_idle_entries_with_their_owner(redis_client):
    dead = await _strand(redis_client)
    await asyncio.sleep(0.05)

    pending = await dead.get_pending(STREAM, GROUP, min_idle_ms=20)

    assert len(pending) == 1
    assert pending[0].consumer == "dead"
    assert pending[0].times_delivered == 1
    assert pending[0].idle_ms >= 20
    assert isinstance(pending[0].message_id, str)


async def test_claim_moves_an_idle_entry_to_the_claiming_consumer(redis_client):
    """Spec 2.1: consumer names change on restart, so recovery must claim."""
    published = _envelope("corr-claim")
    dead = await _strand(redis_client, published)
    await asyncio.sleep(0.05)
    message_id = (await dead.get_pending(STREAM, GROUP, min_idle_ms=20))[0].message_id

    alive = RedisStreamConsumer(redis_client, consumer_name="alive")
    claimed = await alive.claim(STREAM, GROUP, min_idle_ms=20, message_ids=[message_id])

    assert claimed == [(message_id, published)]
    owner = await redis_client.xpending_range(str(STREAM), str(GROUP), "-", "+", 10)
    assert owner[0]["consumer"] in (b"alive", "alive")


async def test_claim_does_not_steal_an_entry_that_is_not_idle_enough(redis_client):
    dead = await _strand(redis_client)
    message_id = (await dead.get_pending(STREAM, GROUP, min_idle_ms=0))[0].message_id

    alive = RedisStreamConsumer(redis_client, consumer_name="alive")
    assert await alive.claim(STREAM, GROUP, min_idle_ms=60_000, message_ids=[message_id]) == []


async def test_claim_returns_none_for_an_unparseable_entry(redis_client):
    dead = RedisStreamConsumer(redis_client, consumer_name="dead")
    await dead.ensure_group(STREAM, GROUP)
    await redis_client.xadd(str(STREAM), {"garbage": "not an envelope"})
    with pytest.raises(ValueError):
        await dead.read(STREAM, GROUP, block_ms=100)
    await asyncio.sleep(0.05)
    message_id = (await dead.get_pending(STREAM, GROUP, min_idle_ms=20))[0].message_id

    claimed = await RedisStreamConsumer(redis_client, "alive").claim(
        STREAM, GROUP, min_idle_ms=20, message_ids=[message_id]
    )

    assert claimed == [(message_id, None)]


async def test_claim_skips_an_entry_deleted_from_the_stream(redis_client):
    dead = await _strand(redis_client)
    await asyncio.sleep(0.05)
    message_id = (await dead.get_pending(STREAM, GROUP, min_idle_ms=20))[0].message_id
    await redis_client.xdel(str(STREAM), message_id)

    claimed = await RedisStreamConsumer(redis_client, "alive").claim(
        STREAM, GROUP, min_idle_ms=20, message_ids=[message_id]
    )

    assert claimed == []


async def test_claim_with_no_ids_does_not_call_redis(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="alive")
    assert await consumer.claim(STREAM, GROUP, min_idle_ms=0, message_ids=[]) == []
```

Append to `tests/integration/test_idempotency.py`:

```python
async def _row(sessions, event_id):
    async with sessions() as session:
        return (
            await session.execute(
                select(ProcessedEvent.status, ProcessedEvent.fail_count).where(
                    ProcessedEvent.event_id == event_id
                )
            )
        ).one()


async def test_mark_failed_permanent_counts_as_processed(sessions):
    """A given-up event must be skipped on replay (spec 2.4)."""
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.mark_failed_permanent(session, event_id, GROUP)
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, GROUP) is True
    assert (await _row(sessions, event_id)).status == ProcessedStatus.FAILED_PERMANENT


async def test_mark_failed_permanent_over_a_failing_row_keeps_the_count(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        for _ in range(3):
            await repo.increment_fail_count(session, event_id, GROUP)
        await repo.mark_failed_permanent(session, event_id, GROUP)
        await session.commit()

    row = await _row(sessions, event_id)
    assert row.status == ProcessedStatus.FAILED_PERMANENT
    assert row.fail_count == 3


async def test_mark_processed_over_a_failing_row_keeps_the_count(sessions):
    """Deliberate (spec 2.4): fail_count is the history of attempts."""
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.increment_fail_count(session, event_id, GROUP)
        await repo.increment_fail_count(session, event_id, GROUP)
        await repo.mark_processed(session, event_id, GROUP)
        await session.commit()

    row = await _row(sessions, event_id)
    assert row.status == ProcessedStatus.PROCESSED
    assert row.fail_count == 2
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/integration/test_streams.py tests/integration/test_idempotency.py -v 2>&1 | tee /tmp/task-2-red.txt`
Expected: FAIL — `AttributeError: 'RedisStreamConsumer' object has no attribute 'get_pending'` / `'claim'`, and `AttributeError: ... 'mark_failed_permanent'`. Pre-existing tests still pass.

- [ ] **Step 3: Write the implementation**

In `shared/notification_shared/streams.py`, replace the module docstring's last sentence block so it reads:

```python
"""Redis Streams access.

Groups are always created at offset 0 so that an event published before its
consumer started is still delivered. Replay is safe because every consumer
checks the idempotency ledger. See spec correction 3.2.

`get_pending` and `claim` exist for `PendingRecoverer`: consumer names are
container hostnames and change on restart, so a crashed consumer's pending
entries can only be recovered by claiming them by idle time. Slice 2 spec 2.1.
"""
```

After `class StreamMessage`, add:

```python
class PendingEntry(NamedTuple):
    message_id: str
    consumer: str
    idle_ms: int
    times_delivered: int


class ClaimedMessage(NamedTuple):
    message_id: str
    # None when the entry cannot be parsed; the recoverer acks and discards it.
    envelope: EventEnvelope | None
```

Append these methods to `RedisStreamConsumer`:

```python
    async def get_pending(
        self, stream: str, group: str, min_idle_ms: int, count: int = 10
    ) -> list[PendingEntry]:
        """XPENDING with an IDLE filter. Slice 1 spec correction 3.5."""
        rows = await self._redis.xpending_range(
            str(stream), str(group), min="-", max="+", count=count, idle=min_idle_ms
        )
        return [
            PendingEntry(
                message_id=_as_str(row["message_id"]),
                consumer=_as_str(row["consumer"]),
                idle_ms=int(row["time_since_delivered"]),
                times_delivered=int(row["times_delivered"]),
            )
            for row in rows
        ]

    async def claim(
        self, stream: str, group: str, min_idle_ms: int, message_ids: list[str]
    ) -> list[ClaimedMessage]:
        """XCLAIM to this consumer. The same min_idle_ms as get_pending means an
        entry another consumer picked up in the meantime is not stolen."""
        if not message_ids:
            return []
        entries = await self._redis.xclaim(
            str(stream),
            str(group),
            self._consumer_name,
            min_idle_time=min_idle_ms,
            message_ids=message_ids,
        )
        claimed: list[ClaimedMessage] = []
        for message_id, fields in entries:
            if message_id is None or not fields:
                # Deleted from the stream since it was delivered.
                continue
            try:
                envelope = EventEnvelope.from_redis(fields)
            except ValueError:
                envelope = None
            claimed.append(ClaimedMessage(_as_str(message_id), envelope))
        return claimed
```

In `shared/notification_shared/idempotency.py`, update the module docstring and append the method:

```python
"""Idempotency ledger access.

All three writers upsert, because each may find a FAILING row left by an
earlier attempt at the same event. None of them resets `fail_count`: it is the
history of how many attempts an event needed (slice 2 spec 2.4).
"""
```

```python
    async def mark_failed_permanent(
        self, session: AsyncSession, event_id: UUID, consumer_group: str
    ) -> None:
        """The recoverer gave up on this event. `is_processed` treats it as
        terminal, so a replay is skipped rather than retried forever."""
        stmt = (
            pg_insert(self.model)
            .values(
                id=uuid4(),
                event_id=event_id,
                consumer_group=str(consumer_group),
                status=ProcessedStatus.FAILED_PERMANENT.value,
                fail_count=0,
            )
            .on_conflict_do_update(
                index_elements=["event_id", "consumer_group"],
                set_={"status": ProcessedStatus.FAILED_PERMANENT.value},
            )
        )
        await session.execute(stmt)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/integration -v`
Expected: PASS. If `test_claim_skips_an_entry_deleted_from_the_stream` fails because redis-py surfaces the deleted entry differently, print `entries` in `claim`, adjust the skip condition to that shape, and record the observed shape in the task report (spec 16, first risk).

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix shared tests/integration && uv run ruff format shared tests/integration
git add shared/notification_shared/streams.py shared/notification_shared/idempotency.py tests/integration/test_streams.py tests/integration/test_idempotency.py
git commit -m "feat(shared): add XPENDING IDLE, XCLAIM and FAILED_PERMANENT

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 3: PendingRecoverer

**Files:**
- Create: `shared/notification_shared/recovery.py`
- Modify: `shared/notification_shared/config.py`
- Test: `tests/unit/test_recovery_policy.py` (new), `tests/integration/test_recovery.py` (new)

**Interfaces:**
- Consumes: `RedisStreamConsumer.get_pending/claim/ack`, `IdempotencyRepository.increment_fail_count/mark_failed_permanent` (Task 2).
- Produces:
  - `MAX_RETRIES_EXCEEDED = "max_retries_exceeded"`
  - `should_give_up(fail_count: int, max_retries: int) -> bool`
  - `RecoverableConsumer(Protocol)`: `async handle(envelope: EventEnvelope) -> bool`; `async give_up(session: AsyncSession, envelope: EventEnvelope) -> None`
  - `PendingRecoverer(*, redis, consumer_name: str, session_factory, idempotency: IdempotencyRepository, pending_timeout_ms: int = 30000, max_retries: int = 3, poll_interval_ms: int = 5000, batch_size: int = 10)` with `register(stream: str, group: str, consumer: RecoverableConsumer) -> None`, `async recover_once() -> int` (entries acked), `async run_forever() -> None`
  - `ConsumerServiceSettings(BaseServiceSettings)`: `pending_timeout_ms: int = 30000`, `pending_max_retries: int = 3`, `recovery_poll_interval_ms: int = 5000`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_recovery_policy.py`:

```python
from notification_shared.config import ConsumerServiceSettings
from notification_shared.recovery import MAX_RETRIES_EXCEEDED, should_give_up


def test_the_give_up_reason_is_the_documented_string():
    assert MAX_RETRIES_EXCEEDED == "max_retries_exceeded"


def test_should_give_up_at_the_limit_and_not_before():
    assert should_give_up(fail_count=2, max_retries=3) is False
    assert should_give_up(fail_count=3, max_retries=3) is True
    assert should_give_up(fail_count=4, max_retries=3) is True


def test_consumer_service_settings_defaults():
    settings = ConsumerServiceSettings(service_name="x", _env_file=None)
    assert settings.pending_timeout_ms == 30000
    assert settings.pending_max_retries == 3
    assert settings.recovery_poll_interval_ms == 5000
```

Create `tests/integration/test_recovery.py`:

```python
import asyncio
from uuid import UUID, uuid4

import pytest
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.idempotency import IdempotencyRepository, ProcessedStatus
from notification_shared.models import ProcessedEventMixin
from notification_shared.recovery import PendingRecoverer
from notification_shared.streams import RedisStreamConsumer, RedisStreamPublisher
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

pytestmark = pytest.mark.integration

STREAM = Stream.NOTIFICATION_ROUTED
GROUP = ConsumerGroup.EMAIL
IDLE_MS = 20


class Base(DeclarativeBase):
    pass


class ProcessedEvent(Base, ProcessedEventMixin):
    __tablename__ = "processed_events"


class GiveUpMarker(Base):
    """Written by FakeConsumer.give_up, to prove it shares the transaction."""

    __tablename__ = "give_up_markers"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)


class FakeConsumer:
    """Scripted outcomes: True acks, False or an exception fails the attempt."""

    def __init__(self, outcomes=(), give_up_error: Exception | None = None) -> None:
        self.outcomes = list(outcomes)
        self.give_up_error = give_up_error
        self.handled: list[UUID] = []
        self.given_up: list[UUID] = []

    async def handle(self, envelope: EventEnvelope) -> bool:
        self.handled.append(envelope.event_id)
        outcome = self.outcomes.pop(0) if self.outcomes else True
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def give_up(self, session, envelope: EventEnvelope) -> None:
        session.add(GiveUpMarker(event_id=envelope.event_id))
        await session.flush()
        if self.give_up_error:
            raise self.give_up_error
        self.given_up.append(envelope.event_id)


def _envelope() -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "recipient": "john@example.com", "body": "Hi"},
        correlation_id="corr-recovery",
    )


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


def _recoverer(redis_client, sessions, *, pending_timeout_ms=IDLE_MS, max_retries=3):
    return PendingRecoverer(
        redis=redis_client,
        consumer_name="recovery",
        session_factory=sessions,
        idempotency=IdempotencyRepository(ProcessedEvent),
        pending_timeout_ms=pending_timeout_ms,
        max_retries=max_retries,
        poll_interval_ms=10,
    )


async def _strand(redis_client, stream=STREAM, group=GROUP) -> EventEnvelope:
    """A consumer that then "dies": it read the message and never acked it."""
    envelope = _envelope()
    dead = RedisStreamConsumer(redis_client, consumer_name="dead")
    await dead.ensure_group(stream, group)
    await RedisStreamPublisher(redis_client).publish(stream, envelope)
    await dead.read(stream, group, block_ms=100)
    await asyncio.sleep(IDLE_MS * 2 / 1000)
    return envelope


async def _pending_count(redis_client, stream=STREAM, group=GROUP) -> int:
    return (await redis_client.xpending(str(stream), str(group)))["pending"]


async def _ledger(sessions, event_id):
    async with sessions() as session:
        return (
            await session.execute(
                select(ProcessedEvent.status, ProcessedEvent.fail_count).where(
                    ProcessedEvent.event_id == event_id
                )
            )
        ).one_or_none()


async def _markers(sessions) -> int:
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(GiveUpMarker))


async def test_a_stranded_message_is_claimed_and_completed(redis_client, sessions):
    envelope = await _strand(redis_client)
    consumer = FakeConsumer([True])
    recoverer = _recoverer(redis_client, sessions)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 1

    assert consumer.handled == [envelope.event_id]
    assert await _pending_count(redis_client) == 0


async def test_a_message_that_is_not_idle_yet_is_left_alone(redis_client, sessions):
    await _strand(redis_client)
    consumer = FakeConsumer([True])
    recoverer = _recoverer(redis_client, sessions, pending_timeout_ms=60_000)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 0
    assert consumer.handled == []
    assert await _pending_count(redis_client) == 1


async def test_each_failed_attempt_increments_the_count(redis_client, sessions):
    envelope = await _strand(redis_client)
    consumer = FakeConsumer([False, RuntimeError("boom")])
    recoverer = _recoverer(redis_client, sessions)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 0
    assert await _ledger(sessions, envelope.event_id) == (ProcessedStatus.FAILING, 1)

    await asyncio.sleep(IDLE_MS * 2 / 1000)
    assert await recoverer.recover_once() == 0
    assert await _ledger(sessions, envelope.event_id) == (ProcessedStatus.FAILING, 2)
    assert await _pending_count(redis_client) == 1


async def test_the_last_failed_attempt_gives_up_and_acks(redis_client, sessions):
    envelope = await _strand(redis_client)
    consumer = FakeConsumer([False, False, False])
    recoverer = _recoverer(redis_client, sessions, max_retries=3)
    recoverer.register(STREAM, GROUP, consumer)

    acked = 0
    for _ in range(3):
        acked += await recoverer.recover_once()
        await asyncio.sleep(IDLE_MS * 2 / 1000)

    assert acked == 1
    assert consumer.given_up == [envelope.event_id]
    assert await _ledger(sessions, envelope.event_id) == (ProcessedStatus.FAILED_PERMANENT, 3)
    assert await _markers(sessions) == 1
    assert await _pending_count(redis_client) == 0


async def test_a_give_up_that_raises_rolls_back_and_stays_pending(redis_client, sessions):
    envelope = await _strand(redis_client)
    consumer = FakeConsumer([False], give_up_error=RuntimeError("outbox down"))
    recoverer = _recoverer(redis_client, sessions, max_retries=1)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 0

    assert await _ledger(sessions, envelope.event_id) == (ProcessedStatus.FAILING, 1)
    assert await _markers(sessions) == 0
    assert await _pending_count(redis_client) == 1


async def test_an_unparseable_entry_is_acked_and_discarded(redis_client, sessions):
    dead = RedisStreamConsumer(redis_client, consumer_name="dead")
    await dead.ensure_group(STREAM, GROUP)
    await redis_client.xadd(str(STREAM), {"garbage": "x"})
    with pytest.raises(ValueError):
        await dead.read(STREAM, GROUP, block_ms=100)
    await asyncio.sleep(IDLE_MS * 2 / 1000)

    consumer = FakeConsumer()
    recoverer = _recoverer(redis_client, sessions)
    recoverer.register(STREAM, GROUP, consumer)

    assert await recoverer.recover_once() == 1
    assert consumer.handled == []
    assert await _pending_count(redis_client) == 0


async def test_every_registration_is_recovered(redis_client, sessions):
    """Results consumers read two streams and are registered once per stream."""
    group = ConsumerGroup.NOTIFICATION_RESULTS
    first = await _strand(redis_client, Stream.DELIVERY_COMPLETED, group)
    second = await _strand(redis_client, Stream.DELIVERY_FAILED, group)
    consumer = FakeConsumer([True, True])
    recoverer = _recoverer(redis_client, sessions)
    recoverer.register(Stream.DELIVERY_COMPLETED, group, consumer)
    recoverer.register(Stream.DELIVERY_FAILED, group, consumer)

    assert await recoverer.recover_once() == 2
    assert sorted(consumer.handled) == sorted([first.event_id, second.event_id])
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_recovery_policy.py tests/integration/test_recovery.py -v 2>&1 | tee /tmp/task-3-red.txt`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.recovery'`, `ImportError: cannot import name 'ConsumerServiceSettings'`.

- [ ] **Step 3: Write the implementation**

Append to `shared/notification_shared/config.py`:

```python
class ConsumerServiceSettings(BaseServiceSettings):
    """Settings for services that run stream consumers and a PendingRecoverer.

    Not on BaseServiceSettings: configuration-service and the gateway have no
    consumers (slice 2 spec section 8).
    """

    pending_timeout_ms: int = 30000
    pending_max_retries: int = 3
    recovery_poll_interval_ms: int = 5000
```

Create `shared/notification_shared/recovery.py`:

```python
"""Recovery of stranded stream entries.

A consumer that crashes, or whose handler fails, leaves its entry pending in
the group. Consumer names are container hostnames and change on restart, so
those entries can only be found by idle time and claimed (slice 2 spec 2.1).

Shared, like OutboxPublisher, because the mechanics are identical everywhere.
The consumers themselves stay explicit: the recoverer only calls their public
`handle` — the same code as the normal read path — and, on the last attempt,
their `give_up` (ADR 0024, ADR 0025).

Only the recoverer counts attempts. A failure on the normal read path just
leaves the entry pending.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from notification_shared.context import set_correlation_id
from notification_shared.events import EventEnvelope
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.streams import ClaimedMessage, RedisStreamConsumer

logger = logging.getLogger(__name__)

MAX_RETRIES_EXCEEDED = "max_retries_exceeded"


class RecoverableConsumer(Protocol):
    async def handle(self, envelope: EventEnvelope) -> bool: ...

    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None: ...


def should_give_up(fail_count: int, max_retries: int) -> bool:
    return fail_count >= max_retries


class PendingRecoverer:
    def __init__(
        self,
        *,
        redis,
        consumer_name: str,
        session_factory,
        idempotency: IdempotencyRepository,
        pending_timeout_ms: int = 30000,
        max_retries: int = 3,
        poll_interval_ms: int = 5000,
        batch_size: int = 10,
    ) -> None:
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._session_factory = session_factory
        self._idempotency = idempotency
        self._pending_timeout_ms = pending_timeout_ms
        self._max_retries = max_retries
        self._poll_interval_ms = poll_interval_ms
        self._batch_size = batch_size
        self._registrations: list[tuple[str, str, RecoverableConsumer]] = []

    def register(self, stream: str, group: str, consumer: RecoverableConsumer) -> None:
        self._registrations.append((stream, group, consumer))

    async def recover_once(self) -> int:
        """One sweep over every registration. Returns how many entries were acked."""
        acked = 0
        for stream, group, consumer in self._registrations:
            pending = await self._consumer.get_pending(
                stream, group, self._pending_timeout_ms, self._batch_size
            )
            if not pending:
                continue
            claimed = await self._consumer.claim(
                stream, group, self._pending_timeout_ms, [entry.message_id for entry in pending]
            )
            for message in claimed:
                if await self._recover(stream, group, consumer, message):
                    await self._consumer.ack(stream, group, message.message_id)
                    acked += 1
        set_correlation_id(None)
        return acked

    async def run_forever(self) -> None:
        while True:
            try:
                await self.recover_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("recovery iteration failed")
            await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _recover(
        self, stream: str, group: str, consumer: RecoverableConsumer, message: ClaimedMessage
    ) -> bool:
        """Returns True when the entry should be acked."""
        if message.envelope is None:
            logger.error(
                "unparseable stream entry %s, acking and discarding",
                message.message_id,
                extra={"stream": stream, "consumer_group": group},
            )
            return True

        envelope = message.envelope
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": group,
            "stream": stream,
        }

        try:
            if await consumer.handle(envelope):
                logger.info("recovered pending entry", extra=log_fields)
                return True
        except Exception:
            logger.exception("recovery attempt failed", extra=log_fields)

        async with self._session_factory() as session:
            fail_count = await self._idempotency.increment_fail_count(
                session, envelope.event_id, group
            )
            await session.commit()

        if not should_give_up(fail_count, self._max_retries):
            logger.warning(
                "attempt %d of %d failed, leaving entry pending",
                fail_count,
                self._max_retries,
                extra=log_fields,
            )
            return False

        try:
            async with self._session_factory() as session:
                await consumer.give_up(session, envelope)
                await self._idempotency.mark_failed_permanent(session, envelope.event_id, group)
                await session.commit()
        except Exception:
            logger.exception("give_up failed, entry stays pending", extra=log_fields)
            return False

        logger.error("gave up after %d attempts", fail_count, extra=log_fields)
        return True
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit tests/integration -v`
Expected: PASS.

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix shared tests && uv run ruff format shared tests
git add shared/notification_shared/recovery.py shared/notification_shared/config.py tests/unit/test_recovery_policy.py tests/integration/test_recovery.py
git commit -m "feat(shared): add PendingRecoverer with max-retry give-up

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 4: Notification Service — recovery hooks, watchdog, wiring

**Files:**
- Modify: `services/notification-service/app/workers/routed_consumer.py`, `services/notification-service/app/workers/results_consumer.py`, `services/notification-service/app/models/notification.py`, `services/notification-service/app/repositories/notification.py`, `services/notification-service/app/core/config.py`, `services/notification-service/app/main.py`
- Create: `services/notification-service/app/workers/watchdog.py`
- Test: `services/notification-service/tests/test_watchdog.py` (new), `services/notification-service/tests/test_give_up.py` (new)

**Interfaces:**
- Consumes: `PendingRecoverer`, `ConsumerServiceSettings` (Task 3).
- Produces:
  - `RoutedConsumer.handle(envelope) -> bool`, `RoutedConsumer.give_up(session, envelope) -> None` (writes nothing); same two on `ResultsConsumer`
  - `PROCESSING_TIMEOUT = "processing_timeout"` in `app/models/notification.py`
  - `NotificationRepository.fail_stale_processing(session, timeout_minutes: int) -> int`
  - `Watchdog(*, session_factory, processing_timeout_minutes: int = 5, interval_seconds: int = 60)` with `async sweep_once() -> int`, `async run_forever()`
  - `Settings` gains `pending_timeout_ms`, `pending_max_retries`, `recovery_poll_interval_ms` (inherited), `processing_timeout_minutes: int = 5`, `watchdog_interval_seconds: int = 60`

- [ ] **Step 1: Write the failing tests**

Create `services/notification-service/tests/test_watchdog.py`:

```python
from datetime import timedelta
from uuid import uuid4

import pytest
from app.models.base import Base
from app.models.notification import PROCESSING_TIMEOUT, Notification, NotificationStatus
from app.workers.results_consumer import ResultsConsumer
from app.workers.watchdog import Watchdog
from notification_shared.events import EventEnvelope, EventType
from sqlalchemy import func, select, update

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


async def _seed(sessions, status: str, minutes_ago: int):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            Notification(
                id=notification_id,
                channel="email",
                recipient="john@example.com",
                subject=None,
                body="Hello",
                status=status,
            )
        )
        await session.flush()
        # An explicit value wins over the column's onupdate=now().
        await session.execute(
            update(Notification)
            .where(Notification.id == notification_id)
            .values(updated_at=func.now() - timedelta(minutes=minutes_ago))
        )
        await session.commit()
    return notification_id


async def _state(sessions, notification_id):
    async with sessions() as session:
        return (
            await session.execute(
                select(Notification.status, Notification.fail_reason).where(
                    Notification.id == notification_id
                )
            )
        ).one()


def _watchdog(sessions) -> Watchdog:
    return Watchdog(session_factory=sessions, processing_timeout_minutes=5, interval_seconds=1)


async def test_a_stale_processing_notification_is_failed(sessions):
    notification_id = await _seed(sessions, NotificationStatus.PROCESSING.value, minutes_ago=10)

    assert await _watchdog(sessions).sweep_once() == 1

    assert await _state(sessions, notification_id) == ("FAILED", PROCESSING_TIMEOUT)


async def test_a_recent_processing_notification_is_left_alone(sessions):
    notification_id = await _seed(sessions, NotificationStatus.PROCESSING.value, minutes_ago=1)

    assert await _watchdog(sessions).sweep_once() == 0

    assert await _state(sessions, notification_id) == ("PROCESSING", None)


@pytest.mark.parametrize("status", ["CREATED", "COMPLETED", "FAILED"])
async def test_other_statuses_are_never_touched(sessions, status):
    notification_id = await _seed(sessions, status, minutes_ago=60)

    assert await _watchdog(sessions).sweep_once() == 0

    assert (await _state(sessions, notification_id))[0] == status


async def test_a_late_delivery_result_does_not_reopen_it(sessions, redis_client):
    """ADR 0028: terminal states never reopen, even after the watchdog."""
    notification_id = await _seed(sessions, NotificationStatus.PROCESSING.value, minutes_ago=10)
    await _watchdog(sessions).sweep_once()

    results = ResultsConsumer(
        session_factory=sessions, redis=redis_client, consumer_name="late", poll_interval_ms=10
    )
    late = EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=notification_id,
        payload={"channel": "email", "delivery_id": str(uuid4())},
        correlation_id="corr-late",
    )
    assert await results.handle(late) is True

    assert await _state(sessions, notification_id) == ("FAILED", PROCESSING_TIMEOUT)
```

Create `services/notification-service/tests/test_give_up.py`:

```python
from uuid import uuid4

import pytest
from app.models.base import Base
from app.workers.results_consumer import ResultsConsumer
from app.workers.routed_consumer import RoutedConsumer
from notification_shared.events import EventEnvelope, EventType

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.mark.parametrize("consumer_cls", [RoutedConsumer, ResultsConsumer])
async def test_notification_consumers_give_up_without_writing(sessions, redis_client, consumer_cls):
    """Spec 2.6: this service owns the notification, so there is no one to tell."""
    consumer = consumer_cls(
        session_factory=sessions, redis=redis_client, consumer_name="x", poll_interval_ms=10
    )
    envelope = EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-give-up",
    )
    async with sessions() as session:
        await consumer.give_up(session, envelope)
        assert not session.new
        assert not session.dirty
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=services/notification-service uv run pytest services/notification-service/tests/test_watchdog.py services/notification-service/tests/test_give_up.py -v 2>&1 | tee /tmp/task-4-red.txt`
Expected: FAIL — `ImportError: cannot import name 'PROCESSING_TIMEOUT'`, `ModuleNotFoundError: No module named 'app.workers.watchdog'`.

- [ ] **Step 3: Rename `_handle` to `handle` and add `give_up`**

```bash
sed -i 's/\b_handle\b/handle/g' services/notification-service/app/workers/routed_consumer.py services/notification-service/app/workers/results_consumer.py
```

In both files add `from sqlalchemy.ext.asyncio import AsyncSession` to the imports, and append this method to each class (`RoutedConsumer` in `routed_consumer.py`, `ResultsConsumer` in `results_consumer.py`):

```python
    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """PendingRecoverer hook after PENDING_MAX_RETRIES failed attempts.

        Nothing to write or publish: this service owns the notification, so
        there is no one to tell. A notification left PROCESSING is closed by
        the watchdog (slice 2 spec 2.6).
        """
```

- [ ] **Step 4: Add the watchdog**

In `services/notification-service/app/models/notification.py`, after `class NotificationStatus`, add:

```python
# fail_reason written by the stale-processing watchdog. ADR 0028.
PROCESSING_TIMEOUT = "processing_timeout"
```

In `services/notification-service/app/repositories/notification.py`, change the imports to:

```python
from collections.abc import Sequence
from datetime import timedelta
from uuid import UUID

from app.models.notification import PROCESSING_TIMEOUT, Notification, NotificationStatus
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
```

and append the method:

```python
    async def fail_stale_processing(self, session: AsyncSession, timeout_minutes: int) -> int:
        """The watchdog's guarded update (slice 1 spec 3.16, slice 2 spec 2.7).

        `now()` is the database's clock, so skew between containers cannot
        matter. Only PROCESSING rows match, so a terminal state never changes.
        """
        result = await session.execute(
            update(Notification)
            .where(
                Notification.status == NotificationStatus.PROCESSING.value,
                Notification.updated_at < func.now() - timedelta(minutes=timeout_minutes),
            )
            .values(status=NotificationStatus.FAILED.value, fail_reason=PROCESSING_TIMEOUT)
        )
        return result.rowcount
```

Create `services/notification-service/app/workers/watchdog.py`:

```python
"""Stale-processing watchdog.

A notification stays PROCESSING when its delivery result never arrives — for
example because this service's own results consumer gave up. This sweep fails
it after PROCESSING_TIMEOUT_MINUTES. It publishes no event, so Routing
Service's route stays PROCESSING, and a result arriving later does not reopen
the notification (ADR 0028).
"""

from __future__ import annotations

import asyncio
import logging

from app.repositories.notification import NotificationRepository

logger = logging.getLogger(__name__)


class Watchdog:
    def __init__(
        self,
        *,
        session_factory,
        processing_timeout_minutes: int = 5,
        interval_seconds: int = 60,
    ) -> None:
        self._session_factory = session_factory
        self._notifications = NotificationRepository()
        self._timeout_minutes = processing_timeout_minutes
        self._interval_seconds = interval_seconds

    async def sweep_once(self) -> int:
        async with self._session_factory() as session:
            failed = await self._notifications.fail_stale_processing(
                session, self._timeout_minutes
            )
            await session.commit()
        if failed:
            logger.warning(
                "failed %d notification(s) stuck in PROCESSING for over %d minutes",
                failed,
                self._timeout_minutes,
            )
        return failed

    async def run_forever(self) -> None:
        while True:
            try:
                await self.sweep_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watchdog sweep failed")
            await asyncio.sleep(self._interval_seconds)
```

- [ ] **Step 5: Wire recovery and the watchdog**

Replace `services/notification-service/app/core/config.py` with:

```python
from notification_shared.config import ConsumerServiceSettings


class Settings(ConsumerServiceSettings):
    service_name: str = "notification-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    consumer_poll_interval_ms: int = 500
    processing_timeout_minutes: int = 5
    watchdog_interval_seconds: int = 60
```

In `services/notification-service/app/main.py`:
- Replace the module docstring with:

```python
"""Notification Service.

Owns the notification aggregate and the client-facing REST surface. Background
work: the outbox publisher, two consumers, the pending-entry recoverer and the
stale-processing watchdog.
"""
```

- Add imports:

```python
from app.models.processed_event import ProcessedEvent
from app.workers.watchdog import Watchdog
from notification_shared.events import ConsumerGroup, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.recovery import PendingRecoverer
```

- Replace the block from `await routed.ensure_groups()` through the `tasks = [...]` list with:

```python
        await routed.ensure_groups()
        await results.ensure_groups()

        recoverer = PendingRecoverer(
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-recovery",
            session_factory=app.state.db.session_factory,
            idempotency=IdempotencyRepository(ProcessedEvent),
            pending_timeout_ms=settings.pending_timeout_ms,
            max_retries=settings.pending_max_retries,
            poll_interval_ms=settings.recovery_poll_interval_ms,
        )
        recoverer.register(Stream.NOTIFICATION_ROUTED, ConsumerGroup.NOTIFICATION_ROUTED, routed)
        for stream in (Stream.DELIVERY_COMPLETED, Stream.DELIVERY_FAILED):
            recoverer.register(stream, ConsumerGroup.NOTIFICATION_RESULTS, results)

        watchdog = Watchdog(
            session_factory=app.state.db.session_factory,
            processing_timeout_minutes=settings.processing_timeout_minutes,
            interval_seconds=settings.watchdog_interval_seconds,
        )

        tasks = [
            asyncio.create_task(publisher.run_forever(), name="outbox-publisher"),
            asyncio.create_task(routed.run_forever(), name="routed-consumer"),
            asyncio.create_task(results.run_forever(), name="results-consumer"),
            asyncio.create_task(recoverer.run_forever(), name="pending-recoverer"),
            asyncio.create_task(watchdog.run_forever(), name="watchdog"),
        ]
```

- [ ] **Step 6: Run the whole service suite**

Run: `PYTHONPATH=services/notification-service uv run pytest services/notification-service/tests -v`
Expected: PASS, pre-existing tests included (the rename touched no test).

- [ ] **Step 7: Lint, format, commit**

```bash
uv run ruff check --fix services/notification-service && uv run ruff format services/notification-service
git add services/notification-service
git commit -m "feat(notification): add recovery hooks and the stale-processing watchdog

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 5: Routing Service — recovery hooks and give-up

**Files:**
- Modify: `services/routing-service/app/workers/notification_consumer.py`, `services/routing-service/app/workers/results_consumer.py`, `services/routing-service/app/core/config.py`, `services/routing-service/app/main.py`
- Test: `services/routing-service/tests/test_notification_consumer.py` (append)

**Interfaces:**
- Consumes: `PendingRecoverer`, `MAX_RETRIES_EXCEEDED`, `ConsumerServiceSettings` (Task 3).
- Produces: `NotificationCreatedConsumer.handle/give_up`; `ResultsConsumer.handle/give_up` (no-op); `NotificationCreatedConsumer.give_up` writes a `FAILED` route with `fail_reason="max_retries_exceeded"` and a `RoutingFailed` on `delivery.failed` with `reason="max_retries_exceeded"`.

- [ ] **Step 1: Write the failing tests**

Append to `services/routing-service/tests/test_notification_consumer.py` (add `import asyncio` to the top imports, and `from app.workers.results_consumer import ResultsConsumer`, `from notification_shared.idempotency import IdempotencyRepository, ProcessedStatus`, `from notification_shared.recovery import PendingRecoverer`):

```python
def _recoverer(sessions, redis_client, consumer) -> PendingRecoverer:
    recoverer = PendingRecoverer(
        redis=redis_client,
        consumer_name="routing-recovery",
        session_factory=sessions,
        idempotency=IdempotencyRepository(ProcessedEvent),
        pending_timeout_ms=20,
        max_retries=3,
        poll_interval_ms=10,
    )
    recoverer.register(Stream.NOTIFICATION_CREATED, ConsumerGroup.ROUTING, consumer)
    return recoverer


async def test_give_up_writes_a_failed_route_and_routing_failed(make_consumer, sessions):
    consumer = make_consumer(_unavailable)
    event = _created_event()

    async with sessions() as session:
        await consumer.give_up(session, event)
        await session.commit()

    async with sessions() as session:
        route = (await session.scalars(select(Route))).one()
        assert route.notification_id == event.aggregate_id
        assert route.status == RouteStatus.FAILED
        assert route.fail_reason == "max_retries_exceeded"

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "delivery.failed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.ROUTING_FAILED
        assert published.payload["reason"] == "max_retries_exceeded"
        assert published.payload["route_id"] == str(route.id)
        assert published.correlation_id == "corr-route"


async def test_a_persistent_outage_ends_in_routing_failed_after_max_retries(
    make_consumer, sessions, redis_client
):
    """The whole slice 2 chain at the service tier: pending -> retries -> give-up."""
    consumer = make_consumer(_unavailable)
    await consumer.ensure_groups()
    event = _created_event()
    await _publish(redis_client, event)
    assert await consumer.consume_once() == 0

    recoverer = _recoverer(sessions, redis_client, consumer)
    for _ in range(3):
        await asyncio.sleep(0.05)
        await recoverer.recover_once()

    async with sessions() as session:
        route = (await session.scalars(select(Route))).one()
        assert (route.status, route.fail_reason) == (RouteStatus.FAILED, "max_retries_exceeded")
        ledger = (
            await session.execute(
                select(ProcessedEvent.status, ProcessedEvent.fail_count).where(
                    ProcessedEvent.event_id == event.event_id
                )
            )
        ).one()
        assert ledger == (ProcessedStatus.FAILED_PERMANENT, 3)
    pending = await redis_client.xpending(
        str(Stream.NOTIFICATION_CREATED), str(ConsumerGroup.ROUTING)
    )
    assert pending["pending"] == 0


async def test_an_outage_that_ends_is_routed_by_recovery(make_consumer, sessions, redis_client):
    await make_consumer(_unavailable).ensure_groups()
    stranded = make_consumer(_unavailable)
    event = _created_event()
    await _publish(redis_client, event)
    assert await stranded.consume_once() == 0

    recovered = make_consumer(_enabled)
    await asyncio.sleep(0.05)
    assert await _recoverer(sessions, redis_client, recovered).recover_once() == 1

    async with sessions() as session:
        route = (await session.scalars(select(Route))).one()
        assert route.status == RouteStatus.PROCESSING
        assert (await session.scalars(select(Outbox))).one().stream == "notification.routed"


async def test_the_results_consumer_gives_up_without_writing(sessions, redis_client):
    results = ResultsConsumer(
        session_factory=sessions, redis=redis_client, consumer_name="x", poll_interval_ms=10
    )
    async with sessions() as session:
        await results.give_up(session, _created_event())
        assert not session.new
        assert not session.dirty
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=services/routing-service uv run pytest services/routing-service/tests/test_notification_consumer.py -v 2>&1 | tee /tmp/task-5-red.txt`
Expected: the four new tests FAIL with `AttributeError: ... has no attribute 'give_up'` / `'handle'`; the pre-existing tests pass.

- [ ] **Step 3: Implement**

```bash
sed -i 's/\b_handle\b/handle/g' services/routing-service/app/workers/notification_consumer.py services/routing-service/app/workers/results_consumer.py
```

In `services/routing-service/app/workers/notification_consumer.py`:
- Add imports `from notification_shared.recovery import MAX_RETRIES_EXCEEDED` and `from sqlalchemy.ext.asyncio import AsyncSession`.
- Replace the transient branch comment inside `handle` with:

```python
            # Transient. Do not ack: the message stays pending and
            # PendingRecoverer retries it after PENDING_TIMEOUT_MS, giving up
            # after PENDING_MAX_RETRIES (slice 2 spec 2.3). An outage must
            # never be recorded as an ordinary routing failure.
```

- Replace everything in `handle` from `async with self._session_factory() as session:` (the second one, which builds the `Route`) through `await session.commit()` with:

```python
        async with self._session_factory() as session:
            await self._record_decision(session, envelope, fail_reason)
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()
```

- Add these two methods after `handle` (before `_decide`):

```python
    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """PendingRecoverer hook after PENDING_MAX_RETRIES failed attempts.

        Records the route as FAILED and publishes RoutingFailed, so the
        notification goes CREATED -> FAILED instead of staying CREATED forever
        (ADR 0024). No Configuration Service call: give_up must not fail for
        the reason handle did.
        """
        await self._record_decision(session, envelope, MAX_RETRIES_EXCEEDED)

    async def _record_decision(
        self, session: AsyncSession, envelope: EventEnvelope, fail_reason: str | None
    ) -> None:
        """The route row and its outbox event. The caller owns the transaction."""
        channel = envelope.payload["channel"]
        route = Route(
            id=uuid4(),
            notification_id=envelope.aggregate_id,
            channel=channel,
            status=(RouteStatus.PROCESSING.value if fail_reason is None else RouteStatus.FAILED.value),
            fail_reason=fail_reason,
        )
        await self._routes.add(session, route)
        await session.flush()

        if fail_reason is None:
            await self._outbox.save(
                session,
                Stream.NOTIFICATION_ROUTED,
                EventEnvelope.new(
                    event_type=EventType.NOTIFICATION_ROUTED,
                    aggregate_id=envelope.aggregate_id,
                    payload={
                        # Forwarded from NotificationCreated: the delivery
                        # services cannot read this notification from
                        # another service's database. See spec 3.20.
                        "channel": channel,
                        "recipient": envelope.payload["recipient"],
                        "subject": envelope.payload.get("subject"),
                        "body": envelope.payload["body"],
                        "route_id": str(route.id),
                    },
                    correlation_id=envelope.correlation_id,
                ),
            )
        else:
            await self._outbox.save(
                session,
                Stream.DELIVERY_FAILED,
                EventEnvelope.new(
                    event_type=EventType.ROUTING_FAILED,
                    aggregate_id=envelope.aggregate_id,
                    payload={"channel": channel, "route_id": str(route.id), "reason": fail_reason},
                    correlation_id=envelope.correlation_id,
                ),
            )
```

In `services/routing-service/app/workers/results_consumer.py` add `from sqlalchemy.ext.asyncio import AsyncSession` and append:

```python
    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """PendingRecoverer hook. Nothing to publish; the route stays
        PROCESSING, a documented limitation (slice 2 spec 2.6)."""
```

Replace `services/routing-service/app/core/config.py` with:

```python
from notification_shared.config import ConsumerServiceSettings


class Settings(ConsumerServiceSettings):
    service_name: str = "routing-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    configuration_service_url: str = "http://configuration-service:8000"
    http_timeout_seconds: float = 5.0
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
```

In `services/routing-service/app/main.py` add imports:

```python
from app.models.processed_event import ProcessedEvent
from notification_shared.events import ConsumerGroup, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.recovery import PendingRecoverer
```

and replace the `tasks = [...]` list with:

```python
        recoverer = PendingRecoverer(
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-recovery",
            session_factory=app.state.db.session_factory,
            idempotency=IdempotencyRepository(ProcessedEvent),
            pending_timeout_ms=settings.pending_timeout_ms,
            max_retries=settings.pending_max_retries,
            poll_interval_ms=settings.recovery_poll_interval_ms,
        )
        recoverer.register(Stream.NOTIFICATION_CREATED, ConsumerGroup.ROUTING, consumer)
        for stream in (Stream.DELIVERY_COMPLETED, Stream.DELIVERY_FAILED):
            recoverer.register(stream, ConsumerGroup.ROUTING_RESULTS, results)

        tasks = [
            asyncio.create_task(consumer.run_forever(), name="notification-consumer"),
            asyncio.create_task(results.run_forever(), name="results-consumer"),
            asyncio.create_task(publisher.run_forever(), name="outbox-publisher"),
            asyncio.create_task(recoverer.run_forever(), name="pending-recoverer"),
        ]
```

- [ ] **Step 4: Run the whole service suite**

Run: `PYTHONPATH=services/routing-service uv run pytest services/routing-service/tests -v`
Expected: PASS.

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix services/routing-service && uv run ruff format services/routing-service
git add services/routing-service
git commit -m "feat(routing): give up with RoutingFailed after max retries

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 6: Email Service — recovery hooks and give-up

**Files:**
- Modify: `services/email-service/app/workers/routed_consumer.py`, `services/email-service/app/core/config.py`, `services/email-service/app/main.py`
- Test: `services/email-service/tests/test_routed_consumer.py` (append)

**Interfaces:**
- Consumes: `PendingRecoverer`, `MAX_RETRIES_EXCEEDED`, `ConsumerServiceSettings` (Task 3).
- Produces: `RoutedConsumer.handle(envelope) -> bool`, `RoutedConsumer.give_up(session, envelope) -> None`, private `_record_outcome(session, envelope, fail_reason: str | None, sent_at: datetime | None) -> None` (Task 7 keeps using it).

- [ ] **Step 1: Write the failing test**

Append to `services/email-service/tests/test_routed_consumer.py`:

```python
async def test_give_up_records_a_failed_delivery_and_publishes_delivery_failed(
    consumer, sessions
):
    event = _routed_event()

    async with sessions() as session:
        await consumer.give_up(session, event)
        await session.commit()

    async with sessions() as session:
        delivery = (await session.scalars(select(EmailDelivery))).one()
        assert delivery.notification_id == event.aggregate_id
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.fail_reason == "max_retries_exceeded"
        assert delivery.sent_at is None

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "delivery.failed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.DELIVERY_FAILED
        assert published.payload["reason"] == "max_retries_exceeded"
        assert published.payload["delivery_id"] == str(delivery.id)

        # give_up does not mark processed; the recoverer does, in the same txn.
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `PYTHONPATH=services/email-service uv run pytest services/email-service/tests/test_routed_consumer.py -v 2>&1 | tee /tmp/task-6-red.txt`
Expected: the new test FAILS with `AttributeError: 'RoutedConsumer' object has no attribute 'give_up'`.

- [ ] **Step 3: Implement**

```bash
sed -i 's/\b_handle\b/handle/g' services/email-service/app/workers/routed_consumer.py
```

In `services/email-service/app/workers/routed_consumer.py`:
- Add imports `from notification_shared.recovery import MAX_RETRIES_EXCEEDED` and `from sqlalchemy.ext.asyncio import AsyncSession`.
- In `handle`, replace everything from `async with self._session_factory() as session:` (the one after `delivered_at = datetime.now(UTC)`) through `await session.commit()` with:

```python
        async with self._session_factory() as session:
            await self._record_outcome(session, envelope, fail_reason, sent_at=delivered_at)
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()
```

- Add these methods after `handle`:

```python
    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """PendingRecoverer hook after PENDING_MAX_RETRIES failed attempts.

        Records a FAILED delivery and publishes DeliveryFailed, so the
        notification closes as FAILED instead of waiting for the watchdog
        (ADR 0024). No send here: give_up must not fail for the reason handle
        did.
        """
        await self._record_outcome(session, envelope, MAX_RETRIES_EXCEEDED, sent_at=None)

    async def _record_outcome(
        self,
        session: AsyncSession,
        envelope: EventEnvelope,
        fail_reason: str | None,
        sent_at: datetime | None,
    ) -> None:
        """The delivery row, inserted already terminal (ADR 0022), and its
        outbox event. The caller owns the transaction."""
        recipient = envelope.payload["recipient"]
        delivery = EmailDelivery(
            id=uuid4(),
            notification_id=envelope.aggregate_id,
            recipient=recipient,
            status=(
                DeliveryStatus.DELIVERED.value if fail_reason is None else DeliveryStatus.FAILED.value
            ),
            fail_reason=fail_reason,
            sent_at=sent_at,
        )
        await self._deliveries.add(session, delivery)
        await session.flush()

        if fail_reason is None:
            await self._outbox.save(
                session,
                Stream.DELIVERY_COMPLETED,
                EventEnvelope.new(
                    event_type=EventType.DELIVERY_COMPLETED,
                    aggregate_id=envelope.aggregate_id,
                    payload={
                        "channel": Channel.EMAIL.value,
                        "delivery_id": str(delivery.id),
                        "recipient": recipient,
                        "delivered_at": sent_at.isoformat(),
                    },
                    correlation_id=envelope.correlation_id,
                ),
            )
        else:
            await self._outbox.save(
                session,
                Stream.DELIVERY_FAILED,
                EventEnvelope.new(
                    event_type=EventType.DELIVERY_FAILED,
                    aggregate_id=envelope.aggregate_id,
                    payload={
                        "channel": Channel.EMAIL.value,
                        "delivery_id": str(delivery.id),
                        "reason": fail_reason,
                    },
                    correlation_id=envelope.correlation_id,
                ),
            )
```

Replace `services/email-service/app/core/config.py` with:

```python
from notification_shared.config import ConsumerServiceSettings


class Settings(ConsumerServiceSettings):
    service_name: str = "email-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    delivery_latency_ms_max: int = 500
```

In `services/email-service/app/main.py` add imports:

```python
from app.models.processed_event import ProcessedEvent
from notification_shared.events import ConsumerGroup, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.recovery import PendingRecoverer
```

and replace the `tasks = [...]` list with:

```python
        recoverer = PendingRecoverer(
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-recovery",
            session_factory=app.state.db.session_factory,
            idempotency=IdempotencyRepository(ProcessedEvent),
            pending_timeout_ms=settings.pending_timeout_ms,
            max_retries=settings.pending_max_retries,
            poll_interval_ms=settings.recovery_poll_interval_ms,
        )
        recoverer.register(Stream.NOTIFICATION_ROUTED, ConsumerGroup.EMAIL, consumer)

        tasks = [
            asyncio.create_task(consumer.run_forever(), name="routed-consumer"),
            asyncio.create_task(publisher.run_forever(), name="outbox-publisher"),
            asyncio.create_task(recoverer.run_forever(), name="pending-recoverer"),
        ]
```

- [ ] **Step 4: Run the whole service suite**

Run: `PYTHONPATH=services/email-service uv run pytest services/email-service/tests -v`
Expected: PASS.

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix services/email-service && uv run ruff format services/email-service
git add services/email-service
git commit -m "feat(email): give up with DeliveryFailed after max retries

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 7: Email Service — SMTP sender and delivery mode

**Files:**
- Create: `services/email-service/app/senders.py`
- Modify: `services/email-service/app/workers/routed_consumer.py`, `services/email-service/app/core/config.py`, `services/email-service/app/api/v1/endpoints/system.py`, `services/email-service/app/main.py`, `services/email-service/pyproject.toml` (via `uv add`), root `pyproject.toml` (via `uv add`)
- Test: `services/email-service/tests/test_senders.py` (new), `services/email-service/tests/test_routed_consumer.py` (append)

**Interfaces:**
- Consumes: `is_failure_recipient`, `is_reserved_recipient`, `SIMULATED_FAILURE` (Task 1); `_record_outcome` (Task 6).
- Produces:
  - `EmailRejectedError`, `EmailUnavailableError`
  - `EmailSender(Protocol)`: `mode: str`; `async send(recipient: str, subject: str | None, body: str) -> None`
  - `SimulatedSender(latency_ms_max: int = 500)` (`mode = "simulated"`), `SmtpSender(*, host: str, port: int, sender_address: str, username: str | None = None, password: str | None = None, security: str = "starttls", timeout: float = 5.0)` (`mode = "smtp"`)
  - `build_sender(settings) -> EmailSender`, `delivery_mode(settings) -> str`
  - `SMTP_REJECTED = "smtp_rejected"`, `DEFAULT_SUBJECT = "Notification"`
  - `RoutedConsumer(..., sender: EmailSender | None = None)`
  - `GET /version` → `{"service", "version", "delivery_mode"}`

- [ ] **Step 1: Verify the new dependencies on Python 3.14 (spec 16)**

```bash
uv add --package email-service aiosmtplib
uv add --group dev aiosmtpd
uv run python -c "import aiosmtplib, aiosmtpd.controller; print(aiosmtplib.__version__)"
```

Expected: a version number. If either package fails to resolve or import on 3.14, **stop and report BLOCKED** with the error; do not substitute another library without a ruling.

- [ ] **Step 2: Write the failing tests**

Create `services/email-service/tests/test_senders.py`:

```python
import base64
import logging
import socket
from email import message_from_bytes, policy

import httpx
import pytest
from aiosmtpd.controller import Controller
from aiosmtpd.smtp import AuthResult, LoginPassword
from app.core.config import Settings
from app.main import create_app
from app.senders import (
    DEFAULT_SUBJECT,
    EmailRejectedError,
    EmailUnavailableError,
    SimulatedSender,
    SmtpSender,
    build_sender,
    delivery_mode,
)
from pydantic import ValidationError

USER = "mailer"
PASSWORD = "s3cret-Pa55word"
DB_URL = "postgresql+asyncpg://u:p@localhost/db"


class RecordingHandler:
    """In-process SMTP server behaviour: `reject…` gets 550, `busy…` gets 451."""

    def __init__(self) -> None:
        self.messages = []

    async def handle_RCPT(self, server, session, envelope, address, rcpt_options):  # noqa: N802
        if address.startswith("reject"):
            return "550 5.1.1 No such user"
        if address.startswith("busy"):
            return "451 4.3.0 Try again later"
        envelope.rcpt_tos.append(address)
        return "250 OK"

    async def handle_DATA(self, server, session, envelope):  # noqa: N802
        raw = envelope.original_content or envelope.content
        self.messages.append(message_from_bytes(raw, policy=policy.default))
        return "250 Message accepted"


def _authenticator(server, session, envelope, mechanism, auth_data):
    ok = (
        isinstance(auth_data, LoginPassword)
        and auth_data.login == USER.encode()
        and auth_data.password == PASSWORD.encode()
    )
    return AuthResult(success=ok, handled=False)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def smtp_server():
    # The fake server logs every received line at DEBUG, AUTH included. Those
    # are the test double's logs, not ours; silence them so the password test
    # measures only the code under test.
    logging.getLogger("mail.log").setLevel(logging.WARNING)
    handler = RecordingHandler()
    controller = Controller(
        handler,
        hostname="127.0.0.1",
        port=_free_port(),
        authenticator=_authenticator,
        auth_require_tls=False,
    )
    controller.start()
    yield controller, handler
    controller.stop()


def _sender(controller, **overrides) -> SmtpSender:
    kwargs = {
        "host": controller.hostname,
        "port": controller.port,
        "sender_address": "platform@example.com",
        "security": "none",
        "timeout": 5.0,
    }
    kwargs.update(overrides)
    return SmtpSender(**kwargs)


async def test_an_accepted_message_reaches_the_server(smtp_server):
    controller, handler = smtp_server
    await _sender(controller).send("someone@real-domain.org", "Welcome", "Hello!")

    (message,) = handler.messages
    assert message["From"] == "platform@example.com"
    assert message["To"] == "someone@real-domain.org"
    assert message["Subject"] == "Welcome"
    assert message.get_content().strip() == "Hello!"


async def test_a_non_ascii_subject_and_body_arrive_intact(smtp_server):
    """Review Focus 3."""
    controller, handler = smtp_server
    await _sender(controller).send("someone@real-domain.org", "Città ✓", "Ciao, città! 🎉")

    (message,) = handler.messages
    assert message["Subject"] == "Città ✓"
    assert message.get_content().strip() == "Ciao, città! 🎉"


async def test_a_missing_subject_uses_the_default(smtp_server):
    controller, handler = smtp_server
    await _sender(controller).send("someone@real-domain.org", None, "Hello!")
    assert handler.messages[0]["Subject"] == DEFAULT_SUBJECT == "Notification"


async def test_a_5xx_refusal_is_permanent(smtp_server):
    controller, _ = smtp_server
    with pytest.raises(EmailRejectedError):
        await _sender(controller).send("reject@real-domain.org", "s", "b")


async def test_a_4xx_refusal_is_transient(smtp_server):
    controller, _ = smtp_server
    with pytest.raises(EmailUnavailableError):
        await _sender(controller).send("busy@real-domain.org", "s", "b")


async def test_valid_credentials_authenticate(smtp_server):
    controller, handler = smtp_server
    await _sender(controller, username=USER, password=PASSWORD).send("a@real-domain.org", "s", "b")
    assert len(handler.messages) == 1


async def test_an_authentication_failure_is_transient_and_logged(smtp_server, caplog):
    controller, handler = smtp_server
    caplog.set_level(logging.ERROR)
    with pytest.raises(EmailUnavailableError):
        await _sender(controller, username=USER, password="wrong").send("a@real-domain.org", "s", "b")
    assert handler.messages == []
    assert "authentication failed" in caplog.text


async def test_an_unreachable_server_is_transient():
    sender = SmtpSender(
        host="127.0.0.1", port=_free_port(), sender_address="p@example.com", security="none", timeout=2.0
    )
    with pytest.raises(EmailUnavailableError):
        await sender.send("a@real-domain.org", "s", "b")


async def test_the_password_never_reaches_the_logs(smtp_server, caplog):
    controller, _ = smtp_server
    caplog.set_level(logging.DEBUG)
    await _sender(controller, username=USER, password=PASSWORD).send("a@real-domain.org", "s", "b")
    with pytest.raises(EmailUnavailableError):
        await _sender(controller, username=USER, password=PASSWORD + "x").send("a@real-domain.org", "s", "b")
    with pytest.raises(EmailRejectedError):
        await _sender(controller, username=USER, password=PASSWORD).send("reject@real-domain.org", "s", "b")

    plain_blob = base64.b64encode(f"\0{USER}\0{PASSWORD}".encode()).decode()
    assert PASSWORD not in caplog.text
    assert plain_blob not in caplog.text


def _settings(**overrides) -> Settings:
    return Settings(database_url=DB_URL, _env_file=None, **overrides)


def test_no_smtp_host_means_simulated():
    settings = _settings()
    assert isinstance(build_sender(settings), SimulatedSender)
    assert delivery_mode(settings) == "simulated"


def test_an_smtp_host_means_smtp():
    settings = _settings(smtp_host="smtp.real-domain.org", smtp_from="p@real-domain.org")
    assert isinstance(build_sender(settings), SmtpSender)
    assert delivery_mode(settings) == "smtp"


def test_an_smtp_host_without_a_from_address_fails_at_startup():
    with pytest.raises(ValidationError, match="SMTP_FROM"):
        _settings(smtp_host="smtp.real-domain.org")


def test_smtp_security_only_accepts_the_three_modes():
    with pytest.raises(ValidationError):
        _settings(smtp_security="tls")


async def test_version_reports_the_delivery_mode():
    settings = _settings(smtp_host="smtp.real-domain.org", smtp_from="p@real-domain.org")
    app = create_app(settings)
    app.state.settings = settings
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        body = (await client.get("/version")).json()
    assert body == {"service": "email-service", "version": "1.0.0", "delivery_mode": "smtp"}
```

Append to `services/email-service/tests/test_routed_consumer.py` (add `from app.senders import EmailRejectedError, EmailUnavailableError` to the imports):

```python
class RecordingSender:
    """Stands in for a real sender; records who it was asked to reach."""

    mode = "smtp"

    def __init__(self, error: Exception | None = None) -> None:
        self.sent: list[str] = []
        self.error = error

    async def send(self, recipient, subject, body) -> None:
        if self.error:
            raise self.error
        self.sent.append(recipient)


def _real_consumer(sessions, redis_client, sender) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions,
        redis=redis_client,
        consumer_name="email-real",
        poll_interval_ms=10,
        delivery_latency_ms_max=0,
        sender=sender,
    )


async def _only_delivery(sessions):
    async with sessions() as session:
        return (await session.scalars(select(EmailDelivery))).one()


async def test_a_reserved_recipient_never_reaches_the_configured_sender(sessions, redis_client):
    sender = RecordingSender()
    consumer = _real_consumer(sessions, redis_client, sender)
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="john@example.com"))

    assert await consumer.consume_once() == 1
    assert sender.sent == []
    assert (await _only_delivery(sessions)).status == DeliveryStatus.DELIVERED


async def test_a_real_recipient_goes_to_the_configured_sender(sessions, redis_client):
    sender = RecordingSender()
    consumer = _real_consumer(sessions, redis_client, sender)
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="john@gmail.com"))

    assert await consumer.consume_once() == 1
    assert sender.sent == ["john@gmail.com"]
    assert (await _only_delivery(sessions)).status == DeliveryStatus.DELIVERED


async def test_the_failure_marker_precedes_the_real_sender(sessions, redis_client):
    """Review Focus 1."""
    sender = RecordingSender()
    consumer = _real_consumer(sessions, redis_client, sender)
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="failla@gmail.com"))

    await consumer.consume_once()
    assert sender.sent == []
    delivery = await _only_delivery(sessions)
    assert (delivery.status, delivery.fail_reason) == (DeliveryStatus.FAILED, "simulated_failure")


async def test_an_smtp_rejection_fails_with_smtp_rejected(sessions, redis_client):
    consumer = _real_consumer(sessions, redis_client, RecordingSender(EmailRejectedError("550")))
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="john@gmail.com"))

    assert await consumer.consume_once() == 1
    delivery = await _only_delivery(sessions)
    assert (delivery.status, delivery.fail_reason) == (DeliveryStatus.FAILED, "smtp_rejected")
    async with sessions() as session:
        published = EventEnvelope.model_validate((await session.scalars(select(Outbox))).one().payload)
    assert published.payload["reason"] == "smtp_rejected"


async def test_a_transient_smtp_failure_writes_nothing_and_stays_pending(sessions, redis_client):
    consumer = _real_consumer(sessions, redis_client, RecordingSender(EmailUnavailableError("451")))
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="john@gmail.com"))

    with pytest.raises(EmailUnavailableError):
        await consumer.consume_once()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
    pending = await redis_client.xpending(str(Stream.NOTIFICATION_ROUTED), str(ConsumerGroup.EMAIL))
    assert pending["pending"] == 1
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `PYTHONPATH=services/email-service uv run pytest services/email-service/tests -v 2>&1 | tee /tmp/task-7-red.txt`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.senders'` (collection error for both files).

- [ ] **Step 4: Write the implementation**

Create `services/email-service/app/senders.py`:

```python
"""Email senders.

The consumer applies the shared rules first — failure marker, then reserved
recipients (slice 2 spec section 3) — and only then calls one of these. Which
sender is configured is decided once at startup from SMTP_HOST (ADR 0029).
"""

from __future__ import annotations

import asyncio
import logging
import random
from email.message import EmailMessage
from typing import Protocol

import aiosmtplib

logger = logging.getLogger(__name__)

DEFAULT_SUBJECT = "Notification"
SMTP_REJECTED = "smtp_rejected"


class EmailRejectedError(Exception):
    """Permanent: the server refused this message or recipient."""


class EmailUnavailableError(Exception):
    """Transient: worth retrying. The message stays pending for recovery."""


class EmailSender(Protocol):
    mode: str

    async def send(self, recipient: str, subject: str | None, body: str) -> None: ...


class SimulatedSender:
    mode = "simulated"

    def __init__(self, latency_ms_max: int = 500) -> None:
        self._latency_ms_max = latency_ms_max

    async def send(self, recipient: str, subject: str | None, body: str) -> None:
        logger.info("simulating email to %s", recipient)
        if self._latency_ms_max:
            await asyncio.sleep(random.uniform(0, self._latency_ms_max) / 1000)


class SmtpSender:
    mode = "smtp"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        sender_address: str,
        username: str | None = None,
        password: str | None = None,
        security: str = "starttls",
        timeout: float = 5.0,
    ) -> None:
        # aiosmtplib logs protocol lines at DEBUG, AUTH included.
        logging.getLogger("aiosmtplib").setLevel(logging.WARNING)
        self._host = host
        self._port = port
        self._from = sender_address
        self._username = username or None
        self._password = password or None
        self._security = security
        self._timeout = timeout

    async def send(self, recipient: str, subject: str | None, body: str) -> None:
        message = EmailMessage()
        message["From"] = self._from
        message["To"] = recipient
        message["Subject"] = subject or DEFAULT_SUBJECT
        message.set_content(body)
        logger.info("sending email to %s via SMTP", recipient)
        try:
            await aiosmtplib.send(
                message,
                hostname=self._host,
                port=self._port,
                username=self._username,
                password=self._password,
                use_tls=self._security == "ssl",
                start_tls=self._security == "starttls",
                timeout=self._timeout,
            )
        except aiosmtplib.SMTPRecipientsRefused as exc:
            codes = [refused.code for refused in exc.recipients]
            if codes and all(code >= 500 for code in codes):
                raise EmailRejectedError(f"recipient refused {codes}") from None
            raise EmailUnavailableError(f"recipient deferred {codes}") from None
        except aiosmtplib.SMTPAuthenticationError as exc:
            # A configuration problem, not a recipient problem: transient, so
            # fixed credentials let the notification through (spec 6.2).
            logger.error(
                "SMTP authentication failed (%s); check SMTP_USERNAME and SMTP_PASSWORD", exc.code
            )
            raise EmailUnavailableError("SMTP authentication failed") from None
        except aiosmtplib.SMTPResponseException as exc:
            if exc.code >= 500:
                raise EmailRejectedError(f"SMTP {exc.code}") from None
            raise EmailUnavailableError(f"SMTP {exc.code}") from None
        except (aiosmtplib.SMTPException, OSError, TimeoutError) as exc:
            raise EmailUnavailableError(f"SMTP unreachable ({type(exc).__name__})") from None


def build_sender(settings) -> EmailSender:
    if not settings.smtp_host:
        return SimulatedSender(settings.delivery_latency_ms_max)
    return SmtpSender(
        host=settings.smtp_host,
        port=settings.smtp_port,
        sender_address=settings.smtp_from,
        username=settings.smtp_username,
        password=settings.smtp_password,
        security=settings.smtp_security,
        timeout=settings.http_timeout_seconds,
    )


def delivery_mode(settings) -> str:
    """The configured mode. Reserved recipients are simulated regardless."""
    return SmtpSender.mode if settings.smtp_host else SimulatedSender.mode
```

Replace `services/email-service/app/core/config.py` with:

```python
from typing import Literal, Self

from notification_shared.config import ConsumerServiceSettings
from pydantic import model_validator


class Settings(ConsumerServiceSettings):
    service_name: str = "email-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    delivery_latency_ms_max: int = 500
    # SMTP connect timeout; reused rather than adding a separate variable.
    http_timeout_seconds: float = 5.0
    # Empty SMTP_HOST means simulated delivery (ADR 0029).
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from: str = ""
    smtp_security: Literal["starttls", "ssl", "none"] = "starttls"

    @model_validator(mode="after")
    def _smtp_from_is_required_with_a_host(self) -> Self:
        # Fail at startup, so a half-configured service never reaches healthy.
        if self.smtp_host and not self.smtp_from:
            raise ValueError("SMTP_FROM is required when SMTP_HOST is set")
        return self
```

In `services/email-service/app/workers/routed_consumer.py`:
- Replace the module docstring with:

```python
"""Consumes notification.routed and delivers email.

Before any send, the shared rules apply in order (slice 2 spec section 3): a
recipient containing "fail" fails deterministically, and a reserved recipient
is always simulated. Everything else goes to the configured sender, simulated
or SMTP (ADR 0029, ADR 0030).
"""
```

- Remove `import random` and the `FAILURE_MARKER = "fail"` constant. Add imports:

```python
from app.senders import SMTP_REJECTED, EmailRejectedError, EmailSender, SimulatedSender
from notification_shared.delivery import (
    SIMULATED_FAILURE,
    is_failure_recipient,
    is_reserved_recipient,
)
```

- Replace `__init__` with:

```python
    def __init__(
        self,
        *,
        session_factory,
        redis,
        consumer_name: str,
        poll_interval_ms: int = 500,
        delivery_latency_ms_max: int = 500,
        sender: EmailSender | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._deliveries = EmailDeliveryRepository()
        self._outbox = OutboxRepository(Outbox)
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms
        # Reserved recipients always use the simulated sender, whatever is configured.
        self._simulated = SimulatedSender(delivery_latency_ms_max)
        self._sender = sender or self._simulated
```

- Replace `_deliver` with:

```python
    async def _deliver(self, recipient: str, subject: str | None, body: str) -> str | None:
        """Returns None on success, or a permanent failure reason.

        Raises EmailUnavailableError for a transient failure: nothing is
        written, the message stays pending, PendingRecoverer retries it.
        """
        if is_failure_recipient(recipient):
            return SIMULATED_FAILURE
        reserved = is_reserved_recipient(Channel.EMAIL, recipient)
        sender = self._simulated if reserved else self._sender
        try:
            await sender.send(recipient, subject, body)
        except EmailRejectedError as exc:
            logger.warning("email rejected by the server: %s", exc)
            return SMTP_REJECTED
        return None
```

Replace the `version` endpoint in `services/email-service/app/api/v1/endpoints/system.py` with (and add `from app.senders import delivery_mode`):

```python
@router.get("/version", summary="Service version and delivery mode")
async def version(request: Request) -> dict:
    settings = request.app.state.settings
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "delivery_mode": delivery_mode(settings),
    }
```

In `services/email-service/app/main.py`: add `import logging`, `from app.senders import build_sender`, a module-level `logger = logging.getLogger(__name__)`, and change the consumer construction to:

```python
        sender = build_sender(settings)
        logger.info("email delivery mode: %s", sender.mode)
        consumer = RoutedConsumer(
            session_factory=app.state.db.session_factory,
            redis=app.state.redis,
            consumer_name=socket.gethostname(),
            poll_interval_ms=settings.consumer_poll_interval_ms,
            delivery_latency_ms_max=settings.delivery_latency_ms_max,
            sender=sender,
        )
```

- [ ] **Step 5: Run the whole service suite**

Run: `PYTHONPATH=services/email-service uv run pytest services/email-service/tests -v`
Expected: PASS, including every slice 1 test (`fail@example.com` still fails with `simulated_failure`; `john@example.com` still delivers).

- [ ] **Step 6: Lint, format, commit**

```bash
uv run ruff check --fix services/email-service && uv run ruff format services/email-service
git add services/email-service pyproject.toml uv.lock
git commit -m "feat(email): add SMTP delivery with reserved recipients always simulated

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 8: Telegram Service — simulated delivery

**Files:**
- Create: `services/telegram-service/` (layout in spec 4.1): `pyproject.toml`, `Dockerfile`, `entrypoint.sh`, `alembic.ini`, `alembic/env.py`, `alembic/script.py.mako`, `alembic/versions/0001_create_telegram_tables.py`, `app/main.py`, `app/core/config.py`, `app/core/database.py`, `app/api/v1/router.py`, `app/api/v1/endpoints/system.py`, `app/models/{base,outbox,processed_event,telegram_delivery}.py`, `app/repositories/telegram_delivery.py`, `app/senders.py`, `app/workers/__init__.py`, `app/workers/routed_consumer.py`, `tests/conftest.py`
- Test: `services/telegram-service/tests/test_routed_consumer.py`

**Interfaces:**
- Consumes: shared delivery rules (Task 1), `PendingRecoverer`, `MAX_RETRIES_EXCEEDED`, `ConsumerServiceSettings` (Task 3).
- Produces:
  - `TelegramDelivery` model, `TelegramDeliveryRepository.add/get_by_notification`
  - `app.senders`: `TELEGRAM_REJECTED = "telegram_rejected"`, `TelegramRejectedError`, `TelegramUnavailableError`, `TelegramSender(Protocol)` (`mode: str`; `async send(chat_id: str, text: str) -> None`; `async aclose() -> None`), `SimulatedSender(latency_ms_max: int = 500)`, `build_text(subject: str | None, body: str) -> str`, `build_sender(settings, transport=None) -> TelegramSender`, `delivery_mode(settings) -> str`
  - `RoutedConsumer(*, session_factory, redis, consumer_name, poll_interval_ms=500, delivery_latency_ms_max=500, sender=None)` with `ensure_groups`, `consume_once`, `run_forever`, `handle`, `give_up`

- [ ] **Step 1: Scaffold the files that are identical to email-service**

```bash
S=services/telegram-service
mkdir -p $S/app/api/v1/endpoints $S/app/core $S/app/models $S/app/repositories $S/app/workers $S/alembic/versions $S/tests
cp services/email-service/alembic.ini $S/
cp services/email-service/alembic/script.py.mako $S/alembic/
cp services/email-service/entrypoint.sh $S/
cp services/email-service/app/core/database.py $S/app/core/
cp services/email-service/app/models/base.py services/email-service/app/models/outbox.py services/email-service/app/models/processed_event.py $S/app/models/
cp services/email-service/app/api/v1/router.py $S/app/api/v1/
cp services/email-service/app/api/v1/endpoints/system.py $S/app/api/v1/endpoints/
cp services/email-service/app/workers/__init__.py $S/app/workers/
cp services/email-service/tests/conftest.py $S/tests/
sed 's/email-service/telegram-service/g' services/email-service/Dockerfile > $S/Dockerfile
sed 's/app.models.email_delivery/app.models.telegram_delivery/' services/email-service/alembic/env.py > $S/alembic/env.py
```

Create `services/telegram-service/pyproject.toml`:

```toml
[project]
name = "telegram-service"
version = "1.0.0"
requires-python = ">=3.14"
dependencies = [
    "notification-shared",
    "fastapi>=0.141.1",
    "uvicorn>=0.52.4",
    "sqlalchemy>=2.0.52",
    "asyncpg>=0.31.0",
    "alembic>=1.20.0",
    "pydantic-settings>=2.15.0",
    "redis>=8.1.0",
    "httpx>=0.28.1",
]

[tool.uv.sources]
notification-shared = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["app"]
```

Run: `uv lock && uv sync --all-packages --group playground`
Expected: `telegram-service` appears in the resolution (the root workspace glob `services/*` picks it up).

- [ ] **Step 2: Write the failing tests**

Create `services/telegram-service/tests/test_routed_consumer.py`:

```python
from uuid import uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from app.core.config import Settings
from app.main import create_app
from app.models.base import Base
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.models.telegram_delivery import DeliveryStatus, TelegramDelivery
from app.senders import SimulatedSender, build_sender, build_text, delivery_mode
from app.workers.routed_consumer import RoutedConsumer
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.integration

ALEMBIC_DIR = "services/telegram-service"
DB_URL = "postgresql+asyncpg://u:p@localhost/db"


def _routed_event(channel: str = "telegram", recipient: str = "sim-e2e", subject="Hi") -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={
            "channel": channel,
            "recipient": recipient,
            "subject": subject,
            "body": "Hello there",
            "route_id": str(uuid4()),
        },
        correlation_id="corr-telegram",
    )


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def consumer(sessions, redis_client) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions,
        redis=redis_client,
        consumer_name="telegram-test",
        poll_interval_ms=10,
        delivery_latency_ms_max=0,
    )


async def _publish(redis_client, envelope: EventEnvelope) -> None:
    await RedisStreamPublisher(redis_client).publish(Stream.NOTIFICATION_ROUTED, envelope)


async def _count(sessions, model) -> int:
    async with sessions() as session:
        return await session.scalar(select(func.count()).select_from(model))


def test_build_text_joins_subject_and_body():
    assert build_text("Welcome", "Hello") == "Welcome\n\nHello"
    assert build_text(None, "Hello") == "Hello"
    assert build_text("", "Hello") == "Hello"


async def test_a_delivery_records_delivered_and_publishes_completed(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    event = _routed_event()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
        assert delivery.notification_id == event.aggregate_id
        assert delivery.chat_id == "sim-e2e"
        assert delivery.status == DeliveryStatus.DELIVERED
        assert delivery.sent_at is not None

        published = EventEnvelope.model_validate((await session.scalars(select(Outbox))).one().payload)
        assert published.event_type is EventType.DELIVERY_COMPLETED
        assert published.payload["channel"] == "telegram"
        assert published.payload["recipient"] == "sim-e2e"
        assert published.payload["delivery_id"] == str(delivery.id)
        assert published.correlation_id == "corr-telegram"


async def test_a_chat_id_containing_fail_fails_deterministically(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="sim-e2e-FAIL"))

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
        assert (delivery.status, delivery.fail_reason) == (DeliveryStatus.FAILED, "simulated_failure")
        published = EventEnvelope.model_validate((await session.scalars(select(Outbox))).one().payload)
        assert published.event_type is EventType.DELIVERY_FAILED
        assert published.payload["reason"] == "simulated_failure"


async def test_an_email_event_is_acked_skipped_and_not_recorded(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(channel="email", recipient="john@example.com"))

    assert await consumer.consume_once() == 1

    assert await _count(sessions, TelegramDelivery) == 0
    assert await _count(sessions, ProcessedEvent) == 0
    pending = await redis_client.xpending(str(Stream.NOTIFICATION_ROUTED), str(ConsumerGroup.TELEGRAM))
    assert pending["pending"] == 0


async def test_a_replayed_event_delivers_once(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    event = _routed_event()
    await _publish(redis_client, event)
    await consumer.consume_once()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1
    assert await _count(sessions, TelegramDelivery) == 1
    assert await _count(sessions, Outbox) == 1


async def test_give_up_records_a_failed_delivery_and_publishes_delivery_failed(consumer, sessions):
    event = _routed_event()
    async with sessions() as session:
        await consumer.give_up(session, event)
        await session.commit()

    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
        assert (delivery.status, delivery.fail_reason) == (DeliveryStatus.FAILED, "max_retries_exceeded")
        published = EventEnvelope.model_validate((await session.scalars(select(Outbox))).one().payload)
        assert published.payload["reason"] == "max_retries_exceeded"


async def test_consume_once_on_an_empty_stream_returns_zero(consumer):
    await consumer.ensure_groups()
    assert await consumer.consume_once() == 0


async def test_the_notification_id_unique_constraint_blocks_a_second_delivery(sessions):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(TelegramDelivery(notification_id=notification_id, chat_id="sim-a", status="DELIVERED"))
        await session.commit()
    with pytest.raises(IntegrityError):
        async with sessions() as session:
            session.add(TelegramDelivery(notification_id=notification_id, chat_id="sim-a", status="DELIVERED"))
            await session.commit()


def test_no_token_means_simulated():
    settings = Settings(database_url=DB_URL, _env_file=None)
    assert isinstance(build_sender(settings), SimulatedSender)
    assert delivery_mode(settings) == "simulated"


async def test_version_reports_the_delivery_mode():
    settings = Settings(database_url=DB_URL, _env_file=None)
    app = create_app(settings)
    app.state.settings = settings
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        body = (await client.get("/version")).json()
    assert body == {"service": "telegram-service", "version": "1.0.0", "delivery_mode": "simulated"}


def test_alembic_upgrade_head_matches_the_models(postgres_url):
    """Synchronous on purpose: alembic's env.py calls asyncio.run internally."""
    import asyncio

    from alembic.autogenerate import compare_metadata
    from alembic.runtime.migration import MigrationContext
    from sqlalchemy.ext.asyncio import create_async_engine

    config = Config(f"{ALEMBIC_DIR}/alembic.ini")
    config.set_main_option("script_location", f"{ALEMBIC_DIR}/alembic")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.upgrade(config, "head")
    try:

        def _compare(sync_connection):
            return compare_metadata(MigrationContext.configure(sync_connection), Base.metadata)

        async def _diff():
            engine = create_async_engine(postgres_url)
            async with engine.connect() as conn:
                result = await conn.run_sync(_compare)
            await engine.dispose()
            return result

        assert asyncio.run(_diff()) == []
    finally:
        command.downgrade(config, "base")
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `PYTHONPATH=services/telegram-service uv run pytest services/telegram-service/tests -v 2>&1 | tee /tmp/task-8-red.txt`
Expected: FAIL at collection — `ModuleNotFoundError: No module named 'app.core.config'` (or `app.models.telegram_delivery`).

- [ ] **Step 4: Write the implementation**

`services/telegram-service/app/models/telegram_delivery.py`:

```python
from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from app.models.base import Base
from notification_shared.models import TimestampMixin
from sqlalchemy import DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column


class DeliveryStatus(StrEnum):
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"


class TelegramDelivery(Base, TimestampMixin):
    """Inserted once, already terminal (ADR 0022)."""

    __tablename__ = "telegram_delivery"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    notification_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    chat_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("notification_id", name="uq_telegram_delivery_notification_id"),
    )
```

`services/telegram-service/app/repositories/telegram_delivery.py`:

```python
from uuid import UUID

from app.models.telegram_delivery import TelegramDelivery
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


class TelegramDeliveryRepository:
    async def add(self, session: AsyncSession, delivery: TelegramDelivery) -> None:
        session.add(delivery)

    async def get_by_notification(
        self, session: AsyncSession, notification_id: UUID
    ) -> TelegramDelivery | None:
        return await session.scalar(
            select(TelegramDelivery).where(TelegramDelivery.notification_id == notification_id)
        )
```

`services/telegram-service/alembic/versions/0001_create_telegram_tables.py`:

```python
"""create telegram tables

Revision ID: 0001
Revises:
Create Date: 2026-09-24 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "telegram_delivery",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("notification_id", sa.UUID(), nullable=False),
        sa.Column("chat_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("fail_reason", sa.String(length=100), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("notification_id", name="uq_telegram_delivery_notification_id"),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("stream", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("published", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_outbox_pending", "outbox", ["published", "created_at"], unique=False)
    op.create_table(
        "processed_events",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("event_id", sa.UUID(), nullable=False),
        sa.Column("consumer_group", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("fail_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "processed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id", "consumer_group", name="uq_processed_events_event_group"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("processed_events")
    op.drop_index("ix_outbox_pending", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("telegram_delivery")
```

`services/telegram-service/app/core/config.py`:

```python
from notification_shared.config import ConsumerServiceSettings


class Settings(ConsumerServiceSettings):
    service_name: str = "telegram-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    delivery_latency_ms_max: int = 500
    http_timeout_seconds: float = 5.0
    # Empty means simulated delivery. The recipient is the chat id (ADR 0026).
    telegram_bot_token: str = ""
```

`services/telegram-service/app/senders.py`:

```python
"""Telegram senders.

The consumer applies the shared rules first — failure marker, then reserved
`sim-` chat ids (slice 2 spec section 3) — and only then calls one of these.
The notification's recipient is the chat id (ADR 0026).
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Protocol

logger = logging.getLogger(__name__)

TELEGRAM_REJECTED = "telegram_rejected"


class TelegramRejectedError(Exception):
    """Permanent: the Bot API refused this chat or message (400/403)."""


class TelegramUnavailableError(Exception):
    """Transient: worth retrying. The message stays pending for recovery."""


class TelegramSender(Protocol):
    mode: str

    async def send(self, chat_id: str, text: str) -> None: ...

    async def aclose(self) -> None: ...


class SimulatedSender:
    mode = "simulated"

    def __init__(self, latency_ms_max: int = 500) -> None:
        self._latency_ms_max = latency_ms_max

    async def send(self, chat_id: str, text: str) -> None:
        logger.info("simulating telegram message to %s", chat_id)
        if self._latency_ms_max:
            await asyncio.sleep(random.uniform(0, self._latency_ms_max) / 1000)

    async def aclose(self) -> None:
        return None


def build_text(subject: str | None, body: str) -> str:
    return f"{subject}\n\n{body}" if subject else body


def build_sender(settings, transport=None) -> TelegramSender:
    """Task 9 adds the Bot API sender; until then delivery is always simulated."""
    return SimulatedSender(settings.delivery_latency_ms_max)


def delivery_mode(settings) -> str:
    return SimulatedSender.mode
```

Replace the `version` endpoint in `services/telegram-service/app/api/v1/endpoints/system.py` with (and add `from app.senders import delivery_mode`):

```python
@router.get("/version", summary="Service version and delivery mode")
async def version(request: Request) -> dict:
    settings = request.app.state.settings
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "delivery_mode": delivery_mode(settings),
    }
```

`services/telegram-service/app/workers/routed_consumer.py`:

```python
"""Consumes notification.routed and delivers Telegram messages.

Mirrors email-service's consumer. Before any send, the shared rules apply in
order (slice 2 spec section 3): a chat id containing "fail" fails
deterministically, a `sim-` chat id is always simulated, everything else goes
to the configured sender.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from uuid import uuid4

from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.models.telegram_delivery import DeliveryStatus, TelegramDelivery
from app.repositories.telegram_delivery import TelegramDeliveryRepository
from app.senders import (
    TELEGRAM_REJECTED,
    SimulatedSender,
    TelegramRejectedError,
    TelegramSender,
    build_text,
)
from notification_shared.context import set_correlation_id
from notification_shared.delivery import (
    SIMULATED_FAILURE,
    is_failure_recipient,
    is_reserved_recipient,
)
from notification_shared.events import Channel, ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.outbox import OutboxRepository
from notification_shared.recovery import MAX_RETRIES_EXCEEDED
from notification_shared.streams import RedisStreamConsumer
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

STREAM = Stream.NOTIFICATION_ROUTED
GROUP = ConsumerGroup.TELEGRAM


class RoutedConsumer:
    def __init__(
        self,
        *,
        session_factory,
        redis,
        consumer_name: str,
        poll_interval_ms: int = 500,
        delivery_latency_ms_max: int = 500,
        sender: TelegramSender | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._deliveries = TelegramDeliveryRepository()
        self._outbox = OutboxRepository(Outbox)
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms
        # Reserved chat ids always use the simulated sender, whatever is configured.
        self._simulated = SimulatedSender(delivery_latency_ms_max)
        self._sender = sender or self._simulated

    async def ensure_groups(self) -> None:
        await self._consumer.ensure_group(STREAM, GROUP)

    async def consume_once(self) -> int:
        messages = await self._consumer.read(STREAM, GROUP, block_ms=self._poll_interval_ms)
        acked = 0
        for message in messages:
            if await self.handle(message.envelope):
                await self._consumer.ack(STREAM, GROUP, message.message_id)
                acked += 1
        return acked

    async def run_forever(self) -> None:
        while True:
            try:
                await self.consume_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("telegram consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        if envelope.payload.get("channel") != Channel.TELEGRAM:
            # Both delivery services read this stream; each handles its own
            # channel. Acking without recording anything is correct.
            logger.debug("not a telegram event, acking and skipping", extra=log_fields)
            return True

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                logger.debug("event already processed, acking", extra=log_fields)
                return True

        chat_id = envelope.payload["recipient"]
        text = build_text(envelope.payload.get("subject"), envelope.payload["body"])
        fail_reason = await self._deliver(chat_id, text)
        delivered_at = datetime.now(UTC)

        async with self._session_factory() as session:
            await self._record_outcome(session, envelope, fail_reason, sent_at=delivered_at)
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        logger.info(
            "telegram delivered" if fail_reason is None else "telegram delivery failed",
            extra=log_fields,
        )
        return True

    async def give_up(self, session: AsyncSession, envelope: EventEnvelope) -> None:
        """PendingRecoverer hook after PENDING_MAX_RETRIES failed attempts.

        Records a FAILED delivery and publishes DeliveryFailed (ADR 0024). No
        Bot API call: give_up must not fail for the reason handle did.
        """
        await self._record_outcome(session, envelope, MAX_RETRIES_EXCEEDED, sent_at=None)

    async def _deliver(self, chat_id: str, text: str) -> str | None:
        """Returns None on success, or a permanent failure reason.

        Raises TelegramUnavailableError for a transient failure: nothing is
        written, the message stays pending, PendingRecoverer retries it.
        """
        if is_failure_recipient(chat_id):
            return SIMULATED_FAILURE
        reserved = is_reserved_recipient(Channel.TELEGRAM, chat_id)
        sender = self._simulated if reserved else self._sender
        try:
            await sender.send(chat_id, text)
        except TelegramRejectedError as exc:
            logger.warning("telegram message rejected: %s", exc)
            return TELEGRAM_REJECTED
        return None

    async def _record_outcome(
        self,
        session: AsyncSession,
        envelope: EventEnvelope,
        fail_reason: str | None,
        sent_at: datetime | None,
    ) -> None:
        """The delivery row, inserted already terminal, and its outbox event.
        The caller owns the transaction."""
        chat_id = envelope.payload["recipient"]
        delivery = TelegramDelivery(
            id=uuid4(),
            notification_id=envelope.aggregate_id,
            chat_id=chat_id,
            status=(
                DeliveryStatus.DELIVERED.value if fail_reason is None else DeliveryStatus.FAILED.value
            ),
            fail_reason=fail_reason,
            sent_at=sent_at,
        )
        await self._deliveries.add(session, delivery)
        await session.flush()

        if fail_reason is None:
            await self._outbox.save(
                session,
                Stream.DELIVERY_COMPLETED,
                EventEnvelope.new(
                    event_type=EventType.DELIVERY_COMPLETED,
                    aggregate_id=envelope.aggregate_id,
                    payload={
                        "channel": Channel.TELEGRAM.value,
                        "delivery_id": str(delivery.id),
                        "recipient": chat_id,
                        "delivered_at": sent_at.isoformat(),
                    },
                    correlation_id=envelope.correlation_id,
                ),
            )
        else:
            await self._outbox.save(
                session,
                Stream.DELIVERY_FAILED,
                EventEnvelope.new(
                    event_type=EventType.DELIVERY_FAILED,
                    aggregate_id=envelope.aggregate_id,
                    payload={
                        "channel": Channel.TELEGRAM.value,
                        "delivery_id": str(delivery.id),
                        "reason": fail_reason,
                    },
                    correlation_id=envelope.correlation_id,
                ),
            )
```

`services/telegram-service/app/main.py`:

```python
"""Telegram Service.

Delivers Telegram notifications. No REST endpoints for domain operations: its
whole job runs in background workers reacting to notification.routed.
"""

from __future__ import annotations

import asyncio
import logging
import socket
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from app.api.v1.router import router
from app.core.config import Settings
from app.core.database import Database
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.senders import build_sender
from app.workers.routed_consumer import RoutedConsumer
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from notification_shared.events import ConsumerGroup, Stream
from notification_shared.exceptions import ServiceError, ValidationFailedError
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.logging import configure_logging
from notification_shared.middleware import CorrelationIDMiddleware
from notification_shared.outbox import OutboxRepository
from notification_shared.publisher import OutboxPublisher
from notification_shared.recovery import PendingRecoverer
from notification_shared.streams import RedisStreamPublisher
from starlette.requests import Request

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.db = Database(settings.database_url)
        app.state.redis = aioredis.from_url(settings.redis_url)
        await app.state.redis.ping()

        sender = build_sender(settings)
        logger.info("telegram delivery mode: %s", sender.mode)
        consumer = RoutedConsumer(
            session_factory=app.state.db.session_factory,
            redis=app.state.redis,
            consumer_name=socket.gethostname(),
            poll_interval_ms=settings.consumer_poll_interval_ms,
            delivery_latency_ms_max=settings.delivery_latency_ms_max,
            sender=sender,
        )
        await consumer.ensure_groups()

        publisher = OutboxPublisher(
            session_factory=app.state.db.session_factory,
            repository=OutboxRepository(Outbox),
            publisher=RedisStreamPublisher(app.state.redis),
            poll_interval_ms=settings.outbox_poll_interval_ms,
            batch_size=settings.outbox_batch_size,
        )
        recoverer = PendingRecoverer(
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-recovery",
            session_factory=app.state.db.session_factory,
            idempotency=IdempotencyRepository(ProcessedEvent),
            pending_timeout_ms=settings.pending_timeout_ms,
            max_retries=settings.pending_max_retries,
            poll_interval_ms=settings.recovery_poll_interval_ms,
        )
        recoverer.register(Stream.NOTIFICATION_ROUTED, ConsumerGroup.TELEGRAM, consumer)

        tasks = [
            asyncio.create_task(consumer.run_forever(), name="routed-consumer"),
            asyncio.create_task(publisher.run_forever(), name="outbox-publisher"),
            asyncio.create_task(recoverer.run_forever(), name="pending-recoverer"),
        ]

        yield

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await sender.aclose()
        await app.state.redis.aclose()
        await app.state.db.dispose()

    app = FastAPI(title="Telegram Service", version=settings.service_version, lifespan=lifespan)
    app.add_middleware(CorrelationIDMiddleware)
    app.include_router(router)

    @app.exception_handler(ServiceError)
    async def handle_service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code, content=exc.to_response().model_dump(mode="json")
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(
        _request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error = ValidationFailedError(str(exc.errors()))
        return JSONResponse(
            status_code=error.status_code,
            content=error.to_response().model_dump(mode="json"),
        )

    return app
```

- [ ] **Step 5: Run the service suite**

Run: `PYTHONPATH=services/telegram-service uv run pytest services/telegram-service/tests -v`
Expected: PASS.

- [ ] **Step 6: Lint, format, commit**

```bash
uv run ruff check --fix services/telegram-service && uv run ruff format services/telegram-service
git add services/telegram-service uv.lock
git commit -m "feat(telegram): add telegram-service with simulated delivery

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 9: Telegram Service — Bot API sender

**Files:**
- Modify: `services/telegram-service/app/senders.py`
- Test: `services/telegram-service/tests/test_bot_api.py` (new)

**Interfaces:**
- Consumes: Task 8's `app.senders` and `RoutedConsumer`.
- Produces:
  - `TELEGRAM_API_BASE_URL = "https://api.telegram.org"`
  - `classify_response(status_code: int, ok: bool) -> str` returning `"delivered" | "rejected" | "transient"`
  - `BotApiSender(token: str, *, timeout: float = 5.0, transport: httpx.AsyncBaseTransport | None = None)` (`mode = "bot_api"`)
  - `build_sender(settings, transport=None)` returns `BotApiSender` when `telegram_bot_token` is set; `delivery_mode(settings)` returns `"bot_api"` then

- [ ] **Step 1: Write the failing tests**

Create `services/telegram-service/tests/test_bot_api.py`:

```python
import json
import logging
from uuid import uuid4

import httpx
import pytest
from app.core.config import Settings
from app.models.base import Base
from app.models.telegram_delivery import DeliveryStatus, TelegramDelivery
from app.senders import (
    BotApiSender,
    TelegramRejectedError,
    TelegramUnavailableError,
    build_sender,
    classify_response,
    delivery_mode,
)
from app.workers.routed_consumer import RoutedConsumer
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import func, select

TOKEN = "123456:SECRET-token-value"
DB_URL = "postgresql+asyncpg://u:p@localhost/db"


class FakeBotApi:
    """httpx.MockTransport handler that records every call."""

    def __init__(self, status: int = 200, ok: bool = True, error: Exception | None = None):
        self.status, self.ok, self.error = status, ok, error
        self.calls: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.error:
            raise self.error
        return httpx.Response(self.status, json={"ok": self.ok, "description": "test"})


def _sender(api: FakeBotApi) -> BotApiSender:
    return BotApiSender(TOKEN, timeout=1.0, transport=httpx.MockTransport(api))


@pytest.mark.parametrize(
    ("status", "ok", "expected"),
    [
        (200, True, "delivered"),
        (200, False, "transient"),
        (400, False, "rejected"),
        (403, False, "rejected"),
        (429, False, "transient"),
        (500, False, "transient"),
        (502, False, "transient"),
        (418, False, "transient"),
    ],
)
def test_classify_response(status, ok, expected):
    assert classify_response(status, ok) == expected


async def test_a_successful_send_posts_to_send_message():
    api = FakeBotApi()
    await _sender(api).send("-1001234567890", "Hello")

    (request,) = api.calls
    assert request.method == "POST"
    assert request.url.host == "api.telegram.org"
    assert request.url.path == f"/bot{TOKEN}/sendMessage"
    # A negative group chat id is passed through as the same string.
    assert json.loads(request.content) == {"chat_id": "-1001234567890", "text": "Hello"}


@pytest.mark.parametrize("status", [400, 403])
async def test_a_refusal_is_permanent(status):
    with pytest.raises(TelegramRejectedError):
        await _sender(FakeBotApi(status=status, ok=False)).send("42", "Hi")


@pytest.mark.parametrize(
    "api",
    [
        FakeBotApi(status=429, ok=False),
        FakeBotApi(status=500, ok=False),
        FakeBotApi(error=httpx.ReadTimeout("timed out")),
        FakeBotApi(error=httpx.ConnectError("refused")),
    ],
)
async def test_rate_limits_server_errors_and_network_failures_are_transient(api):
    with pytest.raises(TelegramUnavailableError):
        await _sender(api).send("42", "Hi")


async def test_the_token_never_reaches_logs_or_exception_text(caplog):
    caplog.set_level(logging.DEBUG)
    await _sender(FakeBotApi()).send("42", "Hi")
    for api in (FakeBotApi(status=400, ok=False), FakeBotApi(error=httpx.ReadTimeout("t"))):
        with pytest.raises(Exception) as excinfo:
            await _sender(api).send("42", "Hi")
        assert TOKEN not in str(excinfo.value)
        assert excinfo.value.__cause__ is None
    assert TOKEN not in caplog.text


def test_a_token_means_bot_api():
    settings = Settings(database_url=DB_URL, telegram_bot_token=TOKEN, _env_file=None)
    assert isinstance(build_sender(settings), BotApiSender)
    assert delivery_mode(settings) == "bot_api"


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


def _consumer(sessions, redis_client, api: FakeBotApi) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions,
        redis=redis_client,
        consumer_name="telegram-bot",
        poll_interval_ms=10,
        delivery_latency_ms_max=0,
        sender=_sender(api),
    )


async def _publish_routed(redis_client, chat_id: str) -> None:
    envelope = EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={"channel": "telegram", "recipient": chat_id, "subject": None, "body": "Hi", "route_id": str(uuid4())},
        correlation_id="corr-bot",
    )
    await RedisStreamPublisher(redis_client).publish(Stream.NOTIFICATION_ROUTED, envelope)


@pytest.mark.integration
async def test_a_sim_chat_id_never_reaches_the_bot_api(sessions, redis_client):
    api = FakeBotApi()
    consumer = _consumer(sessions, redis_client, api)
    await consumer.ensure_groups()
    await _publish_routed(redis_client, "sim-e2e")

    assert await consumer.consume_once() == 1
    assert api.calls == []


@pytest.mark.integration
async def test_a_real_chat_id_is_sent_through_the_bot_api(sessions, redis_client):
    api = FakeBotApi()
    consumer = _consumer(sessions, redis_client, api)
    await consumer.ensure_groups()
    await _publish_routed(redis_client, "123456789")

    assert await consumer.consume_once() == 1
    assert len(api.calls) == 1
    async with sessions() as session:
        assert (await session.scalars(select(TelegramDelivery))).one().status == DeliveryStatus.DELIVERED


@pytest.mark.integration
async def test_a_refused_chat_fails_with_telegram_rejected(sessions, redis_client):
    consumer = _consumer(sessions, redis_client, FakeBotApi(status=400, ok=False))
    await consumer.ensure_groups()
    await _publish_routed(redis_client, "123456789")

    await consumer.consume_once()
    async with sessions() as session:
        delivery = (await session.scalars(select(TelegramDelivery))).one()
    assert (delivery.status, delivery.fail_reason) == (DeliveryStatus.FAILED, "telegram_rejected")


@pytest.mark.integration
async def test_a_transient_failure_writes_nothing_and_stays_pending(sessions, redis_client):
    consumer = _consumer(sessions, redis_client, FakeBotApi(status=500, ok=False))
    await consumer.ensure_groups()
    await _publish_routed(redis_client, "123456789")

    with pytest.raises(TelegramUnavailableError):
        await consumer.consume_once()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(TelegramDelivery)) == 0
    pending = await redis_client.xpending(str(Stream.NOTIFICATION_ROUTED), str(ConsumerGroup.TELEGRAM))
    assert pending["pending"] == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=services/telegram-service uv run pytest services/telegram-service/tests/test_bot_api.py -v 2>&1 | tee /tmp/task-9-red.txt`
Expected: FAIL — `ImportError: cannot import name 'BotApiSender' from 'app.senders'`.

- [ ] **Step 3: Write the implementation**

In `services/telegram-service/app/senders.py`, add `import httpx` to the imports, add after `TELEGRAM_REJECTED`:

```python
TELEGRAM_API_BASE_URL = "https://api.telegram.org"
```

replace `build_sender` and `delivery_mode` with the versions below, and add `classify_response`, `silence_client_logs` and `BotApiSender` before them:

```python
def classify_response(status_code: int, ok: bool) -> str:
    """Spec 4.5: 400/403 are permanent; 429, 5xx and anything unexpected are
    transient and left to PendingRecoverer."""
    if status_code == 200 and ok:
        return "delivered"
    if status_code in (400, 403):
        return "rejected"
    return "transient"


def silence_client_logs() -> None:
    """httpx logs every request URL at INFO, and the URL carries the token."""
    for name in ("httpx", "httpcore"):
        logging.getLogger(name).setLevel(logging.WARNING)


class BotApiSender:
    mode = "bot_api"

    def __init__(
        self,
        token: str,
        *,
        timeout: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        silence_client_logs()
        self._path = f"/bot{token}/sendMessage"
        self._client = httpx.AsyncClient(
            base_url=TELEGRAM_API_BASE_URL, timeout=timeout, transport=transport
        )

    async def send(self, chat_id: str, text: str) -> None:
        logger.info("sending telegram message to %s via the Bot API", chat_id)
        # `from None` throughout: a chained httpx exception can carry the
        # request URL, and with it the token, into a logged traceback.
        try:
            response = await self._client.post(self._path, json={"chat_id": chat_id, "text": text})
        except httpx.TimeoutException:
            raise TelegramUnavailableError("Bot API timed out") from None
        except httpx.TransportError as exc:
            raise TelegramUnavailableError(f"Bot API unreachable ({type(exc).__name__})") from None

        try:
            ok = bool(response.json().get("ok"))
        except ValueError:
            ok = False
        outcome = classify_response(response.status_code, ok)
        if outcome == "rejected":
            raise TelegramRejectedError(f"Bot API refused the message ({response.status_code})")
        if outcome == "transient":
            raise TelegramUnavailableError(f"Bot API returned {response.status_code}")

    async def aclose(self) -> None:
        await self._client.aclose()


def build_sender(settings, transport=None) -> TelegramSender:
    if not settings.telegram_bot_token:
        return SimulatedSender(settings.delivery_latency_ms_max)
    return BotApiSender(
        settings.telegram_bot_token, timeout=settings.http_timeout_seconds, transport=transport
    )


def delivery_mode(settings) -> str:
    """The configured mode. `sim-` chat ids are simulated regardless."""
    return BotApiSender.mode if settings.telegram_bot_token else SimulatedSender.mode
```

- [ ] **Step 4: Run the service suite**

Run: `PYTHONPATH=services/telegram-service uv run pytest services/telegram-service/tests -v`
Expected: PASS.

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix services/telegram-service && uv run ruff format services/telegram-service
git add services/telegram-service
git commit -m "feat(telegram): add Bot API delivery with token hygiene

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 10: API Gateway

**Files:**
- Create: `gateway/pyproject.toml`, `gateway/Dockerfile`, `gateway/entrypoint.sh`, `gateway/app/main.py`, `gateway/app/core/config.py`, `gateway/app/api/v1/router.py`, `gateway/app/api/v1/proxy.py`, `gateway/app/api/v1/system.py`
- Modify: root `pyproject.toml` (workspace members), `services/*/Dockerfile` (all five)
- Test: `gateway/tests/test_proxy.py`

**Interfaces:**
- Consumes: `CorrelationIDMiddleware`, `CORRELATION_ID_HEADER`, `GatewayTimeoutError`, `BadGatewayError`, `BaseServiceSettings`, `configure_logging`.
- Produces: `create_app(settings: Settings | None = None, transport: httpx.AsyncBaseTransport | None = None) -> FastAPI`; routes of spec 5.2; `Settings` with `notification_service_url`, `configuration_service_url`, `gateway_timeout_seconds`.

- [ ] **Step 1: Add the workspace member**

In the root `pyproject.toml` change the members line to:

```toml
members = ["gateway", "services/*", "shared"]
```

Create `gateway/pyproject.toml`:

```toml
[project]
name = "gateway"
version = "1.0.0"
requires-python = ">=3.14"
dependencies = [
    "notification-shared",
    "fastapi>=0.141.1",
    "uvicorn>=0.52.4",
    "httpx>=0.28.1",
    "pydantic-settings>=2.15.0",
]

[tool.uv.sources]
notification-shared = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["app"]
```

Run: `mkdir -p gateway/app/core gateway/app/api/v1 gateway/tests && uv lock && uv sync --all-packages --group playground`
Expected: `gateway` resolves.

- [ ] **Step 2: Write the failing tests**

Create `gateway/tests/test_proxy.py`:

```python
import json
from contextlib import asynccontextmanager

import httpx
import pytest
from app.core.config import Settings
from app.main import create_app

NOTIFICATION = "http://notification-service:8000"
CONFIGURATION = "http://configuration-service:8000"


class Downstream:
    """MockTransport handler standing in for both downstream services."""

    def __init__(self, respond=None, error: Exception | None = None) -> None:
        self.calls: list[httpx.Request] = []
        self.respond = respond
        self.error = error

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.error:
            raise self.error
        if self.respond:
            return self.respond(request)
        return httpx.Response(200, json={"ok": True})


@asynccontextmanager
async def gateway(downstream: Downstream):
    app = create_app(Settings(_env_file=None), transport=httpx.MockTransport(downstream))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://gateway"
        ) as client:
            yield client


@pytest.mark.parametrize(
    ("method", "path", "body", "expected_url"),
    [
        ("POST", "/api/v1/notifications", {"channel": "email"}, f"{NOTIFICATION}/notifications"),
        ("GET", "/api/v1/notifications/abc", None, f"{NOTIFICATION}/notifications/abc"),
        ("GET", "/api/v1/notifications", None, f"{NOTIFICATION}/notifications"),
        ("GET", "/api/v1/channels", None, f"{CONFIGURATION}/channels"),
        ("PUT", "/api/v1/channels/email", {"enabled": False}, f"{CONFIGURATION}/channels/email"),
    ],
)
async def test_each_route_forwards_to_its_service(method, path, body, expected_url):
    downstream = Downstream()
    async with gateway(downstream) as client:
        response = await client.request(method, path, json=body)

    assert response.status_code == 200
    (request,) = downstream.calls
    assert request.method == method
    assert str(request.url) == expected_url
    if body is not None:
        assert json.loads(request.content) == body
        assert request.headers["content-type"] == "application/json"


async def test_repeated_query_parameters_are_forwarded_unchanged():
    """Review Focus 5."""
    downstream = Downstream()
    async with gateway(downstream) as client:
        await client.get("/api/v1/notifications?status=FAILED&status=CREATED&limit=5")

    assert downstream.calls[0].url.params.multi_items() == [
        ("status", "FAILED"),
        ("status", "CREATED"),
        ("limit", "5"),
    ]


async def test_the_client_host_header_is_not_forwarded():
    downstream = Downstream()
    async with gateway(downstream) as client:
        await client.get("/api/v1/channels")
    assert downstream.calls[0].headers["host"] == "configuration-service:8000"


async def test_a_correlation_id_is_generated_when_absent_and_sent_downstream():
    downstream = Downstream()
    async with gateway(downstream) as client:
        response = await client.get("/api/v1/channels")

    generated = response.headers["X-Correlation-ID"]
    assert generated
    assert downstream.calls[0].headers.get_list("x-correlation-id") == [generated]


async def test_a_client_correlation_id_is_propagated_exactly_once():
    downstream = Downstream()
    async with gateway(downstream) as client:
        response = await client.get("/api/v1/channels", headers={"X-Correlation-ID": "corr-gw"})

    assert response.headers["X-Correlation-ID"] == "corr-gw"
    assert downstream.calls[0].headers.get_list("x-correlation-id") == ["corr-gw"]


@pytest.mark.parametrize(
    ("status", "content_type", "content"),
    [
        (404, "application/json", b'{"error":{"code":"NOT_FOUND","message":"x"}}'),
        (422, "application/json", b'{"error":{"code":"VALIDATION_ERROR","message":"x"}}'),
        (500, "text/plain; charset=utf-8", b"Internal Server Error"),
    ],
)
async def test_downstream_errors_pass_through_unchanged(status, content_type, content):
    """Review Focus 5: including a non-JSON body."""
    downstream = Downstream(
        respond=lambda _r: httpx.Response(
            status, content=content, headers={"content-type": content_type}
        )
    )
    async with gateway(downstream) as client:
        response = await client.get("/api/v1/notifications/abc")

    assert response.status_code == status
    assert response.content == content
    assert response.headers["content-type"] == content_type


async def test_a_downstream_timeout_is_a_504():
    async with gateway(Downstream(error=httpx.ReadTimeout("slow"))) as client:
        response = await client.get("/api/v1/notifications")

    assert response.status_code == 504
    assert response.json()["error"]["code"] == "GATEWAY_TIMEOUT"


async def test_an_unreachable_downstream_is_a_502():
    async with gateway(Downstream(error=httpx.ConnectError("refused"))) as client:
        response = await client.post("/api/v1/notifications", json={"channel": "email"})

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "BAD_GATEWAY"


async def test_health_reports_only_the_gateway():
    downstream = Downstream()
    async with gateway(downstream) as client:
        response = await client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {"status": "UP", "service": "gateway"}
    assert downstream.calls == []
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `PYTHONPATH=gateway uv run pytest gateway/tests -v 2>&1 | tee /tmp/task-10-red.txt`
Expected: FAIL at collection — `ModuleNotFoundError: No module named 'app.core'`.

- [ ] **Step 4: Write the implementation**

`gateway/app/core/config.py`:

```python
from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "gateway"
    notification_service_url: str = "http://notification-service:8000"
    configuration_service_url: str = "http://configuration-service:8000"
    gateway_timeout_seconds: float = 10.0
```

`gateway/app/api/v1/proxy.py`:

```python
"""Thin pass-through proxy (slice 2 spec 5.3).

Explicit routes rather than a catch-all, so the Gateway's Swagger lists exactly
what it exposes. No validation, no payload transformation, no knowledge of
channels: status, body and content-type come back exactly as the service sent
them.
"""

from __future__ import annotations

import httpx
from fastapi import APIRouter
from notification_shared.exceptions import BadGatewayError, GatewayTimeoutError
from notification_shared.middleware import CORRELATION_ID_HEADER
from starlette.requests import Request
from starlette.responses import Response

router = APIRouter(prefix="/api/v1", tags=["proxy"])

# Hop-by-hop headers describe this connection, not the request; the correlation
# header is re-added once below so it can never be sent twice.
_NOT_FORWARDED = {
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "keep-alive",
    "upgrade",
    "te",
    "trailer",
    "proxy-authorization",
    "proxy-authenticate",
    CORRELATION_ID_HEADER.lower(),
}


async def _forward(request: Request, base_url: str, path: str) -> Response:
    client: httpx.AsyncClient = request.app.state.http
    headers = {k: v for k, v in request.headers.items() if k.lower() not in _NOT_FORWARDED}
    headers[CORRELATION_ID_HEADER] = request.state.correlation_id
    try:
        upstream = await client.request(
            request.method,
            f"{base_url}{path}",
            params=request.query_params.multi_items(),
            content=await request.body(),
            headers=headers,
        )
    except httpx.TimeoutException:
        raise GatewayTimeoutError(f"{request.method} {path} timed out") from None
    except httpx.TransportError:
        raise BadGatewayError(f"{request.method} {path}: service unreachable") from None
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type"),
    )


def _notifications(request: Request) -> str:
    return request.app.state.settings.notification_service_url


def _configuration(request: Request) -> str:
    return request.app.state.settings.configuration_service_url


@router.post("/notifications", summary="Submit a notification")
async def create_notification(request: Request) -> Response:
    return await _forward(request, _notifications(request), "/notifications")


@router.get("/notifications/{notification_id}", summary="Read notification status")
async def get_notification(notification_id: str, request: Request) -> Response:
    return await _forward(request, _notifications(request), f"/notifications/{notification_id}")


@router.get("/notifications", summary="List notifications")
async def list_notifications(request: Request) -> Response:
    return await _forward(request, _notifications(request), "/notifications")


@router.get("/channels", summary="List channels")
async def list_channels(request: Request) -> Response:
    return await _forward(request, _configuration(request), "/channels")


@router.put("/channels/{name}", summary="Enable or disable a channel")
async def set_channel(name: str, request: Request) -> Response:
    return await _forward(request, _configuration(request), f"/channels/{name}")
```

`gateway/app/api/v1/system.py`:

```python
from fastapi import APIRouter
from starlette.requests import Request

router = APIRouter(prefix="/api/v1", tags=["system"])


@router.get("/health", summary="The Gateway's own health; does not aggregate services")
async def health(request: Request) -> dict:
    return {"status": "UP", "service": request.app.state.settings.service_name}
```

`gateway/app/api/v1/router.py`:

```python
from app.api.v1 import proxy, system
from fastapi import APIRouter

router = APIRouter()
router.include_router(system.router)
router.include_router(proxy.router)
```

`gateway/app/main.py`:

```python
"""API Gateway.

The single client entry point: a thin FastAPI reverse proxy over
notification-service and configuration-service (slice 2 spec section 5). No
database, no Redis, no business logic.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from app.api.v1.router import router
from app.core.config import Settings
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from notification_shared.exceptions import ServiceError
from notification_shared.logging import configure_logging
from notification_shared.middleware import CorrelationIDMiddleware
from starlette.requests import Request


def create_app(
    settings: Settings | None = None, transport: httpx.AsyncBaseTransport | None = None
) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.http = httpx.AsyncClient(
            timeout=settings.gateway_timeout_seconds, transport=transport
        )
        yield
        await app.state.http.aclose()

    app = FastAPI(title="API Gateway", version=settings.service_version, lifespan=lifespan)
    app.add_middleware(CorrelationIDMiddleware)
    app.include_router(router)

    @app.exception_handler(ServiceError)
    async def handle_service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code, content=exc.to_response().model_dump(mode="json")
        )

    return app
```

`gateway/entrypoint.sh`:

```sh
#!/bin/sh
set -e
exec /app/.venv/bin/uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8000
```

`gateway/Dockerfile`:

```dockerfile
FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY shared/ ./shared/
COPY gateway/ ./gateway/
RUN uv sync --frozen --no-dev --no-editable --package gateway

FROM python:3.14-slim AS runtime
RUN adduser --disabled-password appuser && apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder --chown=appuser /app/.venv /app/.venv
COPY --chown=appuser gateway/ /app/
RUN chmod +x /app/entrypoint.sh
USER appuser
EXPOSE 8000
ENTRYPOINT ["/app/entrypoint.sh"]
```

`gateway` is now an **explicitly named** workspace member, and uv refuses to resolve the workspace if an explicitly named member has no `pyproject.toml`. Every service image copies only `shared/` and its own directory, so add this line to all five service Dockerfiles (`services/{notification,routing,configuration,email,telegram}-service/Dockerfile`), right after `COPY shared/ ./shared/`:

```dockerfile
# The root workspace names gateway explicitly; uv needs its manifest to resolve.
COPY gateway/pyproject.toml ./gateway/pyproject.toml
```

- [ ] **Step 5: Run the tests and check the images build**

Run: `PYTHONPATH=gateway uv run pytest gateway/tests -v`
Expected: PASS.

Run: `docker compose build email-service`
Expected: builds. (The gateway image is built in Task 11 once compose knows it.)

- [ ] **Step 6: Lint, format, commit**

```bash
uv run ruff check --fix gateway && uv run ruff format gateway
git add gateway pyproject.toml uv.lock services/*/Dockerfile
git commit -m "feat(gateway): add FastAPI reverse proxy with 504 and 502 handling

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 11: Compose, secrets template, editor config, end-to-end tests

**Files:**
- Modify: `docker-compose.yml`, `.vscode/launch.json`, `.vscode/tasks.json`, `tests/e2e/conftest.py`
- Create: `.env.example`, `tests/e2e/test_slice2.py`

**Interfaces:**
- Consumes: every earlier task.
- Produces: a twelve-container stack; e2e fixtures `gateway` (an `httpx.AsyncClient` whose base URL is `{GATEWAY_URL}/api/v1`) and `telegram_enabled`; `GATEWAY_URL`/`TELEGRAM_URL` env overrides.

- [ ] **Step 1: Extend compose**

In `docker-compose.yml`, add to `email-service.environment`:

```yaml
      # Real SMTP only when SMTP_HOST is set in a local .env (see .env.example).
      SMTP_HOST: ${SMTP_HOST:-}
      SMTP_PORT: ${SMTP_PORT:-587}
      SMTP_USERNAME: ${SMTP_USERNAME:-}
      SMTP_PASSWORD: ${SMTP_PASSWORD:-}
      SMTP_FROM: ${SMTP_FROM:-}
      SMTP_SECURITY: ${SMTP_SECURITY:-starttls}
```

Add these services before `networks:`:

```yaml
  telegram-db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: notif
      POSTGRES_PASSWORD: notif
      POSTGRES_DB: telegramdb
    ports: ["5437:5432"]
    volumes: ["telegram-data:/var/lib/postgresql/data"]
    networks: [notification-net]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U notif -d telegramdb"]
      interval: 10s
      timeout: 5s
      retries: 5

  telegram-service:
    build:
      context: .
      dockerfile: services/telegram-service/Dockerfile
    environment:
      DATABASE_URL: postgresql+asyncpg://notif:notif@telegram-db:5432/telegramdb
      REDIS_URL: redis://redis:6379/0
      SERVICE_NAME: telegram-service
      LOG_LEVEL: INFO
      # Same reason as email-service: PROCESSING must be observable in e2e.
      DELIVERY_LATENCY_MS_MAX: "2000"
      # Real Bot API only when set in a local .env (see .env.example).
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN:-}
    ports: ["8005:8000"]
    networks: [notification-net]
    restart: on-failure
    depends_on:
      telegram-db: { condition: service_healthy }
      redis: { condition: service_healthy }
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

  gateway:
    build:
      context: .
      dockerfile: gateway/Dockerfile
    environment:
      NOTIFICATION_SERVICE_URL: http://notification-service:8000
      CONFIGURATION_SERVICE_URL: http://configuration-service:8000
      SERVICE_NAME: gateway
      LOG_LEVEL: INFO
    ports: ["8000:8000"]
    networks: [notification-net]
    restart: on-failure
    depends_on:
      notification-service: { condition: service_healthy }
      configuration-service: { condition: service_healthy }
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/api/v1/health"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
```

Add `telegram-data:` under `volumes:`.

Create `.env.example`:

```dotenv
# Copy to .env (git-ignored) next to docker-compose.yml to enable real delivery.
# Leave a value empty to keep that channel simulated. Reserved recipients
# (@example.com/.org/.net, .test/.invalid/.example domains, sim-… chat ids)
# are ALWAYS simulated, whatever is set here.

# Telegram: a bot token from @BotFather. A notification's recipient is the chat id.
TELEGRAM_BOT_TOKEN=

# Email: any SMTP server. SMTP_FROM is required when SMTP_HOST is set.
SMTP_HOST=
SMTP_PORT=587
SMTP_USERNAME=
SMTP_PASSWORD=
SMTP_FROM=
# starttls (port 587), ssl (port 465) or none (a local server such as Mailpit)
SMTP_SECURITY=starttls
```

Run: `docker compose up --build -d --wait && docker compose ps --format "{{.Service}} {{.Status}}"`
Expected: twelve services, every one reporting `(healthy)`.

- [ ] **Step 2: Extend the e2e fixtures**

In `tests/e2e/conftest.py`, add after `EMAIL_URL`:

```python
TELEGRAM_URL = os.environ.get("TELEGRAM_URL", "http://localhost:8005")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")
```

change `ALL_SERVICE_URLS` to include `TELEGRAM_URL` (the Gateway is excluded: its health lives at `/api/v1/health`):

```python
ALL_SERVICE_URLS = (NOTIFICATION_URL, ROUTING_URL, CONFIGURATION_URL, EMAIL_URL, TELEGRAM_URL)
```

and add fixtures:

```python
@pytest.fixture
async def gateway() -> AsyncIterator[httpx.AsyncClient]:
    """Base URL ends in /api/v1, so `settle` works unchanged through the Gateway."""
    async with httpx.AsyncClient(base_url=f"{GATEWAY_URL}/api/v1", timeout=10.0) as client:
        yield client


@pytest.fixture
async def telegram_enabled(configuration: httpx.AsyncClient) -> AsyncIterator[None]:
    await configuration.put("/channels/telegram", json={"enabled": True})
    yield
    await configuration.put("/channels/telegram", json={"enabled": True})
```

- [ ] **Step 3: Write the slice 2 e2e tests**

Create `tests/e2e/test_slice2.py`:

```python
"""Slice 2 end-to-end: the Gateway, Telegram, and recovery against the live stack.

Every recipient is reserved (spec section 3), so this suite sends nothing real
even on a stack with credentials in .env.
"""

import asyncio
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

REPO_ROOT = Path(__file__).resolve().parents[2]


def _compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=REPO_ROOT, check=True, capture_output=True)


async def _submit(gateway, **payload) -> str:
    response = await gateway.post("/notifications", json=payload)
    assert response.status_code == 202, response.text
    return response.json()["notification_id"]


async def test_the_gateway_reports_its_own_health(gateway):
    response = await gateway.get("/health")
    assert response.json() == {"status": "UP", "service": "gateway"}


async def test_the_happy_path_through_the_gateway(gateway, email_enabled, settle):
    response = await gateway.post(
        "/notifications",
        json={"channel": "email", "recipient": "john@example.com", "body": "Hello"},
        headers={"X-Correlation-ID": "e2e-gateway"},
    )
    assert response.status_code == 202
    assert response.headers["X-Correlation-ID"] == "e2e-gateway"

    settled = await settle(gateway, response.json()["notification_id"])
    assert settled["status"] == "COMPLETED"


async def test_a_disabled_channel_through_the_gateway(gateway, email_enabled, settle):
    assert (await gateway.put("/channels/email", json={"enabled": False})).status_code == 200
    notification_id = await _submit(
        gateway, channel="email", recipient="john@example.com", body="Hello"
    )

    settled = await settle(gateway, notification_id)
    assert (settled["status"], settled["fail_reason"]) == ("FAILED", "channel_disabled")


async def test_a_failing_recipient_through_the_gateway(gateway, email_enabled, settle):
    notification_id = await _submit(
        gateway, channel="email", recipient="fail@example.com", body="Hello"
    )

    settled = await settle(gateway, notification_id)
    assert (settled["status"], settled["fail_reason"]) == ("FAILED", "simulated_failure")


async def test_an_unknown_notification_is_a_404_through_the_gateway(gateway):
    response = await gateway.get("/notifications/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


async def test_telegram_happy_path(gateway, telegram_enabled, settle):
    notification_id = await _submit(
        gateway, channel="telegram", recipient="sim-e2e", subject="Hi", body="Hello"
    )

    settled = await settle(gateway, notification_id)
    assert settled["status"] == "COMPLETED"
    assert settled["channel"] == "telegram"


async def test_telegram_failing_chat_id(gateway, telegram_enabled, settle):
    notification_id = await _submit(
        gateway, channel="telegram", recipient="sim-e2e-fail", body="Hello"
    )

    settled = await settle(gateway, notification_id)
    assert (settled["status"], settled["fail_reason"]) == ("FAILED", "simulated_failure")


async def test_recovery_routes_a_notification_stranded_by_a_configuration_outage(
    gateway, email_enabled, settle
):
    """Spec 10.5: stop configuration-service, submit, restart, and the
    PendingRecoverer routes the stranded message after PENDING_TIMEOUT_MS."""
    _compose("stop", "configuration-service")
    try:
        notification_id = await _submit(
            gateway, channel="email", recipient="john@example.com", body="Recovery"
        )
        await asyncio.sleep(3)
        stranded = (await gateway.get(f"/notifications/{notification_id}")).json()
        assert stranded["status"] == "CREATED"
    finally:
        _compose("up", "-d", "--wait", "configuration-service")

    settled = await settle(gateway, notification_id, timeout_s=90)
    assert settled["status"] == "COMPLETED"
```

- [ ] **Step 4: Run the end-to-end suite**

Run: `uv run pytest tests/e2e -v`
Expected: PASS — the seven slice 1 tests and the nine slice 2 tests. The recovery test takes 30–60 s.

- [ ] **Step 5: Update the editor configuration**

In `.vscode/launch.json`, add two configurations after `email-service`:

```json
    {
      "name": "telegram-service",
      "type": "debugpy",
      "request": "launch",
      "module": "uvicorn",
      "args": ["app.main:create_app", "--factory", "--port", "8005", "--reload"],
      "cwd": "${workspaceFolder}/services/telegram-service",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/services/telegram-service",
        "DATABASE_URL": "postgresql+asyncpg://notif:notif@localhost:5437/telegramdb",
        "REDIS_URL": "redis://localhost:6379/0",
        "SERVICE_NAME": "telegram-service",
        "LOG_LEVEL": "DEBUG"
      },
      "justMyCode": false
    },
    {
      "name": "gateway",
      "type": "debugpy",
      "request": "launch",
      "module": "uvicorn",
      "args": ["app.main:create_app", "--factory", "--port", "8000", "--reload"],
      "cwd": "${workspaceFolder}/gateway",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/gateway",
        "NOTIFICATION_SERVICE_URL": "http://localhost:8001",
        "CONFIGURATION_SERVICE_URL": "http://localhost:8003",
        "SERVICE_NAME": "gateway",
        "LOG_LEVEL": "DEBUG"
      },
      "justMyCode": false
    },
```

In `.vscode/tasks.json`:
- "compose up (infra only)" command → `docker compose up -d redis notification-db routing-db configuration-db email-db telegram-db`
- "test: integration (all services)" command → `for s in configuration-service notification-service routing-service email-service telegram-service; do PYTHONPATH=services/$s uv run pytest services/$s/tests -v || exit 1; done && PYTHONPATH=gateway uv run pytest gateway/tests -v`
- add a task:

```json
    {
      "label": "test: playground",
      "type": "shell",
      "command": "PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests -v",
      "options": { "shell": { "executable": "bash.exe" } },
      "problemMatcher": []
    },
```

- [ ] **Step 6: Commit**

```bash
uv run ruff check --fix tests/e2e && uv run ruff format tests/e2e
git add docker-compose.yml .env.example .vscode/launch.json .vscode/tasks.json tests/e2e
git commit -m "feat: add telegram and gateway to compose with slice 2 e2e tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 12: Load test engine

**Files:**
- Create: `tools/playground/loadtest.py`
- Test: `tools/playground/tests/test_loadtest.py`

**Interfaces:**
- Consumes: `is_reserved_recipient`, `SIMULATED_FAILURE` (Task 1). Talks to the Gateway routes of Task 10.
- Produces:
  - `LoadTestConfig(total: int, concurrency: int = 10, telegram_ratio: float = 0.0, fail_ratio: float = 0.0, settle_timeout_s: float = 120.0, base_url: str = "http://localhost:8000", poll_interval_s: float = 0.25, seed: int = 0)` — frozen; raises `ValueError` out of range (`total` 1–2000, `concurrency` 1–50, ratios 0–1, `settle_timeout_s` > 0)
  - `PlannedNotification(index, channel, recipient, expect_failure)` with `.expected -> str`
  - `plan_notifications(config) -> list[PlannedNotification]`
  - `Percentiles(p50, p95, max)`, `percentiles(values: list[float]) -> Percentiles`
  - `LoadTestReport` (fields `total, accepted, submit_errors, accepted_per_s, submit_latency, e2e_latency, results, matched, wrong_state, unsettled, completions_per_second`; property `verdict_ok`)
  - `async run_load_test(config, *, transport=None, progress=None, clock=time.monotonic) -> LoadTestReport`; `progress(done: int, total: int, phase: "submit" | "settle")`

- [ ] **Step 1: Write the failing tests**

Create `tools/playground/tests/test_loadtest.py`:

```python
import json
import time
from collections import Counter
from uuid import uuid4

import httpx
import pytest
from loadtest import (
    LoadTestConfig,
    percentiles,
    plan_notifications,
    run_load_test,
)
from notification_shared.delivery import is_reserved_recipient


class FakePlatform:
    """MockTransport stand-in for the Gateway, with a scripted saga."""

    def __init__(
        self,
        polls_before_terminal: int = 1,
        never_settle: bool = False,
        ignore_fail_marker: bool = False,
        submit_status=None,
    ) -> None:
        self.polls_before_terminal = polls_before_terminal
        self.never_settle = never_settle
        self.ignore_fail_marker = ignore_fail_marker
        self.submit_status = submit_status
        self.recipients: dict[str, str] = {}
        self.polls: Counter = Counter()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            status = self.submit_status(body) if self.submit_status else 202
            if status != 202:
                return httpx.Response(status, text="boom")
            notification_id = str(uuid4())
            self.recipients[notification_id] = body["recipient"]
            return httpx.Response(202, json={"notification_id": notification_id, "status": "CREATED"})

        notification_id = request.url.path.rsplit("/", 1)[-1]
        self.polls[notification_id] += 1
        if self.never_settle or self.polls[notification_id] <= self.polls_before_terminal:
            return httpx.Response(200, json={"status": "PROCESSING", "fail_reason": None})
        if "fail" in self.recipients[notification_id] and not self.ignore_fail_marker:
            return httpx.Response(200, json={"status": "FAILED", "fail_reason": "simulated_failure"})
        return httpx.Response(200, json={"status": "COMPLETED", "fail_reason": None})


def _config(**overrides) -> LoadTestConfig:
    values = {"total": 20, "concurrency": 5, "fail_ratio": 0.25, "telegram_ratio": 0.5, "poll_interval_s": 0.01, "settle_timeout_s": 5.0}
    values.update(overrides)
    return LoadTestConfig(**values)


def test_every_generated_recipient_is_reserved():
    planned = plan_notifications(_config(total=500, telegram_ratio=0.4, fail_ratio=0.3))
    assert all(is_reserved_recipient(p.channel, p.recipient) for p in planned)


def test_the_shares_match_the_ratios():
    planned = plan_notifications(_config(total=100, telegram_ratio=0.4, fail_ratio=0.25))
    assert sum(p.channel == "telegram" for p in planned) == 40
    assert sum(p.expect_failure for p in planned) == 25
    assert all(("fail" in p.recipient) == p.expect_failure for p in planned)


def test_the_plan_is_deterministic_for_a_seed():
    assert plan_notifications(_config(seed=7)) == plan_notifications(_config(seed=7))


@pytest.mark.parametrize(
    "overrides",
    [{"total": 0}, {"total": 2001}, {"concurrency": 0}, {"concurrency": 51}, {"fail_ratio": 1.5}, {"telegram_ratio": -0.1}, {"settle_timeout_s": 0}],
)
def test_out_of_range_configs_are_rejected(overrides):
    with pytest.raises(ValueError):
        _config(**overrides)


def test_percentiles():
    result = percentiles([float(v) for v in range(1, 101)])
    assert (result.p50, result.p95, result.max) == (50.0, 95.0, 100.0)
    assert percentiles([]).p50 is None


async def test_a_clean_run_has_a_passing_verdict():
    report = await run_load_test(_config(), transport=httpx.MockTransport(FakePlatform()))

    assert report.verdict_ok
    assert (report.total, report.accepted, report.matched) == (20, 20, 20)
    assert report.results == {"COMPLETED": 15, "FAILED/simulated_failure": 5}
    assert report.wrong_state == report.unsettled == 0
    assert report.e2e_latency.p50 is not None
    assert sum(count for _, count in report.completions_per_second) == 20


async def test_a_wrong_final_state_fails_the_verdict():
    platform = FakePlatform(ignore_fail_marker=True)
    report = await run_load_test(_config(), transport=httpx.MockTransport(platform))

    assert not report.verdict_ok
    assert report.wrong_state == 5


async def test_a_5xx_during_submit_is_counted_not_raised():
    platform = FakePlatform(submit_status=lambda body: 500 if body["recipient"].startswith("load-1") else 202)
    report = await run_load_test(_config(telegram_ratio=0.0), transport=httpx.MockTransport(platform))

    assert report.submit_errors["500"] > 0
    assert report.accepted == 20 - report.submit_errors["500"]
    assert not report.verdict_ok


async def test_an_unreachable_gateway_is_reported_not_raised():
    """Review Focus 4."""

    def down(_request):
        raise httpx.ConnectError("connection refused")

    report = await run_load_test(_config(), transport=httpx.MockTransport(down))

    assert report.submit_errors == {"ConnectError": 20}
    assert report.accepted == 0
    assert not report.verdict_ok


async def test_the_settle_timeout_bounds_the_run():
    started = time.monotonic()
    report = await run_load_test(
        _config(settle_timeout_s=0.2), transport=httpx.MockTransport(FakePlatform(never_settle=True))
    )

    assert time.monotonic() - started < 3
    assert report.unsettled == 20
    assert report.results == {"UNSETTLED": 20}


async def test_progress_is_reported_for_both_phases():
    seen = []
    await run_load_test(
        _config(total=4), transport=httpx.MockTransport(FakePlatform()), progress=lambda d, t, p: seen.append((d, t, p))
    )
    assert (4, 4, "submit") in seen
    assert (4, 4, "settle") in seen
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests -v 2>&1 | tee /tmp/task-12-red.txt`
Expected: FAIL at collection — `ModuleNotFoundError: No module named 'loadtest'`.

- [ ] **Step 3: Write the implementation**

Create `tools/playground/loadtest.py`:

```python
"""Simulated load test engine for the notification platform.

No Streamlit import: pages/2_Load_test.py drives it, and the tests drive it
through httpx.MockTransport. Recipients are generated here and are always
reserved (slice 2 spec section 3), so a load test cannot send a real email or
Telegram message even on a stack with real credentials.
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

import httpx
from notification_shared.delivery import SIMULATED_FAILURE, is_reserved_recipient

TERMINAL = frozenset({"COMPLETED", "FAILED"})
MAX_TOTAL = 2000
MAX_CONCURRENCY = 50
UNSETTLED = "UNSETTLED"
NOTIFICATIONS_PATH = "/api/v1/notifications"

ProgressCallback = Callable[[int, int, str], None]


@dataclass(frozen=True)
class LoadTestConfig:
    total: int
    concurrency: int = 10
    telegram_ratio: float = 0.0
    fail_ratio: float = 0.0
    settle_timeout_s: float = 120.0
    base_url: str = "http://localhost:8000"
    poll_interval_s: float = 0.25
    seed: int = 0

    def __post_init__(self) -> None:
        if not 1 <= self.total <= MAX_TOTAL:
            raise ValueError(f"total must be between 1 and {MAX_TOTAL}")
        if not 1 <= self.concurrency <= MAX_CONCURRENCY:
            raise ValueError(f"concurrency must be between 1 and {MAX_CONCURRENCY}")
        for name in ("telegram_ratio", "fail_ratio"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.settle_timeout_s <= 0:
            raise ValueError("settle_timeout_s must be positive")


@dataclass(frozen=True)
class PlannedNotification:
    index: int
    channel: str
    recipient: str
    expect_failure: bool

    @property
    def expected(self) -> str:
        return f"FAILED/{SIMULATED_FAILURE}" if self.expect_failure else "COMPLETED"


def plan_notifications(config: LoadTestConfig) -> list[PlannedNotification]:
    rng = random.Random(config.seed)
    indices = range(config.total)
    telegram = set(rng.sample(indices, round(config.total * config.telegram_ratio)))
    failing = set(rng.sample(indices, round(config.total * config.fail_ratio)))
    planned = []
    for i in indices:
        channel = "telegram" if i in telegram else "email"
        suffix = "-fail" if i in failing else ""
        recipient = f"sim-load-{i}{suffix}" if channel == "telegram" else f"load-{i}{suffix}@example.com"
        if not is_reserved_recipient(channel, recipient):
            # Cannot happen with the patterns above; refuse rather than risk a real send.
            raise RuntimeError(f"generated recipient {recipient!r} is not reserved")
        planned.append(PlannedNotification(i, channel, recipient, bool(suffix)))
    return planned


@dataclass
class Outcome:
    planned: PlannedNotification
    submit_started: float = 0.0
    submit_latency_s: float | None = None
    submit_status: str | None = None
    notification_id: str | None = None
    settled_at: float | None = None
    final_status: str | None = None
    fail_reason: str | None = None

    @property
    def accepted(self) -> bool:
        return self.notification_id is not None

    @property
    def result(self) -> str:
        if self.final_status is None:
            return UNSETTLED
        if self.final_status == "FAILED":
            return f"FAILED/{self.fail_reason}"
        return self.final_status

    @property
    def e2e_latency_s(self) -> float | None:
        return None if self.settled_at is None else self.settled_at - self.submit_started


@dataclass(frozen=True)
class Percentiles:
    p50: float | None
    p95: float | None
    max: float | None


def percentiles(values: list[float]) -> Percentiles:
    """Nearest-rank percentiles."""
    if not values:
        return Percentiles(None, None, None)
    ordered = sorted(values)

    def rank(p: float) -> float:
        return ordered[max(0, math.ceil(p * len(ordered)) - 1)]

    return Percentiles(rank(0.50), rank(0.95), ordered[-1])


@dataclass(frozen=True)
class LoadTestReport:
    total: int
    accepted: int
    submit_errors: dict[str, int]
    accepted_per_s: float
    submit_latency: Percentiles
    e2e_latency: Percentiles
    results: dict[str, int]
    matched: int
    wrong_state: int
    unsettled: int
    completions_per_second: list[tuple[int, int]]

    @property
    def verdict_ok(self) -> bool:
        """Every notification accepted and ended in the state its recipient implies."""
        return self.matched == self.total


def build_report(outcomes: list[Outcome], run_started: float, submit_finished: float) -> LoadTestReport:
    accepted = [o for o in outcomes if o.accepted]
    results = Counter(o.result for o in accepted)
    matched = sum(1 for o in accepted if o.result == o.planned.expected)
    unsettled = results.get(UNSETTLED, 0)
    buckets = Counter(int(o.settled_at - run_started) for o in accepted if o.settled_at is not None)
    return LoadTestReport(
        total=len(outcomes),
        accepted=len(accepted),
        submit_errors=dict(Counter(o.submit_status for o in outcomes if not o.accepted)),
        accepted_per_s=len(accepted) / max(submit_finished - run_started, 1e-9),
        submit_latency=percentiles([o.submit_latency_s for o in outcomes if o.submit_latency_s is not None]),
        e2e_latency=percentiles([o.e2e_latency_s for o in accepted if o.e2e_latency_s is not None]),
        results=dict(results),
        matched=matched,
        wrong_state=len(accepted) - matched - unsettled,
        unsettled=unsettled,
        completions_per_second=sorted(buckets.items()),
    )


async def run_load_test(
    config: LoadTestConfig,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    progress: ProgressCallback | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> LoadTestReport:
    outcomes = [Outcome(planned) for planned in plan_notifications(config)]
    notify = progress or (lambda _done, _total, _phase: None)
    semaphore = asyncio.Semaphore(config.concurrency)

    async with httpx.AsyncClient(base_url=config.base_url, timeout=10.0, transport=transport) as client:
        run_started = clock()
        await _submit_all(client, outcomes, semaphore, notify, clock)
        submit_finished = clock()
        await _settle_all(
            client,
            [o for o in outcomes if o.accepted],
            semaphore,
            notify,
            clock,
            deadline=clock() + config.settle_timeout_s,
            poll_interval_s=config.poll_interval_s,
        )
    return build_report(outcomes, run_started, submit_finished)


async def _submit_all(client, outcomes, semaphore, notify, clock) -> None:
    done = 0

    async def submit(outcome: Outcome) -> None:
        nonlocal done
        planned = outcome.planned
        payload = {
            "channel": planned.channel,
            "recipient": planned.recipient,
            "subject": "Load test",
            "body": f"Load test message {planned.index}",
        }
        async with semaphore:
            outcome.submit_started = clock()
            try:
                response = await client.post(NOTIFICATIONS_PATH, json=payload)
            except httpx.HTTPError as exc:
                outcome.submit_status = type(exc).__name__
            else:
                outcome.submit_status = str(response.status_code)
                if response.status_code == 202:
                    outcome.notification_id = response.json()["notification_id"]
            outcome.submit_latency_s = clock() - outcome.submit_started
        done += 1
        notify(done, len(outcomes), "submit")

    await asyncio.gather(*(submit(o) for o in outcomes))


async def _settle_all(client, accepted, semaphore, notify, clock, *, deadline, poll_interval_s) -> None:
    done = 0

    async def settle(outcome: Outcome) -> None:
        nonlocal done
        while clock() < deadline:
            # The semaphore bounds concurrent requests, not waiting: holding it
            # across the sleep would serialise polling and inflate latencies.
            async with semaphore:
                try:
                    response = await client.get(f"{NOTIFICATIONS_PATH}/{outcome.notification_id}")
                    body = response.json() if response.status_code == 200 else None
                except httpx.HTTPError:
                    body = None
            if body and body.get("status") in TERMINAL:
                outcome.settled_at = clock()
                outcome.final_status = body["status"]
                outcome.fail_reason = body.get("fail_reason")
                break
            await asyncio.sleep(poll_interval_s)
        done += 1
        notify(done, len(accepted), "settle")

    await asyncio.gather(*(settle(o) for o in accepted))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests -v`
Expected: PASS.

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix tools && uv run ruff format tools
git add tools/playground/loadtest.py tools/playground/tests
git commit -m "feat(playground): add simulated load test engine

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 13: Playground console and load test page

**Files:**
- Rewrite: `tools/playground/app.py`
- Create: `tools/playground/pages/2_Load_test.py`
- Test: `tools/playground/tests/test_pages.py`

**Interfaces:**
- Consumes: `run_load_test`, `LoadTestConfig`, `LoadTestReport` (Task 12); `is_failure_recipient`, `is_reserved_recipient` (Task 1); `/version` `delivery_mode` (Tasks 7–9); Gateway routes (Task 10).
- Produces: the two pages of spec 7.2 and 7.4.

- [ ] **Step 1: Write the failing smoke tests**

Create `tools/playground/tests/test_pages.py`:

```python
"""Both pages render without raising when the whole stack is unreachable."""

from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

PLAYGROUND = Path(__file__).resolve().parents[1]
DOWN = "http://127.0.0.1:9"


@pytest.fixture(autouse=True)
def stack_is_down(monkeypatch):
    for name in ("GATEWAY_URL", "NOTIFICATION_URL", "ROUTING_URL", "CONFIGURATION_URL", "EMAIL_URL", "TELEGRAM_URL"):
        monkeypatch.setenv(name, DOWN)


def test_the_console_renders_errors_instead_of_raising():
    at = AppTest.from_file(str(PLAYGROUND / "app.py"), default_timeout=30).run()
    assert not at.exception
    assert "unreachable" in " ".join(m.value for m in at.sidebar.markdown)


def test_the_load_test_page_renders():
    at = AppTest.from_file(str(PLAYGROUND / "pages" / "2_Load_test.py"), default_timeout=30).run()
    assert not at.exception
    assert at.title[0].value == "Load test"
```

Run: `PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests/test_pages.py -v 2>&1 | tee /tmp/task-13-red.txt`
Expected: `test_the_load_test_page_renders` FAILS (file not found); the console test may pass or fail against the slice 1 app — record which.

- [ ] **Step 2: Rewrite the console**

Replace `tools/playground/app.py` with:

```python
"""Manual test console for the notification platform.

Console page: service health and delivery modes, channel toggles, a send form
that asks for confirmation before a real send, one-click saga scenarios, a
live timeline and the notification table. The Load test page is in pages/.

    docker compose up --build -d --wait
    uv run --group playground streamlit run tools/playground/app.py
"""

from __future__ import annotations

import os
import time

import httpx
import streamlit as st
from notification_shared.delivery import is_failure_recipient, is_reserved_recipient

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")
API_URL = f"{GATEWAY_URL}/api/v1"
# Same variables as tests/e2e/conftest.py. Health and /version stay per
# service: the Gateway reports only its own health (spec 5.2).
SERVICES = {
    "notification": os.environ.get("NOTIFICATION_URL", "http://localhost:8001"),
    "routing": os.environ.get("ROUTING_URL", "http://localhost:8002"),
    "configuration": os.environ.get("CONFIGURATION_URL", "http://localhost:8003"),
    "email": os.environ.get("EMAIL_URL", "http://localhost:8004"),
    "telegram": os.environ.get("TELEGRAM_URL", "http://localhost:8005"),
}
DELIVERY_SERVICES = ("email", "telegram")
TERMINAL = {"COMPLETED", "FAILED"}
SETTLE_TIMEOUT_S = 30.0
START_HINT = "Is the stack up? Run `docker compose up --build -d --wait`."
RESERVED_HINT = (
    "Reserved recipients are always simulated: @example.com / .org / .net, any .test, "
    ".invalid or .example domain, and Telegram chat ids starting with `sim-`. "
    "A recipient containing `fail` fails by design and sends nothing."
)

# Every scenario recipient is reserved, so scenarios stay simulated even when
# real credentials are configured (spec section 3).
SCENARIOS = {
    "Email: happy path": ("email", "john@example.com", False),
    "Email: channel disabled": ("email", "john@example.com", True),
    "Email: delivery failure": ("email", "fail@example.com", False),
    "Telegram: happy path": ("telegram", "sim-playground", False),
    "Telegram: delivery failure": ("telegram", "sim-playground-fail", False),
}


@st.cache_resource
def client() -> httpx.Client:
    return httpx.Client(timeout=5.0)


def set_channel(name: str, enabled: bool) -> None:
    client().put(f"{API_URL}/channels/{name}", json={"enabled": enabled})


def on_toggle(name: str) -> None:
    set_channel(name, st.session_state[f"channel_{name}"])


def delivery_modes() -> dict[str, str]:
    modes = {}
    for name in DELIVERY_SERVICES:
        try:
            modes[name] = client().get(f"{SERVICES[name]}/version").json().get("delivery_mode", "unknown")
        except (httpx.HTTPError, ValueError):
            modes[name] = "unknown"
    return modes


def is_real_send(channel: str, recipient: str, modes: dict[str, str]) -> bool:
    """True when this send would leave the platform. An unknown mode counts as real."""
    if is_failure_recipient(recipient) or is_reserved_recipient(channel, recipient):
        return False
    return modes.get(channel, "unknown") != "simulated"


def submit(channel: str, recipient: str, subject: str, body: str, disable: bool) -> None:
    if disable:
        set_channel(channel, False)
    payload = {"channel": channel, "recipient": recipient, "body": body}
    if subject:
        payload["subject"] = subject
    response = client().post(f"{API_URL}/notifications", json=payload)
    response.raise_for_status()
    st.session_state.tracked = {
        "id": response.json()["notification_id"],
        "started": time.monotonic(),
        "observed": [],
        # Re-enabled only once the notification settles: routing reads the
        # channel asynchronously, so re-enabling right after the POST could
        # flip the outcome.
        "reenable": channel if disable else None,
        "done": False,
    }


def sidebar(modes: dict[str, str]) -> None:
    st.sidebar.header("Services")
    try:
        gateway = client().get(f"{API_URL}/health").json()
        icon = "🟢" if gateway.get("status") == "UP" else "🔴"
        st.sidebar.write(f"{icon} **gateway** ({GATEWAY_URL})")
    except (httpx.HTTPError, ValueError):
        st.sidebar.write(f"🔴 **gateway** unreachable ({GATEWAY_URL})")
    for name, url in SERVICES.items():
        try:
            health = client().get(f"{url}/health").json()
            version = client().get(f"{url}/version").json()["version"]
            icon = "🟢" if health["status"] == "UP" else "🔴"
            st.sidebar.write(f"{icon} **{name}** v{version} · {health.get('checks', {})}")
        except (httpx.HTTPError, ValueError, KeyError):
            st.sidebar.write(f"🔴 **{name}** unreachable ({url})")

    st.sidebar.header("Delivery mode")
    for name, mode in modes.items():
        badge = "🟢 SIMULATED" if mode == "simulated" else f"🟠 REAL ({mode})"
        st.sidebar.write(f"**{name}**: {badge}")
    st.sidebar.caption(RESERVED_HINT)

    st.sidebar.header("Channels")
    try:
        channels = client().get(f"{API_URL}/channels").json()
    except (httpx.HTTPError, ValueError):
        st.sidebar.error(f"Channels unavailable. {START_HINT}")
        return
    for channel in channels:
        key = f"channel_{channel['name']}"
        # Server state wins on every full rerun, so a scenario that disabled a
        # channel is reflected here.
        st.session_state[key] = channel["enabled"]
        st.sidebar.toggle(channel["name"], key=key, on_change=on_toggle, args=(channel["name"],))


def send_panel(modes: dict[str, str]) -> None:
    st.subheader("Send a notification")
    with st.form("send"):
        channel = st.selectbox("Channel", ["email", "telegram"])
        recipient = st.text_input("Recipient (email address or Telegram chat id)", "john@example.com")
        subject = st.text_input("Subject", "Welcome")
        body = st.text_area("Body", "Hello John!")
        confirmed = st.checkbox("I understand this may send a real message")
        sent = st.form_submit_button("Send")
    st.caption(RESERVED_HINT)

    if sent:
        if is_failure_recipient(recipient):
            st.info(f"`{recipient}` contains `fail`: it ends FAILED / simulated_failure by design.")
        if is_real_send(channel, recipient, modes) and not confirmed:
            st.warning(
                f"{channel} is configured for REAL delivery ({modes.get(channel)}) and "
                f"`{recipient}` is not a reserved recipient. Tick the confirmation and send again."
            )
        else:
            try:
                submit(channel, recipient, subject, body, disable=False)
            except httpx.HTTPError as exc:
                st.error(f"Send failed: {exc}. {START_HINT}")

    st.write("Or run a scenario (always simulated):")
    columns = st.columns(len(SCENARIOS))
    for column, (name, (sc_channel, sc_recipient, disable)) in zip(columns, SCENARIOS.items(), strict=True):
        if column.button(name, width="stretch"):
            try:
                submit(sc_channel, sc_recipient, "Welcome", "Hello!", disable)
            except httpx.HTTPError as exc:
                st.error(f"Send failed: {exc}. {START_HINT}")


@st.fragment(run_every=0.5)
def timeline() -> None:
    st.subheader("Saga timeline")
    tracked = st.session_state.get("tracked")
    if tracked is None:
        st.caption("Send a notification to follow its status here.")
        return

    if not tracked["done"]:
        elapsed = time.monotonic() - tracked["started"]
        try:
            body = client().get(f"{API_URL}/notifications/{tracked['id']}").json()
        except (httpx.HTTPError, ValueError) as exc:
            st.error(f"Polling failed: {exc}. {START_HINT}")
            return
        observed = tracked["observed"]
        if not observed or observed[-1]["status"] != body["status"]:
            observed.append({"status": body["status"], "at": f"{elapsed:.2f}s", "reason": body["fail_reason"]})
        if body["status"] in TERMINAL or elapsed > SETTLE_TIMEOUT_S:
            tracked["done"] = True
            if tracked["reenable"]:
                set_channel(tracked["reenable"], True)
            # Full rerun so the sidebar and the table pick up the final state.
            st.rerun()

    st.write(f"`{tracked['id']}`")
    st.table(tracked["observed"])
    last = tracked["observed"][-1]["status"] if tracked["observed"] else None
    if tracked["done"] and last not in TERMINAL:
        st.warning(
            f"Still {last} after {SETTLE_TIMEOUT_S:.0f}s. Recovery retries after "
            "PENDING_TIMEOUT_MS (30s); the watchdog fails PROCESSING after 5 minutes."
        )
    elif not tracked["done"]:
        st.caption("Polling...")


@st.fragment(run_every=2)
def notifications_table() -> None:
    st.subheader("Notifications")
    left, right = st.columns(2)
    status = left.selectbox("Status", ["", "CREATED", "PROCESSING", "COMPLETED", "FAILED"])
    channel = right.selectbox("Channel filter", ["", "email", "telegram"])
    params = {"limit": 50}
    if status:
        params["status"] = status
    if channel:
        params["channel"] = channel
    try:
        rows = client().get(f"{API_URL}/notifications", params=params).json()
    except (httpx.HTTPError, ValueError) as exc:
        st.error(f"Listing failed: {exc}. {START_HINT}")
        return
    st.dataframe(rows, width="stretch", hide_index=True)


st.set_page_config(page_title="Notification Playground", layout="wide")
st.title("Notification Playground")
current_modes = delivery_modes()
sidebar(current_modes)
send_panel(current_modes)
timeline()
notifications_table()
```

- [ ] **Step 3: Write the load test page**

Create `tools/playground/pages/2_Load_test.py`:

```python
"""Simulated load test page. Every recipient is reserved: nothing real is sent."""

from __future__ import annotations

import asyncio
import os

import pandas as pd
import streamlit as st
from loadtest import MAX_CONCURRENCY, MAX_TOTAL, LoadTestConfig, LoadTestReport, run_load_test

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")


def render(report: LoadTestReport) -> None:
    if report.verdict_ok:
        st.success(f"Correct: all {report.total} notifications ended in their expected state.")
    else:
        st.error(
            f"{report.matched}/{report.total} as expected — wrong state: {report.wrong_state}, "
            f"never settled: {report.unsettled}, rejected at submit: {report.total - report.accepted}."
        )

    cols = st.columns(4)
    cols[0].metric("Accepted", f"{report.accepted}/{report.total}")
    cols[1].metric("Accepted / s", f"{report.accepted_per_s:.1f}")
    cols[2].metric("E2E p50", _seconds(report.e2e_latency.p50))
    cols[3].metric("E2E p95", _seconds(report.e2e_latency.p95))

    st.table(
        [
            {"measure": "POST latency", "p50": _seconds(report.submit_latency.p50), "p95": _seconds(report.submit_latency.p95), "max": _seconds(report.submit_latency.max)},
            {"measure": "End-to-end latency", "p50": _seconds(report.e2e_latency.p50), "p95": _seconds(report.e2e_latency.p95), "max": _seconds(report.e2e_latency.max)},
        ]
    )
    st.write("Outcomes")
    st.table([{"outcome": key, "count": value} for key, value in sorted(report.results.items())])
    if report.submit_errors:
        st.write("Submit errors")
        st.table([{"error": key, "count": value} for key, value in report.submit_errors.items()])
    if report.completions_per_second:
        chart = pd.DataFrame(report.completions_per_second, columns=["second", "completed"])
        st.bar_chart(chart.set_index("second"))


def _seconds(value: float | None) -> str:
    return "—" if value is None else f"{value:.2f}s"


st.set_page_config(page_title="Load test", layout="wide")
st.title("Load test")
st.caption(
    "Sends notifications through the Gateway to generated reserved recipients "
    "(load-N@example.com, sim-load-N), so nothing real is ever sent, then follows each "
    "one to its final state. End-to-end latency resolution is the 250 ms poll interval. "
    "Interacting with the page during a run restarts it."
)
st.info(
    "In compose each consumer group has one consumer and simulated delivery sleeps up to 2 s, "
    "so throughput reflects those deliberate limits, not the code's ceiling."
)

with st.form("loadtest"):
    total = st.number_input("Notifications", min_value=1, max_value=MAX_TOTAL, value=200)
    concurrency = st.number_input("Concurrency", min_value=1, max_value=MAX_CONCURRENCY, value=10)
    telegram_ratio = st.slider("Telegram share", 0.0, 1.0, 0.3, 0.05)
    fail_ratio = st.slider("Failing share", 0.0, 1.0, 0.1, 0.05)
    settle_timeout = st.number_input("Settle timeout (s)", min_value=10, max_value=600, value=120)
    run = st.form_submit_button("Run")

if run:
    config = LoadTestConfig(
        total=int(total),
        concurrency=int(concurrency),
        telegram_ratio=float(telegram_ratio),
        fail_ratio=float(fail_ratio),
        settle_timeout_s=float(settle_timeout),
        base_url=GATEWAY_URL,
    )
    bars = {
        "submit": st.progress(0.0, text="Submitting"),
        "settle": st.progress(0.0, text="Waiting for final states"),
    }

    def progress(done: int, total_: int, phase: str) -> None:
        bars[phase].progress(done / total_, text=f"{phase}: {done}/{total_}")

    st.session_state.load_report = asyncio.run(run_load_test(config, progress=progress))

if report := st.session_state.get("load_report"):
    render(report)
```

- [ ] **Step 4: Run the playground suite and a manual check**

Run: `PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests -v`
Expected: PASS.

Manual, with the stack up (`docker compose up --build -d --wait`): `uv run --group playground streamlit run tools/playground/app.py`. On the Console, check that all six services are green, both delivery badges read SIMULATED, and each of the five scenarios reaches its expected final state. On Load test, run 200 notifications (concurrency 10, telegram 0.3, failing 0.1) and check the verdict is green.

- [ ] **Step 5: Lint, format, commit**

```bash
uv run ruff check --fix tools && uv run ruff format tools
git add tools/playground
git commit -m "feat(playground): console through the gateway, real-send guard, load test page

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 14: Documentation and ADRs

**Files:**
- Create: `docs/adr/0024-a-consumer-that-gives-up-publishes-a-failure-event.md`, `docs/adr/0025-recovery-claims-by-idle-time-with-one-shared-recoverer.md`, `docs/adr/0026-telegram-recipient-is-the-chat-id.md`, `docs/adr/0027-gateway-returns-502-for-an-unreachable-service.md`, `docs/adr/0028-the-watchdog-publishes-no-event.md`, `docs/adr/0029-real-email-through-generic-smtp.md`, `docs/adr/0030-reserved-recipients-are-always-simulated.md`, `services/telegram-service/README.md`, `gateway/README.md`
- Modify: `README.md`, `docs/architecture.md`, `docs/event-flows.md`, `docs/patterns.md`, `docs/local-development.md`, `services/email-service/README.md`, `services/notification-service/README.md`, `services/routing-service/README.md`

**Interfaces:** none (documentation). Every claim must match the code as committed by Tasks 1–13; check names by opening the files, not from this plan.

- [ ] **Step 1: Write the ADRs**

Use the existing ADR format (`# NNNN: Title`, then `## Status` Accepted, `## Context`, `## Decision`, `## Consequences`). Content for each:

**0024 — A consumer that gives up publishes a failure event.** Context: slice 1 spec 15 left max-retry as "discard"; discarding alone leaves the notification `CREATED` (routing gave up) or `PROCESSING` (delivery gave up) forever, and the slice 1 watchdog only covers `PROCESSING`. Decision: after `PENDING_MAX_RETRIES` failed recovery attempts, `PendingRecoverer` calls the consumer's `give_up`, which writes its own failure record and a failure event through the outbox — `RoutingFailed` from routing, `DeliveryFailed` from email and telegram — with reason `max_retries_exceeded`, in the same transaction as `mark_failed_permanent`; then `XACK`. Notification Service's own consumers and Routing's results consumer write nothing. Consequences: the notification ends `FAILED` with a precise reason within about two minutes; `give_up` must never call an external system; consumers own a second method; tested by `services/routing-service/tests/test_notification_consumer.py::test_a_persistent_outage_ends_in_routing_failed_after_max_retries`.

**0025 — Recovery claims by idle time, with one shared recoverer.** Context: consumer names are `socket.gethostname()`, the container id, which changes on restart; a restarted consumer never sees its predecessor's pending entries. Decision: `XPENDING ... IDLE PENDING_TIMEOUT_MS` finds idle entries of any owner and `XCLAIM` (same `min_idle_ms`) moves them; one `PendingRecoverer` per service in `shared/notification_shared/recovery.py`, shared like `OutboxPublisher`, calls each consumer's public `handle`; only the recoverer counts attempts, in `processed_events.fail_count`, which is never reset. Consequences: recovery latency is at least `PENDING_TIMEOUT_MS`; dead consumer names accumulate in `XINFO CONSUMERS`; unparseable entries are acked and discarded; the per-service consumers stay explicit (slice 1 ruling F4).

**0026 — Telegram's recipient is the chat id.** Context: the slice 1 spec said real Bot API delivery needs `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`, but every notification already carries a `recipient`. Decision: the recipient is the `chat_id`; `TELEGRAM_CHAT_ID` is removed; the token alone switches real delivery on. Consequences: one recipient per notification on both channels; the `fail` marker applies to chat ids; 400/403 are permanent (`telegram_rejected`), 429/5xx/timeouts transient.

**0027 — The Gateway returns 502 for an unreachable service.** Context: the slice 1 spec defined only `504` on timeout; a refused connection would otherwise surface as a generic `500`. Decision: `httpx.TimeoutException` → `504 GATEWAY_TIMEOUT`, any other `httpx.TransportError` → `502 BAD_GATEWAY`, both in the common error model; downstream responses, errors included, pass through unchanged. Consequences: clients can tell "slow" from "down"; the Gateway never invents other statuses.

**0028 — The watchdog publishes no event.** Context: the stale-processing watchdog closes notifications whose result never arrived. Decision: a guarded SQL update to `FAILED`/`processing_timeout` for `PROCESSING` rows older than `PROCESSING_TIMEOUT_MINUTES`, using the database clock, publishing nothing. Consequences: Routing's `routes` row stays `PROCESSING`; a delivery result arriving later does not reopen the notification, because terminal states never reopen (ADR 0016) — the notification reads `FAILED` although the message was delivered. Pinned by `services/notification-service/tests/test_watchdog.py::test_a_late_delivery_result_does_not_reopen_it`.

**0029 — Real email through generic SMTP.** Context: slice 1 simulated every email. Decision: `SmtpSender` over `aiosmtplib`, switched on by `SMTP_HOST`, with `SMTP_SECURITY` `starttls`/`ssl`/`none`; `SMTP_FROM` required with a host (startup fails otherwise); 5xx permanent (`smtp_rejected`), 4xx/network transient, auth failure transient with an ERROR log; the password is never logged. Consequences: any provider works; no provider-specific API; real sending is not covered by automated tests (an in-process `aiosmtpd` server stands in).

**0030 — Reserved recipients are always simulated.** Context: with credentials in `.env`, e2e tests, README examples and load tests would otherwise send real messages. Decision: `notification_shared/delivery.py` — after the `fail` marker, recipients on RFC 2606 names (`example.com`/`.org`/`.net` and subdomains, TLDs `.example`/`.invalid`/`.test`) and chat ids starting `sim-` always use the simulated sender, whatever is configured; the load test generates only reserved recipients. Consequences: every automated path is safe on a real stack; a user who types a reserved address expecting a real send gets a simulation, which the playground states.

- [ ] **Step 2: Update the README**

- Title → `# Universal Notification Platform — Slice 2`.
- "What this is", second paragraph → state that slice 2 adds recovery, max-retry give-up, the watchdog, Telegram Service, the API Gateway, optional real SMTP and Bot API delivery, and the playground; slice 3 (operations guide, metrics, API key) remains.
- Architecture diagram: add `GW["gateway"]` and `TS["telegram-service"]`; `Client -- REST --> GW`, `GW -- REST --> NS`, `GW -- REST --> CS`; `SNR -. "XREADGROUP (telegram-service)" .-> TS`; `TS -. "XADD" .-> SDC`; `TS -. "XADD" .-> SDF`. Remove the direct `Client -- REST --> NS` / `CS` edges.
- Quick start: the stack is twelve containers; every `curl` example goes to `http://localhost:8000/api/v1/...`.
- New section **Real delivery** after Quick start: copy `.env.example` to `.env`, fill `TELEGRAM_BOT_TOKEN` and/or the `SMTP_*` values, `docker compose up -d`; the recipient of a Telegram notification is the chat id; the reserved-recipient rule and the `fail` rule, with the exact domains and prefix; the playground shows each channel's mode and asks for confirmation before a real send.
- Stream topology: `telegram-service` row → "Filters `payload.channel == "telegram"`, delivers"; `delivery.completed` publishers → email-service, telegram-service; `delivery.failed` publishers → routing-service, email-service, telegram-service.
- Polling guide: add the Telegram happy path (`sim-demo`) and the recovery demonstration (`docker compose stop configuration-service`, submit, `docker compose start configuration-service`, watch it complete within about 40 s).
- Configuration table: add every variable of spec section 8 with its default and service; remove `TELEGRAM_CHAT_ID`; mark the recovery/watchdog/gateway/SMTP/Telegram rows "Yes".
- Running the tests: add the telegram-service, gateway and playground invocations.
- Known limitations: replace with spec section 14 (carried, "No real SMTP" removed, and the new items).

- [ ] **Step 3: Update the architecture, event-flow and pattern docs**

`docs/architecture.md`:
- Opening paragraph → "Slice 2 of the Universal Notification Platform: six services …" and drop the "are slice 2" sentence.
- Services table: add `telegram-service` (owns `telegram_delivery`, `telegramdb`, no domain endpoints) and `gateway` (owns nothing, no database, `/api/v1/*` proxy plus `/api/v1/health`).
- Replace "What slice 1 does not have" with **"Recovery and failure handling"**: the recoverer, max-retry give-up and the watchdog in three short paragraphs pointing to ADRs 0024, 0025, 0028; then **"Still not built"**: API key, metrics, `docs/operations.md` (slice 3).
- Component diagram: the same additions as the README diagram.

`docs/event-flows.md`:
- Stream topology: the same edits as the README.
- Event payloads: `DeliveryFailed.reason` is one of `simulated_failure`, `smtp_rejected`, `telegram_rejected`, `max_retries_exceeded`; `RoutingFailed.reason` adds `max_retries_exceeded`; `DeliveryCompleted.recipient` is the chat id for Telegram.
- New section **"Give-up after max retries"** with this sequence diagram:

```mermaid
sequenceDiagram
    participant NS as notification-service
    participant R as Redis Streams
    participant RS as routing-service
    participant CS as configuration-service
    NS->>R: XADD notification.created
    R->>RS: XREADGROUP (routing-service)
    RS->>CS: GET /channels/email
    CS--xRS: connection refused (transient)
    Note over RS: no XACK, nothing written, entry pending
    loop every PENDING_TIMEOUT_MS, up to PENDING_MAX_RETRIES
        RS->>R: XPENDING IDLE + XCLAIM (recoverer)
        RS->>CS: GET /channels/email
        CS--xRS: still down
        Note over RS: processed_events.fail_count += 1
    end
    Note over RS: give_up: routes FAILED + outbox RoutingFailed(max_retries_exceeded) + FAILED_PERMANENT, one transaction
    RS->>R: XACK
    RS->>R: XADD delivery.failed (RoutingFailed)
    R->>NS: XREADGROUP (notification-service-results)
    Note over NS: CREATED -> FAILED, fail_reason = max_retries_exceeded
```

`docs/patterns.md`:
- "Guarded monotonic transitions" → "Where it lives" names both `NotificationRepository.advance_status` and `RouteRepository.set_status` (`services/routing-service/app/repositories/route.py`, guard `status = 'PROCESSING'` from the results consumer). This closes the slice 1 parked item.
- New sections in the existing format (**What it does / Where it lives / What breaks without it / The test**):
  - **Recovery by idle claim** — `shared/notification_shared/recovery.py`; without it a crashed consumer's entry stays pending forever; tests `tests/integration/test_recovery.py::test_a_stranded_message_is_claimed_and_completed` and the e2e `test_recovery_routes_a_notification_stranded_by_a_configuration_outage`.
  - **Max-retry and give-up** — `PendingRecoverer._recover` + each consumer's `give_up`; without it a poison message is retried forever or discarded silently; tests `test_the_last_failed_attempt_gives_up_and_acks`, `test_a_persistent_outage_ends_in_routing_failed_after_max_retries`.
  - **Stale-processing watchdog** — `services/notification-service/app/workers/watchdog.py`; tests in `test_watchdog.py`.
  - **Reserved recipients** — `shared/notification_shared/delivery.py`; without it a test run on a stack with credentials sends real messages; tests `tests/unit/test_delivery.py`, `test_a_reserved_recipient_never_reaches_the_configured_sender`, `test_a_sim_chat_id_never_reaches_the_bot_api`.

`docs/local-development.md`:
- Test section: eight invocations — add `PYTHONPATH=services/telegram-service ...`, `PYTHONPATH=gateway uv run pytest gateway/tests`, `PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests`.
- Debugging: the infra-only command gains `telegram-db`; the port list gains `8000` gateway and `8005` telegram; migrations apply to telegram-service the same way; the gateway has no migrations.
- Playground section: the two pages; `GATEWAY_URL`; the real-send confirmation; the load test only ever uses reserved recipients.
- A note under "uv workspace": if `uv sync` fails with "Accesso negato" on `.venv\Scripts`, close VS Code, whose language servers hold the venv.

- [ ] **Step 4: Service READMEs**

- `services/email-service/README.md`: environment table adds `PENDING_*`, `RECOVERY_POLL_INTERVAL_MS`, `HTTP_TIMEOUT_SECONDS`, the `SMTP_*` variables; a **Delivery** section with the rule order (fail marker, reserved, configured sender) and the SMTP outcome table from spec 6.2; `GET /version` reports `delivery_mode`; Workers: three tasks (consumer, outbox publisher, pending recoverer).
- `services/notification-service/README.md` and `services/routing-service/README.md`: Workers add the pending recoverer (and the watchdog for notification); environment tables add the new variables.
- Create `services/telegram-service/README.md` in the same structure as email-service's README: tables (`telegram_delivery`, `outbox`, `processed_events`), streams consumed (`notification.routed`, group `telegram-service`, filters `channel == "telegram"`), streams published, environment, endpoints, workers, and the Bot API outcome table from spec 4.5.
- Create `gateway/README.md`: purpose, the route table from spec 5.2, forwarding rules from 5.3, the 504/502 table from 5.4, environment (`NOTIFICATION_SERVICE_URL`, `CONFIGURATION_SERVICE_URL`, `GATEWAY_TIMEOUT_SECONDS`), what it deliberately does not do (5.5).

- [ ] **Step 5: Verify the docs against the code**

For every file path, class, method, test name, variable and default mentioned in the changed docs, open the code and confirm it exists with that exact name. Fix the doc, never the code, for any mismatch.

- [ ] **Step 6: Commit**

```bash
git add README.md docs services/*/README.md gateway/README.md
git commit -m "docs: document slice 2 recovery, telegram, gateway, real delivery and playground

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 15: Definition of Done

**Files:** none unless a gate fails.

- [ ] **Step 1: Test tiers**

```bash
uv run pytest tests/unit -v
uv run pytest tests/integration -v
for s in configuration-service notification-service routing-service email-service telegram-service; do PYTHONPATH=services/$s uv run pytest services/$s/tests -v || exit 1; done
PYTHONPATH=gateway uv run pytest gateway/tests -v
PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests -v
```

Expected: every suite PASS. Record the per-suite counts in the task report.

- [ ] **Step 2: Lint**

Run: `uv run ruff check . && uv run ruff format --check .`
Expected: clean.

- [ ] **Step 3: Full stack without a `.env`**

```bash
test -e .env && { echo "A .env exists: stop and ask the user before moving it"; exit 1; }
docker compose down -v && docker compose up --build -d --wait
uv run pytest tests/e2e -v
```

Expected: twelve containers healthy; e2e PASS.

- [ ] **Step 4: Full stack with fake real credentials — proves reserved recipients never reach a real sender**

```bash
cat > .env <<'EOF'
TELEGRAM_BOT_TOKEN=000000:fake-token-for-the-dod-gate
SMTP_HOST=smtp.invalid
SMTP_FROM=platform@example.com
EOF
docker compose up -d --wait
curl -s http://localhost:8004/version; echo; curl -s http://localhost:8005/version; echo
uv run pytest tests/e2e -v
rm .env
docker compose up -d --wait
```

Expected: `/version` reports `smtp` and `bot_api`; the e2e suite still PASSES. It could not if any reserved recipient reached a real sender: `smtp.invalid` does not resolve and the fake token is refused, so a real send would leave the notification stuck or `FAILED` with the wrong reason. After `rm .env`, both `/version` endpoints report `simulated` again.

- [ ] **Step 5: Manual gate (spec 12, gate 7)**

With the stack up and no `.env`: run the playground and a 200-notification load test (concurrency 10, telegram 0.3, failing 0.1). Expected: a green verdict. Record the throughput and p50/p95 in the task report. Real delivery to the user's own address and chat is checked by the user, not by this plan.

- [ ] **Step 6: Report**

List any deviation from this plan or the spec, each with its reason, and every parked item. The e2e `PROCESSING` flakiness from slice 1 stays parked (spec 13).

