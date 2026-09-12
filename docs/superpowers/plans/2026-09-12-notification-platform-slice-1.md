# Notification Platform — Slice 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the notification saga end-to-end in all three outcomes (completed, channel disabled, delivery failed) across four services, with the Outbox Pattern and idempotent consumers fully implemented and proven by tests.

**Architecture:** Four FastAPI services, each owning a Postgres database, communicating only through Redis Streams for domain operations. Every state change and its outgoing event are written in one database transaction; a background publisher moves outbox rows to Redis. Consumers check an idempotency table before acting and acknowledge only after their transaction commits. Notification status transitions are guarded in SQL so out-of-order consumers cannot corrupt state.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.x async + asyncpg, Alembic, PostgreSQL 16, Redis 7 (Streams only), redis-py 8.x async, Pydantic v2, pydantic-settings, HTTPX, Uvicorn, uv workspace, Docker Compose, pytest + pytest-asyncio + testcontainers, Ruff.

**Spec:** `docs/superpowers/specs/2026-09-12-notification-platform-design.md`

## Global Constraints

- Python **3.14** everywhere: `requires-python = ">=3.14"`, `.python-version` = `3.14`, Docker base `python:3.14-slim`.
- `uv` **exclusively**. Installed uv is **0.11.7**, which rejects `uv pip install` as a legacy interface — use `uv add`, `uv remove`, `uv sync`, `uv run` only.
- Pinned versions, verified to resolve and import on Python 3.14: `asyncpg>=0.31.0` (ships a `cp314` binary wheel), `sqlalchemy>=2.0.52`, `pydantic>=2.13.5`, `pydantic-settings>=2.15.0`, `fastapi>=0.141.1`, `uvicorn>=0.52.4`, `redis>=8.1.0`, `httpx>=0.28.1`, `alembic>=1.20.0`. Dev: `pytest>=9.1.1`, `pytest-asyncio>=1.4.0`, `testcontainers>=4.15.0`, `ruff>=0.16.7`.
- testcontainers 4.15 import paths are `testcontainers.community.postgres` and `testcontainers.community.redis`. The old `testcontainers.postgres` / `.redis` paths emit `DeprecationWarning` — do not use them.
- **Redis Streams only.** No Pub/Sub, no Redis as a cache. Every `XADD` writes a single field named `envelope` holding the JSON-serialized `EventEnvelope`.
- **No service reads another service's database.** No service calls another service's REST API for domain operations. The only REST calls are client → service and Routing → Configuration.
- Every consumer handler runs **one** database transaction containing the idempotency check, the domain state change, any outbox row, and `mark_processed`. `XACK` happens only after that transaction commits.
- `XGROUP CREATE <stream> <group> 0 MKSTREAM` — start offset is `0`, never `$`.
- Layering: route handlers call the service layer only; the service layer calls repositories; only repositories contain SQLAlchemy `select`/`insert`/`update`. Background tasks start in the FastAPI `lifespan`, never as per-request `BackgroundTasks`.
- All logs are structured JSON on stdout, including `service` and `correlation_id`.
- Commit after every task. Commit messages follow Conventional Commits.

## Out of scope for slice 1

Do not build these — they belong to slice 2 and slice 3, and building them early breaks the review boundary:

- `XPENDING` / `XCLAIM` recovery, and therefore `RedisStreamConsumer.get_pending()` and `.claim()`
- max-retry handling and the `FAILED_PERMANENT` status transition
- the stale-processing watchdog
- Telegram Service, API Gateway
- `docs/operations.md`, Prometheus metrics, Gateway API key

Consequence to accept rather than fix: a notification whose consumer crashes mid-flight stays in its current state until slice 2 lands.

---

## File Structure

```
shared/notification_shared/     shared library, one responsibility per module
  events.py                     EventEnvelope, Channel, EventType, Stream, ConsumerGroup
  exceptions.py                 ErrorCode, ErrorResponse, ServiceError + subclasses
  context.py                    correlation_id ContextVar
  logging.py                    JSONFormatter, configure_logging()
  config.py                     BaseServiceSettings
  models.py                     TimestampMixin, OutboxMixin, ProcessedEventMixin (abstract)
  outbox.py                     OutboxRepository
  idempotency.py                IdempotencyRepository, ProcessedStatus
  streams.py                    RedisStreamPublisher, RedisStreamConsumer, StreamMessage
  publisher.py                  OutboxPublisher (the one worker loop identical across services)
  middleware.py                 CorrelationIDMiddleware
  http_client.py                ServiceClient

services/<name>/app/
  main.py                       FastAPI app + lifespan (owns worker task lifecycle)
  core/config.py                service Settings
  core/database.py              async engine, session factory, get_db
  models/base.py                the service's own DeclarativeBase
  models/*.py                   one file per table
  schemas/*.py                  Pydantic request/response
  repositories/*.py             data access only
  services/*.py                 business logic
  workers/*.py                  one file per consumer loop
  alembic/                      per-service migration history
```

Each consumer loop is its own file and each table its own model file. Consumer handlers are written explicitly per service rather than behind a generic abstraction: the handlers genuinely differ, and an explicit thirty-line loop is the thing a reader of this project is meant to learn. Only the outbox publisher, which is identical across services, is shared.

### Test layout

```
tests/unit/                     shared library, no I/O
tests/integration/              shared library, real Postgres + Redis
tests/e2e/                      full docker compose stack
services/<name>/tests/          that service's integration tests
shared/notification_shared/testing.py   the container fixtures, imported by all of the above
```

Every service defines a module named `app`, so two service suites cannot share one pytest process. Each therefore runs on its own:

```bash
uv run pytest tests/unit
uv run pytest tests/integration
PYTHONPATH=services/configuration-service uv run pytest services/configuration-service/tests
PYTHONPATH=services/notification-service  uv run pytest services/notification-service/tests
PYTHONPATH=services/routing-service       uv run pytest services/routing-service/tests
PYTHONPATH=services/email-service         uv run pytest services/email-service/tests
```

Always invoke pytest from the repository root: some tests reference repository-relative paths such as a service's `alembic.ini`. Task 17 wraps these six commands in a `tasks.json` entry.

---

## Task 1: Workspace, toolchain, repository hygiene

**Files:**
- Create: `pyproject.toml`, `.python-version`, `.gitignore`, `.gitattributes`, `.dockerignore`
- Create: `shared/pyproject.toml`, `shared/notification_shared/__init__.py`
- Create: `tests/unit/__init__.py`, `tests/integration/__init__.py`, `tests/e2e/__init__.py`

**Interfaces:**
- Consumes: nothing
- Produces: a root `.venv` containing the whole workspace; `uv run pytest` and `uv run ruff check .` both operational

- [ ] **Step 1: Write `.gitattributes` and `.gitignore` first**

Git warned about LF→CRLF conversion on the spec commit. Normalize before adding source files.

`.gitattributes`:
```
* text=auto eol=lf
*.png binary
*.pyd binary
```

`.gitignore`:
```
.venv/
__pycache__/
*.py[cod]
*.egg-info/
.pytest_cache/
.ruff_cache/
.env
.superpowers/
```

`.superpowers/` holds the execution workspace (task briefs, reports, review packages) and must never be committed.

- [ ] **Step 2: Write the root `pyproject.toml`**

```toml
[project]
name = "notification-platform"
version = "1.0.0"
description = "Universal Notification Platform - microservices demo"
requires-python = ">=3.14"
dependencies = []

[tool.uv.workspace]
members = ["services/*", "shared"]

[dependency-groups]
dev = [
    "pytest>=9.1.1",
    "pytest-asyncio>=1.4.0",
    "testcontainers>=4.15.0",
    "ruff>=0.16.7",
]

[tool.ruff]
line-length = 100
target-version = "py314"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
markers = [
    "integration: requires Docker (testcontainers)",
    "e2e: requires the full docker compose stack",
]
```

`asyncio_mode = "auto"` matters: pytest-asyncio 1.x otherwise skips bare `async def` tests instead of running them, and skipped tests look like passing tests.

`.python-version`:
```
3.14
```

- [ ] **Step 3: Write `shared/pyproject.toml`**

```toml
[project]
name = "notification-shared"
version = "1.0.0"
requires-python = ">=3.14"
dependencies = [
    "pydantic>=2.13.5",
    "pydantic-settings>=2.15.0",
    "sqlalchemy>=2.0.52",
    "asyncpg>=0.31.0",
    "redis>=8.1.0",
    "httpx>=0.28.1",
    "starlette>=0.49.0",
]

[project.optional-dependencies]
testing = ["pytest>=9.1.1", "testcontainers>=4.15.0"]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

The `testing` extra declares what `notification_shared.testing` (Task 5) imports. The root dev group already installs both, so nothing changes operationally — but a package that ships a module importing undeclared dependencies is a defect, and the service images build with `--no-dev`, which excludes them.

Create `shared/notification_shared/__init__.py` as an empty file, plus empty `__init__.py` in each of the three test directories.

- [ ] **Step 4: Write the root `.dockerignore`**

The build context is the repository root, because the workspace needs `shared/` and the root `uv.lock`. This one file therefore governs every service image.

```
.venv/
.git/
__pycache__/
*.py[cod]
*.egg-info/
.pytest_cache/
.ruff_cache/
.vscode/
docs/
tests/
```

- [ ] **Step 5: Sync and verify the workspace**

```bash
uv sync --all-packages
uv run ruff check .
uv run pytest
```

Expected: sync creates `.venv` and `uv.lock`; ruff reports no errors; pytest exits 5 with "no tests ran", not a collection error.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: set up uv workspace and toolchain"
```

---

## Task 2: Shared library — events

**Files:**
- Create: `shared/notification_shared/events.py`
- Test: `tests/unit/test_events.py`

**Interfaces:**
- Consumes: nothing
- Produces: `Channel`, `EventType`, `Stream`, `ConsumerGroup` (all `StrEnum`); `ENVELOPE_FIELD`; `EventEnvelope` with classmethod `new(*, event_type, aggregate_id, payload, correlation_id)`, instance method `to_redis() -> dict[str, str]`, classmethod `from_redis(fields) -> EventEnvelope`. Every later task imports these names.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_events.py`:
```python
import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from notification_shared.events import (
    Channel,
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)


def test_new_sets_version_one_and_aware_timestamp():
    envelope = EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-1",
    )
    assert envelope.event_version == 1
    assert envelope.occurred_at.tzinfo is not None
    assert envelope.event_id is not None


def test_redis_round_trip_preserves_every_field():
    original = EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "recipient": "a@b.com", "subject": None},
        correlation_id="corr-2",
    )
    fields = original.to_redis()
    assert set(fields) == {"envelope"}
    assert isinstance(fields["envelope"], str)
    assert EventEnvelope.from_redis(fields) == original


def test_from_redis_accepts_bytes_keys_and_values():
    original = EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-3",
    )
    raw = {b"envelope": original.to_redis()["envelope"].encode()}
    assert EventEnvelope.from_redis(raw) == original


def test_from_redis_rejects_an_entry_without_the_envelope_field():
    with pytest.raises(ValueError, match="envelope"):
        EventEnvelope.from_redis({"payload": "{}"})


def test_unknown_event_type_is_rejected():
    with pytest.raises(ValidationError):
        EventEnvelope(
            event_id=uuid4(),
            event_type="NotAnEvent",
            occurred_at=datetime.now(UTC),
            correlation_id="c",
            aggregate_id=uuid4(),
            payload={},
        )


def test_unknown_channel_is_rejected():
    with pytest.raises(ValueError):
        Channel("carrier-pigeon")


def test_stream_and_group_names_match_the_spec():
    assert Stream.NOTIFICATION_CREATED == "notification.created"
    assert Stream.NOTIFICATION_ROUTED == "notification.routed"
    assert Stream.DELIVERY_COMPLETED == "delivery.completed"
    assert Stream.DELIVERY_FAILED == "delivery.failed"
    assert ConsumerGroup.ROUTING == "routing-service"
    assert ConsumerGroup.ROUTING_RESULTS == "routing-service-results"
    assert ConsumerGroup.EMAIL == "email-service"
    assert ConsumerGroup.NOTIFICATION_ROUTED == "notification-service-routed"
    assert ConsumerGroup.NOTIFICATION_RESULTS == "notification-service-results"


def test_nested_payload_values_survive_serialization():
    envelope = EventEnvelope.new(
        event_type=EventType.DELIVERY_FAILED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "reason": "simulated_failure", "attempt": 1},
        correlation_id="corr-4",
    )
    decoded = json.loads(envelope.to_redis()["envelope"])
    assert decoded["payload"]["attempt"] == 1
    assert decoded["event_version"] == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_events.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.events'`

- [ ] **Step 3: Implement `events.py`**

```python
"""Event vocabulary shared by every service.

Stream and consumer group names live here so a publisher and its consumers
cannot drift apart.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Channel(StrEnum):
    EMAIL = "email"
    TELEGRAM = "telegram"


class EventType(StrEnum):
    NOTIFICATION_CREATED = "NotificationCreated"
    NOTIFICATION_ROUTED = "NotificationRouted"
    ROUTING_FAILED = "RoutingFailed"
    DELIVERY_COMPLETED = "DeliveryCompleted"
    DELIVERY_FAILED = "DeliveryFailed"


class Stream(StrEnum):
    NOTIFICATION_CREATED = "notification.created"
    NOTIFICATION_ROUTED = "notification.routed"
    DELIVERY_COMPLETED = "delivery.completed"
    DELIVERY_FAILED = "delivery.failed"


class ConsumerGroup(StrEnum):
    ROUTING = "routing-service"
    ROUTING_RESULTS = "routing-service-results"
    EMAIL = "email-service"
    TELEGRAM = "telegram-service"
    NOTIFICATION_ROUTED = "notification-service-routed"
    NOTIFICATION_RESULTS = "notification-service-results"


ENVELOPE_FIELD = "envelope"


class EventEnvelope(BaseModel):
    """An immutable fact published to a stream.

    `aggregate_id` is always the notification_id, so the whole saga correlates
    on one identifier.
    """

    event_id: UUID
    event_type: EventType
    event_version: int = 1
    occurred_at: datetime
    correlation_id: str
    aggregate_id: UUID
    payload: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def new(
        cls,
        *,
        event_type: EventType,
        aggregate_id: UUID,
        payload: dict[str, Any],
        correlation_id: str,
    ) -> Self:
        return cls(
            event_id=uuid4(),
            event_type=event_type,
            occurred_at=datetime.now(UTC),
            correlation_id=correlation_id,
            aggregate_id=aggregate_id,
            payload=payload,
        )

    def to_redis(self) -> dict[str, str]:
        """One field, JSON value. See spec correction 3.19."""
        return {ENVELOPE_FIELD: self.model_dump_json()}

    @classmethod
    def from_redis(cls, fields: dict[Any, Any]) -> Self:
        """Parse a stream entry. redis-py may return bytes keys and values."""
        for key, value in fields.items():
            name = key.decode() if isinstance(key, bytes) else key
            if name == ENVELOPE_FIELD:
                raw = value.decode() if isinstance(value, bytes) else value
                return cls.model_validate_json(raw)
        raise ValueError(f"stream entry has no {ENVELOPE_FIELD} field: {list(fields)}")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_events.py -v`
Expected: PASS, 8 tests

- [ ] **Step 5: Commit**

```bash
git add shared/notification_shared/events.py tests/unit/test_events.py
git commit -m "feat(shared): add event envelope and stream vocabulary"
```

---

## Task 3: Shared library — errors, correlation context, JSON logging, settings

**Files:**
- Create: `shared/notification_shared/exceptions.py`, `context.py`, `logging.py`, `config.py` (all under `shared/notification_shared/`)
- Test: `tests/unit/test_exceptions.py`, `tests/unit/test_logging.py`

**Interfaces:**
- Consumes: nothing
- Produces: `ErrorCode`, `ErrorDetail`, `ErrorResponse`, `ServiceError` (attrs `code`, `message`, `status_code`, method `to_response()`), `ValidationFailedError`, `NotFoundError`, `ServiceUnavailableError`; `set_correlation_id(value)`, `get_correlation_id()`, `correlation_id_var`; `JSONFormatter(service_name)`, `configure_logging(service_name, log_level)`; `BaseServiceSettings`.

`NotFoundError` and `ServiceUnavailableError` **must be sibling types, neither a subclass of the other**. Task 12 depends on that distinction to tell "channel does not exist" (permanent → publish `RoutingFailed`) from "Configuration Service is down" (transient → do not ack). See spec 3.18.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_exceptions.py`:
```python
import pytest
from pydantic import ValidationError

from notification_shared.exceptions import (
    ErrorCode,
    ErrorResponse,
    NotFoundError,
    ServiceError,
    ServiceUnavailableError,
    ValidationFailedError,
)


def test_service_error_renders_the_common_error_model():
    error = ServiceError(ErrorCode.CHANNEL_DISABLED, "Channel telegram is currently disabled")
    assert error.to_response().model_dump(mode="json") == {
        "error": {
            "code": "CHANNEL_DISABLED",
            "message": "Channel telegram is currently disabled",
        }
    }


def test_not_found_and_unavailable_are_sibling_types():
    assert issubclass(NotFoundError, ServiceError)
    assert issubclass(ServiceUnavailableError, ServiceError)
    assert not issubclass(NotFoundError, ServiceUnavailableError)
    assert not issubclass(ServiceUnavailableError, NotFoundError)


def test_each_subclass_carries_its_status_and_code():
    assert NotFoundError("channel 'sms' not found").status_code == 404
    assert NotFoundError("x").code is ErrorCode.NOT_FOUND
    assert ServiceUnavailableError("configuration-service timed out").status_code == 503
    assert ValidationFailedError("body must not be empty").status_code == 422
    assert ValidationFailedError("x").code is ErrorCode.VALIDATION_ERROR


def test_error_response_rejects_a_bare_string():
    with pytest.raises(ValidationError):
        ErrorResponse(error="oops")


def test_every_spec_error_code_exists():
    for name in (
        "VALIDATION_ERROR",
        "NOT_FOUND",
        "CHANNEL_DISABLED",
        "ROUTING_UNAVAILABLE",
        "CONFIGURATION_UNAVAILABLE",
        "DELIVERY_UNAVAILABLE",
        "STREAM_ERROR",
    ):
        assert ErrorCode[name].value == name
```

`tests/unit/test_logging.py`:
```python
import json
import logging
from uuid import uuid4

from notification_shared.context import get_correlation_id, set_correlation_id
from notification_shared.logging import JSONFormatter


def _record(message: str = "hello", **extra) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def _format(record: logging.LogRecord) -> dict:
    return json.loads(JSONFormatter(service_name="test-service").format(record))


def test_output_is_json_with_the_required_keys():
    payload = _format(_record())
    assert payload["level"] == "INFO"
    assert payload["service"] == "test-service"
    assert payload["message"] == "hello"
    assert "timestamp" in payload


def test_correlation_id_is_read_from_the_context_var():
    set_correlation_id("corr-42")
    try:
        assert _format(_record())["correlation_id"] == "corr-42"
    finally:
        set_correlation_id(None)


def test_correlation_id_is_null_when_unset():
    set_correlation_id(None)
    assert _format(_record())["correlation_id"] is None


def test_event_fields_passed_via_extra_are_included():
    event_id = str(uuid4())
    payload = _format(_record(event_id=event_id, event_type="NotificationCreated"))
    assert payload["event_id"] == event_id
    assert payload["event_type"] == "NotificationCreated"


def test_absent_event_fields_are_omitted_rather_than_nulled():
    assert "event_id" not in _format(_record())


def test_set_and_get_correlation_id_round_trip():
    set_correlation_id("abc")
    try:
        assert get_correlation_id() == "abc"
    finally:
        set_correlation_id(None)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_exceptions.py tests/unit/test_logging.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.exceptions'`

- [ ] **Step 3: Implement `exceptions.py`**

```python
"""Common error vocabulary.

`NotFoundError` and `ServiceUnavailableError` are separate types on purpose:
Routing Service treats a missing channel as a permanent routing failure and a
Configuration Service outage as transient. See spec correction 3.18.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class ErrorCode(StrEnum):
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    CHANNEL_DISABLED = "CHANNEL_DISABLED"
    ROUTING_UNAVAILABLE = "ROUTING_UNAVAILABLE"
    CONFIGURATION_UNAVAILABLE = "CONFIGURATION_UNAVAILABLE"
    DELIVERY_UNAVAILABLE = "DELIVERY_UNAVAILABLE"
    STREAM_ERROR = "STREAM_ERROR"


class ErrorDetail(BaseModel):
    code: ErrorCode
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class ServiceError(Exception):
    """Base for errors that map onto the common HTTP error model."""

    status_code: int = 500

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def to_response(self) -> ErrorResponse:
        return ErrorResponse(error=ErrorDetail(code=self.code, message=self.message))


class ValidationFailedError(ServiceError):
    status_code = 422

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.VALIDATION_ERROR, message)


class NotFoundError(ServiceError):
    status_code = 404

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.NOT_FOUND, message)


class ServiceUnavailableError(ServiceError):
    status_code = 503

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.CONFIGURATION_UNAVAILABLE,
    ) -> None:
        super().__init__(code, message)
```

- [ ] **Step 4: Implement `context.py`**

```python
"""Correlation id carried across HTTP handlers and worker tasks.

A ContextVar rather than a parameter: the HTTP middleware sets it once per
request and each worker sets it once per consumed event, so `JSONFormatter`
finds it without every log call passing it along.
"""

from __future__ import annotations

from contextvars import ContextVar

correlation_id_var: ContextVar[str | None] = ContextVar("correlation_id", default=None)


def set_correlation_id(value: str | None) -> None:
    correlation_id_var.set(value)


def get_correlation_id() -> str | None:
    return correlation_id_var.get()
```

- [ ] **Step 5: Implement `logging.py`**

```python
"""Structured JSON logging to stdout."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

from notification_shared.context import get_correlation_id

_OPTIONAL_FIELDS = ("event_id", "event_type", "notification_id", "stream", "consumer_group")


class JSONFormatter(logging.Formatter):
    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "service": self.service_name,
            "correlation_id": get_correlation_id(),
            "message": record.getMessage(),
        }
        for field in _OPTIONAL_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                payload[field] = str(value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging(service_name: str, log_level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter(service_name))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level.upper())
    # Uvicorn installs its own handlers; route them through ours instead.
    for name in ("uvicorn", "uvicorn.access", "uvicorn.error"):
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = True
```

- [ ] **Step 6: Implement `config.py`**

```python
"""Base settings every service extends."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str
    service_version: str = "1.0.0"
    log_level: str = "INFO"
```

- [ ] **Step 7: Run the whole unit tier to verify it passes**

Run: `uv run pytest tests/unit -v`
Expected: PASS, 19 tests (8 from Task 2, 5 exceptions, 6 logging)

- [ ] **Step 8: Commit**

```bash
git add shared/notification_shared tests/unit
git commit -m "feat(shared): add error model, correlation context, JSON logging"
```

---

## Task 4: Shared library — abstract model mixins

**Files:**
- Create: `shared/notification_shared/models.py`
- Test: `tests/unit/test_models.py`

**Interfaces:**
- Consumes: nothing
- Produces: `TimestampMixin`, `OutboxMixin`, `ProcessedEventMixin`. None declares `__tablename__` or `__abstract__`; each service materializes them on its own `DeclarativeBase`. Concrete column names: `created_at`, `updated_at`; outbox `id`, `stream`, `payload`, `published`, `created_at`; processed-events `id`, `event_id`, `consumer_group`, `status`, `fail_count`, `processed_at`.

Mixins rather than concrete tables — see spec correction 3.12. A single shared `Base` breaks test isolation once one pytest process imports models from two services.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_models.py`:
```python
from sqlalchemy.orm import DeclarativeBase

from notification_shared.models import OutboxMixin, ProcessedEventMixin, TimestampMixin


class Base(DeclarativeBase):
    pass


class Outbox(Base, OutboxMixin):
    __tablename__ = "outbox"


class ProcessedEvent(Base, ProcessedEventMixin):
    __tablename__ = "processed_events"


class Thing(Base, TimestampMixin):
    __tablename__ = "thing"
    from sqlalchemy.orm import Mapped, mapped_column

    id: Mapped[int] = mapped_column(primary_key=True)


def test_mixins_declare_no_table_of_their_own():
    assert not hasattr(OutboxMixin, "__tablename__")
    assert not hasattr(ProcessedEventMixin, "__tablename__")
    assert not hasattr(TimestampMixin, "__tablename__")


def test_outbox_columns_match_the_spec():
    assert set(Outbox.__table__.columns.keys()) == {
        "id",
        "stream",
        "payload",
        "published",
        "created_at",
    }
    assert Outbox.__table__.c.published.nullable is False
    assert Outbox.__table__.c.payload.nullable is False


def test_outbox_has_an_index_supporting_the_pending_query():
    indexed = [tuple(ix.columns.keys()) for ix in Outbox.__table__.indexes]
    assert ("published", "created_at") in indexed


def test_processed_events_columns_match_the_spec():
    assert set(ProcessedEvent.__table__.columns.keys()) == {
        "id",
        "event_id",
        "consumer_group",
        "status",
        "fail_count",
        "processed_at",
    }


def test_processed_events_is_unique_on_event_and_group():
    unique_sets = [
        set(c.columns.keys())
        for c in ProcessedEvent.__table__.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert {"event_id", "consumer_group"} in unique_sets


def test_timestamp_mixin_adds_both_columns_with_server_defaults():
    assert Thing.__table__.c.created_at.server_default is not None
    assert Thing.__table__.c.updated_at.server_default is not None
    assert Thing.__table__.c.updated_at.onupdate is not None


def test_two_bases_can_materialize_the_mixins_independently():
    class OtherBase(DeclarativeBase):
        pass

    class OtherOutbox(OtherBase, OutboxMixin):
        __tablename__ = "outbox"

    assert "outbox" in OtherBase.metadata.tables
    assert "outbox" in Base.metadata.tables
    assert OtherBase.metadata is not Base.metadata
```

The last test is the point of the whole task: two services each get their own `outbox` table without a registry collision.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.models'`

- [ ] **Step 3: Implement `models.py`**

```python
"""Abstract mixins for tables every event-driven service needs.

These are mixins, not concrete models, so each service materializes them on
its own DeclarativeBase. A shared Base would put every service's tables into
one registry, which breaks schema isolation in the test suite. See spec 3.12.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, declared_attr, mapped_column


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class OutboxMixin:
    """Events awaiting publication, written in the same transaction as the state change."""

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    stream: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    published: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    @declared_attr.directive
    def __table_args__(cls) -> tuple:  # noqa: N805
        return (
            Index(f"ix_{cls.__tablename__}_pending", "published", "created_at"),
        )


class ProcessedEventMixin:
    """Idempotency ledger.

    `status` separates "done" from "has failed N times" — without it a single
    failed attempt makes a never-processed event look processed. See spec 3.4.
    """

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    @declared_attr.directive
    def __table_args__(cls) -> tuple:  # noqa: N805
        return (
            UniqueConstraint(
                "event_id", "consumer_group", name=f"uq_{cls.__tablename__}_event_group"
            ),
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_models.py -v`
Expected: PASS, 7 tests

- [ ] **Step 5: Commit**

```bash
git add shared/notification_shared/models.py tests/unit/test_models.py
git commit -m "feat(shared): add abstract model mixins for outbox and idempotency"
```

---

## Task 5: Shared library — OutboxRepository and the integration test harness

**Files:**
- Create: `shared/notification_shared/outbox.py`, `shared/notification_shared/testing.py`
- Create: `tests/integration/conftest.py`
- Test: `tests/integration/test_outbox.py`

**Interfaces:**
- Consumes: `EventEnvelope` (Task 2), `OutboxMixin` (Task 4)
- Produces: `OutboxRepository(model)` with `async save(session, stream, envelope) -> None`, `async get_pending(session, limit=100) -> Sequence`, `async mark_published(session, ids) -> None`. Also the fixtures `postgres_url` (session), `engine`, `make_schema`, later `redis_url` and `redis_client`.

`save()` must **not** commit. Its whole purpose is to enlist in the caller's transaction.

- [ ] **Step 1: Write the integration harness**

The fixtures live in the shared library rather than in a conftest, because four
service test suites need them and each service runs in its own pytest process.

`shared/notification_shared/testing.py`:
```python
"""Fixtures backed by real Postgres and Redis containers.

Container startup is session-scoped because it is slow; engines and clients are
function-scoped because pytest-asyncio runs each test in its own event loop and
a connection created in one loop cannot be reused in another.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterator

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from testcontainers.community.postgres import PostgresContainer


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as container:
        yield container.get_connection_url()


@pytest.fixture
async def engine(postgres_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(postgres_url, poolclass=NullPool)
    yield engine
    await engine.dispose()


@pytest.fixture
async def make_schema(engine: AsyncEngine) -> AsyncIterator[Callable]:
    """Create a MetaData's tables and hand back a session factory.

    Each test brings its own DeclarativeBase, so the harness stays agnostic
    about which service is under test.
    """
    created = []

    async def _make(metadata) -> async_sessionmaker:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.run_sync(metadata.create_all)
        created.append(metadata)
        return async_sessionmaker(engine, expire_on_commit=False)

    yield _make

    for metadata in reversed(created):
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
```

`tests/integration/conftest.py` — one line, repeated later in each service's `tests/conftest.py`:
```python
from notification_shared.testing import (  # noqa: F401
    engine,
    make_schema,
    postgres_url,
)
```

- [ ] **Step 2: Write the failing tests**

`tests/integration/test_outbox.py`:
```python
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import DeclarativeBase

from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.models import OutboxMixin
from notification_shared.outbox import OutboxRepository

pytestmark = pytest.mark.integration


class Base(DeclarativeBase):
    pass


class Outbox(Base, OutboxMixin):
    __tablename__ = "outbox"


def _envelope() -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "recipient": "a@b.com"},
        correlation_id="corr-outbox",
    )


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


async def test_save_enlists_in_the_callers_transaction(sessions):
    repo = OutboxRepository(Outbox)
    async with sessions() as session:
        await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.commit()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_a_rolled_back_transaction_leaves_no_outbox_row(sessions):
    repo = OutboxRepository(Outbox)
    async with sessions() as session:
        await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.rollback()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_get_pending_returns_only_unpublished_rows_oldest_first(sessions):
    """One transaction per row on purpose.

    `created_at` defaults to `func.now()`, which in Postgres is the
    *transaction* start time — three rows saved in one transaction would share
    a timestamp and the ORDER BY could not be asserted at all.
    """
    repo = OutboxRepository(Outbox)
    saved_ids = []
    for _ in range(3):
        async with sessions() as session:
            await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
            await session.commit()
        async with sessions() as session:
            newest = (await repo.get_pending(session))[-1]
            saved_ids.append(newest.id)

    async with sessions() as session:
        pending = await repo.get_pending(session)
        assert [row.id for row in pending] == saved_ids, "not returned oldest-first"
        await repo.mark_published(session, [pending[0].id])
        await session.commit()

    async with sessions() as session:
        remaining = await repo.get_pending(session)
        assert [row.id for row in remaining] == saved_ids[1:]
        assert all(row.published is False for row in remaining)


async def test_get_pending_respects_the_limit(sessions):
    repo = OutboxRepository(Outbox)
    async with sessions() as session:
        for _ in range(5):
            await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.commit()

    async with sessions() as session:
        assert len(await repo.get_pending(session, limit=2)) == 2


async def test_mark_published_with_an_empty_list_is_a_no_op(sessions):
    """Must distinguish a guarded no-op from an unguarded one.

    Asserting only that the call does not raise proves nothing: an empty
    `IN ()` compiles to an always-false predicate, so an unguarded UPDATE
    would also pass. Seed a row and prove it was left alone.
    """
    repo = OutboxRepository(Outbox)
    async with sessions() as session:
        await repo.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.commit()

    async with sessions() as session:
        await repo.mark_published(session, [])
        await session.commit()

    async with sessions() as session:
        assert len(await repo.get_pending(session)) == 1


async def test_the_stored_payload_round_trips_back_into_an_envelope(sessions):
    repo = OutboxRepository(Outbox)
    original = _envelope()
    async with sessions() as session:
        await repo.save(session, Stream.NOTIFICATION_CREATED, original)
        await session.commit()

    async with sessions() as session:
        row = (await repo.get_pending(session))[0]
        assert row.stream == "notification.created"
        assert EventEnvelope.model_validate(row.payload) == original
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/integration/test_outbox.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.outbox'`. Docker must be running; the first run pulls `postgres:16-alpine`.

- [ ] **Step 4: Implement `outbox.py`**

```python
"""Outbox access.

`save()` deliberately does not commit: the atomicity guarantee comes from the
row being written inside the caller's transaction, alongside the state change.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from notification_shared.events import EventEnvelope


class OutboxRepository:
    def __init__(self, model: type[Any]) -> None:
        self.model = model

    async def save(self, session: AsyncSession, stream: str, envelope: EventEnvelope) -> None:
        session.add(
            self.model(stream=str(stream), payload=envelope.model_dump(mode="json"))
        )

    async def get_pending(self, session: AsyncSession, limit: int = 100) -> Sequence[Any]:
        stmt = (
            select(self.model)
            .where(self.model.published.is_(False))
            .order_by(self.model.created_at)
            .limit(limit)
        )
        return (await session.scalars(stmt)).all()

    async def mark_published(self, session: AsyncSession, ids: Sequence[UUID]) -> None:
        if not ids:
            return
        await session.execute(
            update(self.model).where(self.model.id.in_(list(ids))).values(published=True)
        )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/integration/test_outbox.py -v`
Expected: PASS, 6 tests

- [ ] **Step 6: Commit**

```bash
git add shared/notification_shared/outbox.py tests/integration
git commit -m "feat(shared): add outbox repository with testcontainers harness"
```

---

## Task 6: Shared library — IdempotencyRepository

**Files:**
- Create: `shared/notification_shared/idempotency.py`
- Test: `tests/integration/test_idempotency.py`

**Interfaces:**
- Consumes: `ProcessedEventMixin` (Task 4), the `make_schema` fixture (Task 5)
- Produces: `ProcessedStatus` (`PROCESSED`, `FAILING`, `FAILED_PERMANENT`); `IdempotencyRepository(model)` with `async is_processed(session, event_id, consumer_group) -> bool`, `async mark_processed(session, event_id, consumer_group) -> None`, `async increment_fail_count(session, event_id, consumer_group) -> int`.

Both writers upsert on `(event_id, consumer_group)`: `mark_processed` may find a `FAILING` row left by an earlier attempt.

`increment_fail_count` and `ProcessedStatus.FAILED_PERMANENT` are **not called anywhere in slice 1** — max-retry handling is slice 2. They are built now anyway, because `increment_fail_count` is the same upsert as `mark_processed` and splitting this repository across slices would mean writing and reviewing it twice. This is the only piece of slice 1 built ahead of its caller; everything else has a consumer within the slice.

- [ ] **Step 1: Write the failing tests**

`tests/integration/test_idempotency.py`:
```python
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import DeclarativeBase

from notification_shared.events import ConsumerGroup
from notification_shared.idempotency import IdempotencyRepository, ProcessedStatus
from notification_shared.models import ProcessedEventMixin

pytestmark = pytest.mark.integration

GROUP = ConsumerGroup.ROUTING


class Base(DeclarativeBase):
    pass


class ProcessedEvent(Base, ProcessedEventMixin):
    __tablename__ = "processed_events"


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


async def test_an_unseen_event_is_not_processed(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    async with sessions() as session:
        assert await repo.is_processed(session, uuid4(), GROUP) is False


async def test_mark_processed_then_is_processed(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.mark_processed(session, event_id, GROUP)
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, GROUP) is True


async def test_a_failing_row_is_not_treated_as_processed(sessions):
    """Spec 3.4 — the bug the status column exists to prevent."""
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        assert await repo.increment_fail_count(session, event_id, GROUP) == 1
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, GROUP) is False
        status = await session.scalar(
            select(ProcessedEvent.status).where(ProcessedEvent.event_id == event_id)
        )
        assert status == ProcessedStatus.FAILING


async def test_increment_accumulates_and_returns_the_new_count(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        assert await repo.increment_fail_count(session, event_id, GROUP) == 1
        assert await repo.increment_fail_count(session, event_id, GROUP) == 2
        assert await repo.increment_fail_count(session, event_id, GROUP) == 3
        await session.commit()


async def test_mark_processed_overwrites_a_failing_row(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.increment_fail_count(session, event_id, GROUP)
        await repo.mark_processed(session, event_id, GROUP)
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, GROUP) is True


async def test_mark_processed_twice_does_not_violate_the_unique_constraint(sessions):
    """Asserting "did not raise" would not prove the upsert worked.

    Without ON CONFLICT the second call raises; with it, exactly one row must
    remain. Count the rows.
    """
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.mark_processed(session, event_id, GROUP)
        await repo.mark_processed(session, event_id, GROUP)
        await session.commit()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1
        assert await repo.is_processed(session, event_id, GROUP) is True


async def test_the_same_event_is_tracked_per_consumer_group(sessions):
    repo = IdempotencyRepository(ProcessedEvent)
    event_id = uuid4()
    async with sessions() as session:
        await repo.mark_processed(session, event_id, ConsumerGroup.ROUTING)
        await session.commit()

    async with sessions() as session:
        assert await repo.is_processed(session, event_id, ConsumerGroup.ROUTING) is True
        assert await repo.is_processed(session, event_id, ConsumerGroup.EMAIL) is False
```

The last test matters: `delivery.failed` is read by two different groups, and each must process it once independently.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/integration/test_idempotency.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.idempotency'`

- [ ] **Step 3: Implement `idempotency.py`**

```python
"""Idempotency ledger access.

Both writers upsert, because `mark_processed` may find a FAILING row left by an
earlier attempt at the same event.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession


class ProcessedStatus(StrEnum):
    PROCESSED = "PROCESSED"
    FAILING = "FAILING"
    FAILED_PERMANENT = "FAILED_PERMANENT"


_TERMINAL = (ProcessedStatus.PROCESSED.value, ProcessedStatus.FAILED_PERMANENT.value)


class IdempotencyRepository:
    def __init__(self, model: type[Any]) -> None:
        self.model = model

    async def is_processed(
        self, session: AsyncSession, event_id: UUID, consumer_group: str
    ) -> bool:
        status = await session.scalar(
            select(self.model.status).where(
                self.model.event_id == event_id,
                self.model.consumer_group == str(consumer_group),
            )
        )
        return status in _TERMINAL

    async def mark_processed(
        self, session: AsyncSession, event_id: UUID, consumer_group: str
    ) -> None:
        stmt = (
            pg_insert(self.model)
            .values(
                id=uuid4(),
                event_id=event_id,
                consumer_group=str(consumer_group),
                status=ProcessedStatus.PROCESSED.value,
                fail_count=0,
            )
            .on_conflict_do_update(
                index_elements=["event_id", "consumer_group"],
                set_={"status": ProcessedStatus.PROCESSED.value},
            )
        )
        await session.execute(stmt)

    async def increment_fail_count(
        self, session: AsyncSession, event_id: UUID, consumer_group: str
    ) -> int:
        stmt = (
            pg_insert(self.model)
            .values(
                id=uuid4(),
                event_id=event_id,
                consumer_group=str(consumer_group),
                status=ProcessedStatus.FAILING.value,
                fail_count=1,
            )
            .on_conflict_do_update(
                index_elements=["event_id", "consumer_group"],
                set_={
                    "fail_count": self.model.fail_count + 1,
                    "status": ProcessedStatus.FAILING.value,
                },
            )
            .returning(self.model.fail_count)
        )
        return await session.scalar(stmt)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/integration/test_idempotency.py -v`
Expected: PASS, 7 tests

- [ ] **Step 5: Commit**

```bash
git add shared/notification_shared/idempotency.py tests/integration/test_idempotency.py
git commit -m "feat(shared): add idempotency repository with processed/failing states"
```

---

## Task 7: Shared library — Redis Streams publisher and consumer

**Files:**
- Create: `shared/notification_shared/streams.py`
- Modify: `shared/notification_shared/testing.py` (add the Redis fixtures), `tests/integration/conftest.py`
- Test: `tests/integration/test_streams.py`

**Interfaces:**
- Consumes: `EventEnvelope` (Task 2)
- Produces: `StreamMessage(message_id: str, envelope: EventEnvelope)`; `RedisStreamPublisher(redis)` with `async publish(stream, envelope) -> str`; `RedisStreamConsumer(redis, consumer_name)` with `async ensure_group(stream, group)`, `async read(stream, group, count=10, block_ms=1000) -> list[StreamMessage]`, `async ack(stream, group, message_id)`. Fixtures `redis_url` (session) and `redis_client` (function).

`get_pending()` and `claim()` are **slice 2** — do not add them here.

- [ ] **Step 1: Add the Redis fixtures to `shared/notification_shared/testing.py`**

Append these imports and fixtures to the file created in Task 5, and add `redis_client` and `redis_url` to the re-export in `tests/integration/conftest.py`:

```python
import redis.asyncio as aioredis
from testcontainers.community.redis import RedisContainer


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer("redis:7-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest.fixture
async def redis_client(redis_url: str) -> AsyncIterator[aioredis.Redis]:
    client = aioredis.from_url(redis_url)
    await client.flushall()
    yield client
    await client.aclose()
```

`flushall()` before each test, not after: a test that fails mid-way then leaves its state available for inspection.

- [ ] **Step 2: Write the failing tests**

`tests/integration/test_streams.py`:
```python
from uuid import uuid4

import pytest

from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.streams import RedisStreamConsumer, RedisStreamPublisher

pytestmark = pytest.mark.integration

STREAM = Stream.NOTIFICATION_CREATED
GROUP = ConsumerGroup.ROUTING


def _envelope(correlation_id: str = "corr-stream") -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "recipient": "a@b.com"},
        correlation_id=correlation_id,
    )


async def test_publish_returns_a_string_message_id(redis_client):
    message_id = await RedisStreamPublisher(redis_client).publish(STREAM, _envelope())
    assert isinstance(message_id, str)
    assert "-" in message_id


async def test_ensure_group_creates_the_stream_when_it_does_not_exist(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    assert await redis_client.exists(str(STREAM)) == 1


async def test_ensure_group_is_idempotent(redis_client):
    """Two calls must leave exactly one group, not merely avoid raising.

    A BUSYGROUP error that was swallowed too broadly, or a second group
    created under the same name, would both pass a no-assert test.
    """
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    await consumer.ensure_group(STREAM, GROUP)

    groups = await redis_client.xinfo_groups(str(STREAM))
    assert len(groups) == 1
    name = groups[0]["name"]
    assert (name.decode() if isinstance(name, bytes) else name) == str(GROUP)


async def test_a_group_created_at_offset_zero_sees_earlier_messages(redis_client):
    """Spec 3.2 — the first-boot race. With '$' this test would read nothing."""
    published = _envelope()
    await RedisStreamPublisher(redis_client).publish(STREAM, published)

    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)

    messages = await consumer.read(STREAM, GROUP, block_ms=100)
    assert [m.envelope for m in messages] == [published]


async def test_read_returns_the_envelope_intact(redis_client):
    published = _envelope("corr-intact")
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    await RedisStreamPublisher(redis_client).publish(STREAM, published)

    message = (await consumer.read(STREAM, GROUP, block_ms=100))[0]
    assert message.envelope == published
    assert isinstance(message.message_id, str)


async def test_an_acked_message_is_not_redelivered(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    await RedisStreamPublisher(redis_client).publish(STREAM, _envelope())

    first = await consumer.read(STREAM, GROUP, block_ms=100)
    assert len(first) == 1
    await consumer.ack(STREAM, GROUP, first[0].message_id)

    assert await consumer.read(STREAM, GROUP, block_ms=100) == []
    pending = await redis_client.xpending(str(STREAM), str(GROUP))
    assert pending["pending"] == 0


async def test_an_unacked_message_stays_pending(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    await RedisStreamPublisher(redis_client).publish(STREAM, _envelope())

    await consumer.read(STREAM, GROUP, block_ms=100)
    pending = await redis_client.xpending(str(STREAM), str(GROUP))
    assert pending["pending"] == 1


async def test_two_groups_on_one_stream_each_receive_every_message(redis_client):
    """delivery.failed is read independently by routing and notification."""
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, ConsumerGroup.ROUTING)
    await consumer.ensure_group(STREAM, ConsumerGroup.NOTIFICATION_RESULTS)
    published = _envelope()
    await RedisStreamPublisher(redis_client).publish(STREAM, published)

    for group in (ConsumerGroup.ROUTING, ConsumerGroup.NOTIFICATION_RESULTS):
        messages = await consumer.read(STREAM, group, block_ms=100)
        assert [m.envelope for m in messages] == [published]


async def test_read_on_an_empty_stream_returns_an_empty_list(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    assert await consumer.read(STREAM, GROUP, block_ms=50) == []


async def test_count_caps_the_batch_size(redis_client):
    consumer = RedisStreamConsumer(redis_client, consumer_name="c1")
    await consumer.ensure_group(STREAM, GROUP)
    publisher = RedisStreamPublisher(redis_client)
    for _ in range(5):
        await publisher.publish(STREAM, _envelope())

    assert len(await consumer.read(STREAM, GROUP, count=2, block_ms=100)) == 2
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/integration/test_streams.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.streams'`

- [ ] **Step 4: Implement `streams.py`**

```python
"""Redis Streams access.

Groups are always created at offset 0 so that an event published before its
consumer started is still delivered. Replay is safe because every consumer
checks the idempotency ledger. See spec correction 3.2.
"""

from __future__ import annotations

from typing import Any, NamedTuple

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from notification_shared.events import EventEnvelope


class StreamMessage(NamedTuple):
    message_id: str
    envelope: EventEnvelope


def _as_str(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class RedisStreamPublisher:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def publish(self, stream: str, envelope: EventEnvelope) -> str:
        message_id = await self._redis.xadd(str(stream), envelope.to_redis())
        return _as_str(message_id)


class RedisStreamConsumer:
    def __init__(self, redis: Redis, consumer_name: str) -> None:
        self._redis = redis
        self._consumer_name = consumer_name

    async def ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(
                name=str(stream), groupname=str(group), id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def read(
        self, stream: str, group: str, count: int = 10, block_ms: int = 1000
    ) -> list[StreamMessage]:
        response = await self._redis.xreadgroup(
            groupname=str(group),
            consumername=self._consumer_name,
            streams={str(stream): ">"},
            count=count,
            block=block_ms,
        )
        messages: list[StreamMessage] = []
        for _stream_name, entries in response or []:
            for message_id, fields in entries:
                messages.append(
                    StreamMessage(_as_str(message_id), EventEnvelope.from_redis(fields))
                )
        return messages

    async def ack(self, stream: str, group: str, message_id: str) -> None:
        await self._redis.xack(str(stream), str(group), message_id)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/integration/test_streams.py -v`
Expected: PASS, 10 tests

- [ ] **Step 6: Commit**

```bash
git add shared/notification_shared/streams.py tests/integration
git commit -m "feat(shared): add Redis Streams publisher and consumer at offset 0"
```

---

## Task 8: Shared library — OutboxPublisher worker

**Files:**
- Create: `shared/notification_shared/publisher.py`
- Test: `tests/integration/test_publisher.py`

**Interfaces:**
- Consumes: `OutboxRepository` (Task 5), `RedisStreamPublisher` (Task 7)
- Produces: `OutboxPublisher(*, session_factory, repository, publisher, poll_interval_ms=500, batch_size=100)` with `async publish_once() -> int` (returns how many rows were published) and `async run_forever() -> None`.

This is the only worker loop in the shared library, because it is identical in every service. `publish_once()` exists separately from `run_forever()` so tests can drive one iteration without racing a background task.

- [ ] **Step 1: Write the failing tests**

`tests/integration/test_publisher.py`:
```python
from uuid import uuid4

import pytest
from sqlalchemy.orm import DeclarativeBase

from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.models import OutboxMixin
from notification_shared.outbox import OutboxRepository
from notification_shared.publisher import OutboxPublisher
from notification_shared.streams import RedisStreamPublisher

pytestmark = pytest.mark.integration


class Base(DeclarativeBase):
    pass


class Outbox(Base, OutboxMixin):
    __tablename__ = "outbox"


def _envelope() -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-pub",
    )


@pytest.fixture
async def harness(make_schema, redis_client):
    sessions = await make_schema(Base.metadata)
    repository = OutboxRepository(Outbox)
    worker = OutboxPublisher(
        session_factory=sessions,
        repository=repository,
        publisher=RedisStreamPublisher(redis_client),
    )
    return sessions, repository, worker


async def _seed(sessions, repository, count: int = 1) -> None:
    async with sessions() as session:
        for _ in range(count):
            await repository.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await session.commit()


async def test_publish_once_on_an_empty_outbox_publishes_nothing(harness, redis_client):
    _sessions, _repo, worker = harness
    assert await worker.publish_once() == 0
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 0


async def test_publish_once_moves_pending_rows_to_the_stream(harness, redis_client):
    sessions, repository, worker = harness
    await _seed(sessions, repository, count=3)

    assert await worker.publish_once() == 3
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 3


async def test_published_rows_are_marked_and_not_republished(harness, redis_client):
    sessions, repository, worker = harness
    await _seed(sessions, repository, count=2)

    await worker.publish_once()
    assert await worker.publish_once() == 0
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 2

    async with sessions() as session:
        assert await repository.get_pending(session) == []


async def test_a_crash_between_xadd_and_the_mark_causes_a_duplicate(
    harness, redis_client, monkeypatch
):
    """The at-least-once proof, spec 10.2.

    The event reaches Redis, the mark never lands, and the next iteration
    publishes it again. Duplicates are a fact of this design; consumer
    idempotency is what makes them harmless.
    """
    sessions, repository, worker = harness
    await _seed(sessions, repository, count=1)

    async def boom(*_args, **_kwargs):
        raise RuntimeError("crash after XADD, before the mark")

    monkeypatch.setattr(repository, "mark_published", boom)
    with pytest.raises(RuntimeError):
        await worker.publish_once()

    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 1
    async with sessions() as session:
        assert len(await repository.get_pending(session)) == 1

    monkeypatch.undo()
    assert await worker.publish_once() == 1
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 2

    entries = await redis_client.xrange(str(Stream.NOTIFICATION_CREATED))
    envelopes = [EventEnvelope.from_redis(fields) for _id, fields in entries]
    assert envelopes[0].event_id == envelopes[1].event_id


async def test_batch_size_caps_one_iteration(harness, redis_client):
    sessions, repository, _worker = harness
    await _seed(sessions, repository, count=5)
    worker = OutboxPublisher(
        session_factory=sessions,
        repository=repository,
        publisher=RedisStreamPublisher(redis_client),
        batch_size=2,
    )
    assert await worker.publish_once() == 2


async def test_rows_are_published_to_the_stream_named_on_each_row(harness, redis_client):
    sessions, repository, worker = harness
    async with sessions() as session:
        await repository.save(session, Stream.NOTIFICATION_CREATED, _envelope())
        await repository.save(session, Stream.DELIVERY_COMPLETED, _envelope())
        await session.commit()

    assert await worker.publish_once() == 2
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 1
    assert await redis_client.xlen(str(Stream.DELIVERY_COMPLETED)) == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/integration/test_publisher.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.publisher'`

- [ ] **Step 3: Implement `publisher.py`**

```python
"""The outbox publisher loop.

Identical in every service, so it lives here rather than being copied four
times. Consumer handlers are not shared: they differ per service and reading
them is the point.
"""

from __future__ import annotations

import asyncio
import logging

from notification_shared.context import set_correlation_id
from notification_shared.events import EventEnvelope
from notification_shared.outbox import OutboxRepository
from notification_shared.streams import RedisStreamPublisher

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(
        self,
        *,
        session_factory,
        repository: OutboxRepository,
        publisher: RedisStreamPublisher,
        poll_interval_ms: int = 500,
        batch_size: int = 100,
    ) -> None:
        self._session_factory = session_factory
        self._repository = repository
        self._publisher = publisher
        self._poll_interval_ms = poll_interval_ms
        self._batch_size = batch_size

    async def publish_once(self) -> int:
        """Publish one batch. Returns the number of rows published.

        If the process dies after XADD but before the mark commits, the row
        stays pending and is published again next iteration. That duplicate is
        expected: consumers are idempotent.
        """
        async with self._session_factory() as session:
            rows = await self._repository.get_pending(session, limit=self._batch_size)
            if not rows:
                return 0

            published_ids = []
            for row in rows:
                envelope = EventEnvelope.model_validate(row.payload)
                set_correlation_id(envelope.correlation_id)
                await self._publisher.publish(row.stream, envelope)
                logger.debug(
                    "published event to stream",
                    extra={
                        "event_id": envelope.event_id,
                        "event_type": envelope.event_type,
                        "stream": row.stream,
                    },
                )
                published_ids.append(row.id)

            await self._repository.mark_published(session, published_ids)
            await session.commit()
            return len(published_ids)

    async def run_forever(self) -> None:
        while True:
            try:
                count = await self.publish_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("outbox publisher iteration failed")
                count = 0
            if count == 0:
                await asyncio.sleep(self._poll_interval_ms / 1000)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/integration/test_publisher.py -v`
Expected: PASS, 6 tests

- [ ] **Step 5: Commit**

```bash
git add shared/notification_shared/publisher.py tests/integration/test_publisher.py
git commit -m "feat(shared): add outbox publisher worker"
```

---

## Task 9: Shared library — correlation middleware and ServiceClient

**Files:**
- Create: `shared/notification_shared/middleware.py`, `shared/notification_shared/http_client.py`
- Test: `tests/unit/test_middleware.py`, `tests/unit/test_http_client.py`

**Interfaces:**
- Consumes: `set_correlation_id`/`get_correlation_id` (Task 3), `NotFoundError`/`ServiceUnavailableError` (Task 3)
- Produces: `CORRELATION_ID_HEADER = "X-Correlation-ID"`; `CorrelationIDMiddleware`; `ServiceClient(base_url, timeout=5.0, transport=None)` with `async get(path) -> dict`, `async put(path, json) -> dict`, `async aclose()`.

`ServiceClient` takes an optional `transport` purely so tests can inject `httpx.MockTransport`. It maps 404 to `NotFoundError` and timeout / transport error / 5xx to `ServiceUnavailableError` — the distinction Task 12 depends on.

- [ ] **Step 1: Write the failing tests**

`tests/unit/test_middleware.py`:
```python
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from notification_shared.context import get_correlation_id
from notification_shared.middleware import CORRELATION_ID_HEADER, CorrelationIDMiddleware


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(CorrelationIDMiddleware)

    @app.get("/probe")
    async def probe(request: Request) -> dict:
        # The `Request` annotation is required: without it FastAPI treats
        # `request` as a mandatory query parameter instead of injecting the
        # request object, and every assertion below fails with a 422.
        return {
            "from_state": request.state.correlation_id,
            "from_context": get_correlation_id(),
        }

    return app


def test_an_absent_header_is_generated():
    with TestClient(_app()) as client:
        response = client.get("/probe")
    assert response.status_code == 200
    assert len(response.headers[CORRELATION_ID_HEADER]) == 36


def test_a_supplied_header_is_preserved_and_echoed():
    with TestClient(_app()) as client:
        response = client.get("/probe", headers={CORRELATION_ID_HEADER: "corr-supplied"})
    assert response.headers[CORRELATION_ID_HEADER] == "corr-supplied"
    assert response.json()["from_state"] == "corr-supplied"


def test_the_context_var_is_set_for_the_duration_of_the_request():
    with TestClient(_app()) as client:
        response = client.get("/probe", headers={CORRELATION_ID_HEADER: "corr-ctx"})
    assert response.json()["from_context"] == "corr-ctx"


def test_two_requests_get_different_generated_ids():
    with TestClient(_app()) as client:
        first = client.get("/probe").headers[CORRELATION_ID_HEADER]
        second = client.get("/probe").headers[CORRELATION_ID_HEADER]
    assert first != second
```

`tests/unit/test_http_client.py`:
```python
import httpx
import pytest

from notification_shared.context import set_correlation_id
from notification_shared.exceptions import NotFoundError, ServiceUnavailableError
from notification_shared.http_client import ServiceClient
from notification_shared.middleware import CORRELATION_ID_HEADER


def _client(handler) -> ServiceClient:
    return ServiceClient(
        base_url="http://configuration-service:8000",
        transport=httpx.MockTransport(handler),
    )


async def test_a_200_returns_the_decoded_body():
    client = _client(lambda _r: httpx.Response(200, json={"name": "email", "enabled": True}))
    try:
        assert await client.get("/channels/email") == {"name": "email", "enabled": True}
    finally:
        await client.aclose()


async def test_a_404_raises_not_found():
    client = _client(lambda _r: httpx.Response(404, json={}))
    try:
        with pytest.raises(NotFoundError):
            await client.get("/channels/sms")
    finally:
        await client.aclose()


async def test_a_500_raises_service_unavailable():
    client = _client(lambda _r: httpx.Response(500, text="boom"))
    try:
        with pytest.raises(ServiceUnavailableError):
            await client.get("/channels/email")
    finally:
        await client.aclose()


async def test_a_timeout_raises_service_unavailable():
    def handler(request):
        raise httpx.ReadTimeout("too slow", request=request)

    client = _client(handler)
    try:
        with pytest.raises(ServiceUnavailableError):
            await client.get("/channels/email")
    finally:
        await client.aclose()


async def test_a_connect_error_raises_service_unavailable():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    client = _client(handler)
    try:
        with pytest.raises(ServiceUnavailableError):
            await client.get("/channels/email")
    finally:
        await client.aclose()


async def test_not_found_and_unavailable_stay_distinguishable():
    """Spec 3.18 — Routing Service branches on exactly this difference."""
    not_found = _client(lambda _r: httpx.Response(404, json={}))
    unavailable = _client(lambda _r: httpx.Response(503, text=""))
    try:
        with pytest.raises(NotFoundError):
            await not_found.get("/channels/sms")
        with pytest.raises(ServiceUnavailableError):
            await unavailable.get("/channels/email")
    finally:
        await not_found.aclose()
        await unavailable.aclose()


async def test_the_correlation_id_is_forwarded_downstream():
    seen = {}

    def handler(request):
        seen["header"] = request.headers.get(CORRELATION_ID_HEADER)
        return httpx.Response(200, json={})

    set_correlation_id("corr-forwarded")
    client = _client(handler)
    try:
        await client.get("/channels/email")
    finally:
        await client.aclose()
        set_correlation_id(None)
    assert seen["header"] == "corr-forwarded"


async def test_put_sends_the_json_body():
    seen = {}

    def handler(request):
        seen["body"] = request.content
        return httpx.Response(200, json={"name": "email", "enabled": False})

    client = _client(handler)
    try:
        result = await client.put("/channels/email", json={"enabled": False})
    finally:
        await client.aclose()
    assert b"false" in seen["body"]
    assert result["enabled"] is False
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_middleware.py tests/unit/test_http_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notification_shared.middleware'`

- [ ] **Step 3: Implement `middleware.py`**

```python
"""Correlation id propagation for HTTP requests."""

from __future__ import annotations

from uuid import uuid4

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from notification_shared.context import set_correlation_id

CORRELATION_ID_HEADER = "X-Correlation-ID"


class CorrelationIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        correlation_id = request.headers.get(CORRELATION_ID_HEADER) or str(uuid4())
        request.state.correlation_id = correlation_id
        set_correlation_id(correlation_id)
        response = await call_next(request)
        response.headers[CORRELATION_ID_HEADER] = correlation_id
        return response
```

- [ ] **Step 4: Implement `http_client.py`**

```python
"""HTTPX wrapper for the few remaining synchronous service calls.

404 and 5xx are mapped to different exception types on purpose: a missing
channel is a permanent routing failure, an unreachable Configuration Service
is transient. See spec correction 3.18.
"""

from __future__ import annotations

from typing import Any

import httpx

from notification_shared.context import get_correlation_id
from notification_shared.exceptions import NotFoundError, ServiceUnavailableError
from notification_shared.middleware import CORRELATION_ID_HEADER


class ServiceClient:
    def __init__(
        self,
        base_url: str,
        timeout: float = 5.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport
        )

    async def get(self, path: str) -> dict[str, Any]:
        return await self._send("GET", path)

    async def put(self, path: str, json: dict[str, Any]) -> dict[str, Any]:
        return await self._send("PUT", path, json=json)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _send(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        headers = {}
        correlation_id = get_correlation_id()
        if correlation_id:
            headers[CORRELATION_ID_HEADER] = correlation_id

        try:
            response = await self._client.request(method, path, headers=headers, **kwargs)
        except httpx.TimeoutException as exc:
            raise ServiceUnavailableError(f"{method} {path} timed out") from exc
        except httpx.TransportError as exc:
            raise ServiceUnavailableError(f"{method} {path} failed: {exc}") from exc

        if response.status_code == 404:
            raise NotFoundError(f"{method} {path} returned 404")
        if response.status_code >= 500:
            raise ServiceUnavailableError(
                f"{method} {path} returned {response.status_code}"
            )
        return response.json()
```

- [ ] **Step 5: Run the whole test suite to verify it passes**

```bash
uv run pytest tests/unit -v
uv run pytest tests/integration -v
uv run ruff check .
```

Expected: unit tier 38 tests PASS; integration tier 29 tests PASS; ruff clean.

- [ ] **Step 6: Commit**

```bash
git add shared/notification_shared tests/unit
git commit -m "feat(shared): add correlation middleware and service HTTP client"
```

---

## Task 10: Configuration Service — and the service template

**Files:**
- Create: `services/configuration-service/pyproject.toml`, `Dockerfile`, `entrypoint.sh`, `alembic.ini`
- Create: `services/configuration-service/app/` — `main.py`, `core/config.py`, `core/database.py`, `models/base.py`, `models/channel.py`, `schemas/channel.py`, `repositories/channel.py`, `services/channel.py`, `api/v1/router.py`, `api/v1/endpoints/channels.py`, `api/v1/endpoints/system.py`
- Create: `services/configuration-service/alembic/env.py`, `alembic/script.py.mako`, `alembic/versions/0001_create_channels.py`
- Create: `docker-compose.yml` (first two containers)
- Test: `services/configuration-service/tests/conftest.py`, `services/configuration-service/tests/test_configuration_service.py`

**Interfaces:**
- Consumes: `BaseServiceSettings`, `configure_logging`, `CorrelationIDMiddleware`, `ServiceError`, `NotFoundError`, `ErrorResponse` (Tasks 3, 9)
- Produces: the service layout every later service copies — `create_app(settings=None) -> FastAPI` factory, `Database` class with `session_factory` / `ping()` / `dispose()`, `get_db(request)` dependency reading `request.app.state.db`. REST surface `GET /channels`, `GET /channels/{name}`, `PUT /channels/{name}`, `GET /health`, `GET /version`.

This service is built first because it is the only one with no events, no Redis and no workers, so the template lands without the saga on top of it. `create_app` is a factory rather than a module-level `app` so tests can build an instance with injected settings; Uvicorn is therefore launched with `--factory`.

The ORM model is named `ChannelConfig`, not `Channel`, so it cannot be confused with `notification_shared.events.Channel` in a test that imports both.

- [ ] **Step 1: Write `services/configuration-service/pyproject.toml`**

```toml
[project]
name = "configuration-service"
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
]

[tool.uv.sources]
notification-shared = { workspace = true }

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["app"]
```

Then run `uv sync --all-packages` and confirm the new member resolves.

- [ ] **Step 2: Write the failing tests**

Each service owns its tests, because each defines a module named `app` and two of them cannot share one pytest process.

`services/configuration-service/tests/conftest.py`:
```python
from notification_shared.testing import (  # noqa: F401
    engine,
    make_schema,
    postgres_url,
    redis_client,
    redis_url,
)
```

`services/configuration-service/tests/test_configuration_service.py`:
```python
import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import select

from app.core.config import Settings
from app.core.database import Database
from app.main import create_app
from app.models.base import Base
from app.models.channel import ChannelConfig

pytestmark = pytest.mark.integration

ALEMBIC_DIR = "services/configuration-service"


def _settings(url: str) -> Settings:
    return Settings(service_name="configuration-service", database_url=url)


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
async def client(postgres_url, sessions):
    """Build the app without running lifespan, injecting the test database.

    `get_db` reads `request.app.state.db`, so setting state directly is enough.
    The real lifespan path is covered by the end-to-end tests in Task 16.
    """
    settings = _settings(postgres_url)
    app = create_app(settings)
    app.state.settings = settings
    app.state.db = Database(postgres_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http_client:
        yield http_client
    await app.state.db.dispose()


async def _seed(sessions) -> None:
    async with sessions() as session:
        session.add_all(
            [ChannelConfig(name="email", enabled=True), ChannelConfig(name="telegram", enabled=True)]
        )
        await session.commit()


async def test_list_channels_returns_the_seeded_rows(client, sessions):
    await _seed(sessions)
    response = await client.get("/channels")
    assert response.status_code == 200
    assert sorted(response.json(), key=lambda c: c["name"]) == [
        {"name": "email", "enabled": True},
        {"name": "telegram", "enabled": True},
    ]


async def test_get_one_channel(client, sessions):
    await _seed(sessions)
    response = await client.get("/channels/email")
    assert response.status_code == 200
    assert response.json() == {"name": "email", "enabled": True}


async def test_an_unknown_channel_returns_404_with_the_common_error_model(client, sessions):
    await _seed(sessions)
    response = await client.get("/channels/sms")
    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "NOT_FOUND", "message": "channel 'sms' not found"}
    }


async def test_put_disables_a_channel_and_the_change_is_readable(client, sessions):
    await _seed(sessions)
    response = await client.put("/channels/telegram", json={"enabled": False})
    assert response.status_code == 200
    assert response.json() == {"name": "telegram", "enabled": False}
    assert (await client.get("/channels/telegram")).json()["enabled"] is False


async def test_put_on_an_unknown_channel_returns_404(client, sessions):
    await _seed(sessions)
    assert (await client.put("/channels/sms", json={"enabled": False})).status_code == 404


async def test_put_rejects_a_body_without_enabled(client, sessions):
    await _seed(sessions)
    assert (await client.put("/channels/email", json={})).status_code == 422


async def test_health_reports_the_database_up(client, sessions):
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "UP"
    assert body["service"] == "configuration-service"
    assert body["checks"] == {"database": "UP"}


async def test_health_has_no_redis_check(client, sessions):
    assert "redis" not in (await client.get("/health")).json()["checks"]


async def test_version_reports_name_and_version(client, sessions):
    assert (await client.get("/version")).json() == {
        "service": "configuration-service",
        "version": "1.0.0",
    }


async def test_the_correlation_id_header_is_echoed(client, sessions):
    response = await client.get("/health", headers={"X-Correlation-ID": "corr-cfg"})
    assert response.headers["X-Correlation-ID"] == "corr-cfg"


def test_alembic_upgrade_head_creates_and_seeds_channels(postgres_url):
    """Synchronous on purpose: alembic's env.py calls asyncio.run internally,
    which cannot be nested inside a running event loop."""
    config = Config(f"{ALEMBIC_DIR}/alembic.ini")
    config.set_main_option("script_location", f"{ALEMBIC_DIR}/alembic")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.upgrade(config, "head")
    try:
        import asyncio

        from sqlalchemy.ext.asyncio import create_async_engine

        async def read_seeds():
            engine = create_async_engine(postgres_url)
            async with engine.connect() as conn:
                rows = (await conn.execute(select(ChannelConfig.name, ChannelConfig.enabled))).all()
            await engine.dispose()
            return sorted(rows)

        assert asyncio.run(read_seeds()) == [("email", True), ("telegram", True)]
    finally:
        command.downgrade(config, "base")
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `PYTHONPATH=services/configuration-service uv run pytest services/configuration-service/tests -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app'`

- [ ] **Step 4: Implement `core/config.py` and `core/database.py`**

`app/core/config.py`:
```python
from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "configuration-service"
    database_url: str
```

`app/core/database.py`:
```python
"""Engine and session lifecycle.

The engine lives on `app.state` rather than at module scope so tests can inject
their own, and so the process does not open connections at import time.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request


class Database:
    def __init__(self, url: str) -> None:
        self._engine = create_async_engine(url, pool_pre_ping=True)
        self.session_factory = async_sessionmaker(self._engine, expire_on_commit=False)

    async def ping(self) -> bool:
        async with self._engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        return True

    async def dispose(self) -> None:
        await self._engine.dispose()


async def get_db(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.db.session_factory() as session:
        yield session
```

- [ ] **Step 5: Implement the model, schemas, repository and service layer**

`app/models/base.py`:
```python
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """This service's own registry. See spec correction 3.12."""
```

`app/models/channel.py`:
```python
from sqlalchemy import Boolean, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class ChannelConfig(Base):
    """Named ChannelConfig to avoid colliding with the Channel enum in tests."""

    __tablename__ = "channels"

    name: Mapped[str] = mapped_column(String(50), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
```

`app/schemas/channel.py`:
```python
from pydantic import BaseModel, ConfigDict


class ChannelRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    enabled: bool


class ChannelUpdate(BaseModel):
    enabled: bool
```

`app/repositories/channel.py`:
```python
from collections.abc import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.channel import ChannelConfig


class ChannelRepository:
    async def list_all(self, session: AsyncSession) -> Sequence[ChannelConfig]:
        return (await session.scalars(select(ChannelConfig).order_by(ChannelConfig.name))).all()

    async def get(self, session: AsyncSession, name: str) -> ChannelConfig | None:
        return await session.get(ChannelConfig, name)
```

`app/services/channel.py`:
```python
from sqlalchemy.ext.asyncio import AsyncSession

from notification_shared.exceptions import NotFoundError

from app.repositories.channel import ChannelRepository
from app.schemas.channel import ChannelRead


class ChannelService:
    def __init__(self, repository: ChannelRepository | None = None) -> None:
        self._repository = repository or ChannelRepository()

    async def list_channels(self, session: AsyncSession) -> list[ChannelRead]:
        rows = await self._repository.list_all(session)
        return [ChannelRead.model_validate(row) for row in rows]

    async def get_channel(self, session: AsyncSession, name: str) -> ChannelRead:
        row = await self._repository.get(session, name)
        if row is None:
            raise NotFoundError(f"channel '{name}' not found")
        return ChannelRead.model_validate(row)

    async def set_enabled(self, session: AsyncSession, name: str, enabled: bool) -> ChannelRead:
        row = await self._repository.get(session, name)
        if row is None:
            raise NotFoundError(f"channel '{name}' not found")
        row.enabled = enabled
        await session.commit()
        return ChannelRead.model_validate(row)
```

- [ ] **Step 6: Implement the API layer**

`app/api/v1/endpoints/channels.py`:
```python
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.channel import ChannelRead, ChannelUpdate
from app.services.channel import ChannelService

router = APIRouter(tags=["channels"])
service = ChannelService()


@router.get(
    "/channels",
    response_model=list[ChannelRead],
    summary="List channels",
    description="Every configured channel and whether it is currently enabled.",
)
async def list_channels(session: AsyncSession = Depends(get_db)) -> list[ChannelRead]:
    return await service.list_channels(session)


@router.get(
    "/channels/{name}",
    response_model=ChannelRead,
    summary="Read one channel",
    description="Read a single channel's state. Returns 404 if the channel is not configured.",
)
async def get_channel(name: str, session: AsyncSession = Depends(get_db)) -> ChannelRead:
    return await service.get_channel(session, name)


@router.put(
    "/channels/{name}",
    response_model=ChannelRead,
    summary="Enable or disable a channel",
    description=(
        "Takes effect on the next routing decision: Routing Service reads channel "
        "state per event and holds no cache."
    ),
)
async def update_channel(
    name: str, body: ChannelUpdate, session: AsyncSession = Depends(get_db)
) -> ChannelRead:
    return await service.set_enabled(session, name, body.enabled)
```

`app/api/v1/endpoints/system.py`:
```python
from fastapi import APIRouter, Response
from starlette.requests import Request

router = APIRouter(tags=["system"])


@router.get("/health", summary="Health check", description="Reports Postgres connectivity.")
async def health(request: Request, response: Response) -> dict:
    try:
        await request.app.state.db.ping()
        database = "UP"
    except Exception:
        database = "DOWN"
    if database == "DOWN":
        response.status_code = 503
    return {
        "status": "UP" if database == "UP" else "DOWN",
        "service": request.app.state.settings.service_name,
        "checks": {"database": database},
    }


@router.get("/version", summary="Service version")
async def version(request: Request) -> dict:
    settings = request.app.state.settings
    return {"service": settings.service_name, "version": settings.service_version}
```

`app/api/v1/router.py`:
```python
from fastapi import APIRouter

from app.api.v1.endpoints import channels, system

router = APIRouter()
router.include_router(channels.router)
router.include_router(system.router)
```

- [ ] **Step 7: Implement `app/main.py`**

```python
"""Configuration Service.

Runtime channel state, read by Routing Service once per event. Publishes no
events: configuration is read-model state, not a domain event stream.
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.requests import Request

from notification_shared.exceptions import ServiceError
from notification_shared.logging import configure_logging
from notification_shared.middleware import CorrelationIDMiddleware

from app.api.v1.router import router
from app.core.config import Settings
from app.core.database import Database


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.db = Database(settings.database_url)
        yield
        await app.state.db.dispose()

    app = FastAPI(
        title="Configuration Service",
        version=settings.service_version,
        lifespan=lifespan,
    )
    app.add_middleware(CorrelationIDMiddleware)
    app.include_router(router)

    @app.exception_handler(ServiceError)
    async def handle_service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code, content=exc.to_response().model_dump(mode="json")
        )

    return app
```

- [ ] **Step 8: Implement Alembic**

`services/configuration-service/alembic.ini`:
```ini
[alembic]
script_location = alembic
prepend_sys_path = .

[loggers]
keys = root

[handlers]
keys = console

[formatters]
keys = generic

[logger_root]
level = WARN
handlers = console

[handler_console]
class = StreamHandler
args = (sys.stderr,)
formatter = generic

[formatter_generic]
format = %(levelname)-5.5s [%(name)s] %(message)s
```

`services/configuration-service/alembic/env.py`:
```python
import asyncio
import os

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

import app.models.channel  # noqa: F401  register the table on the metadata
from app.models.base import Base

config = context.config
if not config.get_main_option("sqlalchemy.url", None):
    config.set_main_option("sqlalchemy.url", os.environ["DATABASE_URL"])

target_metadata = Base.metadata


def do_run_migrations(connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


if context.is_offline_mode():
    context.configure(url=config.get_main_option("sqlalchemy.url"), literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()
else:
    asyncio.run(run_async_migrations())
```

`services/configuration-service/alembic/versions/0001_create_channels.py`:
```python
"""create channels and seed the two supported channels

Seeded by a data migration rather than a startup script: deterministic, runs
before Uvicorn, and covered by the alembic integration test. See spec 3.9.
"""

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

channels = sa.table("channels", sa.column("name", sa.String), sa.column("enabled", sa.Boolean))


def upgrade() -> None:
    op.create_table(
        "channels",
        sa.Column("name", sa.String(length=50), primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
    )
    op.bulk_insert(
        channels, [{"name": "email", "enabled": True}, {"name": "telegram", "enabled": True}]
    )


def downgrade() -> None:
    op.drop_table("channels")
```

Copy Alembic's stock `script.py.mako` into `alembic/` so future autogenerate works.

- [ ] **Step 9: Run the tests to verify they pass**

Run: `PYTHONPATH=services/configuration-service uv run pytest services/configuration-service/tests -v`
Expected: PASS, 11 tests

The `ALEMBIC_DIR` constant in the test is repository-root-relative, so pytest must be invoked from the repository root.

- [ ] **Step 10: Write the Dockerfile, entrypoint and first compose containers**

`services/configuration-service/entrypoint.sh`:
```sh
#!/bin/sh
set -e
/app/.venv/bin/alembic upgrade head
exec /app/.venv/bin/uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8000
```

`services/configuration-service/Dockerfile`:
```dockerfile
FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY shared/ ./shared/
COPY services/configuration-service/ ./services/configuration-service/
RUN uv sync --frozen --no-dev --no-editable --package configuration-service

FROM python:3.14-slim AS runtime
RUN adduser --disabled-password appuser && apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder --chown=appuser /app/.venv /app/.venv
COPY --chown=appuser services/configuration-service/ /app/
RUN chmod +x /app/entrypoint.sh
USER appuser
EXPOSE 8000
ENTRYPOINT ["/app/entrypoint.sh"]
```

`docker-compose.yml` (grown in later tasks):
```yaml
name: notification-platform

services:
  configuration-db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: notif
      POSTGRES_PASSWORD: notif
      POSTGRES_DB: configurationdb
    ports: ["5435:5432"]
    volumes: ["configuration-data:/var/lib/postgresql/data"]
    networks: [notification-net]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U notif -d configurationdb"]
      interval: 10s
      timeout: 5s
      retries: 5

  configuration-service:
    build:
      context: .
      dockerfile: services/configuration-service/Dockerfile
    environment:
      DATABASE_URL: postgresql+asyncpg://notif:notif@configuration-db:5432/configurationdb
      SERVICE_NAME: configuration-service
      LOG_LEVEL: INFO
    ports: ["8003:8000"]
    networks: [notification-net]
    restart: on-failure
    depends_on:
      configuration-db: { condition: service_healthy }
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

networks:
  notification-net:

volumes:
  configuration-data:
```

- [ ] **Step 11: Verify the container runs**

```bash
docker compose up --build -d configuration-service
docker compose ps
curl -s http://localhost:8003/channels
curl -s -X PUT http://localhost:8003/channels/telegram -H "Content-Type: application/json" -d '{"enabled": false}'
curl -s http://localhost:8003/health
docker compose down -v
```

Expected: `configuration-service` reaches `healthy`; the first curl returns both channels enabled; the PUT returns `telegram` disabled; health returns `{"status":"UP",...}`. The Alembic seed ran as part of the entrypoint.

- [ ] **Step 12: Commit**

```bash
git add services/configuration-service docker-compose.yml
git commit -m "feat(configuration-service): add channel configuration service

Establishes the service template: create_app factory, Database on app.state,
api/service/repository layering, async Alembic with a seed data migration."
```

---

## Task 11: Notification Service — write path, read path, outbox publisher

**Files:**
- Create: `services/notification-service/` mirroring Task 10's layout, plus `models/notification.py`, `models/outbox.py`, `models/processed_event.py`, `workers/__init__.py`
- Modify: `docker-compose.yml` (add `notification-db`, `notification-service`, `redis`), `pyproject.toml` (extend `pythonpath`)
- Test: `tests/integration/test_notification_service.py`

**Interfaces:**
- Consumes: the Task 10 template; `OutboxRepository`, `OutboxPublisher`, `RedisStreamPublisher`, `EventEnvelope`, `Stream`, `EventType`, `Channel`, `ValidationFailedError`
- Produces: `NotificationStatus` (`CREATED`, `PROCESSING`, `COMPLETED`, `FAILED`); ORM models `Notification`, `Outbox`, `ProcessedEvent` on this service's `Base`; `NotificationService.create()` and `.get()`; REST `POST /notifications`, `GET /notifications/{id}`, `GET /notifications`. Tasks 12 and 14 depend on these model names and on the `notifications.status` values.

- [ ] **Step 1: Write the failing tests**

`tests/integration/test_notification_service.py`:
```python
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.outbox import OutboxRepository
from notification_shared.publisher import OutboxPublisher
from notification_shared.streams import RedisStreamPublisher

from app.core.config import Settings
from app.core.database import Database
from app.main import create_app
from app.models.base import Base
from app.models.notification import Notification, NotificationStatus
from app.models.outbox import Outbox

pytestmark = pytest.mark.integration

VALID_BODY = {
    "channel": "email",
    "recipient": "john@example.com",
    "subject": "Welcome",
    "body": "Hello John!",
}


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
async def client(postgres_url, sessions):
    import httpx

    settings = Settings(service_name="notification-service", database_url=postgres_url)
    app = create_app(settings)
    app.state.settings = settings
    app.state.db = Database(postgres_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http_client:
        yield http_client
    await app.state.db.dispose()


async def test_post_returns_202_with_the_id_and_created_status(client):
    response = await client.post("/notifications", json=VALID_BODY)
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "CREATED"
    assert body["notification_id"]


async def test_post_writes_the_notification_and_the_outbox_row_together(client, sessions):
    notification_id = (await client.post("/notifications", json=VALID_BODY)).json()[
        "notification_id"
    ]

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Notification)) == 1
        rows = (await session.scalars(select(Outbox))).all()
        assert len(rows) == 1
        assert rows[0].stream == "notification.created"
        assert rows[0].published is False
        envelope = EventEnvelope.model_validate(rows[0].payload)
        assert envelope.event_type is EventType.NOTIFICATION_CREATED
        assert str(envelope.aggregate_id) == notification_id
        assert envelope.payload == {
            "channel": "email",
            "recipient": "john@example.com",
            "subject": "Welcome",
            "body": "Hello John!",
        }


async def test_the_request_correlation_id_reaches_the_envelope(client, sessions):
    await client.post(
        "/notifications", json=VALID_BODY, headers={"X-Correlation-ID": "corr-write"}
    )
    async with sessions() as session:
        row = (await session.scalars(select(Outbox))).one()
        assert EventEnvelope.model_validate(row.payload).correlation_id == "corr-write"


async def test_a_new_notification_starts_in_created(client, sessions):
    notification_id = (await client.post("/notifications", json=VALID_BODY)).json()[
        "notification_id"
    ]
    async with sessions() as session:
        notification = await session.get(Notification, notification_id)
        assert notification.status == NotificationStatus.CREATED
        assert notification.fail_reason is None


@pytest.mark.parametrize(
    "payload",
    [
        {**VALID_BODY, "channel": "carrier-pigeon"},
        {**VALID_BODY, "recipient": ""},
        {**VALID_BODY, "body": ""},
        {k: v for k, v in VALID_BODY.items() if k != "recipient"},
    ],
)
async def test_invalid_payloads_are_rejected_and_write_nothing(client, sessions, payload):
    assert (await client.post("/notifications", json=payload)).status_code == 422
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Notification)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_subject_is_optional(client):
    payload = {k: v for k, v in VALID_BODY.items() if k != "subject"}
    assert (await client.post("/notifications", json=payload)).status_code == 202


async def test_get_returns_the_read_model_including_fail_reason(client):
    notification_id = (await client.post("/notifications", json=VALID_BODY)).json()[
        "notification_id"
    ]
    body = (await client.get(f"/notifications/{notification_id}")).json()
    assert set(body) == {
        "notification_id",
        "channel",
        "status",
        "fail_reason",
        "created_at",
        "updated_at",
    }
    assert body["fail_reason"] is None
    assert body["channel"] == "email"


async def test_get_on_an_unknown_id_returns_404(client):
    response = await client.get(f"/notifications/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


async def test_list_supports_limit_offset_and_filters(client):
    await client.post("/notifications", json=VALID_BODY)
    await client.post("/notifications", json={**VALID_BODY, "channel": "telegram"})

    assert len((await client.get("/notifications")).json()) == 2
    assert len((await client.get("/notifications?limit=1")).json()) == 1
    assert len((await client.get("/notifications?channel=telegram")).json()) == 1
    assert len((await client.get("/notifications?status=COMPLETED")).json()) == 0


async def test_the_outbox_publisher_delivers_the_created_event(client, sessions, redis_client):
    await client.post("/notifications", json=VALID_BODY)

    worker = OutboxPublisher(
        session_factory=sessions,
        repository=OutboxRepository(Outbox),
        publisher=RedisStreamPublisher(redis_client),
    )
    assert await worker.publish_once() == 1
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 1

    entries = await redis_client.xrange(str(Stream.NOTIFICATION_CREATED))
    envelope = EventEnvelope.from_redis(entries[0][1])
    assert envelope.event_type is EventType.NOTIFICATION_CREATED


async def test_health_reports_both_database_and_redis(client):
    body = (await client.get("/health")).json()
    assert set(body["checks"]) == {"database", "redis"}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/integration/test_notification_service.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.models.notification'`

- [ ] **Step 3: Write the package manifest**

`services/notification-service/pyproject.toml` is Task 10's manifest with `name = "notification-service"` and `"redis>=8.1.0"` added to `dependencies`. Run `uv sync --all-packages`.

This task's test file goes at `services/notification-service/tests/test_notification_service.py`, with the same `tests/conftest.py` re-export as Task 10 — every service owns its tests, because every service defines a module named `app` and two of them cannot share one pytest process.

- [ ] **Step 4: Implement the models**

`app/models/base.py` — as Task 10.

`app/models/notification.py`:
```python
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import Index, String, Text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from notification_shared.models import TimestampMixin

from app.models.base import Base


class NotificationStatus(StrEnum):
    CREATED = "CREATED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Notification(Base, TimestampMixin):
    __tablename__ = "notifications"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)
    subject: Mapped[str | None] = mapped_column(String(255), nullable=True)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)

    __table_args__ = (Index("ix_notifications_status_created", "status", "created_at"),)
```

`app/models/outbox.py`:
```python
from notification_shared.models import OutboxMixin

from app.models.base import Base


class Outbox(Base, OutboxMixin):
    __tablename__ = "outbox"
```

`app/models/processed_event.py`:
```python
from notification_shared.models import ProcessedEventMixin

from app.models.base import Base


class ProcessedEvent(Base, ProcessedEventMixin):
    __tablename__ = "processed_events"
```

`processed_events` is unused until Task 14, but it is created now so this service has one migration rather than two.

- [ ] **Step 5: Implement schemas, repository, service layer**

`app/schemas/notification.py`:
```python
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from notification_shared.events import Channel


class NotificationCreate(BaseModel):
    channel: Channel
    recipient: str = Field(min_length=1, max_length=255)
    subject: str | None = Field(default=None, max_length=255)
    body: str = Field(min_length=1)


class NotificationAccepted(BaseModel):
    notification_id: UUID
    status: str


class NotificationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    notification_id: UUID = Field(validation_alias="id")
    channel: str
    status: str
    fail_reason: str | None
    created_at: datetime
    updated_at: datetime
```

`min_length=1` on `recipient` and `body` is what turns an empty string into a 422, and `channel: Channel` is what rejects `carrier-pigeon`.

`app/repositories/notification.py`:
```python
from collections.abc import Sequence
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.notification import Notification


class NotificationRepository:
    async def add(self, session: AsyncSession, notification: Notification) -> None:
        session.add(notification)

    async def get(self, session: AsyncSession, notification_id: UUID) -> Notification | None:
        return await session.get(Notification, notification_id)

    async def list(
        self,
        session: AsyncSession,
        limit: int = 20,
        offset: int = 0,
        status: str | None = None,
        channel: str | None = None,
    ) -> Sequence[Notification]:
        stmt = select(Notification).order_by(Notification.created_at.desc())
        if status:
            stmt = stmt.where(Notification.status == status)
        if channel:
            stmt = stmt.where(Notification.channel == channel)
        return (await session.scalars(stmt.limit(limit).offset(offset))).all()
```

`app/services/notification.py`:
```python
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.exceptions import NotFoundError
from notification_shared.outbox import OutboxRepository

from app.models.notification import Notification, NotificationStatus
from app.models.outbox import Outbox
from app.repositories.notification import NotificationRepository
from app.schemas.notification import NotificationCreate, NotificationRead


class NotificationService:
    def __init__(self) -> None:
        self._repository = NotificationRepository()
        self._outbox = OutboxRepository(Outbox)

    async def create(
        self, session: AsyncSession, payload: NotificationCreate, correlation_id: str
    ) -> Notification:
        """Persist the notification and its event in one transaction.

        This single commit is the Outbox Pattern: either both rows land or
        neither does, so an accepted notification can never lack its event.
        """
        notification = Notification(
            id=uuid4(),
            channel=payload.channel.value,
            recipient=payload.recipient,
            subject=payload.subject,
            body=payload.body,
            status=NotificationStatus.CREATED.value,
        )
        await self._repository.add(session, notification)

        envelope = EventEnvelope.new(
            event_type=EventType.NOTIFICATION_CREATED,
            aggregate_id=notification.id,
            payload={
                "channel": payload.channel.value,
                "recipient": payload.recipient,
                "subject": payload.subject,
                "body": payload.body,
            },
            correlation_id=correlation_id,
        )
        await self._outbox.save(session, Stream.NOTIFICATION_CREATED, envelope)
        await session.commit()
        return notification

    async def get(self, session: AsyncSession, notification_id: UUID) -> NotificationRead:
        notification = await self._repository.get(session, notification_id)
        if notification is None:
            raise NotFoundError(f"notification '{notification_id}' not found")
        return NotificationRead.model_validate(notification)

    async def list(self, session: AsyncSession, **filters) -> list[NotificationRead]:
        rows = await self._repository.list(session, **filters)
        return [NotificationRead.model_validate(row) for row in rows]
```

- [ ] **Step 6: Implement the API layer**

`app/api/v1/endpoints/notifications.py`:
```python
from uuid import UUID

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from app.core.database import get_db
from app.schemas.notification import (
    NotificationAccepted,
    NotificationCreate,
    NotificationRead,
)
from app.services.notification import NotificationService

router = APIRouter(tags=["notifications"])
service = NotificationService()


@router.post(
    "/notifications",
    response_model=NotificationAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a notification",
    description=(
        "Accepts the notification and returns immediately. Delivery happens "
        "asynchronously; poll GET /notifications/{id} to observe the status "
        "advance from CREATED to PROCESSING to COMPLETED or FAILED."
    ),
)
async def create_notification(
    body: NotificationCreate, request: Request, session: AsyncSession = Depends(get_db)
) -> NotificationAccepted:
    notification = await service.create(session, body, request.state.correlation_id)
    return NotificationAccepted(notification_id=notification.id, status=notification.status)


@router.get(
    "/notifications/{notification_id}",
    response_model=NotificationRead,
    summary="Read notification status",
    description="Poll this until status is COMPLETED or FAILED.",
)
async def get_notification(
    notification_id: UUID, session: AsyncSession = Depends(get_db)
) -> NotificationRead:
    return await service.get(session, notification_id)


@router.get(
    "/notifications",
    response_model=list[NotificationRead],
    summary="List notifications",
)
async def list_notifications(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: str | None = None,
    channel: str | None = None,
    session: AsyncSession = Depends(get_db),
) -> list[NotificationRead]:
    return await service.list(
        session, limit=limit, offset=offset, status=status, channel=channel
    )
```

`app/api/v1/endpoints/system.py` is Task 10's file with a Redis check added:
```python
@router.get("/health", summary="Health check")
async def health(request: Request, response: Response) -> dict:
    checks = {}
    try:
        await request.app.state.db.ping()
        checks["database"] = "UP"
    except Exception:
        checks["database"] = "DOWN"
    try:
        await request.app.state.redis.ping()
        checks["redis"] = "UP"
    except Exception:
        checks["redis"] = "DOWN"
    healthy = all(value == "UP" for value in checks.values())
    if not healthy:
        response.status_code = 503
    return {
        "status": "UP" if healthy else "DOWN",
        "service": request.app.state.settings.service_name,
        "checks": checks,
    }
```

The test builds the app without lifespan, so `app.state.redis` may be absent; the bare `except Exception` covers that and reports `redis: DOWN`, which is what the test asserts by only checking the key set.

- [ ] **Step 7: Implement `app/main.py` with the publisher in lifespan**

```python
"""Notification Service.

Owns the notification aggregate and the client-facing REST surface. Slice 1
runs one background worker; Task 14 adds two consumers.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.requests import Request

from notification_shared.exceptions import ServiceError
from notification_shared.logging import configure_logging
from notification_shared.middleware import CorrelationIDMiddleware
from notification_shared.outbox import OutboxRepository
from notification_shared.publisher import OutboxPublisher
from notification_shared.streams import RedisStreamPublisher

from app.api.v1.router import router
from app.core.config import Settings
from app.core.database import Database
from app.models.outbox import Outbox


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.db = Database(settings.database_url)
        app.state.redis = aioredis.from_url(settings.redis_url)
        await app.state.redis.ping()

        publisher = OutboxPublisher(
            session_factory=app.state.db.session_factory,
            repository=OutboxRepository(Outbox),
            publisher=RedisStreamPublisher(app.state.redis),
            poll_interval_ms=settings.outbox_poll_interval_ms,
            batch_size=settings.outbox_batch_size,
        )
        tasks = [asyncio.create_task(publisher.run_forever(), name="outbox-publisher")]

        yield

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await app.state.redis.aclose()
        await app.state.db.dispose()

    app = FastAPI(
        title="Notification Service", version=settings.service_version, lifespan=lifespan
    )
    app.add_middleware(CorrelationIDMiddleware)
    app.include_router(router)

    @app.exception_handler(ServiceError)
    async def handle_service_error(_request: Request, exc: ServiceError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code, content=exc.to_response().model_dump(mode="json")
        )

    return app
```

`app/core/config.py`:
```python
from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "notification-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
```

- [ ] **Step 8: Implement Alembic**

Copy Task 10's `alembic.ini` and `alembic/env.py`, changing the model imports:
```python
import app.models.notification  # noqa: F401
import app.models.outbox  # noqa: F401
import app.models.processed_event  # noqa: F401
from app.models.base import Base
```

`alembic/versions/0001_create_notification_tables.py` creates `notifications` (with the `(status, created_at)` index), `outbox` (with the `(published, created_at)` index) and `processed_events` (with the `(event_id, consumer_group)` unique constraint), matching the mixins in Task 4. Generate it with:

```bash
PYTHONPATH=services/notification-service DATABASE_URL=postgresql+asyncpg://notif:notif@localhost:5433/notificationdb \
  uv run alembic -c services/notification-service/alembic.ini revision --autogenerate -m "create notification tables"
```

Then read the generated file and confirm all three tables, both indexes and the unique constraint are present before committing it.

- [ ] **Step 9: Run the tests to verify they pass**

```bash
PYTHONPATH=services/notification-service uv run pytest services/notification-service/tests -v
```

Expected: PASS, 14 tests (the invalid-payload parametrisation counts as four).

- [ ] **Step 10: Add the containers and verify end to end by hand**

Append to `docker-compose.yml`:

```yaml
  redis:
    image: redis:7-alpine
    command: ["redis-server", "/usr/local/etc/redis/redis.conf"]
    volumes:
      - ./docker/redis/redis.conf:/usr/local/etc/redis/redis.conf:ro
      - redis-data:/data
    ports: ["6379:6379"]
    networks: [notification-net]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  notification-db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: notif
      POSTGRES_PASSWORD: notif
      POSTGRES_DB: notificationdb
    ports: ["5433:5432"]
    volumes: ["notification-data:/var/lib/postgresql/data"]
    networks: [notification-net]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U notif -d notificationdb"]
      interval: 10s
      timeout: 5s
      retries: 5

  notification-service:
    build:
      context: .
      dockerfile: services/notification-service/Dockerfile
    environment:
      DATABASE_URL: postgresql+asyncpg://notif:notif@notification-db:5432/notificationdb
      REDIS_URL: redis://redis:6379/0
      SERVICE_NAME: notification-service
      LOG_LEVEL: INFO
    ports: ["8001:8000"]
    networks: [notification-net]
    restart: on-failure
    depends_on:
      notification-db: { condition: service_healthy }
      redis: { condition: service_healthy }
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
```

Add `notification-data:` and `redis-data:` under `volumes:`.

`docker/redis/redis.conf`:
```
appendonly yes
appendfsync everysec
maxmemory-policy noeviction
```

Verify:
```bash
docker compose up --build -d notification-service
curl -s -X POST http://localhost:8001/notifications \
  -H "Content-Type: application/json" \
  -d '{"channel":"email","recipient":"john@example.com","subject":"Hi","body":"Hello"}'
docker compose exec redis redis-cli XLEN notification.created
docker compose down -v
```

Expected: `202` with a `notification_id`, and `XLEN` reports `1` — the publisher drained the outbox within its poll interval. Nothing consumes the stream yet.

- [ ] **Step 11: Commit**

```bash
git add services/notification-service docker-compose.yml docker/redis
git commit -m "feat(notification-service): add write path, read path and outbox publisher"
```

---

## Task 12: Routing Service — the routing decision

**Files:**
- Create: `services/routing-service/` mirroring Task 10's layout, with `models/route.py`, `models/outbox.py`, `models/processed_event.py`, `workers/notification_consumer.py`
- Modify: `docker-compose.yml` (add `routing-db`, `routing-service`)
- Test: `services/routing-service/tests/conftest.py`, `tests/test_notification_consumer.py`

**Interfaces:**
- Consumes: `RedisStreamConsumer`, `IdempotencyRepository`, `OutboxRepository`, `OutboxPublisher`, `ServiceClient`, `NotFoundError`, `ServiceUnavailableError`
- Produces: `RouteStatus` (`PROCESSING`, `COMPLETED`, `FAILED`); ORM `Route` with `notification_id` unique; `NotificationCreatedConsumer` with `async ensure_groups()`, `async consume_once() -> int`, `async run_forever()`. Task 15 adds a second consumer to this service and reuses `Route` and `RouteStatus`.

This service has **no REST endpoints for domain operations** — only `/health` and `/version`. Its whole job runs in consumers.

- [ ] **Step 1: Write the failing tests**

`services/routing-service/tests/conftest.py` is Task 10's re-export file verbatim.

`services/routing-service/tests/test_notification_consumer.py`:
```python
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import func, select

from notification_shared.events import (
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.http_client import ServiceClient
from notification_shared.streams import RedisStreamPublisher

from app.models.base import Base
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.models.route import Route, RouteStatus
from app.workers.notification_consumer import NotificationCreatedConsumer

pytestmark = pytest.mark.integration


def _created_event(channel: str = "email") -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={
            "channel": channel,
            "recipient": "john@example.com",
            "subject": "Welcome",
            "body": "Hello John!",
        },
        correlation_id="corr-route",
    )


def _config_client(handler) -> ServiceClient:
    return ServiceClient(
        base_url="http://configuration-service:8000",
        transport=httpx.MockTransport(handler),
    )


def _enabled(_request):
    return httpx.Response(200, json={"name": "email", "enabled": True})


def _disabled(_request):
    return httpx.Response(200, json={"name": "email", "enabled": False})


def _absent(_request):
    return httpx.Response(404, json={})


def _unavailable(_request):
    return httpx.Response(503, text="")


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def make_consumer(sessions, redis_client):
    created = []

    def _make(handler) -> NotificationCreatedConsumer:
        consumer = NotificationCreatedConsumer(
            session_factory=sessions,
            redis=redis_client,
            consumer_name="routing-test",
            configuration_client=_config_client(handler),
            poll_interval_ms=10,
        )
        created.append(consumer)
        return consumer

    yield _make


async def _publish(redis_client, envelope: EventEnvelope) -> None:
    await RedisStreamPublisher(redis_client).publish(Stream.NOTIFICATION_CREATED, envelope)


async def test_an_enabled_channel_produces_a_processing_route_and_a_routed_event(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_enabled)
    await consumer.ensure_groups()
    event = _created_event()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        route = (await session.scalars(select(Route))).one()
        assert route.notification_id == event.aggregate_id
        assert route.status == RouteStatus.PROCESSING
        assert route.fail_reason is None

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "notification.routed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.NOTIFICATION_ROUTED
        assert published.aggregate_id == event.aggregate_id
        assert published.correlation_id == "corr-route"
        assert published.payload == {
            "channel": "email",
            "recipient": "john@example.com",
            "subject": "Welcome",
            "body": "Hello John!",
            "route_id": str(route.id),
        }


async def test_a_disabled_channel_produces_a_failed_route_and_routing_failed(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_disabled)
    await consumer.ensure_groups()
    event = _created_event()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        route = (await session.scalars(select(Route))).one()
        assert route.status == RouteStatus.FAILED
        assert route.fail_reason == "channel_disabled"

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "delivery.failed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.ROUTING_FAILED
        assert published.payload["reason"] == "channel_disabled"
        assert published.payload["route_id"] == str(route.id)


async def test_an_unknown_channel_fails_with_unknown_channel(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_absent)
    await consumer.ensure_groups()
    await _publish(redis_client, _created_event("email"))

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        assert (await session.scalars(select(Route))).one().fail_reason == "unknown_channel"
        published = EventEnvelope.model_validate((await session.scalars(select(Outbox))).one().payload)
        assert published.payload["reason"] == "unknown_channel"


async def test_configuration_unavailable_writes_nothing_and_leaves_the_message_pending(
    make_consumer, sessions, redis_client
):
    """Spec 3.18 — an outage must not become a permanent routing failure."""
    consumer = make_consumer(_unavailable)
    await consumer.ensure_groups()
    await _publish(redis_client, _created_event())

    assert await consumer.consume_once() == 0

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Route)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0

    pending = await redis_client.xpending(
        str(Stream.NOTIFICATION_CREATED), str(ConsumerGroup.ROUTING)
    )
    assert pending["pending"] == 1


async def test_a_configuration_timeout_behaves_the_same_as_a_5xx(
    make_consumer, sessions, redis_client
):
    def timeout(request):
        raise httpx.ReadTimeout("too slow", request=request)

    consumer = make_consumer(timeout)
    await consumer.ensure_groups()
    await _publish(redis_client, _created_event())

    assert await consumer.consume_once() == 0
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Route)) == 0


async def test_an_outage_leaves_the_message_pending_for_recovery(
    make_consumer, sessions, redis_client
):
    """Nothing is acked during an outage, so the message survives as pending.

    Slice 1 has no XCLAIM worker, so the guarantee this test pins is narrow
    and deliberately so: the message is pending, not lost. Slice 2's recovery
    worker is what actually re-routes it.
    """
    down = make_consumer(_unavailable)
    await down.ensure_groups()
    await _publish(redis_client, _created_event())
    assert await down.consume_once() == 0

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Route)) == 0

    # The message is pending, not lost. Slice 2's XCLAIM worker is what picks
    # it up; asserting the pending count is the slice 1 guarantee.
    pending = await redis_client.xpending(
        str(Stream.NOTIFICATION_CREATED), str(ConsumerGroup.ROUTING)
    )
    assert pending["pending"] == 1


async def test_a_replayed_event_is_acked_without_writing_a_second_route(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_enabled)
    await consumer.ensure_groups()
    event = _created_event()
    await _publish(redis_client, event)
    await consumer.consume_once()

    await _publish(redis_client, event)
    assert await consumer.consume_once() == 1

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Route)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_the_route_is_marked_processed_for_its_own_group_only(
    make_consumer, sessions, redis_client
):
    consumer = make_consumer(_enabled)
    await consumer.ensure_groups()
    await _publish(redis_client, _created_event())
    await consumer.consume_once()

    async with sessions() as session:
        row = (await session.scalars(select(ProcessedEvent))).one()
        assert row.consumer_group == ConsumerGroup.ROUTING
        assert row.status == "PROCESSED"


async def test_consume_once_on_an_empty_stream_returns_zero(make_consumer):
    consumer = make_consumer(_enabled)
    await consumer.ensure_groups()
    assert await consumer.consume_once() == 0


async def test_the_notification_id_unique_constraint_blocks_a_second_route(sessions):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            Route(
                notification_id=notification_id,
                channel="email",
                status=RouteStatus.PROCESSING.value,
            )
        )
        await session.commit()

    from sqlalchemy.exc import IntegrityError

    with pytest.raises(IntegrityError):
        async with sessions() as session:
            session.add(
                Route(
                    notification_id=notification_id,
                    channel="email",
                    status=RouteStatus.PROCESSING.value,
                )
            )
            await session.commit()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=services/routing-service uv run pytest services/routing-service/tests -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.models.route'`

- [ ] **Step 3: Write the manifest, settings and models**

`services/routing-service/pyproject.toml` is Task 10's manifest with `name = "routing-service"`, plus `"redis>=8.1.0"` and `"httpx>=0.28.1"`.

`app/core/config.py`:
```python
from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "routing-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    configuration_service_url: str = "http://configuration-service:8000"
    http_timeout_seconds: float = 5.0
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
```

`app/models/route.py`:
```python
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from notification_shared.models import TimestampMixin

from app.models.base import Base


class RouteStatus(StrEnum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Route(Base, TimestampMixin):
    __tablename__ = "routes"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    notification_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)

    __table_args__ = (
        UniqueConstraint("notification_id", name="uq_routes_notification_id"),
    )
```

`app/models/base.py`, `app/models/outbox.py` and `app/models/processed_event.py` are Task 11's files verbatim.

- [ ] **Step 4: Implement the route repository**

`app/repositories/route.py`:
```python
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.route import Route


class RouteRepository:
    async def add(self, session: AsyncSession, route: Route) -> None:
        session.add(route)

    async def set_status(
        self,
        session: AsyncSession,
        notification_id: UUID,
        status: str,
        fail_reason: str | None = None,
    ) -> int:
        """Returns the number of rows updated. Used by Task 15."""
        result = await session.execute(
            update(Route)
            .where(Route.notification_id == notification_id)
            .values(status=status, fail_reason=fail_reason)
        )
        return result.rowcount

    async def get_by_notification(
        self, session: AsyncSession, notification_id: UUID
    ) -> Route | None:
        return await session.scalar(
            select(Route).where(Route.notification_id == notification_id)
        )
```

- [ ] **Step 5: Implement the consumer**

`app/workers/notification_consumer.py`:
```python
"""Consumes notification.created and makes the routing decision.

The Configuration Service call happens outside the database transaction: a
synchronous REST call must not hold a transaction open. The decision it returns
then drives a single transaction containing the route row, the outbox row and
the idempotency mark.
"""

from __future__ import annotations

import asyncio
import logging
from uuid import uuid4

from notification_shared.context import set_correlation_id
from notification_shared.events import (
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.exceptions import NotFoundError, ServiceUnavailableError
from notification_shared.http_client import ServiceClient
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.outbox import OutboxRepository
from notification_shared.streams import RedisStreamConsumer

from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.models.route import Route, RouteStatus
from app.repositories.route import RouteRepository

logger = logging.getLogger(__name__)

STREAM = Stream.NOTIFICATION_CREATED
GROUP = ConsumerGroup.ROUTING


class NotificationCreatedConsumer:
    def __init__(
        self,
        *,
        session_factory,
        redis,
        consumer_name: str,
        configuration_client: ServiceClient,
        poll_interval_ms: int = 500,
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._config = configuration_client
        self._routes = RouteRepository()
        self._outbox = OutboxRepository(Outbox)
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms

    async def ensure_groups(self) -> None:
        await self._consumer.ensure_group(STREAM, GROUP)

    async def consume_once(self) -> int:
        """Read one batch. Returns how many messages were acked."""
        messages = await self._consumer.read(
            STREAM, GROUP, block_ms=self._poll_interval_ms
        )
        acked = 0
        for message in messages:
            if await self._handle(message.envelope):
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
                logger.exception("routing consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _handle(self, envelope: EventEnvelope) -> bool:
        """Returns True when the message should be acked."""
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                logger.debug("event already processed, acking", extra=log_fields)
                return True

        channel = envelope.payload["channel"]
        try:
            fail_reason = await self._decide(channel)
        except ServiceUnavailableError as exc:
            # Transient. Do not ack: the message stays pending for slice 2's
            # XCLAIM recovery. An outage must never become a routing failure.
            logger.warning(
                "configuration service unavailable, leaving message pending: %s",
                exc,
                extra=log_fields,
            )
            return False

        async with self._session_factory() as session:
            route = Route(
                id=uuid4(),
                notification_id=envelope.aggregate_id,
                channel=channel,
                status=(
                    RouteStatus.PROCESSING.value
                    if fail_reason is None
                    else RouteStatus.FAILED.value
                ),
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
                        payload={
                            "channel": channel,
                            "route_id": str(route.id),
                            "reason": fail_reason,
                        },
                        correlation_id=envelope.correlation_id,
                    ),
                )

            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        logger.info(
            "routed notification" if fail_reason is None else "routing failed",
            extra=log_fields,
        )
        return True

    async def _decide(self, channel: str) -> str | None:
        """None means routable; a string is the RoutingFailed reason.

        A 404 is permanent (the channel does not exist). A timeout or 5xx
        raises ServiceUnavailableError and is handled by the caller as
        transient. See spec 3.18.
        """
        try:
            state = await self._config.get(f"/channels/{channel}")
        except NotFoundError:
            return "unknown_channel"
        return None if state["enabled"] else "channel_disabled"
```

- [ ] **Step 6: Implement `app/main.py`**

Task 11's `create_app` with two differences: the router exposes only `system.router` (no domain endpoints), and the lifespan starts two tasks.

```python
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.settings = settings
        app.state.db = Database(settings.database_url)
        app.state.redis = aioredis.from_url(settings.redis_url)
        await app.state.redis.ping()
        app.state.config_client = ServiceClient(
            settings.configuration_service_url, timeout=settings.http_timeout_seconds
        )

        consumer = NotificationCreatedConsumer(
            session_factory=app.state.db.session_factory,
            redis=app.state.redis,
            consumer_name=socket.gethostname(),
            configuration_client=app.state.config_client,
            poll_interval_ms=settings.consumer_poll_interval_ms,
        )
        await consumer.ensure_groups()

        publisher = OutboxPublisher(
            session_factory=app.state.db.session_factory,
            repository=OutboxRepository(Outbox),
            publisher=RedisStreamPublisher(app.state.redis),
            poll_interval_ms=settings.outbox_poll_interval_ms,
            batch_size=settings.outbox_batch_size,
        )

        tasks = [
            asyncio.create_task(consumer.run_forever(), name="notification-consumer"),
            asyncio.create_task(publisher.run_forever(), name="outbox-publisher"),
        ]

        yield

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await app.state.config_client.aclose()
        await app.state.redis.aclose()
        await app.state.db.dispose()
```

`ensure_groups()` is awaited **before** the tasks start, so the group exists at offset 0 before anything reads.

- [ ] **Step 7: Implement Alembic**

Copy Task 11's `alembic.ini` and `env.py`, importing `app.models.route`, `app.models.outbox`, `app.models.processed_event`. Autogenerate `0001_create_routing_tables.py` and confirm it contains `routes` with the `uq_routes_notification_id` constraint, plus `outbox` and `processed_events`.

- [ ] **Step 8: Run the tests to verify they pass**

Run: `PYTHONPATH=services/routing-service uv run pytest services/routing-service/tests -v`
Expected: PASS, 10 tests

- [ ] **Step 9: Add the containers**

Append to `docker-compose.yml`: `routing-db` (host port 5434, database `routingdb`) following `notification-db`'s shape, and `routing-service` following `notification-service`'s shape with host port 8002, plus:

```yaml
    environment:
      DATABASE_URL: postgresql+asyncpg://notif:notif@routing-db:5432/routingdb
      REDIS_URL: redis://redis:6379/0
      CONFIGURATION_SERVICE_URL: http://configuration-service:8000
      SERVICE_NAME: routing-service
      LOG_LEVEL: INFO
    depends_on:
      routing-db: { condition: service_healthy }
      redis: { condition: service_healthy }
      configuration-service: { condition: service_healthy }
```

Add `routing-data:` under `volumes:`.

- [ ] **Step 10: Verify by hand**

```bash
docker compose up --build -d routing-service notification-service
curl -s -X POST http://localhost:8001/notifications -H "Content-Type: application/json" \
  -d '{"channel":"email","recipient":"john@example.com","body":"Hello"}'
sleep 3
docker compose exec redis redis-cli XLEN notification.routed
docker compose exec routing-db psql -U notif -d routingdb -c "select status, fail_reason from routes;"
docker compose down -v
```

Expected: `XLEN notification.routed` is `1` and the route row is `PROCESSING`. Nothing consumes `notification.routed` yet.

- [ ] **Step 11: Commit**

```bash
git add services/routing-service docker-compose.yml
git commit -m "feat(routing-service): add notification.created consumer and routing decision"
```

---

## Task 13: Email Service — delivery

**Files:**
- Create: `services/email-service/` mirroring Task 10's layout, with `models/email_delivery.py`, `models/outbox.py`, `models/processed_event.py`, `workers/routed_consumer.py`
- Modify: `docker-compose.yml` (add `email-db`, `email-service`)
- Test: `services/email-service/tests/conftest.py`, `tests/test_routed_consumer.py`

**Interfaces:**
- Consumes: the same shared pieces as Task 12
- Produces: `DeliveryStatus` (`SENDING`, `DELIVERED`, `FAILED`); ORM `EmailDelivery` with `notification_id` unique; `RoutedConsumer` with `async ensure_groups()`, `async consume_once() -> int`, `async run_forever()`.

Delivery failure is deterministic: a recipient containing `fail` fails. See spec 3.8. The simulated latency is configurable so tests run at zero.

- [ ] **Step 1: Write the failing tests**

`services/email-service/tests/test_routed_consumer.py`:
```python
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from notification_shared.events import (
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.streams import RedisStreamPublisher

from app.models.base import Base
from app.models.email_delivery import DeliveryStatus, EmailDelivery
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent
from app.workers.routed_consumer import RoutedConsumer

pytestmark = pytest.mark.integration


def _routed_event(channel: str = "email", recipient: str = "john@example.com") -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={
            "channel": channel,
            "recipient": recipient,
            "subject": "Welcome",
            "body": "Hello John!",
            "route_id": str(uuid4()),
        },
        correlation_id="corr-email",
    )


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def consumer(sessions, redis_client) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions,
        redis=redis_client,
        consumer_name="email-test",
        poll_interval_ms=10,
        delivery_latency_ms_max=0,
    )


async def _publish(redis_client, envelope: EventEnvelope) -> None:
    await RedisStreamPublisher(redis_client).publish(Stream.NOTIFICATION_ROUTED, envelope)


async def test_a_successful_delivery_records_delivered_and_publishes_completed(
    consumer, sessions, redis_client
):
    await consumer.ensure_groups()
    event = _routed_event()
    await _publish(redis_client, event)

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        delivery = (await session.scalars(select(EmailDelivery))).one()
        assert delivery.notification_id == event.aggregate_id
        assert delivery.recipient == "john@example.com"
        assert delivery.status == DeliveryStatus.DELIVERED
        assert delivery.sent_at is not None
        assert delivery.fail_reason is None

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "delivery.completed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.DELIVERY_COMPLETED
        assert published.aggregate_id == event.aggregate_id
        assert published.correlation_id == "corr-email"
        assert published.payload["channel"] == "email"
        assert published.payload["delivery_id"] == str(delivery.id)
        assert published.payload["recipient"] == "john@example.com"
        assert "delivered_at" in published.payload


async def test_a_recipient_containing_fail_records_failed_and_publishes_delivery_failed(
    consumer, sessions, redis_client
):
    """Spec 3.8 — the deterministic failure rule."""
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="fail@example.com"))

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        delivery = (await session.scalars(select(EmailDelivery))).one()
        assert delivery.status == DeliveryStatus.FAILED
        assert delivery.fail_reason == "simulated_failure"

        outbox_row = (await session.scalars(select(Outbox))).one()
        assert outbox_row.stream == "delivery.failed"
        published = EventEnvelope.model_validate(outbox_row.payload)
        assert published.event_type is EventType.DELIVERY_FAILED
        assert published.payload["reason"] == "simulated_failure"


async def test_the_fail_rule_is_case_insensitive(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(recipient="FAILURE@example.com"))
    await consumer.consume_once()

    async with sessions() as session:
        assert (await session.scalars(select(EmailDelivery))).one().status == DeliveryStatus.FAILED


async def test_a_telegram_event_is_acked_and_skipped(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(channel="telegram"))

    assert await consumer.consume_once() == 1

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(EmailDelivery)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0

    pending = await redis_client.xpending(
        str(Stream.NOTIFICATION_ROUTED), str(ConsumerGroup.EMAIL)
    )
    assert pending["pending"] == 0


async def test_a_skipped_event_is_not_recorded_as_processed(consumer, sessions, redis_client):
    """Filtering is not processing: no row is written for another channel's event."""
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(channel="telegram"))
    await consumer.consume_once()

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0


async def test_a_replayed_event_delivers_once(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    event = _routed_event()
    await _publish(redis_client, event)
    await consumer.consume_once()

    await _publish(redis_client, event)
    assert await consumer.consume_once() == 1

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(EmailDelivery)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_a_mixed_batch_delivers_only_the_email_events(consumer, sessions, redis_client):
    await consumer.ensure_groups()
    await _publish(redis_client, _routed_event(channel="telegram"))
    await _publish(redis_client, _routed_event(recipient="a@example.com"))
    await _publish(redis_client, _routed_event(channel="telegram"))

    assert await consumer.consume_once() == 3

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(EmailDelivery)) == 1


async def test_consume_once_on_an_empty_stream_returns_zero(consumer):
    await consumer.ensure_groups()
    assert await consumer.consume_once() == 0


async def test_the_notification_id_unique_constraint_blocks_a_second_delivery(sessions):
    from sqlalchemy.exc import IntegrityError

    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            EmailDelivery(
                notification_id=notification_id,
                recipient="a@b.com",
                status=DeliveryStatus.SENDING.value,
            )
        )
        await session.commit()

    with pytest.raises(IntegrityError):
        async with sessions() as session:
            session.add(
                EmailDelivery(
                    notification_id=notification_id,
                    recipient="a@b.com",
                    status=DeliveryStatus.SENDING.value,
                )
            )
            await session.commit()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=services/email-service uv run pytest services/email-service/tests -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.models.email_delivery'`

- [ ] **Step 3: Write the manifest, settings and model**

`services/email-service/pyproject.toml` is Task 12's manifest with `name = "email-service"` and no `httpx` dependency — this service makes no REST calls.

`app/core/config.py`:
```python
from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "email-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    delivery_latency_ms_max: int = 500
```

`app/models/email_delivery.py`:
```python
from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from sqlalchemy import DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from notification_shared.models import TimestampMixin

from app.models.base import Base


class DeliveryStatus(StrEnum):
    SENDING = "SENDING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"


class EmailDelivery(Base, TimestampMixin):
    __tablename__ = "email_delivery"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    notification_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("notification_id", name="uq_email_delivery_notification_id"),
    )
```

- [ ] **Step 4: Implement the consumer**

`app/workers/routed_consumer.py`:
```python
"""Consumes notification.routed and delivers email.

Delivery is simulated. Failure is deterministic rather than random so the
behaviour can be tested and demonstrated: a recipient containing "fail" fails.
See spec correction 3.8.
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import UTC, datetime
from uuid import uuid4

from notification_shared.context import set_correlation_id
from notification_shared.events import (
    Channel,
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.outbox import OutboxRepository
from notification_shared.streams import RedisStreamConsumer

from app.models.email_delivery import DeliveryStatus, EmailDelivery
from app.models.outbox import Outbox
from app.models.processed_event import ProcessedEvent

logger = logging.getLogger(__name__)

STREAM = Stream.NOTIFICATION_ROUTED
GROUP = ConsumerGroup.EMAIL
FAILURE_MARKER = "fail"


class RoutedConsumer:
    def __init__(
        self,
        *,
        session_factory,
        redis,
        consumer_name: str,
        poll_interval_ms: int = 500,
        delivery_latency_ms_max: int = 500,
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._outbox = OutboxRepository(Outbox)
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms
        self._latency_ms_max = delivery_latency_ms_max

    async def ensure_groups(self) -> None:
        await self._consumer.ensure_group(STREAM, GROUP)

    async def consume_once(self) -> int:
        messages = await self._consumer.read(
            STREAM, GROUP, block_ms=self._poll_interval_ms
        )
        acked = 0
        for message in messages:
            if await self._handle(message.envelope):
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
                logger.exception("email consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        if envelope.payload.get("channel") != Channel.EMAIL:
            # Both delivery services read this stream; each handles its own
            # channel. Acking without recording anything is correct: this
            # event was never ours to process.
            logger.debug("not an email event, acking and skipping", extra=log_fields)
            return True

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                logger.debug("event already processed, acking", extra=log_fields)
                return True

        recipient = envelope.payload["recipient"]
        fail_reason = await self._deliver(
            recipient, envelope.payload.get("subject"), envelope.payload["body"]
        )
        delivered_at = datetime.now(UTC)

        async with self._session_factory() as session:
            delivery = EmailDelivery(
                id=uuid4(),
                notification_id=envelope.aggregate_id,
                recipient=recipient,
                status=(
                    DeliveryStatus.DELIVERED.value
                    if fail_reason is None
                    else DeliveryStatus.FAILED.value
                ),
                fail_reason=fail_reason,
                sent_at=delivered_at,
            )
            session.add(delivery)
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
                            "delivered_at": delivered_at.isoformat(),
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

            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        logger.info(
            "email delivered" if fail_reason is None else "email delivery failed",
            extra=log_fields,
        )
        return True

    async def _deliver(self, recipient: str, subject: str | None, body: str) -> str | None:
        """Simulated delivery. Returns None on success, or a failure reason."""
        logger.info("sending email to %s", recipient)
        if self._latency_ms_max:
            await asyncio.sleep(random.uniform(0, self._latency_ms_max) / 1000)
        if FAILURE_MARKER in recipient.lower():
            return "simulated_failure"
        return None
```

- [ ] **Step 5: Implement `main.py` and Alembic**

`create_app` follows Task 12's shape: `system.router` only, lifespan starting `RoutedConsumer.run_forever()` and `OutboxPublisher.run_forever()`, with `await consumer.ensure_groups()` before either task starts. Alembic follows Task 12, importing `app.models.email_delivery`, `app.models.outbox`, `app.models.processed_event`; autogenerate `0001_create_email_tables.py` and confirm `email_delivery` carries `uq_email_delivery_notification_id`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `PYTHONPATH=services/email-service uv run pytest services/email-service/tests -v`
Expected: PASS, 9 tests

- [ ] **Step 7: Add the containers and verify by hand**

Append `email-db` (host port 5436, database `emaildb`) and `email-service` (host port 8004) to `docker-compose.yml`, following Task 12's shape but with no `CONFIGURATION_SERVICE_URL` and no dependency on `configuration-service`. Add `email-data:` under `volumes:`.

```bash
docker compose up --build -d
curl -s -X POST http://localhost:8001/notifications -H "Content-Type: application/json" \
  -d '{"channel":"email","recipient":"john@example.com","body":"Hello"}'
sleep 4
docker compose exec redis redis-cli XLEN delivery.completed
docker compose exec email-db psql -U notif -d emaildb -c "select status from email_delivery;"
docker compose down -v
```

Expected: `XLEN delivery.completed` is `1` and the delivery row is `DELIVERED`. The notification itself is still `CREATED` — Task 14 closes the loop.

- [ ] **Step 8: Commit**

```bash
git add services/email-service docker-compose.yml
git commit -m "feat(email-service): add notification.routed consumer and simulated delivery"
```

---

## Task 14: Notification Service — guarded status transitions

**Files:**
- Create: `services/notification-service/app/workers/routed_consumer.py`, `app/workers/results_consumer.py`
- Modify: `services/notification-service/app/repositories/notification.py` (add `advance_status`), `app/main.py` (start two more tasks)
- Test: `services/notification-service/tests/test_status_transitions.py`

**Interfaces:**
- Consumes: `Notification`, `NotificationStatus` (Task 11)
- Produces: `NotificationRepository.advance_status(session, notification_id, new_status, allowed_from, fail_reason=None) -> int`; `RoutedConsumer` and `ResultsConsumer`, each with `ensure_groups()`, `consume_once() -> int`, `run_forever()`.

This is the task that closes the saga, and the one where correctness is subtle. `notification-service-routed` and `notification-service-results` are independent consumer groups with no ordering guarantee between them, so the transitions are guarded in SQL. See spec 3.16.

Legal transitions: `CREATED → PROCESSING`, `CREATED → FAILED`, `PROCESSING → COMPLETED`, `PROCESSING → FAILED`. Terminal states never reopen. A handler whose guard matches nothing updates zero rows, logs, and still acks.

- [ ] **Step 1: Write the failing tests**

`services/notification-service/tests/test_status_transitions.py`:
```python
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select

from notification_shared.events import (
    Channel,
    EventEnvelope,
    EventType,
    Stream,
)
from notification_shared.streams import RedisStreamPublisher

from app.models.base import Base
from app.models.notification import Notification, NotificationStatus
from app.workers.results_consumer import ResultsConsumer
from app.workers.routed_consumer import RoutedConsumer

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def routed(sessions, redis_client) -> RoutedConsumer:
    return RoutedConsumer(
        session_factory=sessions, redis=redis_client,
        consumer_name="notif-routed-test", poll_interval_ms=10,
    )


@pytest.fixture
def results(sessions, redis_client) -> ResultsConsumer:
    return ResultsConsumer(
        session_factory=sessions, redis=redis_client,
        consumer_name="notif-results-test", poll_interval_ms=10,
    )


async def _seed_notification(sessions, status: str = NotificationStatus.CREATED.value):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            Notification(
                id=notification_id, channel=Channel.EMAIL.value,
                recipient="john@example.com", subject=None, body="Hello",
                status=status,
            )
        )
        await session.commit()
    return notification_id


async def _status(sessions, notification_id):
    async with sessions() as session:
        row = (
            await session.execute(
                select(Notification.status, Notification.fail_reason).where(
                    Notification.id == notification_id
                )
            )
        ).one()
        return row.status, row.fail_reason


def _routed_event(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "recipient": "john@example.com",
            "subject": None, "body": "Hello", "route_id": str(uuid4()),
        },
        correlation_id="corr-transition",
    )


def _completed_event(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "delivery_id": str(uuid4()),
            "recipient": "john@example.com",
            "delivered_at": datetime.now(UTC).isoformat(),
        },
        correlation_id="corr-transition",
    )


def _delivery_failed_event(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.DELIVERY_FAILED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "delivery_id": str(uuid4()),
            "reason": "simulated_failure",
        },
        correlation_id="corr-transition",
    )


def _routing_failed_event(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.ROUTING_FAILED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "route_id": str(uuid4()),
            "reason": "channel_disabled",
        },
        correlation_id="corr-transition",
    )


async def test_the_routed_event_moves_created_to_processing(routed, sessions, redis_client):
    notification_id = await _seed_notification(sessions)
    await routed.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.NOTIFICATION_ROUTED, _routed_event(notification_id)
    )

    assert await routed.consume_once() == 1
    assert await _status(sessions, notification_id) == (NotificationStatus.PROCESSING, None)


async def test_the_completed_event_moves_processing_to_completed(
    results, sessions, redis_client
):
    notification_id = await _seed_notification(sessions, NotificationStatus.PROCESSING.value)
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_COMPLETED, _completed_event(notification_id)
    )

    assert await results.consume_once() == 1
    assert await _status(sessions, notification_id) == (NotificationStatus.COMPLETED, None)


async def test_a_delivery_failure_records_the_reason(results, sessions, redis_client):
    notification_id = await _seed_notification(sessions, NotificationStatus.PROCESSING.value)
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _delivery_failed_event(notification_id)
    )

    assert await results.consume_once() == 1
    assert await _status(sessions, notification_id) == (
        NotificationStatus.FAILED,
        "simulated_failure",
    )


async def test_routing_failed_moves_created_straight_to_failed(
    results, sessions, redis_client
):
    """No notification.routed is published when routing fails, so PROCESSING is skipped."""
    notification_id = await _seed_notification(sessions)
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _routing_failed_event(notification_id)
    )

    assert await results.consume_once() == 1
    assert await _status(sessions, notification_id) == (
        NotificationStatus.FAILED,
        "channel_disabled",
    )


async def test_a_result_arriving_before_the_routed_event_is_not_overwritten(
    routed, results, sessions, redis_client
):
    """Spec 3.16 — the exact race the guarded transitions exist to prevent.

    Without the `status = 'CREATED'` guard on the routed handler, this test
    ends at PROCESSING: a finished notification reopened by a late event.
    """
    notification_id = await _seed_notification(sessions)
    await routed.ensure_groups()
    await results.ensure_groups()

    publisher = RedisStreamPublisher(redis_client)
    await publisher.publish(Stream.NOTIFICATION_ROUTED, _routed_event(notification_id))
    await publisher.publish(Stream.DELIVERY_COMPLETED, _completed_event(notification_id))

    # Results first, out of order.
    assert await results.consume_once() == 1
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.COMPLETED

    # The routed event arrives late and must change nothing.
    assert await routed.consume_once() == 1
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.COMPLETED


async def test_a_completed_notification_is_not_reopened_by_a_later_failure(
    results, sessions, redis_client
):
    notification_id = await _seed_notification(sessions, NotificationStatus.COMPLETED.value)
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _delivery_failed_event(notification_id)
    )

    assert await results.consume_once() == 1
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.COMPLETED


async def test_an_event_for_an_unknown_notification_is_acked(
    results, sessions, redis_client
):
    await results.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_COMPLETED, _completed_event(uuid4())
    )
    assert await results.consume_once() == 1


async def test_a_replayed_routed_event_is_acked_once_and_changes_nothing_twice(
    routed, sessions, redis_client
):
    notification_id = await _seed_notification(sessions)
    await routed.ensure_groups()
    event = _routed_event(notification_id)
    publisher = RedisStreamPublisher(redis_client)

    await publisher.publish(Stream.NOTIFICATION_ROUTED, event)
    await routed.consume_once()
    await publisher.publish(Stream.NOTIFICATION_ROUTED, event)
    assert await routed.consume_once() == 1

    assert (await _status(sessions, notification_id))[0] == NotificationStatus.PROCESSING


async def test_the_results_consumer_reads_both_result_streams(
    results, sessions, redis_client
):
    first = await _seed_notification(sessions, NotificationStatus.PROCESSING.value)
    second = await _seed_notification(sessions, NotificationStatus.PROCESSING.value)
    await results.ensure_groups()

    publisher = RedisStreamPublisher(redis_client)
    await publisher.publish(Stream.DELIVERY_COMPLETED, _completed_event(first))
    await publisher.publish(Stream.DELIVERY_FAILED, _delivery_failed_event(second))

    assert await results.consume_once() == 2
    assert (await _status(sessions, first))[0] == NotificationStatus.COMPLETED
    assert (await _status(sessions, second))[0] == NotificationStatus.FAILED


async def test_the_full_ordered_sequence_ends_completed(
    routed, results, sessions, redis_client
):
    notification_id = await _seed_notification(sessions)
    await routed.ensure_groups()
    await results.ensure_groups()
    publisher = RedisStreamPublisher(redis_client)

    await publisher.publish(Stream.NOTIFICATION_ROUTED, _routed_event(notification_id))
    await routed.consume_once()
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.PROCESSING

    await publisher.publish(Stream.DELIVERY_COMPLETED, _completed_event(notification_id))
    await results.consume_once()
    assert (await _status(sessions, notification_id))[0] == NotificationStatus.COMPLETED
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=services/notification-service uv run pytest services/notification-service/tests/test_status_transitions.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.workers.routed_consumer'`

- [ ] **Step 3: Add the guarded update to the repository**

Append to `app/repositories/notification.py`:

```python
from collections.abc import Sequence

from sqlalchemy import update


    async def advance_status(
        self,
        session: AsyncSession,
        notification_id: UUID,
        new_status: str,
        allowed_from: Sequence[str],
        fail_reason: str | None = None,
    ) -> int:
        """Monotonic guarded transition.

        The guard lives in the WHERE clause rather than in Python because two
        independent consumer groups race here; a read-then-write in
        application code would have a window between the two. Returns the
        number of rows updated, which is 0 when the transition is illegal.
        """
        result = await session.execute(
            update(Notification)
            .where(
                Notification.id == notification_id,
                Notification.status.in_(list(allowed_from)),
            )
            .values(status=new_status, fail_reason=fail_reason)
        )
        return result.rowcount
```

- [ ] **Step 4: Implement the routed consumer**

`app/workers/routed_consumer.py`:
```python
"""Consumes notification.routed and marks the notification PROCESSING.

This is the only writer of PROCESSING. Without it the status the README
documents would be unreachable and slice 2's watchdog would never fire.
See spec correction 3.1.
"""

from __future__ import annotations

import asyncio
import logging

from notification_shared.context import set_correlation_id
from notification_shared.events import ConsumerGroup, EventEnvelope, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.streams import RedisStreamConsumer

from app.models.notification import NotificationStatus
from app.models.processed_event import ProcessedEvent
from app.repositories.notification import NotificationRepository

logger = logging.getLogger(__name__)

STREAM = Stream.NOTIFICATION_ROUTED
GROUP = ConsumerGroup.NOTIFICATION_ROUTED


class RoutedConsumer:
    def __init__(
        self, *, session_factory, redis, consumer_name: str, poll_interval_ms: int = 500
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._notifications = NotificationRepository()
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms

    async def ensure_groups(self) -> None:
        await self._consumer.ensure_group(STREAM, GROUP)

    async def consume_once(self) -> int:
        messages = await self._consumer.read(STREAM, GROUP, block_ms=self._poll_interval_ms)
        acked = 0
        for message in messages:
            if await self._handle(message.envelope):
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
                logger.exception("routed consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                return True

            updated = await self._notifications.advance_status(
                session,
                envelope.aggregate_id,
                NotificationStatus.PROCESSING.value,
                allowed_from=[NotificationStatus.CREATED.value],
            )
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        if updated:
            logger.info("notification is processing", extra=log_fields)
        else:
            # Already past CREATED: the delivery result arrived first, or this
            # notification is gone. Either way the event is fully handled.
            logger.debug("no CREATED row to advance, acking", extra=log_fields)
        return True
```

- [ ] **Step 5: Implement the results consumer**

`app/workers/results_consumer.py`:
```python
"""Consumes delivery.completed and delivery.failed and closes the notification.

Unlike Routing Service's results consumer, this one *does* handle
RoutingFailed: a routing failure must fail the notification. See spec 3.3.
"""

from __future__ import annotations

import asyncio
import logging

from notification_shared.context import set_correlation_id
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.streams import RedisStreamConsumer

from app.models.notification import NotificationStatus
from app.models.processed_event import ProcessedEvent
from app.repositories.notification import NotificationRepository

logger = logging.getLogger(__name__)

STREAMS = (Stream.DELIVERY_COMPLETED, Stream.DELIVERY_FAILED)
GROUP = ConsumerGroup.NOTIFICATION_RESULTS
NON_TERMINAL = [NotificationStatus.CREATED.value, NotificationStatus.PROCESSING.value]


class ResultsConsumer:
    def __init__(
        self, *, session_factory, redis, consumer_name: str, poll_interval_ms: int = 500
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._notifications = NotificationRepository()
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms

    async def ensure_groups(self) -> None:
        for stream in STREAMS:
            await self._consumer.ensure_group(stream, GROUP)

    async def consume_once(self) -> int:
        acked = 0
        for stream in STREAMS:
            messages = await self._consumer.read(
                stream, GROUP, block_ms=self._poll_interval_ms
            )
            for message in messages:
                if await self._handle(message.envelope):
                    await self._consumer.ack(stream, GROUP, message.message_id)
                    acked += 1
        return acked

    async def run_forever(self) -> None:
        while True:
            try:
                await self.consume_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("results consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        if envelope.event_type is EventType.DELIVERY_COMPLETED:
            new_status = NotificationStatus.COMPLETED.value
            fail_reason = None
        else:
            # DeliveryFailed and RoutingFailed both land here, both carry a reason.
            new_status = NotificationStatus.FAILED.value
            fail_reason = envelope.payload.get("reason")

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                return True

            updated = await self._notifications.advance_status(
                session,
                envelope.aggregate_id,
                new_status,
                allowed_from=NON_TERMINAL,
                fail_reason=fail_reason,
            )
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        if updated:
            logger.info("notification reached %s", new_status, extra=log_fields)
        else:
            logger.debug("notification already terminal or absent, acking", extra=log_fields)
        return True
```

- [ ] **Step 6: Start both consumers in `main.py`**

In Task 11's lifespan, before `yield`, add:

```python
        routed = RoutedConsumer(
            session_factory=app.state.db.session_factory,
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-routed",
            poll_interval_ms=settings.consumer_poll_interval_ms,
        )
        results = ResultsConsumer(
            session_factory=app.state.db.session_factory,
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-results",
            poll_interval_ms=settings.consumer_poll_interval_ms,
        )
        await routed.ensure_groups()
        await results.ensure_groups()

        tasks = [
            asyncio.create_task(publisher.run_forever(), name="outbox-publisher"),
            asyncio.create_task(routed.run_forever(), name="routed-consumer"),
            asyncio.create_task(results.run_forever(), name="results-consumer"),
        ]
```

Add `consumer_poll_interval_ms: int = 500` to `app/core/config.py`. The two consumers need distinct consumer names within their respective groups; they are in different groups, but distinct names keep `XPENDING` output readable in slice 2.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `PYTHONPATH=services/notification-service uv run pytest services/notification-service/tests -v`
Expected: PASS, 24 tests (14 from Task 11, 10 here)

- [ ] **Step 8: Verify the whole saga by hand**

```bash
docker compose up --build -d
curl -s -X POST http://localhost:8001/notifications -H "Content-Type: application/json" \
  -d '{"channel":"email","recipient":"john@example.com","subject":"Hi","body":"Hello"}'
```

Poll the returned id until it settles:
```bash
curl -s http://localhost:8001/notifications/<id>
```

Expected: the status advances `CREATED` → `PROCESSING` → `COMPLETED`. Then check the failure paths:

```bash
curl -s -X PUT http://localhost:8003/channels/email -H "Content-Type: application/json" -d '{"enabled": false}'
curl -s -X POST http://localhost:8001/notifications -H "Content-Type: application/json" \
  -d '{"channel":"email","recipient":"a@b.com","body":"x"}'
# poll -> FAILED with fail_reason channel_disabled

curl -s -X PUT http://localhost:8003/channels/email -H "Content-Type: application/json" -d '{"enabled": true}'
curl -s -X POST http://localhost:8001/notifications -H "Content-Type: application/json" \
  -d '{"channel":"email","recipient":"fail@example.com","body":"x"}'
# poll -> FAILED with fail_reason simulated_failure
docker compose down -v
```

- [ ] **Step 9: Commit**

```bash
git add services/notification-service
git commit -m "feat(notification-service): add routed and results consumers

Status transitions are guarded in SQL: the two consumer groups have no
ordering guarantee, so a late routed event must not reopen a terminal
notification."
```

---

## Task 15: Routing Service — results consumer

**Files:**
- Create: `services/routing-service/app/workers/results_consumer.py`
- Modify: `services/routing-service/app/main.py` (start a third task)
- Test: `services/routing-service/tests/test_results_consumer.py`

**Interfaces:**
- Consumes: `RouteRepository.set_status` (Task 12), `Route`, `RouteStatus`
- Produces: `ResultsConsumer` with `ensure_groups()`, `consume_once() -> int`, `run_forever()`, on consumer group `routing-service-results`.

The one subtlety: this consumer must **skip `RoutingFailed`**, which Routing Service published itself. It already marked the route `FAILED` in the same transaction that wrote that event; consuming it back would be a loop. See spec 3.3.

- [ ] **Step 1: Write the failing tests**

`services/routing-service/tests/test_results_consumer.py`:
```python
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.streams import RedisStreamPublisher

from app.models.base import Base
from app.models.processed_event import ProcessedEvent
from app.models.route import Route, RouteStatus
from app.workers.results_consumer import ResultsConsumer

pytestmark = pytest.mark.integration


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
def consumer(sessions, redis_client) -> ResultsConsumer:
    return ResultsConsumer(
        session_factory=sessions, redis=redis_client,
        consumer_name="routing-results-test", poll_interval_ms=10,
    )


async def _seed_route(sessions, status: str = RouteStatus.PROCESSING.value):
    notification_id = uuid4()
    async with sessions() as session:
        session.add(
            Route(notification_id=notification_id, channel="email", status=status)
        )
        await session.commit()
    return notification_id


async def _route(sessions, notification_id):
    async with sessions() as session:
        return (
            await session.execute(
                select(Route.status, Route.fail_reason).where(
                    Route.notification_id == notification_id
                )
            )
        ).one()


def _completed(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "delivery_id": str(uuid4()),
            "recipient": "a@b.com", "delivered_at": datetime.now(UTC).isoformat(),
        },
        correlation_id="corr-rr",
    )


def _delivery_failed(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.DELIVERY_FAILED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "delivery_id": str(uuid4()),
            "reason": "simulated_failure",
        },
        correlation_id="corr-rr",
    )


def _routing_failed(notification_id) -> EventEnvelope:
    return EventEnvelope.new(
        event_type=EventType.ROUTING_FAILED,
        aggregate_id=notification_id,
        payload={
            "channel": "email", "route_id": str(uuid4()),
            "reason": "channel_disabled",
        },
        correlation_id="corr-rr",
    )


async def test_delivery_completed_marks_the_route_completed(
    consumer, sessions, redis_client
):
    notification_id = await _seed_route(sessions)
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_COMPLETED, _completed(notification_id)
    )

    assert await consumer.consume_once() == 1
    row = await _route(sessions, notification_id)
    assert row.status == RouteStatus.COMPLETED
    assert row.fail_reason is None


async def test_delivery_failed_marks_the_route_failed_with_the_reason(
    consumer, sessions, redis_client
):
    notification_id = await _seed_route(sessions)
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _delivery_failed(notification_id)
    )

    assert await consumer.consume_once() == 1
    row = await _route(sessions, notification_id)
    assert row.status == RouteStatus.FAILED
    assert row.fail_reason == "simulated_failure"


async def test_routing_failed_is_acked_and_skipped(consumer, sessions, redis_client):
    """Spec 3.3 — Routing Service must not consume its own failure event.

    The route was already marked FAILED in the transaction that published this
    event. Re-handling it would be a loop.
    """
    notification_id = await _seed_route(sessions, RouteStatus.FAILED.value)
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_FAILED, _routing_failed(notification_id)
    )

    assert await consumer.consume_once() == 1

    row = await _route(sessions, notification_id)
    assert row.status == RouteStatus.FAILED
    assert row.fail_reason is None  # untouched: the seed set no reason

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 0


async def test_an_event_for_an_unknown_route_is_acked(consumer, redis_client):
    await consumer.ensure_groups()
    await RedisStreamPublisher(redis_client).publish(
        Stream.DELIVERY_COMPLETED, _completed(uuid4())
    )
    assert await consumer.consume_once() == 1


async def test_a_replayed_result_updates_once(consumer, sessions, redis_client):
    notification_id = await _seed_route(sessions)
    await consumer.ensure_groups()
    event = _completed(notification_id)
    publisher = RedisStreamPublisher(redis_client)

    await publisher.publish(Stream.DELIVERY_COMPLETED, event)
    await consumer.consume_once()
    await publisher.publish(Stream.DELIVERY_COMPLETED, event)
    assert await consumer.consume_once() == 1

    assert (await _route(sessions, notification_id)).status == RouteStatus.COMPLETED
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1


async def test_both_result_streams_are_read(consumer, sessions, redis_client):
    first = await _seed_route(sessions)
    second = await _seed_route(sessions)
    await consumer.ensure_groups()

    publisher = RedisStreamPublisher(redis_client)
    await publisher.publish(Stream.DELIVERY_COMPLETED, _completed(first))
    await publisher.publish(Stream.DELIVERY_FAILED, _delivery_failed(second))

    assert await consumer.consume_once() == 2
    assert (await _route(sessions, first)).status == RouteStatus.COMPLETED
    assert (await _route(sessions, second)).status == RouteStatus.FAILED


async def test_consume_once_on_empty_streams_returns_zero(consumer):
    await consumer.ensure_groups()
    assert await consumer.consume_once() == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=services/routing-service uv run pytest services/routing-service/tests/test_results_consumer.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.workers.results_consumer'`

- [ ] **Step 3: Implement the consumer**

`app/workers/results_consumer.py`:
```python
"""Consumes delivery results and closes the route.

Skips RoutingFailed: Routing Service published that event itself, having
already marked the route FAILED in the same transaction. Consuming it back
would be a loop in the topology. See spec correction 3.3.
"""

from __future__ import annotations

import asyncio
import logging

from notification_shared.context import set_correlation_id
from notification_shared.events import ConsumerGroup, EventEnvelope, EventType, Stream
from notification_shared.idempotency import IdempotencyRepository
from notification_shared.streams import RedisStreamConsumer

from app.models.processed_event import ProcessedEvent
from app.models.route import RouteStatus
from app.repositories.route import RouteRepository

logger = logging.getLogger(__name__)

STREAMS = (Stream.DELIVERY_COMPLETED, Stream.DELIVERY_FAILED)
GROUP = ConsumerGroup.ROUTING_RESULTS
HANDLED = (EventType.DELIVERY_COMPLETED, EventType.DELIVERY_FAILED)


class ResultsConsumer:
    def __init__(
        self, *, session_factory, redis, consumer_name: str, poll_interval_ms: int = 500
    ) -> None:
        self._session_factory = session_factory
        self._consumer = RedisStreamConsumer(redis, consumer_name)
        self._routes = RouteRepository()
        self._idempotency = IdempotencyRepository(ProcessedEvent)
        self._poll_interval_ms = poll_interval_ms

    async def ensure_groups(self) -> None:
        for stream in STREAMS:
            await self._consumer.ensure_group(stream, GROUP)

    async def consume_once(self) -> int:
        acked = 0
        for stream in STREAMS:
            messages = await self._consumer.read(
                stream, GROUP, block_ms=self._poll_interval_ms
            )
            for message in messages:
                if await self._handle(message.envelope):
                    await self._consumer.ack(stream, GROUP, message.message_id)
                    acked += 1
        return acked

    async def run_forever(self) -> None:
        while True:
            try:
                await self.consume_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("routing results consumer iteration failed")
                await asyncio.sleep(self._poll_interval_ms / 1000)

    async def _handle(self, envelope: EventEnvelope) -> bool:
        set_correlation_id(envelope.correlation_id)
        log_fields = {
            "event_id": envelope.event_id,
            "event_type": envelope.event_type,
            "notification_id": envelope.aggregate_id,
            "consumer_group": GROUP,
        }

        if envelope.event_type not in HANDLED:
            logger.debug("our own routing failure, acking and skipping", extra=log_fields)
            return True

        if envelope.event_type is EventType.DELIVERY_COMPLETED:
            new_status = RouteStatus.COMPLETED.value
            fail_reason = None
        else:
            new_status = RouteStatus.FAILED.value
            fail_reason = envelope.payload.get("reason")

        async with self._session_factory() as session:
            if await self._idempotency.is_processed(session, envelope.event_id, GROUP):
                return True

            updated = await self._routes.set_status(
                session, envelope.aggregate_id, new_status, fail_reason
            )
            await self._idempotency.mark_processed(session, envelope.event_id, GROUP)
            await session.commit()

        if updated:
            logger.info("route reached %s", new_status, extra=log_fields)
        else:
            logger.debug("no route for this notification, acking", extra=log_fields)
        return True
```

- [ ] **Step 4: Start it in `main.py`**

In Task 12's lifespan, add the consumer alongside the other two:

```python
        results = ResultsConsumer(
            session_factory=app.state.db.session_factory,
            redis=app.state.redis,
            consumer_name=f"{socket.gethostname()}-results",
            poll_interval_ms=settings.consumer_poll_interval_ms,
        )
        await results.ensure_groups()
```

and `asyncio.create_task(results.run_forever(), name="results-consumer")` in the task list.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PYTHONPATH=services/routing-service uv run pytest services/routing-service/tests -v`
Expected: PASS, 17 tests (10 from Task 12, 7 here)

- [ ] **Step 6: Commit**

```bash
git add services/routing-service
git commit -m "feat(routing-service): add delivery results consumer

Filters out RoutingFailed, which this service publishes itself."
```

---

## Task 16: Full stack and end-to-end tests

**Files:**
- Modify: `docker-compose.yml` (final review of all nine containers)
- Create: `tests/e2e/conftest.py`, `tests/e2e/test_saga.py`

**Interfaces:**
- Consumes: the running stack
- Produces: the `settle` fixture, returning `async (client, notification_id, timeout_s=30.0) -> dict` that polls to a terminal status and reports the statuses it observed in `_observed`; seven end-to-end scenarios proving the saga from the client's point of view.

These tests talk to the stack over HTTP on localhost and know nothing about internals — they are the only tests that exercise Alembic-on-startup, the real lifespan, and the real worker loops together.

- [ ] **Step 1: Review the completed compose file**

Nine containers: `redis`, `notification-db`, `notification-service`, `routing-db`, `routing-service`, `configuration-db`, `configuration-service`, `email-db`, `email-service`. Verify each of the following before writing tests, since a wrong value here produces confusing test failures:

- every application service has a `/health` healthcheck with `start_period: 30s`
- every application service has `restart: on-failure` and is on `notification-net`
- every application service `depends_on` its own database with `condition: service_healthy`
- every event-driven service also `depends_on` `redis` with `condition: service_healthy`
- `routing-service` additionally `depends_on` `configuration-service`
- host ports are 8001 notification, 8002 routing, 8003 configuration, 8004 email; 5433/5434/5435/5436 for the databases; 6379 Redis
- each Postgres has its own named volume; `redis-data` is mounted and `docker/redis/redis.conf` is mounted read-only

- [ ] **Step 2: Write the end-to-end tests**

`tests/e2e/conftest.py`:
```python
"""Fixtures for tests that run against a live docker compose stack.

Bring the stack up first:

    docker compose up --build -d --wait
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import httpx
import pytest

NOTIFICATION_URL = os.environ.get("NOTIFICATION_URL", "http://localhost:8001")
CONFIGURATION_URL = os.environ.get("CONFIGURATION_URL", "http://localhost:8003")
TERMINAL = {"COMPLETED", "FAILED"}


@pytest.fixture
async def notifications() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=NOTIFICATION_URL, timeout=10.0) as client:
        yield client


@pytest.fixture
async def configuration() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=CONFIGURATION_URL, timeout=10.0) as client:
        yield client


@pytest.fixture
async def email_enabled(configuration: httpx.AsyncClient) -> AsyncIterator[None]:
    """Leave the email channel enabled however the test ends."""
    await configuration.put("/channels/email", json={"enabled": True})
    yield
    await configuration.put("/channels/email", json={"enabled": True})


@pytest.fixture
def settle():
    """Poll the read model until the status is terminal.

    Exposed as a fixture rather than a module-level helper so the tests need no
    import from conftest, which is not reliably importable.
    """

    async def _settle(
        client: httpx.AsyncClient, notification_id: str, timeout_s: float = 30.0
    ) -> dict:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_s
        seen: list[str] = []
        while loop.time() < deadline:
            body = (await client.get(f"/notifications/{notification_id}")).json()
            if not seen or seen[-1] != body["status"]:
                seen.append(body["status"])
            if body["status"] in TERMINAL:
                body["_observed"] = seen
                return body
            await asyncio.sleep(0.25)
        raise AssertionError(
            f"notification {notification_id} did not settle in {timeout_s}s; saw {seen}"
        )

    return _settle
```

`tests/e2e/test_saga.py`:
```python
import httpx
import pytest

pytestmark = pytest.mark.e2e


async def test_every_service_reports_healthy(notifications, configuration):
    for client in (notifications, configuration):
        body = (await client.get("/health")).json()
        assert body["status"] == "UP", body
    for port in (8002, 8004):
        async with httpx.AsyncClient(timeout=10.0) as client:
            body = (await client.get(f"http://localhost:{port}/health")).json()
        assert body["status"] == "UP", body


async def test_the_happy_path_reaches_completed(notifications, email_enabled, settle):
    response = await notifications.post(
        "/notifications",
        json={
            "channel": "email",
            "recipient": "john@example.com",
            "subject": "Welcome",
            "body": "Hello John!",
        },
        headers={"X-Correlation-ID": "e2e-happy"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "CREATED"
    assert response.headers["X-Correlation-ID"] == "e2e-happy"

    settled = await settle(notifications, body["notification_id"])
    assert settled["status"] == "COMPLETED"
    assert settled["fail_reason"] is None
    assert settled["channel"] == "email"


async def test_a_disabled_channel_reaches_failed_with_channel_disabled(
    notifications, configuration, email_enabled, settle
):
    await configuration.put("/channels/email", json={"enabled": False})

    notification_id = (
        await notifications.post(
            "/notifications",
            json={"channel": "email", "recipient": "john@example.com", "body": "Hello"},
        )
    ).json()["notification_id"]

    settled = await settle(notifications, notification_id)
    assert settled["status"] == "FAILED"
    assert settled["fail_reason"] == "channel_disabled"


async def test_a_failing_recipient_reaches_failed_with_the_delivery_reason(
    notifications, email_enabled, settle
):
    notification_id = (
        await notifications.post(
            "/notifications",
            json={"channel": "email", "recipient": "fail@example.com", "body": "Hello"},
        )
    ).json()["notification_id"]

    settled = await settle(notifications, notification_id)
    assert settled["status"] == "FAILED"
    assert settled["fail_reason"] == "simulated_failure"


async def test_the_client_can_observe_the_processing_state(
    notifications, email_enabled, settle
):
    """The polling guide claims PROCESSING is observable. Prove it.

    Delivery latency is up to 500ms and the poll interval is 250ms, so
    PROCESSING is normally seen — but the assertion is on the terminal state so
    the test cannot flake on timing.
    """
    notification_id = (
        await notifications.post(
            "/notifications",
            json={"channel": "email", "recipient": "john@example.com", "body": "Hello"},
        )
    ).json()["notification_id"]

    settled = await settle(notifications, notification_id)
    assert settled["status"] == "COMPLETED"
    assert settled["_observed"][0] in {"CREATED", "PROCESSING"}
    assert settled["_observed"][-1] == "COMPLETED"


async def test_an_invalid_payload_is_rejected_before_anything_is_created(notifications):
    response = await notifications.post(
        "/notifications", json={"channel": "carrier-pigeon", "recipient": "a", "body": "b"}
    )
    assert response.status_code == 422


async def test_the_list_endpoint_returns_the_notifications_created_by_this_run(
    notifications, email_enabled, settle
):
    notification_id = (
        await notifications.post(
            "/notifications",
            json={"channel": "email", "recipient": "john@example.com", "body": "Hello"},
        )
    ).json()["notification_id"]
    await settle(notifications, notification_id)

    listed = (await notifications.get("/notifications?limit=100")).json()
    assert any(item["notification_id"] == notification_id for item in listed)
```

`test_the_client_can_observe_the_processing_state` asserts on the terminal state rather than on having caught `PROCESSING`, because asserting the intermediate state would make the test a race. The guarded-transition integration test in Task 14 is what actually pins that behaviour.

- [ ] **Step 3: Run the end-to-end tests**

```bash
docker compose up --build -d --wait
uv run pytest tests/e2e -v
```

Expected: PASS, 7 tests. If `--wait` times out, inspect with `docker compose ps` and `docker compose logs <service>`; a service stuck outside `healthy` usually means Alembic failed at startup, which the entrypoint surfaces in the logs.

- [ ] **Step 4: Run every tier once, from a clean slate**

```bash
docker compose down -v
uv run ruff check .
uv run pytest tests/unit
uv run pytest tests/integration
PYTHONPATH=services/configuration-service uv run pytest services/configuration-service/tests
PYTHONPATH=services/notification-service  uv run pytest services/notification-service/tests
PYTHONPATH=services/routing-service       uv run pytest services/routing-service/tests
PYTHONPATH=services/email-service         uv run pytest services/email-service/tests
docker compose up --build -d --wait
uv run pytest tests/e2e
docker compose down -v
```

Expected totals: ruff clean; unit 38; integration 29; configuration 11; notification 24; routing 17; email 9; e2e 7.

- [ ] **Step 5: Commit**

```bash
git add docker-compose.yml tests/e2e
git commit -m "test: add end-to-end saga tests over the compose stack"
```

---

## Task 17: Editor configuration and documentation

**Files:**
- Create: `.vscode/extensions.json`, `settings.json`, `launch.json`, `tasks.json`
- Create: `README.md`, `docs/architecture.md`, `docs/event-flows.md`, `docs/patterns.md`, `docs/local-development.md`
- Create: `docs/adr/0001-…` through `docs/adr/0021-…` (one per spec correction)
- Create: `services/<name>/README.md` for all four services

**Interfaces:**
- Consumes: the finished slice 1
- Produces: the documentation set from spec section 11, restricted to what slice 1 built. `docs/operations.md` is slice 3.

- [ ] **Step 1: Write `.vscode/extensions.json`**

```json
{
  "recommendations": [
    "ms-python.python",
    "ms-python.vscode-pylance",
    "ms-python.debugpy",
    "charliermarsh.ruff",
    "ms-azuretools.vscode-docker",
    "tamasfe.even-better-toml",
    "redhat.vscode-yaml",
    "bierner.markdown-mermaid"
  ]
}
```

- [ ] **Step 2: Write `.vscode/settings.json`**

```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/Scripts/python.exe",
  "python.testing.pytestEnabled": true,
  "python.testing.unittestEnabled": false,
  "python.testing.pytestArgs": ["tests"],
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": { "source.organizeImports": "explicit" }
  },
  "ruff.importStrategy": "fromEnvironment",
  "files.exclude": {
    "**/__pycache__": true,
    "**/*.egg-info": true,
    "**/.pytest_cache": true,
    "**/.ruff_cache": true
  },
  "search.exclude": { "**/.venv": true, "**/uv.lock": true }
}
```

`python.testing.pytestArgs` points at `tests` only: the service suites each need their own `PYTHONPATH` and are driven from `tasks.json` instead.

- [ ] **Step 3: Write `.vscode/launch.json`**

One configuration per slice-1 service. Each runs against the containerised dependencies on their localhost ports, so start `docker compose up -d redis notification-db routing-db configuration-db email-db` first (the "Infra only" task from Step 4).

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "notification-service",
      "type": "debugpy",
      "request": "launch",
      "module": "uvicorn",
      "args": ["app.main:create_app", "--factory", "--port", "8001", "--reload"],
      "cwd": "${workspaceFolder}/services/notification-service",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/services/notification-service",
        "DATABASE_URL": "postgresql+asyncpg://notif:notif@localhost:5433/notificationdb",
        "REDIS_URL": "redis://localhost:6379/0",
        "SERVICE_NAME": "notification-service",
        "LOG_LEVEL": "DEBUG"
      },
      "justMyCode": false
    },
    {
      "name": "routing-service",
      "type": "debugpy",
      "request": "launch",
      "module": "uvicorn",
      "args": ["app.main:create_app", "--factory", "--port", "8002", "--reload"],
      "cwd": "${workspaceFolder}/services/routing-service",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/services/routing-service",
        "DATABASE_URL": "postgresql+asyncpg://notif:notif@localhost:5434/routingdb",
        "REDIS_URL": "redis://localhost:6379/0",
        "CONFIGURATION_SERVICE_URL": "http://localhost:8003",
        "SERVICE_NAME": "routing-service",
        "LOG_LEVEL": "DEBUG"
      },
      "justMyCode": false
    },
    {
      "name": "configuration-service",
      "type": "debugpy",
      "request": "launch",
      "module": "uvicorn",
      "args": ["app.main:create_app", "--factory", "--port", "8003", "--reload"],
      "cwd": "${workspaceFolder}/services/configuration-service",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/services/configuration-service",
        "DATABASE_URL": "postgresql+asyncpg://notif:notif@localhost:5435/configurationdb",
        "SERVICE_NAME": "configuration-service",
        "LOG_LEVEL": "DEBUG"
      },
      "justMyCode": false
    },
    {
      "name": "email-service",
      "type": "debugpy",
      "request": "launch",
      "module": "uvicorn",
      "args": ["app.main:create_app", "--factory", "--port", "8004", "--reload"],
      "cwd": "${workspaceFolder}/services/email-service",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/services/email-service",
        "DATABASE_URL": "postgresql+asyncpg://notif:notif@localhost:5436/emaildb",
        "REDIS_URL": "redis://localhost:6379/0",
        "SERVICE_NAME": "email-service",
        "LOG_LEVEL": "DEBUG"
      },
      "justMyCode": false
    },
    {
      "name": "Debug Tests",
      "type": "debugpy",
      "request": "launch",
      "module": "pytest",
      "args": ["${file}", "-v"],
      "console": "integratedTerminal",
      "justMyCode": false
    }
  ]
}
```

A service launched this way runs Uvicorn directly, so Alembic does **not** run: apply migrations by hand first with `uv run alembic -c services/<name>/alembic.ini upgrade head` and `DATABASE_URL` set to the localhost port.

- [ ] **Step 4: Write `.vscode/tasks.json`**

```json
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "uv sync",
      "type": "shell",
      "command": "uv sync --all-packages",
      "problemMatcher": []
    },
    {
      "label": "compose up (build)",
      "type": "shell",
      "command": "docker compose up --build -d --wait",
      "problemMatcher": []
    },
    {
      "label": "compose down (reset volumes)",
      "type": "shell",
      "command": "docker compose down -v",
      "problemMatcher": []
    },
    {
      "label": "compose up (infra only)",
      "type": "shell",
      "command": "docker compose up -d redis notification-db routing-db configuration-db email-db",
      "problemMatcher": []
    },
    {
      "label": "test: unit",
      "type": "shell",
      "command": "uv run pytest tests/unit -v",
      "group": "test",
      "problemMatcher": []
    },
    {
      "label": "test: integration (shared)",
      "type": "shell",
      "command": "uv run pytest tests/integration -v",
      "problemMatcher": []
    },
    {
      "label": "test: integration (all services)",
      "type": "shell",
      "command": "for s in configuration-service notification-service routing-service email-service; do PYTHONPATH=services/$s uv run pytest services/$s/tests -v || exit 1; done",
      "problemMatcher": []
    },
    {
      "label": "test: e2e",
      "type": "shell",
      "command": "uv run pytest tests/e2e -v",
      "dependsOn": "compose up (build)",
      "problemMatcher": []
    },
    {
      "label": "lint",
      "type": "shell",
      "command": "uv run ruff check . && uv run ruff format --check .",
      "problemMatcher": []
    }
  ]
}
```

The "all services" task uses POSIX shell syntax. On Windows set `"options": {"shell": {"executable": "bash.exe"}}` on that task, or run the four commands separately.

- [ ] **Step 5: Write the ADRs**

One file per correction in spec section 3, named `docs/adr/NNNN-<slug>.md`, each with exactly four headings — `## Status`, `## Context`, `## Decision`, `## Consequences` — and no more than a page. Take Context and Decision from the spec; Consequences must state what this costs, not only what it buys.

```
0001-notification-service-consumes-notification-routed.md
0002-consumer-groups-start-at-offset-zero.md
0003-routing-service-skips-its-own-routing-failed.md
0004-processed-events-distinguishes-processed-from-failing.md
0005-xpending-uses-an-idle-filter.md
0006-unique-constraints-as-idempotency-backstops.md
0007-fail-reason-in-the-read-model.md
0008-deterministic-delivery-failure-simulation.md
0009-configuration-seed-via-alembic-data-migration.md
0010-uv-workspace-instead-of-pip-editable.md
0011-python-314-everywhere.md
0012-per-service-declarative-base-with-shared-mixins.md
0013-git-repository-initialised.md
0014-vscode-workspace-configuration-is-committed.md
0015-documentation-written-per-slice.md
0016-status-transitions-guarded-in-sql.md
0017-event-payload-schemas-defined.md
0018-configuration-unavailability-is-transient.md
0019-one-redis-field-named-envelope.md
0020-notification-routed-forwards-the-delivery-payload.md
0021-debug-send-endpoint-dropped.md
```

Three of these carry costs worth stating plainly, and the ADR must say so: 0002 couples the Postgres and Redis volume lifecycles; 0012 means the outbox and idempotency tables are declared once per service rather than once overall; 0020 knowingly breaks the "events are small" principle to avoid a cross-service read.

- [ ] **Step 6: Write `docs/architecture.md`**

Required sections and the specific content each must contain:

- **Services and boundaries** — a table of the four slice-1 services with what each owns, its database, and its HTTP surface. State that Routing and Email have no domain REST endpoints.
- **Why choreography, not orchestration** — no central coordinator holds the saga; each service reacts to facts and publishes its own. Name the cost: the flow is not readable in one place, which is why `event-flows.md` exists.
- **Database per service** — why no service reads another's database, and the consequence that `NotificationRouted` must forward the delivery payload (link ADR 0020).
- **Layering** — `api → services → repositories`, workers alongside, with the rule that only repositories contain SQLAlchemy queries. Name the one deliberate exception: guarded status updates live in the repository as Core `update()` statements rather than ORM mutations, because the guard must be in the SQL.
- **What slice 1 does not have** — no recovery, no watchdog, no Telegram, no Gateway, and what that means for a crashed consumer.

Include a Mermaid component diagram showing REST edges solid and stream edges dashed.

- [ ] **Step 7: Write `docs/event-flows.md`**

- The envelope, field by field, and why `aggregate_id` is always the notification id.
- The stream topology table from spec 5.2, including which groups read which streams.
- Each of the five event types with its exact payload schema, copied from spec 5.3.
- Three Mermaid `sequenceDiagram` blocks: happy path, channel disabled, delivery failure. Each must show the outbox write and the `XADD` as separate steps, because collapsing them hides the pattern.
- A short section on the ordering race between `notification-service-routed` and `notification-service-results`, and how the SQL guard resolves it (link ADR 0016).

- [ ] **Step 8: Write `docs/patterns.md`**

This is the educational core. One section per pattern, each answering three questions: what it does, where it lives, and what breaks without it.

- **Outbox** — `shared/notification_shared/outbox.py` and `publisher.py`. Without it a crash between commit and `XADD` produces an accepted notification with no event. Point at the integration test that proves a rolled-back transaction publishes nothing.
- **Idempotent consumers** — `shared/notification_shared/idempotency.py`. Without it the duplicate that at-least-once delivery guarantees will occur causes a second delivery. Point at the crash-between-XADD-and-mark test as the proof duplicates really happen.
- **At-least-once, not exactly-once** — why exactly-once is not on offer, and why that is acceptable here.
- **Consumer groups at offset 0** — the first-boot race, and the volume-reset consequence.
- **Guarded monotonic transitions** — the race, the SQL, and the test that fails without the guard.
- **Group-scoped idempotency** — `delivery.failed` is read by two groups, so `(event_id, consumer_group)` is the key, not `event_id` alone.

Every claim in this file must name a file and a test. A pattern described without its test is the kind of documentation this project is meant to avoid.

- [ ] **Step 9: Write `docs/local-development.md`**

- uv workspace layout, the single root `.venv`, and the fact that `uv pip install` is rejected by uv 0.11.7 — use `uv add` / `uv sync`.
- The six pytest invocations from the plan's Test layout section, and why each service needs its own process.
- Debugging a service from VSCode: start infrastructure only, apply migrations by hand, then launch. State that the launch configuration does not run Alembic.
- Resetting: `docker compose down -v` resets Postgres *and* Redis together, and why doing one without the other causes a replay.

- [ ] **Step 10: Write the four service READMEs**

Each `services/<name>/README.md` states: responsibilities in two or three sentences; the tables it owns; streams consumed with their consumer group; streams published with their event types; its environment variables with defaults; its HTTP endpoints. Routing and Email must say explicitly that they expose no domain endpoints.

- [ ] **Step 11: Write `README.md`**

- **What this is** — one paragraph, and the statement that it is educational.
- **Architecture overview** — the Mermaid diagram plus a short prose walkthrough, linking `docs/architecture.md`.
- **Quick start** — `docker compose up --build -d --wait`, then a `curl` POST and the polling loop, with real expected output including the status progression.
- **Stream topology** — the table from spec 5.2.
- **Polling guide** — the explicit `CREATED → PROCESSING → COMPLETED` example, plus both failure paths with the `curl` commands that trigger them (disable a channel; use a `fail` recipient).
- **Configuration** — the env var table from spec section 12, marking which variables slice 1 actually reads.
- **Running the tests** — the six commands.
- **Known limitations** — every entry from spec section 15, including the two added by the spec: slice 1 has no recovery or watchdog, and the Postgres/Redis volume coupling.

- [ ] **Step 12: Verify the documentation against the code**

Not a formality — this catches drift that tests cannot:

```bash
grep -o 'notification\.[a-z]*\|delivery\.[a-z]*' README.md docs/*.md | sort -u
grep -rno 'notification-service-[a-z]*\|routing-service[a-z-]*\|email-service' docs/event-flows.md | sort -u
```

Check every stream name, consumer group name, env var name and file path mentioned in the docs against the real code. Fix the docs, not the code.

- [ ] **Step 13: Commit**

```bash
git add .vscode README.md docs services/*/README.md
git commit -m "docs: add architecture, event flows, patterns, ADRs and editor config"
```

---

## Slice 1 Definition of Done

All five gates from spec section 14, verified in one clean pass:

- [ ] `uv run ruff check .` and `uv run ruff format --check .` clean
- [ ] `uv run pytest tests/unit` — 38 passing
- [ ] `uv run pytest tests/integration` — 29 passing
- [ ] All four service suites passing — 11 + 24 + 17 + 9 = 61
- [ ] `docker compose up --build -d --wait` brings all nine containers to `healthy`
- [ ] `uv run pytest tests/e2e` — 7 passing
- [ ] `docs/architecture.md`, `event-flows.md`, `patterns.md`, `local-development.md`, 21 ADRs, four service READMEs and `README.md` all exist and match the code
- [ ] `docker compose down -v` leaves nothing behind

Then slice 2: Telegram Service, API Gateway, `XPENDING`/`XCLAIM` recovery, max-retry, and the stale-processing watchdog.
