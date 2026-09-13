# 0012: Per-service declarative Base, shared mixins

## Status

Accepted

## Context

A single `Base` exported from the shared library works per-service at runtime and for Alembic
autogenerate, because each service process imports only its own models. It breaks in the test
suite: with one root `.venv`, a test importing models from two services registers both on the same
SQLAlchemy registry, and `Base.metadata` then contains every service's tables. Schema creation in
an integration test is no longer scoped to the one service under test.

## Decision

The shared library (`shared/notification_shared/models.py`) exports abstract mixins only —
`TimestampMixin`, `OutboxMixin`, `ProcessedEventMixin` — none carrying `__tablename__`. Each
service declares its own `DeclarativeBase` (`app/models/base.py`) and materializes its tables from
the mixins. `OutboxRepository` and `IdempotencyRepository` receive the model class via their
constructor rather than importing a concrete model.

## Consequences

The outbox and idempotency tables are declared once *per service* rather than once overall: four
services each define their own `Outbox` and `ProcessedEvent` model classes
(`app/models/outbox.py`, `app/models/processed_event.py`), all materializing the same mixins, and a
column added to `OutboxMixin` must still be picked up correctly by four separate Alembic histories
rather than one. This is the direct cost of test isolation.

It also means each service's repository layer is genuinely per-service code, and the project's
convention — one repository per aggregate, even a thin one — is not applied uniformly. Routing
Service has `RouteRepository`, Notification Service has `NotificationRepository`, Email Service has
`EmailDeliveryRepository`, each wrapping its model class. Configuration Service is the one
exception: its `ChannelService` (`app/services/channel.py`) calls `session.commit()` directly after
mutating a row it read through the read-only `ChannelRepository`
(`app/repositories/channel.py`, which exposes only `list_all` and `get`), rather than going through
a repository method that performs the write. This is a known inconsistency, recorded honestly here
rather than described as a uniformity the codebase does not actually have.
