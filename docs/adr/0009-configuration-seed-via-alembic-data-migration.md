# 0009: Configuration seed via Alembic data migration

## Status

Accepted

## Context

An earlier design offered a choice between an Alembic data migration and a startup seed script run
by the application process. A startup script introduces a race (the app can start serving before
seeding completes, or two replicas could seed concurrently) and is not covered by the same
migration tooling and tests as the schema itself.

## Decision

The two channels are seeded by the Alembic migration itself
(`services/configuration-service/alembic/versions/0001_create_channels.py`), via `op.bulk_insert`
in the same revision that creates the `channels` table. It is deterministic, runs before Uvicorn
starts (the container `entrypoint.sh` runs `alembic upgrade head` first), has no startup race, and
is exercised by the `alembic upgrade head` integration test that every service suite runs against
an empty database.

## Consequences

The seed data is now baked into migration history rather than being adjustable at deploy time: to
change the seeded channels or add a third one, a new migration is required rather than an
environment variable or a config file edit. Re-running `alembic upgrade head` against a database
that already has the row is safe only because it is a fresh `create_table` + `bulk_insert` in one
revision — a later migration that needed to seed into an *existing* table would have to guard
against re-inserting rows on re-application.
