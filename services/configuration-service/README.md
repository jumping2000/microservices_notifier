**English** | [Italiano](README.it.md)

# configuration-service

Holds runtime channel enable/disable state, read by Routing Service once per notification. It
publishes no events and runs no background workers: configuration is read-model state, not a
domain event stream.

## Tables

| Table | Purpose |
|---|---|
| `channels` | `name` (primary key), `enabled` — seeded by an Alembic data migration with `email = true`, `telegram = true` (ADR 0009). No `outbox`, no `processed_events`, no timestamps. |

## Streams consumed

None. Configuration Service does not touch Redis at all.

## Streams published

None.

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `DATABASE_URL` | — (required) | `postgresql+asyncpg://...` |
| `SERVICE_NAME` | `configuration-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |

## HTTP endpoints

| Endpoint | Notes |
|---|---|
| `GET /channels` | Every configured channel and whether it is enabled |
| `GET /channels/{name}` | `404` if the channel is not configured |
| `PUT /channels/{name}` | Body `{enabled: bool}`; takes effect on the next routing decision — Routing Service holds no local cache |
| `GET /health` | Checks Postgres only (no Redis dependency); `503` if down |
| `GET /version`, `/docs`, `/redoc` | |

## Notes on layering

`ChannelService` (`app/services/channel.py`) writes and commits directly after mutating a row it
read through `ChannelRepository`, which is read-only (`list_all`, `get`). This is the one service
in the platform where the repository layer does not also own the write — see ADR 0012's
consequences section for why this is recorded as a known inconsistency rather than presented as a
pattern.
