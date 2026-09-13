# 0011: Python 3.14 everywhere

## Status

Accepted

## Context

An earlier design's stack section said "Python 3.14 - 3.13" while its Dockerfile specified
`python:3.13-slim` — an internal contradiction with no stated resolution.

## Decision

Python 3.14 uniformly: `requires-python = ">=3.14"` in every `pyproject.toml`, `.python-version`
containing `3.14`, and `python:3.14-slim` as the base image in every service's multi-stage
Dockerfile.

## Consequences

The platform depends on every pinned library shipping a working 3.14 wheel or sdist before this
project can build at all — `asyncpg` in particular is Cython-based and has historically lagged new
CPython releases. This was treated as a verification step before writing any service code rather
than an assumption: had no 3.14 wheel existed for the pinned `asyncpg` version, the fallback would
have been `psycopg[binary]` in async mode, changing only the `DATABASE_URL` driver portion
(`postgresql+psycopg://`) — a change this ADR records as the accepted contingency, not one that was
exercised. Pinning one Python version everywhere also means there is no gradual per-service upgrade
path: every image moves to a new Python version together.
