# 0010: uv workspace replaces `pip install -e`

## Status

Accepted

## Context

An earlier design specified `pip install -e ../../shared` to make the shared library importable by
each service. Project rules (`CLAUDE.md`) mandate `uv` exclusively and forbid `pip`, `venv`,
`virtualenv`, `conda`, and `poetry`.

## Decision

A uv workspace with a single root `uv.lock`: the root `pyproject.toml` declares
`[tool.uv.workspace] members = ["services/*", "shared"]`, and every service declares
`notification-shared` as a workspace-sourced dependency. `uv sync --all-packages` produces one root
`.venv` containing every service and the shared library.

## Consequences

Every service and the shared library must resolve against the same locked dependency versions —
there is one `uv.lock` for the whole platform, so a service cannot pin a dependency version that
conflicts with another service's without a workspace-wide resolution failure. Container builds pay
for this differently: each Dockerfile runs `uv sync --frozen --no-dev --no-editable --package
<name>` so the runtime image installs only that service's dependency subset, but the build context
must still be the repository root so `shared/` and the root `uv.lock` are reachable — a service's
Dockerfile cannot be built from within its own directory.
