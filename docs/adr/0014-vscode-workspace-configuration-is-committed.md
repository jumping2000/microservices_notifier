# 0014: `.vscode/` workspace configuration is committed

## Status

Accepted

## Context

Without a committed editor configuration, every contributor must independently discover the
correct interpreter path, the pytest root, the debug launch configurations for four services each
needing different environment variables, and the compose/test tasks — all of it re-derived by
trial and error rather than handed to them.

## Decision

Commit `extensions.json`, `settings.json`, `launch.json`, and `tasks.json` under `.vscode/`. See
`docs/local-development.md` for how they are used together.

## Consequences

The committed configuration is opinionated about one editor (VS Code) and one platform's path
convention: `settings.json`'s `python.defaultInterpreterPath` points at
`${workspaceFolder}/.venv/Scripts/python.exe`, the Windows virtualenv layout, because this project
is developed on Windows. A contributor on macOS or Linux must override the interpreter path
locally (their `.venv/bin/python` will not exist at the committed path), and `tasks.json`'s
"test: integration (all services)" task is POSIX shell and needs `bash.exe` on Windows. Committing
editor configuration also means it can drift from the code it configures — a renamed service or a
changed port must be updated here too, and nothing enforces that automatically.
