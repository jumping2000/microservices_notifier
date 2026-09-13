# 0013: Git repository initialised

## Status

Accepted

## Context

The project directory was not a git repository when slice 1 began.

## Decision

A git repository was initialized for the project on 2026-09-12, before any source files were
added, with `.gitattributes` and `.gitignore` written first to normalize line endings ahead of the
first real commit.

## Consequences

All history for this platform starts from this point — there is no prior version control record
of the pre-slice-1 design work beyond the specification and prompt documents kept alongside the
code. Writing `.gitattributes` (`* text=auto eol=lf`) before any source file means every tracked
file is normalized to LF in the repository regardless of the checkout platform's default, which is
what makes the Windows-developed `.vscode/` configuration and the Linux-based Docker images agree
on line endings.
