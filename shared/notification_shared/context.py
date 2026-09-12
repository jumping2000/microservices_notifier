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
