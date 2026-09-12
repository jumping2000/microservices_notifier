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
