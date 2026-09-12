"""Event vocabulary shared by every service.

Stream and consumer group names live here so a publisher and its consumers
cannot drift apart.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


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

    model_config = ConfigDict(frozen=True)

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
