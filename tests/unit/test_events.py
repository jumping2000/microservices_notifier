import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from notification_shared.events import (
    Channel,
    ConsumerGroup,
    EventEnvelope,
    EventType,
    Stream,
)
from pydantic import ValidationError


def test_new_sets_version_one_and_aware_timestamp():
    envelope = EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-1",
    )
    assert envelope.event_version == 1
    assert envelope.occurred_at.tzinfo is not None
    assert envelope.event_id is not None


def test_redis_round_trip_preserves_every_field():
    original = EventEnvelope.new(
        event_type=EventType.NOTIFICATION_ROUTED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "recipient": "a@b.com", "subject": None},
        correlation_id="corr-2",
    )
    fields = original.to_redis()
    assert set(fields) == {"envelope"}
    assert isinstance(fields["envelope"], str)
    assert EventEnvelope.from_redis(fields) == original


def test_from_redis_accepts_bytes_keys_and_values():
    original = EventEnvelope.new(
        event_type=EventType.DELIVERY_COMPLETED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-3",
    )
    raw = {b"envelope": original.to_redis()["envelope"].encode()}
    assert EventEnvelope.from_redis(raw) == original


def test_from_redis_rejects_an_entry_without_the_envelope_field():
    with pytest.raises(ValueError, match="envelope"):
        EventEnvelope.from_redis({"payload": "{}"})


def test_unknown_event_type_is_rejected():
    with pytest.raises(ValidationError):
        EventEnvelope(
            event_id=uuid4(),
            event_type="NotAnEvent",
            occurred_at=datetime.now(UTC),
            correlation_id="c",
            aggregate_id=uuid4(),
            payload={},
        )


def test_unknown_channel_is_rejected():
    with pytest.raises(ValueError):
        Channel("carrier-pigeon")


def test_stream_and_group_names_match_the_spec():
    assert Stream.NOTIFICATION_CREATED == "notification.created"
    assert Stream.NOTIFICATION_ROUTED == "notification.routed"
    assert Stream.DELIVERY_COMPLETED == "delivery.completed"
    assert Stream.DELIVERY_FAILED == "delivery.failed"
    assert ConsumerGroup.ROUTING == "routing-service"
    assert ConsumerGroup.ROUTING_RESULTS == "routing-service-results"
    assert ConsumerGroup.EMAIL == "email-service"
    assert ConsumerGroup.NOTIFICATION_ROUTED == "notification-service-routed"
    assert ConsumerGroup.NOTIFICATION_RESULTS == "notification-service-results"


def test_nested_payload_values_survive_serialization():
    envelope = EventEnvelope.new(
        event_type=EventType.DELIVERY_FAILED,
        aggregate_id=uuid4(),
        payload={"channel": "email", "reason": "simulated_failure", "attempt": 1},
        correlation_id="corr-4",
    )
    decoded = json.loads(envelope.to_redis()["envelope"])
    assert decoded["payload"]["attempt"] == 1
    assert decoded["event_version"] == 1


def test_an_envelope_cannot_be_mutated_after_construction():
    envelope = EventEnvelope.new(
        event_type=EventType.NOTIFICATION_CREATED,
        aggregate_id=uuid4(),
        payload={"channel": "email"},
        correlation_id="corr-frozen",
    )
    with pytest.raises(ValidationError):
        envelope.event_type = EventType.DELIVERY_COMPLETED
