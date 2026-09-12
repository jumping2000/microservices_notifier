import json
import logging
from uuid import uuid4

from notification_shared.context import get_correlation_id, set_correlation_id
from notification_shared.logging import JSONFormatter


def _record(message: str = "hello", **extra) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )
    for key, value in extra.items():
        setattr(record, key, value)
    return record


def _format(record: logging.LogRecord) -> dict:
    return json.loads(JSONFormatter(service_name="test-service").format(record))


def test_output_is_json_with_the_required_keys():
    payload = _format(_record())
    assert payload["level"] == "INFO"
    assert payload["service"] == "test-service"
    assert payload["message"] == "hello"
    assert "timestamp" in payload


def test_correlation_id_is_read_from_the_context_var():
    set_correlation_id("corr-42")
    try:
        assert _format(_record())["correlation_id"] == "corr-42"
    finally:
        set_correlation_id(None)


def test_correlation_id_is_null_when_unset():
    set_correlation_id(None)
    assert _format(_record())["correlation_id"] is None


def test_event_fields_passed_via_extra_are_included():
    event_id = str(uuid4())
    payload = _format(_record(event_id=event_id, event_type="NotificationCreated"))
    assert payload["event_id"] == event_id
    assert payload["event_type"] == "NotificationCreated"


def test_absent_event_fields_are_omitted_rather_than_nulled():
    assert "event_id" not in _format(_record())


def test_set_and_get_correlation_id_round_trip():
    set_correlation_id("abc")
    try:
        assert get_correlation_id() == "abc"
    finally:
        set_correlation_id(None)
