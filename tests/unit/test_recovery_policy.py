from notification_shared.config import ConsumerServiceSettings
from notification_shared.recovery import MAX_RETRIES_EXCEEDED, should_give_up


def test_the_give_up_reason_is_the_documented_string():
    assert MAX_RETRIES_EXCEEDED == "max_retries_exceeded"


def test_should_give_up_at_the_limit_and_not_before():
    assert should_give_up(fail_count=2, max_retries=3) is False
    assert should_give_up(fail_count=3, max_retries=3) is True
    assert should_give_up(fail_count=4, max_retries=3) is True


def test_consumer_service_settings_defaults():
    settings = ConsumerServiceSettings(service_name="x", _env_file=None)
    assert settings.pending_timeout_ms == 30000
    assert settings.pending_max_retries == 3
    assert settings.recovery_poll_interval_ms == 5000
