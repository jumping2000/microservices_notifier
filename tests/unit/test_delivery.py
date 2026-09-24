import pytest
from notification_shared.delivery import (
    FAILURE_MARKER,
    SIMULATED_FAILURE,
    is_failure_recipient,
    is_reserved_recipient,
)


def test_the_constants_are_the_documented_strings():
    assert FAILURE_MARKER == "fail"
    assert SIMULATED_FAILURE == "simulated_failure"


@pytest.mark.parametrize(
    "recipient", ["fail@example.com", "FAILURE@example.com", "sim-x-fail", "a-Fail-b"]
)
def test_the_failure_marker_is_case_insensitive(recipient):
    assert is_failure_recipient(recipient) is True


def test_the_failure_marker_is_checked_on_real_looking_addresses():
    """Review Focus 1: the marker wins even on an address that looks real.

    This is slice 1 spec 3.8 applied as written. The playground warns about
    it; this test pins that the rule is not quietly narrowed to reserved
    recipients.
    """
    assert is_failure_recipient("failla@gmail.com") is True
    assert is_failure_recipient("-100fail42") is True


@pytest.mark.parametrize("recipient", ["john@example.com", "12345", "sim-ok"])
def test_recipients_without_the_marker_do_not_fail(recipient):
    assert is_failure_recipient(recipient) is False


@pytest.mark.parametrize(
    "recipient",
    [
        "john@example.com",
        "john@example.org",
        "john@example.net",
        "John@Example.COM",
        "john@example.com.",
        "x@mail.example.com",
        "x@anything.test",
        "x@anything.invalid",
        "x@host.example",
    ],
)
def test_reserved_email_recipients(recipient):
    """Review Focus 2: casing, trailing dot and subdomains still count."""
    assert is_reserved_recipient("email", recipient) is True


@pytest.mark.parametrize(
    "recipient",
    [
        "john@gmail.com",
        "x@example.com.evil.org",
        "x@notexample.com",
        "x@example.co",
        "no-at-sign-example.com",
        "x@testing.com",
    ],
)
def test_look_alike_email_recipients_are_not_reserved(recipient):
    assert is_reserved_recipient("email", recipient) is False


@pytest.mark.parametrize("chat_id", ["sim-e2e", "sim-load-7-fail", "sim-"])
def test_reserved_chat_ids(chat_id):
    assert is_reserved_recipient("telegram", chat_id) is True


@pytest.mark.parametrize("chat_id", ["123456789", "-1001234567890", "@mychannel", "SIM-x", "xsim-"])
def test_real_chat_ids_are_not_reserved(chat_id):
    assert is_reserved_recipient("telegram", chat_id) is False


def test_the_rules_are_per_channel():
    assert is_reserved_recipient("telegram", "john@example.com") is False
    assert is_reserved_recipient("email", "sim-e2e") is False
    assert is_reserved_recipient("carrier-pigeon", "john@example.com") is False
