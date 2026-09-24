import pytest
from notification_shared.exceptions import (
    ErrorCode,
    ErrorResponse,
    NotFoundError,
    ServiceError,
    ServiceUnavailableError,
    ValidationFailedError,
)
from pydantic import ValidationError


def test_service_error_renders_the_common_error_model():
    error = ServiceError(ErrorCode.CHANNEL_DISABLED, "Channel telegram is currently disabled")
    assert error.to_response().model_dump(mode="json") == {
        "error": {
            "code": "CHANNEL_DISABLED",
            "message": "Channel telegram is currently disabled",
        }
    }


def test_not_found_and_unavailable_are_sibling_types():
    assert issubclass(NotFoundError, ServiceError)
    assert issubclass(ServiceUnavailableError, ServiceError)
    assert not issubclass(NotFoundError, ServiceUnavailableError)
    assert not issubclass(ServiceUnavailableError, NotFoundError)


def test_each_subclass_carries_its_status_and_code():
    assert NotFoundError("channel 'sms' not found").status_code == 404
    assert NotFoundError("x").code is ErrorCode.NOT_FOUND
    assert ServiceUnavailableError("configuration-service timed out").status_code == 503
    assert ValidationFailedError("body must not be empty").status_code == 422
    assert ValidationFailedError("x").code is ErrorCode.VALIDATION_ERROR


def test_error_response_rejects_a_bare_string():
    with pytest.raises(ValidationError):
        ErrorResponse(error="oops")


def test_every_spec_error_code_exists():
    for name in (
        "VALIDATION_ERROR",
        "NOT_FOUND",
        "CHANNEL_DISABLED",
        "ROUTING_UNAVAILABLE",
        "CONFIGURATION_UNAVAILABLE",
        "DELIVERY_UNAVAILABLE",
        "STREAM_ERROR",
    ):
        assert ErrorCode[name].value == name


def test_gateway_errors_carry_their_status_and_code():
    from notification_shared.exceptions import BadGatewayError, GatewayTimeoutError

    timeout = GatewayTimeoutError("GET /notifications timed out")
    assert timeout.status_code == 504
    assert timeout.to_response().model_dump(mode="json") == {
        "error": {"code": "GATEWAY_TIMEOUT", "message": "GET /notifications timed out"}
    }

    bad = BadGatewayError("downstream unreachable")
    assert bad.status_code == 502
    assert bad.to_response().error.code == ErrorCode.BAD_GATEWAY
    assert issubclass(GatewayTimeoutError, ServiceError)
    assert issubclass(BadGatewayError, ServiceError)
