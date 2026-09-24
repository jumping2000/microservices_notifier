"""Common error vocabulary.

`NotFoundError` and `ServiceUnavailableError` are separate types on purpose:
Routing Service treats a missing channel as a permanent routing failure and a
Configuration Service outage as transient. See spec correction 3.18.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel


class ErrorCode(StrEnum):
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    CHANNEL_DISABLED = "CHANNEL_DISABLED"
    ROUTING_UNAVAILABLE = "ROUTING_UNAVAILABLE"
    CONFIGURATION_UNAVAILABLE = "CONFIGURATION_UNAVAILABLE"
    DELIVERY_UNAVAILABLE = "DELIVERY_UNAVAILABLE"
    STREAM_ERROR = "STREAM_ERROR"
    GATEWAY_TIMEOUT = "GATEWAY_TIMEOUT"
    BAD_GATEWAY = "BAD_GATEWAY"


class ErrorDetail(BaseModel):
    code: ErrorCode
    message: str


class ErrorResponse(BaseModel):
    error: ErrorDetail


class ServiceError(Exception):
    """Base for errors that map onto the common HTTP error model."""

    status_code: int = 500

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def to_response(self) -> ErrorResponse:
        return ErrorResponse(error=ErrorDetail(code=self.code, message=self.message))


class ValidationFailedError(ServiceError):
    status_code = 422

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.VALIDATION_ERROR, message)


class NotFoundError(ServiceError):
    status_code = 404

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.NOT_FOUND, message)


class ServiceUnavailableError(ServiceError):
    status_code = 503

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.CONFIGURATION_UNAVAILABLE,
    ) -> None:
        super().__init__(code, message)


class GatewayTimeoutError(ServiceError):
    status_code = 504

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.GATEWAY_TIMEOUT, message)


class BadGatewayError(ServiceError):
    """A downstream service could not be reached at all. ADR 0027."""

    status_code = 502

    def __init__(self, message: str) -> None:
        super().__init__(ErrorCode.BAD_GATEWAY, message)
