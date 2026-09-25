from typing import Literal, Self

from notification_shared.config import ConsumerServiceSettings
from pydantic import model_validator


class Settings(ConsumerServiceSettings):
    service_name: str = "email-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    delivery_latency_ms_max: int = 500
    # Timeout for every SMTP operation (connect, and each command/response);
    # reused rather than adding a separate variable.
    http_timeout_seconds: float = 5.0
    # Empty SMTP_HOST means simulated delivery (ADR 0029).
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    smtp_from: str = ""
    smtp_security: Literal["starttls", "ssl", "none"] = "starttls"

    @model_validator(mode="after")
    def _smtp_from_is_required_with_a_host(self) -> Self:
        # Fail at startup, so a half-configured service never reaches healthy.
        if self.smtp_host and not self.smtp_from:
            raise ValueError("SMTP_FROM is required when SMTP_HOST is set")
        return self
