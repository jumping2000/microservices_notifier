"""Base settings every service extends."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)

    service_name: str
    service_version: str = "1.0.0"
    log_level: str = "INFO"


class ConsumerServiceSettings(BaseServiceSettings):
    """Settings for services that run stream consumers and a PendingRecoverer.

    Not on BaseServiceSettings: configuration-service and the gateway have no
    consumers (slice 2 spec section 8).
    """

    pending_timeout_ms: int = 30000
    pending_max_retries: int = 3
    recovery_poll_interval_ms: int = 5000
