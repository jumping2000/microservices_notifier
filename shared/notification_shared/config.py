"""Base settings every service extends."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str
    service_version: str = "1.0.0"
    log_level: str = "INFO"
