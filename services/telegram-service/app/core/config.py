from notification_shared.config import ConsumerServiceSettings


class Settings(ConsumerServiceSettings):
    service_name: str = "telegram-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    delivery_latency_ms_max: int = 500
    http_timeout_seconds: float = 5.0
    # Empty means simulated delivery. The recipient is the chat id (ADR 0026).
    telegram_bot_token: str = ""
