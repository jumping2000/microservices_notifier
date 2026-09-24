from notification_shared.config import ConsumerServiceSettings


class Settings(ConsumerServiceSettings):
    service_name: str = "notification-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    consumer_poll_interval_ms: int = 500
    processing_timeout_minutes: int = 5
    watchdog_interval_seconds: int = 60
