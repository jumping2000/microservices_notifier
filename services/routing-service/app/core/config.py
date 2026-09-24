from notification_shared.config import ConsumerServiceSettings


class Settings(ConsumerServiceSettings):
    service_name: str = "routing-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    configuration_service_url: str = "http://configuration-service:8000"
    http_timeout_seconds: float = 5.0
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
