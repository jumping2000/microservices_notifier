from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "email-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    consumer_poll_interval_ms: int = 500
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
    delivery_latency_ms_max: int = 500
