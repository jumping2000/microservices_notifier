from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "notification-service"
    database_url: str
    redis_url: str = "redis://redis:6379/0"
    outbox_poll_interval_ms: int = 500
    outbox_batch_size: int = 100
