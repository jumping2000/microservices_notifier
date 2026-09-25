from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "gateway"
    notification_service_url: str = "http://notification-service:8000"
    configuration_service_url: str = "http://configuration-service:8000"
    gateway_timeout_seconds: float = 10.0
