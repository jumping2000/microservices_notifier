from notification_shared.config import BaseServiceSettings


class Settings(BaseServiceSettings):
    service_name: str = "configuration-service"
    database_url: str
