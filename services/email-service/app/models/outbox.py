from app.models.base import Base
from notification_shared.models import OutboxMixin


class Outbox(Base, OutboxMixin):
    __tablename__ = "outbox"
