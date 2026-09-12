from app.models.base import Base
from notification_shared.models import ProcessedEventMixin


class ProcessedEvent(Base, ProcessedEventMixin):
    __tablename__ = "processed_events"
