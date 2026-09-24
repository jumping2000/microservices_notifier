from enum import StrEnum
from uuid import UUID, uuid4

from app.models.base import Base
from notification_shared.models import TimestampMixin
from sqlalchemy import Index, String, Text
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column


class NotificationStatus(StrEnum):
    CREATED = "CREATED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# fail_reason written by the stale-processing watchdog. ADR 0028.
PROCESSING_TIMEOUT = "processing_timeout"


class Notification(Base, TimestampMixin):
    __tablename__ = "notifications"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)
    subject: Mapped[str | None] = mapped_column(String(255), nullable=True)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)

    __table_args__ = (Index("ix_notifications_status_created", "status", "created_at"),)
