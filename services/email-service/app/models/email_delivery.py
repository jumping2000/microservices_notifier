from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from app.models.base import Base
from notification_shared.models import TimestampMixin
from sqlalchemy import DateTime, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column


class DeliveryStatus(StrEnum):
    SENDING = "SENDING"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"


class EmailDelivery(Base, TimestampMixin):
    __tablename__ = "email_delivery"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    notification_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    recipient: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("notification_id", name="uq_email_delivery_notification_id"),
    )
