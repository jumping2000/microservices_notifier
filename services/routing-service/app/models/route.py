from enum import StrEnum
from uuid import UUID, uuid4

from app.models.base import Base
from notification_shared.models import TimestampMixin
from sqlalchemy import String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column


class RouteStatus(StrEnum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Route(Base, TimestampMixin):
    __tablename__ = "routes"

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    notification_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_reason: Mapped[str | None] = mapped_column(String(100), nullable=True)

    __table_args__ = (UniqueConstraint("notification_id", name="uq_routes_notification_id"),)
