from sqlalchemy import Boolean, String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class ChannelConfig(Base):
    """Named ChannelConfig to avoid colliding with the Channel enum in tests."""

    __tablename__ = "channels"

    name: Mapped[str] = mapped_column(String(50), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
