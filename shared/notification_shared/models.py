"""Abstract mixins for tables every event-driven service needs.

These are mixins, not concrete models, so each service materializes them on
its own DeclarativeBase. A shared Base would put every service's tables into
one registry, which breaks schema isolation in the test suite. See spec 3.12.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    DateTime,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, declared_attr, mapped_column


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )


class OutboxMixin:
    """Events awaiting publication, written in the same transaction as the state change."""

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    stream: Mapped[str] = mapped_column(String(100), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    published: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    @declared_attr.directive
    def __table_args__(cls) -> tuple:  # noqa: N805
        return (Index(f"ix_{cls.__tablename__}_pending", "published", "created_at"),)


class ProcessedEventMixin:
    """Idempotency ledger.

    `status` separates "done" from "has failed N times" — without it a single
    failed attempt makes a never-processed event look processed. See spec 3.4.
    """

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    fail_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    @declared_attr.directive
    def __table_args__(cls) -> tuple:  # noqa: N805
        return (
            UniqueConstraint(
                "event_id", "consumer_group", name=f"uq_{cls.__tablename__}_event_group"
            ),
        )
