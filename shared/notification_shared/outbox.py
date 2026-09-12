"""Outbox access.

`save()` deliberately does not commit: the atomicity guarantee comes from the
row being written inside the caller's transaction, alongside the state change.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from notification_shared.events import EventEnvelope


class OutboxRepository:
    def __init__(self, model: type[Any]) -> None:
        self.model = model

    async def save(self, session: AsyncSession, stream: str, envelope: EventEnvelope) -> None:
        session.add(
            self.model(stream=str(stream), payload=envelope.model_dump(mode="json"))
        )

    async def get_pending(self, session: AsyncSession, limit: int = 100) -> Sequence[Any]:
        stmt = (
            select(self.model)
            .where(self.model.published.is_(False))
            .order_by(self.model.created_at)
            .limit(limit)
        )
        return (await session.scalars(stmt)).all()

    async def mark_published(self, session: AsyncSession, ids: Sequence[UUID]) -> None:
        if not ids:
            return
        await session.execute(
            update(self.model).where(self.model.id.in_(list(ids))).values(published=True)
        )
