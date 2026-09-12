from collections.abc import Sequence
from uuid import UUID

from app.models.notification import Notification
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


class NotificationRepository:
    async def add(self, session: AsyncSession, notification: Notification) -> None:
        session.add(notification)

    async def get(self, session: AsyncSession, notification_id: UUID) -> Notification | None:
        return await session.get(Notification, notification_id)

    async def list(
        self,
        session: AsyncSession,
        limit: int = 20,
        offset: int = 0,
        status: str | None = None,
        channel: str | None = None,
    ) -> Sequence[Notification]:
        stmt = select(Notification).order_by(Notification.created_at.desc())
        if status:
            stmt = stmt.where(Notification.status == status)
        if channel:
            stmt = stmt.where(Notification.channel == channel)
        return (await session.scalars(stmt.limit(limit).offset(offset))).all()
