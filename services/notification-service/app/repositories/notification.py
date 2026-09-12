from collections.abc import Sequence
from uuid import UUID

from app.models.notification import Notification
from sqlalchemy import select, update
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

    async def advance_status(
        self,
        session: AsyncSession,
        notification_id: UUID,
        new_status: str,
        allowed_from: Sequence[str],
        fail_reason: str | None = None,
    ) -> int:
        """Monotonic guarded transition.

        The guard lives in the WHERE clause rather than in Python because two
        independent consumer groups race here; a read-then-write in
        application code would have a window between the two. Returns the
        number of rows updated, which is 0 when the transition is illegal.
        """
        result = await session.execute(
            update(Notification)
            .where(
                Notification.id == notification_id,
                Notification.status.in_(list(allowed_from)),
            )
            .values(status=new_status, fail_reason=fail_reason)
        )
        return result.rowcount
