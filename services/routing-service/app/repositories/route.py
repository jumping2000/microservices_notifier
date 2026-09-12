from uuid import UUID

from app.models.route import Route
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession


class RouteRepository:
    async def add(self, session: AsyncSession, route: Route) -> None:
        session.add(route)

    async def set_status(
        self,
        session: AsyncSession,
        notification_id: UUID,
        status: str,
        fail_reason: str | None = None,
    ) -> int:
        """Returns the number of rows updated. Used by Task 15."""
        result = await session.execute(
            update(Route)
            .where(Route.notification_id == notification_id)
            .values(status=status, fail_reason=fail_reason)
        )
        return result.rowcount

    async def get_by_notification(
        self, session: AsyncSession, notification_id: UUID
    ) -> Route | None:
        return await session.scalar(
            select(Route).where(Route.notification_id == notification_id)
        )
