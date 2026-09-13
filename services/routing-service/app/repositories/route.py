from collections.abc import Sequence
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
        allowed_from: Sequence[str],
        fail_reason: str | None = None,
    ) -> int:
        """Monotonic guarded transition.

        The guard lives in the WHERE clause rather than in Python because
        `set_status` today runs safely only by a topology invariant — one
        insert plus one result write per `routes` row — not by a mechanism of
        its own. Idempotency (`IdempotencyRepository`) protects against
        duplicates of the *same* event; this guard protects against ordering
        between *distinct* events, the way `NotificationRepository.
        advance_status` does for `notifications`. A read-then-write in
        application code would have a window between the two. Returns the
        number of rows updated, which is 0 when the transition is illegal.
        """
        result = await session.execute(
            update(Route)
            .where(
                Route.notification_id == notification_id,
                Route.status.in_(list(allowed_from)),
            )
            .values(status=status, fail_reason=fail_reason)
        )
        return result.rowcount

    async def get_by_notification(
        self, session: AsyncSession, notification_id: UUID
    ) -> Route | None:
        return await session.scalar(select(Route).where(Route.notification_id == notification_id))
