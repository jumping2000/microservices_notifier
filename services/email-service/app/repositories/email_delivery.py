from uuid import UUID

from app.models.email_delivery import EmailDelivery
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


class EmailDeliveryRepository:
    async def add(self, session: AsyncSession, delivery: EmailDelivery) -> None:
        session.add(delivery)

    async def get_by_notification(
        self, session: AsyncSession, notification_id: UUID
    ) -> EmailDelivery | None:
        return await session.scalar(
            select(EmailDelivery).where(EmailDelivery.notification_id == notification_id)
        )
