from uuid import UUID

from app.models.telegram_delivery import TelegramDelivery
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


class TelegramDeliveryRepository:
    async def add(self, session: AsyncSession, delivery: TelegramDelivery) -> None:
        session.add(delivery)

    async def get_by_notification(
        self, session: AsyncSession, notification_id: UUID
    ) -> TelegramDelivery | None:
        return await session.scalar(
            select(TelegramDelivery).where(TelegramDelivery.notification_id == notification_id)
        )
