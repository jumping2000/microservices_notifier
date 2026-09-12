from collections.abc import Sequence

from app.models.channel import ChannelConfig
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


class ChannelRepository:
    async def list_all(self, session: AsyncSession) -> Sequence[ChannelConfig]:
        return (await session.scalars(select(ChannelConfig).order_by(ChannelConfig.name))).all()

    async def get(self, session: AsyncSession, name: str) -> ChannelConfig | None:
        return await session.get(ChannelConfig, name)
