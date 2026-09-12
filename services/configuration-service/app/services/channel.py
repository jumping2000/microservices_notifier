from app.repositories.channel import ChannelRepository
from app.schemas.channel import ChannelRead
from notification_shared.exceptions import NotFoundError
from sqlalchemy.ext.asyncio import AsyncSession


class ChannelService:
    def __init__(self, repository: ChannelRepository | None = None) -> None:
        self._repository = repository or ChannelRepository()

    async def list_channels(self, session: AsyncSession) -> list[ChannelRead]:
        rows = await self._repository.list_all(session)
        return [ChannelRead.model_validate(row) for row in rows]

    async def get_channel(self, session: AsyncSession, name: str) -> ChannelRead:
        row = await self._repository.get(session, name)
        if row is None:
            raise NotFoundError(f"channel '{name}' not found")
        return ChannelRead.model_validate(row)

    async def set_enabled(self, session: AsyncSession, name: str, enabled: bool) -> ChannelRead:
        row = await self._repository.get(session, name)
        if row is None:
            raise NotFoundError(f"channel '{name}' not found")
        row.enabled = enabled
        await session.commit()
        return ChannelRead.model_validate(row)
