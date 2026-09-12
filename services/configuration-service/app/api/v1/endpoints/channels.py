from app.core.database import get_db
from app.schemas.channel import ChannelRead, ChannelUpdate
from app.services.channel import ChannelService
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(tags=["channels"])
service = ChannelService()


@router.get(
    "/channels",
    response_model=list[ChannelRead],
    summary="List channels",
    description="Every configured channel and whether it is currently enabled.",
)
async def list_channels(session: AsyncSession = Depends(get_db)) -> list[ChannelRead]:
    return await service.list_channels(session)


@router.get(
    "/channels/{name}",
    response_model=ChannelRead,
    summary="Read one channel",
    description="Read a single channel's state. Returns 404 if the channel is not configured.",
)
async def get_channel(name: str, session: AsyncSession = Depends(get_db)) -> ChannelRead:
    return await service.get_channel(session, name)


@router.put(
    "/channels/{name}",
    response_model=ChannelRead,
    summary="Enable or disable a channel",
    description=(
        "Takes effect on the next routing decision: Routing Service reads channel "
        "state per event and holds no cache."
    ),
)
async def update_channel(
    name: str, body: ChannelUpdate, session: AsyncSession = Depends(get_db)
) -> ChannelRead:
    return await service.set_enabled(session, name, body.enabled)
