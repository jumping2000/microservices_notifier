from fastapi import APIRouter

from app.api.v1.endpoints import channels, system

router = APIRouter()
router.include_router(channels.router)
router.include_router(system.router)
