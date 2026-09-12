from app.api.v1.endpoints import channels, system
from fastapi import APIRouter

router = APIRouter()
router.include_router(channels.router)
router.include_router(system.router)
