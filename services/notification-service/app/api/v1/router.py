from app.api.v1.endpoints import notifications, system
from fastapi import APIRouter

router = APIRouter()
router.include_router(notifications.router)
router.include_router(system.router)
