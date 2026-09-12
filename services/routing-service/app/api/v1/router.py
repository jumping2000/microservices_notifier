from app.api.v1.endpoints import system
from fastapi import APIRouter

router = APIRouter()
router.include_router(system.router)
