from app.api.v1 import proxy, system
from fastapi import APIRouter

router = APIRouter()
router.include_router(system.router)
router.include_router(proxy.router)
