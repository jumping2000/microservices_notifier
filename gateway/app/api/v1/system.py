from fastapi import APIRouter
from starlette.requests import Request

router = APIRouter(prefix="/api/v1", tags=["system"])


@router.get("/health", summary="The Gateway's own health; does not aggregate services")
async def health(request: Request) -> dict:
    return {"status": "UP", "service": request.app.state.settings.service_name}
