from app.senders import delivery_mode
from fastapi import APIRouter, Response
from starlette.requests import Request

router = APIRouter(tags=["system"])


@router.get("/health", summary="Health check")
async def health(request: Request, response: Response) -> dict:
    checks = {}
    try:
        await request.app.state.db.ping()
        checks["database"] = "UP"
    except Exception:
        checks["database"] = "DOWN"
    try:
        await request.app.state.redis.ping()
        checks["redis"] = "UP"
    except Exception:
        checks["redis"] = "DOWN"
    healthy = all(value == "UP" for value in checks.values())
    if not healthy:
        response.status_code = 503
    return {
        "status": "UP" if healthy else "DOWN",
        "service": request.app.state.settings.service_name,
        "checks": checks,
    }


@router.get("/version", summary="Service version and delivery mode")
async def version(request: Request) -> dict:
    settings = request.app.state.settings
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "delivery_mode": delivery_mode(settings),
    }
