from fastapi import APIRouter, Response
from starlette.requests import Request

router = APIRouter(tags=["system"])


@router.get("/health", summary="Health check", description="Reports Postgres connectivity.")
async def health(request: Request, response: Response) -> dict:
    try:
        await request.app.state.db.ping()
        database = "UP"
    except Exception:
        database = "DOWN"
    if database == "DOWN":
        response.status_code = 503
    return {
        "status": "UP" if database == "UP" else "DOWN",
        "service": request.app.state.settings.service_name,
        "checks": {"database": database},
    }


@router.get("/version", summary="Service version")
async def version(request: Request) -> dict:
    settings = request.app.state.settings
    return {"service": settings.service_name, "version": settings.service_version}
