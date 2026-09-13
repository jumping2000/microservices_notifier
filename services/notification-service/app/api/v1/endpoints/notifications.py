from uuid import UUID

from app.core.database import get_db
from app.schemas.notification import (
    NotificationAccepted,
    NotificationCreate,
    NotificationRead,
)
from app.services.notification import NotificationService
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

router = APIRouter(tags=["notifications"])
service = NotificationService()


@router.post(
    "/notifications",
    response_model=NotificationAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a notification",
    description=(
        "Accepts the notification and returns immediately. Delivery happens "
        "asynchronously; poll GET /notifications/{id} to observe the status "
        "advance from CREATED to PROCESSING to COMPLETED or FAILED."
    ),
)
async def create_notification(
    body: NotificationCreate, request: Request, session: AsyncSession = Depends(get_db)
) -> NotificationAccepted:
    notification = await service.create(session, body, request.state.correlation_id)
    return NotificationAccepted(notification_id=notification.id, status=notification.status)


@router.get(
    "/notifications/{notification_id}",
    response_model=NotificationRead,
    summary="Read notification status",
    description="Poll this until status is COMPLETED or FAILED.",
)
async def get_notification(
    notification_id: UUID, session: AsyncSession = Depends(get_db)
) -> NotificationRead:
    return await service.get(session, notification_id)


@router.get(
    "/notifications",
    response_model=list[NotificationRead],
    summary="List notifications",
)
async def list_notifications(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: str | None = None,
    channel: str | None = None,
    session: AsyncSession = Depends(get_db),
) -> list[NotificationRead]:
    return await service.list(session, limit=limit, offset=offset, status=status, channel=channel)
