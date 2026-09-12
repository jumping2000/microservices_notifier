from uuid import UUID, uuid4

from app.models.notification import Notification, NotificationStatus
from app.models.outbox import Outbox
from app.repositories.notification import NotificationRepository
from app.schemas.notification import NotificationCreate, NotificationRead
from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.exceptions import NotFoundError
from notification_shared.outbox import OutboxRepository
from sqlalchemy.ext.asyncio import AsyncSession


class NotificationService:
    def __init__(self) -> None:
        self._repository = NotificationRepository()
        self._outbox = OutboxRepository(Outbox)

    async def create(
        self, session: AsyncSession, payload: NotificationCreate, correlation_id: str
    ) -> Notification:
        """Persist the notification and its event in one transaction.

        This single commit is the Outbox Pattern: either both rows land or
        neither does, so an accepted notification can never lack its event.
        """
        notification = Notification(
            id=uuid4(),
            channel=payload.channel.value,
            recipient=payload.recipient,
            subject=payload.subject,
            body=payload.body,
            status=NotificationStatus.CREATED.value,
        )
        await self._repository.add(session, notification)

        envelope = EventEnvelope.new(
            event_type=EventType.NOTIFICATION_CREATED,
            aggregate_id=notification.id,
            payload={
                "channel": payload.channel.value,
                "recipient": payload.recipient,
                "subject": payload.subject,
                "body": payload.body,
            },
            correlation_id=correlation_id,
        )
        await self._outbox.save(session, Stream.NOTIFICATION_CREATED, envelope)
        await session.commit()
        return notification

    async def get(self, session: AsyncSession, notification_id: UUID) -> NotificationRead:
        notification = await self._repository.get(session, notification_id)
        if notification is None:
            raise NotFoundError(f"notification '{notification_id}' not found")
        return NotificationRead.model_validate(notification)

    async def list(self, session: AsyncSession, **filters) -> list[NotificationRead]:
        rows = await self._repository.list(session, **filters)
        return [NotificationRead.model_validate(row) for row in rows]
