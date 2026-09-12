from datetime import datetime
from uuid import UUID

from notification_shared.events import Channel
from pydantic import BaseModel, ConfigDict, Field


class NotificationCreate(BaseModel):
    channel: Channel
    recipient: str = Field(min_length=1, max_length=255)
    subject: str | None = Field(default=None, max_length=255)
    body: str = Field(min_length=1)


class NotificationAccepted(BaseModel):
    notification_id: UUID
    status: str


class NotificationRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    notification_id: UUID = Field(validation_alias="id")
    channel: str
    status: str
    fail_reason: str | None
    created_at: datetime
    updated_at: datetime
