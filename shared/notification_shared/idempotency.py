"""Idempotency ledger access.

Both writers upsert, because `mark_processed` may find a FAILING row left by an
earlier attempt at the same event.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession


class ProcessedStatus(StrEnum):
    PROCESSED = "PROCESSED"
    FAILING = "FAILING"
    FAILED_PERMANENT = "FAILED_PERMANENT"


_TERMINAL = (ProcessedStatus.PROCESSED.value, ProcessedStatus.FAILED_PERMANENT.value)


class IdempotencyRepository:
    def __init__(self, model: type[Any]) -> None:
        self.model = model

    async def is_processed(
        self, session: AsyncSession, event_id: UUID, consumer_group: str
    ) -> bool:
        status = await session.scalar(
            select(self.model.status).where(
                self.model.event_id == event_id,
                self.model.consumer_group == str(consumer_group),
            )
        )
        return status in _TERMINAL

    async def mark_processed(
        self, session: AsyncSession, event_id: UUID, consumer_group: str
    ) -> None:
        stmt = (
            pg_insert(self.model)
            .values(
                id=uuid4(),
                event_id=event_id,
                consumer_group=str(consumer_group),
                status=ProcessedStatus.PROCESSED.value,
                fail_count=0,
            )
            .on_conflict_do_update(
                index_elements=["event_id", "consumer_group"],
                set_={"status": ProcessedStatus.PROCESSED.value},
            )
        )
        await session.execute(stmt)

    async def increment_fail_count(
        self, session: AsyncSession, event_id: UUID, consumer_group: str
    ) -> int:
        stmt = (
            pg_insert(self.model)
            .values(
                id=uuid4(),
                event_id=event_id,
                consumer_group=str(consumer_group),
                status=ProcessedStatus.FAILING.value,
                fail_count=1,
            )
            .on_conflict_do_update(
                index_elements=["event_id", "consumer_group"],
                set_={
                    "fail_count": self.model.fail_count + 1,
                    "status": ProcessedStatus.FAILING.value,
                },
            )
            .returning(self.model.fail_count)
        )
        return await session.scalar(stmt)
