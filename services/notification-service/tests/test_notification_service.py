from uuid import uuid4

import pytest
from app.core.config import Settings
from app.core.database import Database
from app.main import create_app
from app.models.base import Base
from app.models.notification import Notification, NotificationStatus
from app.models.outbox import Outbox
from notification_shared.events import EventEnvelope, EventType, Stream
from notification_shared.outbox import OutboxRepository
from notification_shared.publisher import OutboxPublisher
from notification_shared.streams import RedisStreamPublisher
from sqlalchemy import func, select

pytestmark = pytest.mark.integration

VALID_BODY = {
    "channel": "email",
    "recipient": "john@example.com",
    "subject": "Welcome",
    "body": "Hello John!",
}


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
async def client(postgres_url, sessions):
    import httpx

    settings = Settings(service_name="notification-service", database_url=postgres_url)
    app = create_app(settings)
    app.state.settings = settings
    app.state.db = Database(postgres_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http_client:
        yield http_client
    await app.state.db.dispose()


async def test_post_returns_202_with_the_id_and_created_status(client):
    response = await client.post("/notifications", json=VALID_BODY)
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "CREATED"
    assert body["notification_id"]


async def test_post_writes_the_notification_and_the_outbox_row_together(client, sessions):
    notification_id = (await client.post("/notifications", json=VALID_BODY)).json()[
        "notification_id"
    ]

    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Notification)) == 1
        rows = (await session.scalars(select(Outbox))).all()
        assert len(rows) == 1
        assert rows[0].stream == "notification.created"
        assert rows[0].published is False
        envelope = EventEnvelope.model_validate(rows[0].payload)
        assert envelope.event_type is EventType.NOTIFICATION_CREATED
        assert str(envelope.aggregate_id) == notification_id
        assert envelope.payload == {
            "channel": "email",
            "recipient": "john@example.com",
            "subject": "Welcome",
            "body": "Hello John!",
        }


async def test_the_request_correlation_id_reaches_the_envelope(client, sessions):
    await client.post(
        "/notifications", json=VALID_BODY, headers={"X-Correlation-ID": "corr-write"}
    )
    async with sessions() as session:
        row = (await session.scalars(select(Outbox))).one()
        assert EventEnvelope.model_validate(row.payload).correlation_id == "corr-write"


async def test_a_new_notification_starts_in_created(client, sessions):
    notification_id = (await client.post("/notifications", json=VALID_BODY)).json()[
        "notification_id"
    ]
    async with sessions() as session:
        notification = await session.get(Notification, notification_id)
        assert notification.status == NotificationStatus.CREATED
        assert notification.fail_reason is None


@pytest.mark.parametrize(
    "payload",
    [
        {**VALID_BODY, "channel": "carrier-pigeon"},
        {**VALID_BODY, "recipient": ""},
        {**VALID_BODY, "body": ""},
        {k: v for k, v in VALID_BODY.items() if k != "recipient"},
    ],
)
async def test_invalid_payloads_are_rejected_and_write_nothing(client, sessions, payload):
    assert (await client.post("/notifications", json=payload)).status_code == 422
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Notification)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0


async def test_subject_is_optional(client):
    payload = {k: v for k, v in VALID_BODY.items() if k != "subject"}
    assert (await client.post("/notifications", json=payload)).status_code == 202


async def test_get_returns_the_read_model_including_fail_reason(client):
    notification_id = (await client.post("/notifications", json=VALID_BODY)).json()[
        "notification_id"
    ]
    body = (await client.get(f"/notifications/{notification_id}")).json()
    assert set(body) == {
        "notification_id",
        "channel",
        "status",
        "fail_reason",
        "created_at",
        "updated_at",
    }
    assert body["fail_reason"] is None
    assert body["channel"] == "email"


async def test_get_on_an_unknown_id_returns_404(client):
    response = await client.get(f"/notifications/{uuid4()}")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


async def test_list_supports_limit_offset_and_filters(client):
    await client.post("/notifications", json=VALID_BODY)
    await client.post("/notifications", json={**VALID_BODY, "channel": "telegram"})

    assert len((await client.get("/notifications")).json()) == 2
    assert len((await client.get("/notifications?limit=1")).json()) == 1
    assert len((await client.get("/notifications?channel=telegram")).json()) == 1
    assert len((await client.get("/notifications?status=COMPLETED")).json()) == 0


async def test_the_outbox_publisher_delivers_the_created_event(client, sessions, redis_client):
    await client.post("/notifications", json=VALID_BODY)

    worker = OutboxPublisher(
        session_factory=sessions,
        repository=OutboxRepository(Outbox),
        publisher=RedisStreamPublisher(redis_client),
    )
    assert await worker.publish_once() == 1
    assert await redis_client.xlen(str(Stream.NOTIFICATION_CREATED)) == 1

    entries = await redis_client.xrange(str(Stream.NOTIFICATION_CREATED))
    envelope = EventEnvelope.from_redis(entries[0][1])
    assert envelope.event_type is EventType.NOTIFICATION_CREATED


async def test_health_reports_both_database_and_redis(client):
    body = (await client.get("/health")).json()
    assert set(body["checks"]) == {"database", "redis"}
