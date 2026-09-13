import httpx
import pytest
from alembic import command
from alembic.config import Config
from app.core.config import Settings
from app.core.database import Database
from app.main import create_app
from app.models.base import Base
from app.models.channel import ChannelConfig
from sqlalchemy import select

pytestmark = pytest.mark.integration

ALEMBIC_DIR = "services/configuration-service"


def _settings(url: str) -> Settings:
    return Settings(service_name="configuration-service", database_url=url)


@pytest.fixture
async def sessions(make_schema):
    return await make_schema(Base.metadata)


@pytest.fixture
async def client(postgres_url, sessions):
    """Build the app without running lifespan, injecting the test database.

    `get_db` reads `request.app.state.db`, so setting state directly is enough.
    The real lifespan path is covered by the end-to-end tests in Task 16.
    """
    settings = _settings(postgres_url)
    app = create_app(settings)
    app.state.settings = settings
    app.state.db = Database(postgres_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http_client:
        yield http_client
    await app.state.db.dispose()


async def _seed(sessions) -> None:
    async with sessions() as session:
        session.add_all(
            [
                ChannelConfig(name="email", enabled=True),
                ChannelConfig(name="telegram", enabled=True),
            ]
        )
        await session.commit()


async def test_list_channels_returns_the_seeded_rows(client, sessions):
    await _seed(sessions)
    response = await client.get("/channels")
    assert response.status_code == 200
    assert sorted(response.json(), key=lambda c: c["name"]) == [
        {"name": "email", "enabled": True},
        {"name": "telegram", "enabled": True},
    ]


async def test_get_one_channel(client, sessions):
    await _seed(sessions)
    response = await client.get("/channels/email")
    assert response.status_code == 200
    assert response.json() == {"name": "email", "enabled": True}


async def test_an_unknown_channel_returns_404_with_the_common_error_model(client, sessions):
    await _seed(sessions)
    response = await client.get("/channels/sms")
    assert response.status_code == 404
    assert response.json() == {"error": {"code": "NOT_FOUND", "message": "channel 'sms' not found"}}


async def test_put_disables_a_channel_and_the_change_is_readable(client, sessions):
    await _seed(sessions)
    response = await client.put("/channels/telegram", json={"enabled": False})
    assert response.status_code == 200
    assert response.json() == {"name": "telegram", "enabled": False}
    assert (await client.get("/channels/telegram")).json()["enabled"] is False


async def test_put_on_an_unknown_channel_returns_404(client, sessions):
    await _seed(sessions)
    assert (await client.put("/channels/sms", json={"enabled": False})).status_code == 404


async def test_put_rejects_a_body_without_enabled(client, sessions):
    await _seed(sessions)
    assert (await client.put("/channels/email", json={})).status_code == 422


async def test_health_reports_the_database_up(client, sessions):
    response = await client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "UP"
    assert body["service"] == "configuration-service"
    assert body["checks"] == {"database": "UP"}


async def test_health_has_no_redis_check(client, sessions):
    assert "redis" not in (await client.get("/health")).json()["checks"]


async def test_version_reports_name_and_version(client, sessions):
    assert (await client.get("/version")).json() == {
        "service": "configuration-service",
        "version": "1.0.0",
    }


async def test_the_correlation_id_header_is_echoed(client, sessions):
    response = await client.get("/health", headers={"X-Correlation-ID": "corr-cfg"})
    assert response.headers["X-Correlation-ID"] == "corr-cfg"


def test_alembic_upgrade_head_creates_and_seeds_channels(postgres_url):
    """Synchronous on purpose: alembic's env.py calls asyncio.run internally,
    which cannot be nested inside a running event loop."""
    config = Config(f"{ALEMBIC_DIR}/alembic.ini")
    config.set_main_option("script_location", f"{ALEMBIC_DIR}/alembic")
    config.set_main_option("sqlalchemy.url", postgres_url)
    command.upgrade(config, "head")
    try:
        import asyncio

        from sqlalchemy.ext.asyncio import create_async_engine

        async def read_seeds():
            engine = create_async_engine(postgres_url)
            async with engine.connect() as conn:
                rows = (await conn.execute(select(ChannelConfig.name, ChannelConfig.enabled))).all()
            await engine.dispose()
            return sorted(rows)

        assert asyncio.run(read_seeds()) == [("email", True), ("telegram", True)]
    finally:
        command.downgrade(config, "base")
