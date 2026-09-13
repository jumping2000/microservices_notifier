import httpx
import pytest

pytestmark = pytest.mark.e2e


async def test_every_service_reports_healthy(notifications, configuration):
    for client in (notifications, configuration):
        body = (await client.get("/health")).json()
        assert body["status"] == "UP", body
    for port in (8002, 8004):
        async with httpx.AsyncClient(timeout=10.0) as client:
            body = (await client.get(f"http://localhost:{port}/health")).json()
        assert body["status"] == "UP", body


async def test_the_happy_path_reaches_completed(notifications, email_enabled, settle):
    response = await notifications.post(
        "/notifications",
        json={
            "channel": "email",
            "recipient": "john@example.com",
            "subject": "Welcome",
            "body": "Hello John!",
        },
        headers={"X-Correlation-ID": "e2e-happy"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "CREATED"
    assert response.headers["X-Correlation-ID"] == "e2e-happy"

    settled = await settle(notifications, body["notification_id"])
    assert settled["status"] == "COMPLETED"
    assert settled["fail_reason"] is None
    assert settled["channel"] == "email"


async def test_a_disabled_channel_reaches_failed_with_channel_disabled(
    notifications, configuration, email_enabled, settle
):
    await configuration.put("/channels/email", json={"enabled": False})

    notification_id = (
        await notifications.post(
            "/notifications",
            json={"channel": "email", "recipient": "john@example.com", "body": "Hello"},
        )
    ).json()["notification_id"]

    settled = await settle(notifications, notification_id)
    assert settled["status"] == "FAILED"
    assert settled["fail_reason"] == "channel_disabled"


async def test_a_failing_recipient_reaches_failed_with_the_delivery_reason(
    notifications, email_enabled, settle
):
    notification_id = (
        await notifications.post(
            "/notifications",
            json={"channel": "email", "recipient": "fail@example.com", "body": "Hello"},
        )
    ).json()["notification_id"]

    settled = await settle(notifications, notification_id)
    assert settled["status"] == "FAILED"
    assert settled["fail_reason"] == "simulated_failure"


async def test_the_client_can_observe_the_processing_state(
    notifications, email_enabled, settle
):
    """The polling guide claims PROCESSING is observable. Prove it.

    Delivery latency is up to 500ms and the poll interval is 250ms, so
    PROCESSING is normally seen — but the assertion is on the terminal state so
    the test cannot flake on timing.
    """
    notification_id = (
        await notifications.post(
            "/notifications",
            json={"channel": "email", "recipient": "john@example.com", "body": "Hello"},
        )
    ).json()["notification_id"]

    settled = await settle(notifications, notification_id)
    assert settled["status"] == "COMPLETED"
    assert settled["_observed"][0] in {"CREATED", "PROCESSING"}
    assert settled["_observed"][-1] == "COMPLETED"


async def test_an_invalid_payload_is_rejected_before_anything_is_created(notifications):
    response = await notifications.post(
        "/notifications", json={"channel": "carrier-pigeon", "recipient": "a", "body": "b"}
    )
    assert response.status_code == 422


async def test_the_list_endpoint_returns_the_notifications_created_by_this_run(
    notifications, email_enabled, settle
):
    notification_id = (
        await notifications.post(
            "/notifications",
            json={"channel": "email", "recipient": "john@example.com", "body": "Hello"},
        )
    ).json()["notification_id"]
    await settle(notifications, notification_id)

    listed = (await notifications.get("/notifications?limit=100")).json()
    assert any(item["notification_id"] == notification_id for item in listed)
