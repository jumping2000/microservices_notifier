"""Slice 2 end-to-end: the Gateway, Telegram, and recovery against the live stack.

Every recipient is reserved (spec section 3), so this suite sends nothing real
even on a stack with credentials in .env.
"""

import asyncio
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.e2e

REPO_ROOT = Path(__file__).resolve().parents[2]


def _compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=REPO_ROOT, check=True, capture_output=True)


async def _submit(gateway, **payload) -> str:
    response = await gateway.post("/notifications", json=payload)
    assert response.status_code == 202, response.text
    return response.json()["notification_id"]


async def test_the_gateway_reports_its_own_health(gateway):
    response = await gateway.get("/health")
    assert response.json() == {"status": "UP", "service": "gateway"}


async def test_the_happy_path_through_the_gateway(gateway, email_enabled, settle):
    response = await gateway.post(
        "/notifications",
        json={"channel": "email", "recipient": "john@example.com", "body": "Hello"},
        headers={"X-Correlation-ID": "e2e-gateway"},
    )
    assert response.status_code == 202
    assert response.headers["X-Correlation-ID"] == "e2e-gateway"

    settled = await settle(gateway, response.json()["notification_id"])
    assert settled["status"] == "COMPLETED"


async def test_a_disabled_channel_through_the_gateway(gateway, email_enabled, settle):
    assert (await gateway.put("/channels/email", json={"enabled": False})).status_code == 200
    notification_id = await _submit(
        gateway, channel="email", recipient="john@example.com", body="Hello"
    )

    settled = await settle(gateway, notification_id)
    assert (settled["status"], settled["fail_reason"]) == ("FAILED", "channel_disabled")


async def test_a_failing_recipient_through_the_gateway(gateway, email_enabled, settle):
    notification_id = await _submit(
        gateway, channel="email", recipient="fail@example.com", body="Hello"
    )

    settled = await settle(gateway, notification_id)
    assert (settled["status"], settled["fail_reason"]) == ("FAILED", "simulated_failure")


async def test_an_unknown_notification_is_a_404_through_the_gateway(gateway):
    response = await gateway.get("/notifications/00000000-0000-0000-0000-000000000000")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


async def test_telegram_happy_path(gateway, telegram_enabled, settle):
    notification_id = await _submit(
        gateway, channel="telegram", recipient="sim-e2e", subject="Hi", body="Hello"
    )

    settled = await settle(gateway, notification_id)
    assert settled["status"] == "COMPLETED"
    assert settled["channel"] == "telegram"


async def test_telegram_failing_chat_id(gateway, telegram_enabled, settle):
    notification_id = await _submit(
        gateway, channel="telegram", recipient="sim-e2e-fail", body="Hello"
    )

    settled = await settle(gateway, notification_id)
    assert (settled["status"], settled["fail_reason"]) == ("FAILED", "simulated_failure")


async def test_recovery_routes_a_notification_stranded_by_a_configuration_outage(
    gateway, email_enabled, settle
):
    """Spec 10.5: stop configuration-service, submit, restart, and the
    PendingRecoverer routes the stranded message after PENDING_TIMEOUT_MS."""
    _compose("stop", "configuration-service")
    try:
        notification_id = await _submit(
            gateway, channel="email", recipient="john@example.com", body="Recovery"
        )
        await asyncio.sleep(3)
        stranded = (await gateway.get(f"/notifications/{notification_id}")).json()
        assert stranded["status"] == "CREATED"
    finally:
        _compose("up", "-d", "--wait", "configuration-service")

    settled = await settle(gateway, notification_id, timeout_s=90)
    assert settled["status"] == "COMPLETED"
