"""Manual test console for the notification platform.

Console page: service health and delivery modes, channel toggles, a send form
that asks for confirmation before a real send, one-click saga scenarios, a
live timeline and the notification table. The Load test page is in pages/.

    docker compose up --build -d --wait
    uv run --group playground streamlit run tools/playground/app.py
"""

from __future__ import annotations

import os
import time

import httpx
import streamlit as st
from notification_shared.delivery import is_failure_recipient, is_reserved_recipient

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")
API_URL = f"{GATEWAY_URL}/api/v1"
# Same variables as tests/e2e/conftest.py. Health and /version stay per
# service: the Gateway reports only its own health (spec 5.2).
SERVICES = {
    "notification": os.environ.get("NOTIFICATION_URL", "http://localhost:8001"),
    "routing": os.environ.get("ROUTING_URL", "http://localhost:8002"),
    "configuration": os.environ.get("CONFIGURATION_URL", "http://localhost:8003"),
    "email": os.environ.get("EMAIL_URL", "http://localhost:8004"),
    "telegram": os.environ.get("TELEGRAM_URL", "http://localhost:8005"),
}
DELIVERY_SERVICES = ("email", "telegram")
TERMINAL = {"COMPLETED", "FAILED"}
SETTLE_TIMEOUT_S = 30.0
START_HINT = "Is the stack up? Run `docker compose up --build -d --wait`."
RESERVED_HINT = (
    "Reserved recipients are always simulated: @example.com / .org / .net, any .test, "
    ".invalid or .example domain, and Telegram chat ids starting with `sim-`. "
    "A recipient containing `fail` fails by design and sends nothing."
)

# Every scenario recipient is reserved, so scenarios stay simulated even when
# real credentials are configured (spec section 3).
SCENARIOS = {
    "Email: happy path": ("email", "john@example.com", False),
    "Email: channel disabled": ("email", "john@example.com", True),
    "Email: delivery failure": ("email", "fail@example.com", False),
    "Telegram: happy path": ("telegram", "sim-playground", False),
    "Telegram: delivery failure": ("telegram", "sim-playground-fail", False),
}


@st.cache_resource
def client() -> httpx.Client:
    return httpx.Client(timeout=5.0)


def set_channel(name: str, enabled: bool) -> None:
    client().put(f"{API_URL}/channels/{name}", json={"enabled": enabled})


def on_toggle(name: str) -> None:
    set_channel(name, st.session_state[f"channel_{name}"])


def delivery_modes() -> dict[str, str]:
    modes = {}
    for name in DELIVERY_SERVICES:
        try:
            version = client().get(f"{SERVICES[name]}/version").json()
            modes[name] = version.get("delivery_mode", "unknown")
        except httpx.HTTPError, ValueError:
            modes[name] = "unknown"
    return modes


def is_real_send(channel: str, recipient: str, modes: dict[str, str]) -> bool:
    """True when this send would leave the platform. An unknown mode counts as real."""
    if is_failure_recipient(recipient) or is_reserved_recipient(channel, recipient):
        return False
    return modes.get(channel, "unknown") != "simulated"


def submit(channel: str, recipient: str, subject: str, body: str, disable: bool) -> None:
    if disable:
        set_channel(channel, False)
    payload = {"channel": channel, "recipient": recipient, "body": body}
    if subject:
        payload["subject"] = subject
    response = client().post(f"{API_URL}/notifications", json=payload)
    response.raise_for_status()
    st.session_state.tracked = {
        "id": response.json()["notification_id"],
        "started": time.monotonic(),
        "observed": [],
        # Re-enabled only once the notification settles: routing reads the
        # channel asynchronously, so re-enabling right after the POST could
        # flip the outcome.
        "reenable": channel if disable else None,
        "done": False,
    }


def sidebar(modes: dict[str, str]) -> None:
    st.sidebar.header("Services")
    try:
        gateway = client().get(f"{API_URL}/health").json()
        icon = "🟢" if gateway.get("status") == "UP" else "🔴"
        st.sidebar.write(f"{icon} **gateway** ({GATEWAY_URL})")
    except httpx.HTTPError, ValueError:
        st.sidebar.write(f"🔴 **gateway** unreachable ({GATEWAY_URL})")
    for name, url in SERVICES.items():
        try:
            health = client().get(f"{url}/health").json()
            version = client().get(f"{url}/version").json()["version"]
            icon = "🟢" if health["status"] == "UP" else "🔴"
            st.sidebar.write(f"{icon} **{name}** v{version} · {health.get('checks', {})}")
        except httpx.HTTPError, ValueError, KeyError:
            st.sidebar.write(f"🔴 **{name}** unreachable ({url})")

    st.sidebar.header("Delivery mode")
    for name, mode in modes.items():
        badge = "🟢 SIMULATED" if mode == "simulated" else f"🟠 REAL ({mode})"
        st.sidebar.write(f"**{name}**: {badge}")
    st.sidebar.caption(RESERVED_HINT)

    st.sidebar.header("Channels")
    try:
        channels = client().get(f"{API_URL}/channels").json()
    except httpx.HTTPError, ValueError:
        st.sidebar.error(f"Channels unavailable. {START_HINT}")
        return
    for channel in channels:
        key = f"channel_{channel['name']}"
        # Server state wins on every full rerun, so a scenario that disabled a
        # channel is reflected here.
        st.session_state[key] = channel["enabled"]
        st.sidebar.toggle(channel["name"], key=key, on_change=on_toggle, args=(channel["name"],))


def send_panel(modes: dict[str, str]) -> None:
    st.subheader("Send a notification")
    with st.form("send"):
        channel = st.selectbox("Channel", ["email", "telegram"])
        recipient = st.text_input(
            "Recipient (email address or Telegram chat id)", "john@example.com"
        )
        subject = st.text_input("Subject", "Welcome")
        body = st.text_area("Body", "Hello John!")
        confirmed = st.checkbox("I understand this may send a real message")
        sent = st.form_submit_button("Send")
    st.caption(RESERVED_HINT)

    if sent:
        if is_failure_recipient(recipient):
            st.info(f"`{recipient}` contains `fail`: it ends FAILED / simulated_failure by design.")
        if is_real_send(channel, recipient, modes) and not confirmed:
            st.warning(
                f"{channel} is configured for REAL delivery ({modes.get(channel)}) and "
                f"`{recipient}` is not a reserved recipient. Tick the confirmation and send again."
            )
        else:
            try:
                submit(channel, recipient, subject, body, disable=False)
            except httpx.HTTPError as exc:
                st.error(f"Send failed: {exc}. {START_HINT}")

    st.write("Or run a scenario (always simulated):")
    columns = st.columns(len(SCENARIOS))
    scenario_columns = zip(columns, SCENARIOS.items(), strict=True)
    for column, (name, (sc_channel, sc_recipient, disable)) in scenario_columns:
        if column.button(name, width="stretch"):
            try:
                submit(sc_channel, sc_recipient, "Welcome", "Hello!", disable)
            except httpx.HTTPError as exc:
                st.error(f"Send failed: {exc}. {START_HINT}")


@st.fragment(run_every=0.5)
def timeline() -> None:
    st.subheader("Saga timeline")
    tracked = st.session_state.get("tracked")
    if tracked is None:
        st.caption("Send a notification to follow its status here.")
        return

    if not tracked["done"]:
        elapsed = time.monotonic() - tracked["started"]
        try:
            body = client().get(f"{API_URL}/notifications/{tracked['id']}").json()
        except (httpx.HTTPError, ValueError) as exc:
            st.error(f"Polling failed: {exc}. {START_HINT}")
            return
        observed = tracked["observed"]
        if not observed or observed[-1]["status"] != body["status"]:
            observed.append(
                {"status": body["status"], "at": f"{elapsed:.2f}s", "reason": body["fail_reason"]}
            )
        if body["status"] in TERMINAL or elapsed > SETTLE_TIMEOUT_S:
            tracked["done"] = True
            if tracked["reenable"]:
                set_channel(tracked["reenable"], True)
            # Full rerun so the sidebar and the table pick up the final state.
            st.rerun()

    st.write(f"`{tracked['id']}`")
    st.table(tracked["observed"])
    last = tracked["observed"][-1]["status"] if tracked["observed"] else None
    if tracked["done"] and last not in TERMINAL:
        st.warning(
            f"Still {last} after {SETTLE_TIMEOUT_S:.0f}s. Recovery retries after "
            "PENDING_TIMEOUT_MS (30s); the watchdog fails PROCESSING after 5 minutes."
        )
    elif not tracked["done"]:
        st.caption("Polling...")


@st.fragment(run_every=2)
def notifications_table() -> None:
    st.subheader("Notifications")
    left, right = st.columns(2)
    status = left.selectbox("Status", ["", "CREATED", "PROCESSING", "COMPLETED", "FAILED"])
    channel = right.selectbox("Channel filter", ["", "email", "telegram"])
    params = {"limit": 50}
    if status:
        params["status"] = status
    if channel:
        params["channel"] = channel
    try:
        rows = client().get(f"{API_URL}/notifications", params=params).json()
    except (httpx.HTTPError, ValueError) as exc:
        st.error(f"Listing failed: {exc}. {START_HINT}")
        return
    st.dataframe(rows, width="stretch", hide_index=True)


st.set_page_config(page_title="Notification Playground", layout="wide")
st.title("Notification Playground")
current_modes = delivery_modes()
sidebar(current_modes)
send_panel(current_modes)
timeline()
notifications_table()
