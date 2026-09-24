"""Streamlit playground for poking the slice-1 notification platform by hand.

Bring the stack up first, then start the dashboard from the repository root:

    docker compose up --build -d --wait
    uv run --group playground streamlit run tools/playground/app.py
"""

from __future__ import annotations

import os
import time

import httpx
import streamlit as st

# Same variables as tests/e2e/conftest.py, so both point at the same stack.
NOTIFICATION_URL = os.environ.get("NOTIFICATION_URL", "http://localhost:8001")
ROUTING_URL = os.environ.get("ROUTING_URL", "http://localhost:8002")
CONFIGURATION_URL = os.environ.get("CONFIGURATION_URL", "http://localhost:8003")
EMAIL_URL = os.environ.get("EMAIL_URL", "http://localhost:8004")
SERVICES = {
    "notification": NOTIFICATION_URL,
    "routing": ROUTING_URL,
    "configuration": CONFIGURATION_URL,
    "email": EMAIL_URL,
}
TERMINAL = {"COMPLETED", "FAILED"}
SETTLE_TIMEOUT_S = 30.0
START_HINT = "Is the stack up? Run `docker compose up --build -d --wait`."

# The three saga outcomes from the README's polling guide.
SCENARIOS = {
    "Happy path": {"recipient": "john@example.com", "disable_email": False},
    "Channel disabled": {"recipient": "john@example.com", "disable_email": True},
    "Delivery failure": {"recipient": "fail@example.com", "disable_email": False},
}


@st.cache_resource
def client() -> httpx.Client:
    return httpx.Client(timeout=5.0)


def set_channel(name: str, enabled: bool) -> None:
    client().put(f"{CONFIGURATION_URL}/channels/{name}", json={"enabled": enabled})


def on_toggle(name: str) -> None:
    set_channel(name, st.session_state[f"channel_{name}"])


def submit(channel: str, recipient: str, subject: str, body: str, disable_email: bool) -> None:
    if disable_email:
        set_channel("email", False)
    payload = {"channel": channel, "recipient": recipient, "body": body}
    if subject:
        payload["subject"] = subject
    response = client().post(f"{NOTIFICATION_URL}/notifications", json=payload)
    response.raise_for_status()
    st.session_state.tracked = {
        "id": response.json()["notification_id"],
        "started": time.monotonic(),
        "observed": [],
        # Re-enabled only once the notification settles: routing reads the
        # channel asynchronously, so re-enabling right after the POST could
        # flip the outcome.
        "reenable_email": disable_email,
        "done": False,
    }


def sidebar() -> None:
    st.sidebar.header("Services")
    for name, url in SERVICES.items():
        try:
            health = client().get(f"{url}/health").json()
            version = client().get(f"{url}/version").json()["version"]
            icon = "🟢" if health["status"] == "UP" else "🔴"
            st.sidebar.write(f"{icon} **{name}** v{version} · {health.get('checks', {})}")
        except httpx.HTTPError:
            st.sidebar.write(f"🔴 **{name}** unreachable ({url})")

    st.sidebar.header("Channels")
    try:
        channels = client().get(f"{CONFIGURATION_URL}/channels").json()
    except httpx.HTTPError:
        st.sidebar.error(f"Configuration service unreachable. {START_HINT}")
        return
    for channel in channels:
        key = f"channel_{channel['name']}"
        # Server state wins on every full rerun, so a scenario that disabled
        # email is reflected here.
        st.session_state[key] = channel["enabled"]
        st.sidebar.toggle(channel["name"], key=key, on_change=on_toggle, args=(channel["name"],))


def send_panel() -> None:
    st.subheader("Send a notification")
    with st.form("send"):
        channel = st.selectbox("Channel", ["email", "telegram"])
        recipient = st.text_input("Recipient", "john@example.com")
        subject = st.text_input("Subject", "Welcome")
        body = st.text_area("Body", "Hello John!")
        sent = st.form_submit_button("Send")
    if channel == "telegram":
        st.warning("Slice 1 has no Telegram consumer: a telegram notification stays PROCESSING.")

    st.write("Or run a scenario:")
    columns = st.columns(len(SCENARIOS))
    scenario = None
    for column, name in zip(columns, SCENARIOS, strict=True):
        if column.button(name, width="stretch"):
            scenario = SCENARIOS[name]

    try:
        if sent:
            submit(channel, recipient, subject, body, disable_email=False)
        elif scenario:
            submit("email", scenario["recipient"], "Welcome", "Hello!", scenario["disable_email"])
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
            body = client().get(f"{NOTIFICATION_URL}/notifications/{tracked['id']}").json()
        except httpx.HTTPError as exc:
            st.error(f"Polling failed: {exc}. {START_HINT}")
            return
        observed = tracked["observed"]
        if not observed or observed[-1]["status"] != body["status"]:
            observed.append(
                {"status": body["status"], "at": f"{elapsed:.2f}s", "reason": body["fail_reason"]}
            )
        if body["status"] in TERMINAL or elapsed > SETTLE_TIMEOUT_S:
            tracked["done"] = True
            if tracked["reenable_email"]:
                set_channel("email", True)
            # Full rerun so the sidebar and the table pick up the final state.
            st.rerun()

    st.write(f"`{tracked['id']}`")
    st.table(tracked["observed"])
    last = tracked["observed"][-1]["status"] if tracked["observed"] else None
    if tracked["done"] and last not in TERMINAL:
        st.warning(f"Still {last} after {SETTLE_TIMEOUT_S:.0f}s: nothing moves it in slice 1.")
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
        rows = client().get(f"{NOTIFICATION_URL}/notifications", params=params).json()
    except httpx.HTTPError as exc:
        st.error(f"Listing failed: {exc}. {START_HINT}")
        return
    st.dataframe(rows, width="stretch", hide_index=True)


st.set_page_config(page_title="Notification Playground", layout="wide")
st.title("Notification Playground")
sidebar()
send_panel()
timeline()
notifications_table()
