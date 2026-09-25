"""Simulated load test page. Every recipient is reserved: nothing real is sent."""

from __future__ import annotations

import asyncio
import os

import pandas as pd
import streamlit as st
from loadtest import MAX_CONCURRENCY, MAX_TOTAL, LoadTestConfig, LoadTestReport, run_load_test

GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://localhost:8000")
# PROCESSING_TIMEOUT_MINUTES (5) x 60: past this, the watchdog (ADR 0028) fails a
# still-queued notification with FAILED/processing_timeout.
WATCHDOG_TIMEOUT_S = 300
WATCHDOG_RESULT_PREFIX = "FAILED/processing_timeout"


def estimate_backlog_s(total: int, telegram_ratio: float, fail_ratio: float) -> float:
    """Rough per-channel backlog: (non-failing count on that channel) x the ~1s
    average simulated delivery latency, one consumer per channel."""
    telegram_count = round(total * telegram_ratio)
    email_count = total - telegram_count
    non_failing_ratio = 1.0 - fail_ratio
    return max(telegram_count, email_count) * non_failing_ratio


def render(report: LoadTestReport) -> None:
    if any(key.startswith(WATCHDOG_RESULT_PREFIX) for key in report.results):
        st.warning(
            "Some notifications show FAILED/processing_timeout: the watchdog (ADR 0028) failed "
            "them after they queued past PROCESSING_TIMEOUT_MINUTES (5 min) waiting for a "
            "consumer. They are not lost or misrouted."
        )
    if report.verdict_ok:
        st.success(f"Correct: all {report.total} notifications ended in their expected state.")
    else:
        st.error(
            f"{report.matched}/{report.total} as expected — wrong state: {report.wrong_state}, "
            f"never settled: {report.unsettled}, "
            f"rejected at submit: {report.total - report.accepted}."
        )

    cols = st.columns(4)
    cols[0].metric("Accepted", f"{report.accepted}/{report.total}")
    cols[1].metric("Accepted / s", f"{report.accepted_per_s:.1f}")
    cols[2].metric("E2E p50", _seconds(report.e2e_latency.p50))
    cols[3].metric("E2E p95", _seconds(report.e2e_latency.p95))

    st.table(
        [
            {
                "measure": "POST latency",
                "p50": _seconds(report.submit_latency.p50),
                "p95": _seconds(report.submit_latency.p95),
                "max": _seconds(report.submit_latency.max),
            },
            {
                "measure": "End-to-end latency",
                "p50": _seconds(report.e2e_latency.p50),
                "p95": _seconds(report.e2e_latency.p95),
                "max": _seconds(report.e2e_latency.max),
            },
        ]
    )
    st.write("Outcomes")
    st.table([{"outcome": key, "count": value} for key, value in sorted(report.results.items())])
    if report.submit_errors:
        st.write("Submit errors")
        st.table([{"error": key, "count": value} for key, value in report.submit_errors.items()])
    if report.completions_per_second:
        chart = pd.DataFrame(report.completions_per_second, columns=["second", "completed"])
        st.bar_chart(chart.set_index("second"))


def _seconds(value: float | None) -> str:
    return "—" if value is None else f"{value:.2f}s"


st.set_page_config(page_title="Load test", layout="wide")
st.title("Load test")
st.caption(
    "Sends notifications through the Gateway to generated reserved recipients "
    "(load-N@example.com, sim-load-N), so nothing real is ever sent, then follows each "
    "one to its final state. End-to-end latency resolution is the 250 ms poll interval. "
    "Interacting with the page during a run restarts it."
)
st.info(
    "In compose each consumer group has one consumer and simulated delivery sleeps up to 2 s, "
    "so throughput reflects those deliberate limits, not the code's ceiling. A large run needs "
    "a settle timeout of roughly (email share × total × 2 s), so unsettled entries at a short "
    'timeout mean "still queued", not "lost". Above roughly 450 notifications, the resulting '
    "backlog can exceed PROCESSING_TIMEOUT_MINUTES (5 min) and the watchdog (ADR 0028) will fail "
    "queued notifications with processing_timeout instead of completing them."
)

with st.form("loadtest"):
    total = st.number_input("Notifications", min_value=1, max_value=MAX_TOTAL, value=200)
    concurrency = st.number_input("Concurrency", min_value=1, max_value=MAX_CONCURRENCY, value=10)
    telegram_ratio = st.slider("Telegram share", 0.0, 1.0, 0.3, 0.05)
    fail_ratio = st.slider("Failing share", 0.0, 1.0, 0.1, 0.05)
    settle_timeout = st.number_input("Settle timeout (s)", min_value=10, max_value=600, value=300)
    run = st.form_submit_button("Run")

if run:
    backlog_s = estimate_backlog_s(int(total), float(telegram_ratio), float(fail_ratio))
    if backlog_s > WATCHDOG_TIMEOUT_S:
        st.warning(
            f"Estimated backlog on the busier channel is about {backlog_s:.0f}s of queued "
            "delivery, past PROCESSING_TIMEOUT_MINUTES (5 min). The watchdog (ADR 0028) will fail "
            "notifications still queued past that timeout with FAILED/processing_timeout, and the "
            "verdict below will count them as wrong state. Consider a smaller total."
        )
    config = LoadTestConfig(
        total=int(total),
        concurrency=int(concurrency),
        telegram_ratio=float(telegram_ratio),
        fail_ratio=float(fail_ratio),
        settle_timeout_s=float(settle_timeout),
        base_url=GATEWAY_URL,
    )
    bars = {
        "submit": st.progress(0.0, text="Submitting"),
        "settle": st.progress(0.0, text="Waiting for final states"),
    }

    def progress(done: int, total_: int, phase: str) -> None:
        bars[phase].progress(done / total_, text=f"{phase}: {done}/{total_}")

    st.session_state.load_report = asyncio.run(run_load_test(config, progress=progress))

if report := st.session_state.get("load_report"):
    render(report)
