import json
import time
from collections import Counter
from uuid import uuid4

import httpx
import pytest
from loadtest import (
    LoadTestConfig,
    percentiles,
    plan_notifications,
    run_load_test,
)
from notification_shared.delivery import is_reserved_recipient


class FakePlatform:
    """MockTransport stand-in for the Gateway, with a scripted saga."""

    def __init__(
        self,
        polls_before_terminal: int = 1,
        never_settle: bool = False,
        ignore_fail_marker: bool = False,
        submit_status=None,
    ) -> None:
        self.polls_before_terminal = polls_before_terminal
        self.never_settle = never_settle
        self.ignore_fail_marker = ignore_fail_marker
        self.submit_status = submit_status
        self.recipients: dict[str, str] = {}
        self.polls: Counter = Counter()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            body = json.loads(request.content)
            status = self.submit_status(body) if self.submit_status else 202
            if status != 202:
                return httpx.Response(status, text="boom")
            notification_id = str(uuid4())
            self.recipients[notification_id] = body["recipient"]
            return httpx.Response(
                202, json={"notification_id": notification_id, "status": "CREATED"}
            )

        notification_id = request.url.path.rsplit("/", 1)[-1]
        self.polls[notification_id] += 1
        if self.never_settle or self.polls[notification_id] <= self.polls_before_terminal:
            return httpx.Response(200, json={"status": "PROCESSING", "fail_reason": None})
        if "fail" in self.recipients[notification_id] and not self.ignore_fail_marker:
            return httpx.Response(
                200, json={"status": "FAILED", "fail_reason": "simulated_failure"}
            )
        return httpx.Response(200, json={"status": "COMPLETED", "fail_reason": None})


def _config(**overrides) -> LoadTestConfig:
    values = {
        "total": 20,
        "concurrency": 5,
        "fail_ratio": 0.25,
        "telegram_ratio": 0.5,
        "poll_interval_s": 0.01,
        "settle_timeout_s": 5.0,
    }
    values.update(overrides)
    return LoadTestConfig(**values)


def test_every_generated_recipient_is_reserved():
    planned = plan_notifications(_config(total=500, telegram_ratio=0.4, fail_ratio=0.3))
    assert all(is_reserved_recipient(p.channel, p.recipient) for p in planned)


def test_the_shares_match_the_ratios():
    planned = plan_notifications(_config(total=100, telegram_ratio=0.4, fail_ratio=0.25))
    assert sum(p.channel == "telegram" for p in planned) == 40
    assert sum(p.expect_failure for p in planned) == 25
    assert all(("fail" in p.recipient) == p.expect_failure for p in planned)


def test_the_plan_is_deterministic_for_a_seed():
    assert plan_notifications(_config(seed=7)) == plan_notifications(_config(seed=7))


@pytest.mark.parametrize(
    "overrides",
    [
        {"total": 0},
        {"total": 2001},
        {"concurrency": 0},
        {"concurrency": 51},
        {"fail_ratio": 1.5},
        {"telegram_ratio": -0.1},
        {"settle_timeout_s": 0},
    ],
)
def test_out_of_range_configs_are_rejected(overrides):
    with pytest.raises(ValueError):
        _config(**overrides)


def test_percentiles():
    result = percentiles([float(v) for v in range(1, 101)])
    assert (result.p50, result.p95, result.max) == (50.0, 95.0, 100.0)
    assert percentiles([]).p50 is None


async def test_a_clean_run_has_a_passing_verdict():
    report = await run_load_test(_config(), transport=httpx.MockTransport(FakePlatform()))

    assert report.verdict_ok
    assert (report.total, report.accepted, report.matched) == (20, 20, 20)
    assert report.results == {"COMPLETED": 15, "FAILED/simulated_failure": 5}
    assert report.wrong_state == report.unsettled == 0
    assert report.e2e_latency.p50 is not None
    assert sum(count for _, count in report.completions_per_second) == 20


async def test_a_wrong_final_state_fails_the_verdict():
    platform = FakePlatform(ignore_fail_marker=True)
    report = await run_load_test(_config(), transport=httpx.MockTransport(platform))

    assert not report.verdict_ok
    assert report.wrong_state == 5


async def test_a_5xx_during_submit_is_counted_not_raised():
    platform = FakePlatform(
        submit_status=lambda body: 500 if body["recipient"].startswith("load-1") else 202
    )
    report = await run_load_test(
        _config(telegram_ratio=0.0), transport=httpx.MockTransport(platform)
    )

    assert report.submit_errors["500"] > 0
    assert report.accepted == 20 - report.submit_errors["500"]
    assert not report.verdict_ok


async def test_an_unreachable_gateway_is_reported_not_raised():
    """Review Focus 4."""

    def down(_request):
        raise httpx.ConnectError("connection refused")

    report = await run_load_test(_config(), transport=httpx.MockTransport(down))

    assert report.submit_errors == {"ConnectError": 20}
    assert report.accepted == 0
    assert not report.verdict_ok


async def test_the_settle_timeout_bounds_the_run():
    started = time.monotonic()
    report = await run_load_test(
        _config(settle_timeout_s=0.2),
        transport=httpx.MockTransport(FakePlatform(never_settle=True)),
    )

    assert time.monotonic() - started < 3
    assert report.unsettled == 20
    assert report.results == {"UNSETTLED": 20}


async def test_progress_is_reported_for_both_phases():
    seen = []
    await run_load_test(
        _config(total=4),
        transport=httpx.MockTransport(FakePlatform()),
        progress=lambda d, t, p: seen.append((d, t, p)),
    )
    assert (4, 4, "submit") in seen
    assert (4, 4, "settle") in seen
