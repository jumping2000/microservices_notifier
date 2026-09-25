"""Simulated load test engine for the notification platform.

No Streamlit import: pages/2_Load_test.py drives it, and the tests drive it
through httpx.MockTransport. Recipients are generated here and are always
reserved (slice 2 spec section 3), so a load test cannot send a real email or
Telegram message even on a stack with real credentials.
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

import httpx
from notification_shared.delivery import SIMULATED_FAILURE, is_reserved_recipient

TERMINAL = frozenset({"COMPLETED", "FAILED"})
MAX_TOTAL = 2000
MAX_CONCURRENCY = 50
UNSETTLED = "UNSETTLED"
NOTIFICATIONS_PATH = "/api/v1/notifications"

ProgressCallback = Callable[[int, int, str], None]


@dataclass(frozen=True)
class LoadTestConfig:
    total: int
    concurrency: int = 10
    telegram_ratio: float = 0.0
    fail_ratio: float = 0.0
    settle_timeout_s: float = 120.0
    base_url: str = "http://localhost:8000"
    poll_interval_s: float = 0.25
    seed: int = 0

    def __post_init__(self) -> None:
        if not 1 <= self.total <= MAX_TOTAL:
            raise ValueError(f"total must be between 1 and {MAX_TOTAL}")
        if not 1 <= self.concurrency <= MAX_CONCURRENCY:
            raise ValueError(f"concurrency must be between 1 and {MAX_CONCURRENCY}")
        for name in ("telegram_ratio", "fail_ratio"):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")
        if self.settle_timeout_s <= 0:
            raise ValueError("settle_timeout_s must be positive")


@dataclass(frozen=True)
class PlannedNotification:
    index: int
    channel: str
    recipient: str
    expect_failure: bool

    @property
    def expected(self) -> str:
        return f"FAILED/{SIMULATED_FAILURE}" if self.expect_failure else "COMPLETED"


def plan_notifications(config: LoadTestConfig) -> list[PlannedNotification]:
    rng = random.Random(config.seed)
    indices = range(config.total)
    telegram = set(rng.sample(indices, round(config.total * config.telegram_ratio)))
    failing = set(rng.sample(indices, round(config.total * config.fail_ratio)))
    planned = []
    for i in indices:
        channel = "telegram" if i in telegram else "email"
        suffix = "-fail" if i in failing else ""
        recipient = (
            f"sim-load-{i}{suffix}" if channel == "telegram" else f"load-{i}{suffix}@example.com"
        )
        if not is_reserved_recipient(channel, recipient):
            # Cannot happen with the patterns above; refuse rather than risk a real send.
            raise RuntimeError(f"generated recipient {recipient!r} is not reserved")
        planned.append(PlannedNotification(i, channel, recipient, bool(suffix)))
    return planned


@dataclass
class Outcome:
    planned: PlannedNotification
    submit_started: float = 0.0
    submit_latency_s: float | None = None
    submit_status: str | None = None
    notification_id: str | None = None
    settled_at: float | None = None
    final_status: str | None = None
    fail_reason: str | None = None

    @property
    def accepted(self) -> bool:
        return self.notification_id is not None

    @property
    def result(self) -> str:
        if self.final_status is None:
            return UNSETTLED
        if self.final_status == "FAILED":
            return f"FAILED/{self.fail_reason}"
        return self.final_status

    @property
    def e2e_latency_s(self) -> float | None:
        return None if self.settled_at is None else self.settled_at - self.submit_started


@dataclass(frozen=True)
class Percentiles:
    p50: float | None
    p95: float | None
    max: float | None


def percentiles(values: list[float]) -> Percentiles:
    """Nearest-rank percentiles."""
    if not values:
        return Percentiles(None, None, None)
    ordered = sorted(values)

    def rank(p: float) -> float:
        return ordered[max(0, math.ceil(p * len(ordered)) - 1)]

    return Percentiles(rank(0.50), rank(0.95), ordered[-1])


@dataclass(frozen=True)
class LoadTestReport:
    total: int
    accepted: int
    submit_errors: dict[str, int]
    accepted_per_s: float
    submit_latency: Percentiles
    e2e_latency: Percentiles
    results: dict[str, int]
    matched: int
    wrong_state: int
    unsettled: int
    completions_per_second: list[tuple[int, int]]

    @property
    def verdict_ok(self) -> bool:
        """Every notification accepted and ended in the state its recipient implies."""
        return self.matched == self.total


def build_report(
    outcomes: list[Outcome], run_started: float, submit_finished: float
) -> LoadTestReport:
    accepted = [o for o in outcomes if o.accepted]
    results = Counter(o.result for o in accepted)
    matched = sum(1 for o in accepted if o.result == o.planned.expected)
    unsettled = results.get(UNSETTLED, 0)
    buckets = Counter(int(o.settled_at - run_started) for o in accepted if o.settled_at is not None)
    return LoadTestReport(
        total=len(outcomes),
        accepted=len(accepted),
        submit_errors=dict(Counter(o.submit_status for o in outcomes if not o.accepted)),
        accepted_per_s=len(accepted) / max(submit_finished - run_started, 1e-9),
        submit_latency=percentiles(
            [o.submit_latency_s for o in outcomes if o.submit_latency_s is not None]
        ),
        e2e_latency=percentiles([o.e2e_latency_s for o in accepted if o.e2e_latency_s is not None]),
        results=dict(results),
        matched=matched,
        wrong_state=len(accepted) - matched - unsettled,
        unsettled=unsettled,
        completions_per_second=sorted(buckets.items()),
    )


async def run_load_test(
    config: LoadTestConfig,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    progress: ProgressCallback | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> LoadTestReport:
    outcomes = [Outcome(planned) for planned in plan_notifications(config)]
    notify = progress or (lambda _done, _total, _phase: None)
    semaphore = asyncio.Semaphore(config.concurrency)

    async with httpx.AsyncClient(
        base_url=config.base_url, timeout=10.0, transport=transport
    ) as client:
        run_started = clock()
        await _submit_all(client, outcomes, semaphore, notify, clock)
        submit_finished = clock()
        await _settle_all(
            client,
            [o for o in outcomes if o.accepted],
            semaphore,
            notify,
            clock,
            deadline=clock() + config.settle_timeout_s,
            poll_interval_s=config.poll_interval_s,
        )
    return build_report(outcomes, run_started, submit_finished)


async def _submit_all(client, outcomes, semaphore, notify, clock) -> None:
    done = 0

    async def submit(outcome: Outcome) -> None:
        nonlocal done
        planned = outcome.planned
        payload = {
            "channel": planned.channel,
            "recipient": planned.recipient,
            "subject": "Load test",
            "body": f"Load test message {planned.index}",
        }
        async with semaphore:
            outcome.submit_started = clock()
            try:
                response = await client.post(NOTIFICATIONS_PATH, json=payload)
            except httpx.HTTPError as exc:
                outcome.submit_status = type(exc).__name__
            else:
                outcome.submit_status = str(response.status_code)
                if response.status_code == 202:
                    outcome.notification_id = response.json()["notification_id"]
            outcome.submit_latency_s = clock() - outcome.submit_started
        done += 1
        notify(done, len(outcomes), "submit")

    await asyncio.gather(*(submit(o) for o in outcomes))


async def _settle_all(
    client, accepted, semaphore, notify, clock, *, deadline, poll_interval_s
) -> None:
    done = 0

    async def settle(outcome: Outcome) -> None:
        nonlocal done
        while clock() < deadline:
            # The semaphore bounds concurrent requests, not waiting: holding it
            # across the sleep would serialise polling and inflate latencies.
            async with semaphore:
                try:
                    response = await client.get(f"{NOTIFICATIONS_PATH}/{outcome.notification_id}")
                    body = response.json() if response.status_code == 200 else None
                except httpx.HTTPError:
                    body = None
            if body and body.get("status") in TERMINAL:
                outcome.settled_at = clock()
                outcome.final_status = body["status"]
                outcome.fail_reason = body.get("fail_reason")
                break
            await asyncio.sleep(poll_interval_s)
        done += 1
        notify(done, len(accepted), "settle")

    await asyncio.gather(*(settle(o) for o in accepted))
