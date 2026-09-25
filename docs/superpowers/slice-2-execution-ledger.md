# Slice 2 — Execution Ledger

Preserved from the subagent-driven execution workspace, which is otherwise scratch. It records the pre-flight scan, every task's completion, the deferred minors, and every ruling taken during execution — decisions taken on the user's behalf that appear nowhere else.

---

# SDD ledger — plan: docs/superpowers/plans/2026-09-24-notification-platform-slice-2.md

Spec: docs/superpowers/specs/2026-09-24-notification-platform-slice-2-design.md (binding), on top of the slice 1 spec.
Workspace: git worktree C:/Users/gpamm/Documents/develop/microservices_notifier-slice2, branch slice-2 from df67f73 — user chose worktree + branch explicitly. Own .venv (avoids the VS Code lock on the main checkout's .venv).
Baseline: tests/unit 41 passed in the worktree venv.

## Pre-flight scan

### Cross-task interface pairs

| Pair | Produced | Consumed | Finding |
|---|---|---|---|
| T1 → T7,T8,T9,T12,T13 | `is_failure_recipient`, `is_reserved_recipient`, `SIMULATED_FAILURE` | email/telegram `_deliver`, loadtest planner, console `is_real_send` | agree |
| T1 → T10 | `GatewayTimeoutError`, `BadGatewayError`, ErrorCode members | gateway proxy | agree |
| T2 → T3 | `get_pending`, `claim`, `ClaimedMessage`, `mark_failed_permanent` | `PendingRecoverer` | agree |
| T3 → T4,T5,T6,T8 | `PendingRecoverer(...)`, `register`, `MAX_RETRIES_EXCEEDED`, `ConsumerServiceSettings` | four services' main.py + config + give_up | agree; kwarg names identical at every call site |
| T4 internal | `PROCESSING_TIMEOUT` in models | repository + watchdog + tests | agree |
| T6 → T7 | `_record_outcome(session, envelope, fail_reason, sent_at)` | T7 `handle` unchanged, `_deliver` replaced | agree |
| T7 → T8 | email `system.py` (after T7 it already imports `delivery_mode`) | T8 copies it | see F1 |
| T8 → T9 | `app.senders` stubs `build_sender(settings, transport=None)`, `delivery_mode` | T9 replaces both | agree |
| T8 → T10 | telegram Dockerfile (sed of email's) | T10 adds `COPY gateway/pyproject.toml` to five Dockerfiles | agree; order correct |
| T10 → T11 | gateway routes, `/api/v1/health` | e2e `gateway` fixture with base `/api/v1`, compose healthcheck | agree |
| T10 → T12,T13 | `/api/v1/notifications`, `/api/v1/channels` | loadtest `NOTIFICATIONS_PATH`, console `API_URL` | agree |
| T7,T8,T9 → T13 | `/version` `delivery_mode` | console badges | agree |
| T12 → T13 | `LoadTestConfig`, `run_load_test`, `LoadTestReport`, `MAX_TOTAL`, `MAX_CONCURRENCY` | load test page | agree |
| T1–T13 → T14 | names, files, tests | docs | Step 5 verifies against code |

### Per-task self-consistency

| Task | Tests vs code | Files created vs later touched | Finding |
|---|---|---|---|
| T1 | 9 test fns incl. look-alikes | delivery.py later only read | agree |
| T2 | claim/pending/idempotency | streams.py, idempotency.py | agree; deleted-entry shape is a flagged spec risk, plan tells the implementer what to do |
| T3 | FakeConsumer + marker table prove give_up shares the txn | — | agree |
| T4 | watchdog + no-op give_up | main.py | agree |
| T5 | give_up + full chain at service tier | notification_consumer refactor via `_record_decision` | agree |
| T6 | give_up | routed_consumer | agree |
| T7 | aiosmtpd suite + consumer suite | — | see F2 |
| T8 | mirror suite | scaffold copies | see F1 |
| T9 | MockTransport + token hygiene | — | agree |
| T10 | proxy suite | five Dockerfiles | agree |
| T11 | e2e | compose | see F4 |
| T12 | engine suite | — | agree |
| T13 | AppTest smoke | app.py rewrite | see F3 |
| T14 | docs | — | agree |
| T15 | gates | — | see F4 |

### Findings and rulings

**F1 — T8 copies email's system.py after T7 already changed its /version.** T8 then says "replace the version endpoint and add the import", which is already done by the copy.
Ruling: no change; tell the T8 implementer the copy already carries the T7 version and the import, so the step is a verification. Costs if wrong: none.

**F2 — RED capture path `/tmp/task-N-red.txt`.** On Git Bash `/tmp` is a per-user temp outside the plan workspace, invisible to the reviewer.
Ruling: every implementer writes its RED capture to `<workspace>/task-N-red.txt` instead. Costs if wrong: none.

**F3 — T13 AppTest smoke tests point every URL at 127.0.0.1:9.** On Windows a refused localhost connection can take ~2 s (SYN retries), and the console makes ~15 calls per run, so `default_timeout=30` risks a spurious timeout.
Ruling: T13 uses `default_timeout=120` in both AppTest calls. Costs if wrong: a slower failing test, nothing else.

**F4 — T15 Step 3 runs `docker compose down -v`.** The compose project name is fixed (`notification-platform`), so the worktree stack IS the user's local dev stack; `-v` wipes its Postgres and Redis volumes — an irreversible operation the gate does not need (the spec gate is "comes up healthy").
Ruling: T15 uses `docker compose up --build -d --wait` without `down -v`; volumes are preserved. If a stale-state problem appears, stop and ask rather than wipe. Costs if wrong: the gate runs on a stack with prior data; offset-0 replays are idempotent, so correctness is unaffected.

**F5 — shared compose project.** Tasks 10/11/15 rebuild images from the worktree into the same `notification-platform` project the main checkout uses. Expected and harmless (no volume removal after F4); noted so nobody is surprised the main checkout's containers change.

Scan complete: 5 findings, 5 ruled, 0 plan edits (rulings travel in dispatches).

## Task progress
Task 1: complete (commits df67f73..8ce5e8f, review clean) — reviewer ⚠️ "fail marker checked before reserved at call sites" resolved: lives in T7/T8, pinned by test_the_failure_marker_precedes_the_real_sender (T7) and the telegram rule order (T8).
Task 2: complete (commits 8ce5e8f..4c99689, review clean) — deleted-entry XCLAIM shape needed no adjustment (open risk spec 16 closed).
Task 2: minor (deferred): test output carries a uv warning — inherited VIRTUAL_ENV points at the main checkout's .venv; uv correctly ignores it. Environment noise, not code.
Task 3: complete (commits 4c99689..cb108d8, review clean)
Task 3: minor (deferred): recovery.py — an unparseable entry's ERROR log can carry the previous message's correlation_id (cleared only at sweep end); same pattern as the slice 1 OutboxPublisher minor.
Task 4: complete (commits cb108d8..5b0345d, review clean)
Task 4: minor (deferred): task-4-report narrative says ruff reflowed a line that is in fact a single 100-char line; report inaccuracy only, code is lint-clean.
Task 5: complete (commits 5b0345d..1cc4cf1, review clean)
Task 5: Ruling: implementer wrapped the plan's 103-char `status=(...)` line in `_record_decision` to satisfy ruff E501 — accepted; plan code defect, non-behavioural, and ruff-clean is a global constraint — costs if wrong: none.
Task 5: minor (deferred): notification_consumer.py — `channel` read from the payload both in `handle` and inside `_record_decision`; harmless duplication from the extraction.
Task 6: complete (commits 1cc4cf1..d8ef919, review clean)
Task 7: implementer hit an API session limit at the end of its run; controller verified directly: commit b88e43a present, tree clean, email suite 30/30, ruff clean.
Task 7: Ruling: commit trailers from Tasks 5-7 name "Claude Sonnet 5" (the implementer's own attribution) instead of the plan's "Claude Opus 5.5" — accepted; the trailer should name the model that authored the commit — costs if wrong: cosmetic trailer inconsistency in history.
Task 7: complete (commits d8ef919..b88e43a, review clean)
Task 7: minor (deferred): SmtpSender.__init__ sets the aiosmtplib logger to WARNING as a construction side effect (process-global); harmless with one construction at startup. Same pattern planned for BotApiSender (T9).
Task 7: minor (deferred): five plan test lines over 100 chars wrapped by the implementer, no behaviour change.
Task 8: complete (commits b88e43a..843b57f, review clean) — F1 verified: copied system.py already matched the brief.
Task 8: minor (deferred): telegram routed_consumer reads recipient in handle and again in _record_outcome (needed for the give_up path); build_sender's transport param unused until T9 (closed by T9).
Task 9: review Approved with 1 Important (plan-mandated): token-hygiene test runs through MockTransport, which bypasses httpcore, so removing the httpcore WARNING line would pass every test.
Task 9: Ruling: fix it rather than park — the SECURITY constraint names httpcore explicitly and the pin is one small test; assert both loggers are at WARNING after constructing BotApiSender — costs if wrong: one extra test.
Task 9: minor (deferred): two reviewer nits copied verbatim from the brief (a docstring omission; BotApiSender catches TimeoutException/TransportError, not every httpx exception type).
Task 9: fix round 1/5 (1 addressed, 0 open — httpx+httpcore WARNING pinned by a load-bearing test; commits 23e5f94..844d84a)
Task 9: complete (commits 843b57f..844d84a, review clean)
Task 10: complete (commits 844d84a..145ac01, review clean) — RED failed via PEP 420 namespace merge of `app` packages in the shared venv (pydantic `database_url` required) instead of the predicted ModuleNotFoundError; still genuinely red; known one-app-per-process property.
Task 10: minor (deferred): gateway proxy.py builds request headers as a plain dict, so a repeated request header keeps only its last value; untested, out of brief scope.
Task 11: complete (commits 145ac01..aa477c2, review clean) — 12/12 containers healthy, e2e 15 passed (7 slice 1 + 8 slice 2, recovery included). Stack left running; no .env; no volume removal.
Task 11: minor (deferred): plan prose said "nine slice 2 tests"; the plan's code defines eight. Prose-only.
Task 12: complete (commits aa477c2..199b8c6, review clean)
Task 12: minor (deferred): loadtest._submit_all parses response.json()["notification_id"] outside the try — a 202 with a malformed body would abort the run instead of counting a submit error; unreachable today (gateway passes the service's schema-valid body through).
Task 13: review Needs fixes — 1 Important (plan-mandated): page default settle timeout 120 s yields a red verdict for its own default 200-notification run (193 matched, 0 wrong, 7 unsettled, e2e p95 108 s) on the documented one-consumer topology.
Task 13: Ruling: raise the load test PAGE's default settle timeout to 300 s (worst case ≈140 serial email deliveries × 2 s); keep LoadTestConfig's engine default at 120 s as the plan specifies — the default run must be a trustworthy manual gate (spec 12 gate 7) — costs if wrong: a slow default run waits longer before reporting.
Task 13: minor (deferred): app.py uses PEP 758 unparenthesized `except A, B:` (ruff UP autofix under py314) — valid, but the only such spelling in the repo and easy to misread.
Task 13: minor (deferred): submit() callers catch only httpx.HTTPError; a 2xx with a malformed body would raise ValueError/KeyError in the page. Pre-existing pattern.
Task 13: fix round 1/5 (1 addressed, 0 open — page default settle timeout 300 s + sizing note; live 200-run verdict green, e2e p95 121.7 s; commits 0163370..aca875a)
Task 13: complete (commits 199b8c6..aca875a, review clean)
Task 14: Ruling: implementer's "Python 2 except syntax is a SyntaxError in app.py" concern is false — PEP 758 (3.14) allows unparenthesized multi-type except; controller verified ast.parse on 3.14.7 and test_pages.py 2/2 pass — no fix dispatched. Costs if wrong: none (verified). Note: two agents misread it; the Task 13 readability minor is flagged for the final review.
Task 14: complete (commits aca875a..e190a6f, review clean — 20 doc-vs-code spot checks, 0 mismatches; implementer corrected two brief inaccuracies against the code)
Task 15: complete (no file changes; gates: 289 tests pass across 9 suites, ruff clean, 12/12 healthy, e2e 15/15 without .env AND 15/15 with fake real credentials (/version smtp + bot_api) — reserved recipients proven never to reach a real sender; .env removed via trap; load test 200 verdict green, e2e p95 119 s). Deviations per rulings F4 and Task 13 (settle 300 s) only.
ALL 15 TASKS COMPLETE.
Final review (opus, df67f73..e190a6f): With fixes — 0 Critical, 2 Important (load test vs watchdog above ~450 notifications; give_up failing on a malformed payload → infinite retry), 7 Minor. Deferred-minor triage: all stay deferred. Rulings: reviewer agreed with all.
Final review: Ruling: fix Important 1-2 plus Minors 1-5 and the doc precision items in ONE pass; defer to slice 3 the per-message catch in consume_once, the e2e correlation-propagation check, and the PEP 758 except spelling — the deferred three change slice 1 loop shape or are readability-only — costs if wrong: a transient real-delivery failure still delays its batch by ~30 s; the e2e correlation test stays weak.
Fix wave: complete (commits e190a6f..d654944, scoped re-review clean — all 9 findings ADDRESSED, no new breakage). 8 new tests.
