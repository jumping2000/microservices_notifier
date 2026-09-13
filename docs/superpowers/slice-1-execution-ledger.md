# Slice 1 — Execution Ledger

Preserved from the subagent-driven execution workspace, which is otherwise scratch.
It records the pre-flight conflict scan, every task's completion, the 28 deferred
Minor findings, the 3 parked items, and the 23 rulings taken during execution.

Kept because most of these rulings appear nowhere else: the commit messages carry
some, this file carries all of them. A decision taken on the user's behalf that
survives only in a deleted scratch directory was a decision taken in secret.

---

# SDD ledger — plan: docs/superpowers/plans/2026-09-12-notification-platform-slice-1.md

Spec: docs/superpowers/specs/2026-09-12-notification-platform-design.md (read, binding authority)
Workspace: .superpowers/sdd/2026-09-12-notification-platform-slice-1/
Branch: master — user chose this explicitly over a worktree or feature branch when asked.
Ruling: implementation proceeds on master — user's explicit answer to a direct question; no remote exists so EnterWorktree's origin-based base ref was unavailable anyway — costs: rollback requires `git reset --hard 9827d78`, which discards all slice-1 work.

## Pre-flight scan

### Cross-task interface pairs (produces vs consumes)

| Pair | Produced | Consumed | Finding |
|---|---|---|---|
| T2 → T5,7,8,11,12,13,14,15 | `Stream`, `EventType`, `ConsumerGroup`, `EventEnvelope.new/to_redis/from_redis` | all stream work | agree |
| T3 → T9 | `NotFoundError`, `ServiceUnavailableError` as siblings | `ServiceClient` maps 404 vs 5xx | agree; T3 test asserts the sibling relation explicitly |
| T3 → T12 | same two types | `_decide` catches NotFound, lets Unavailable propagate | agree — this is spec 3.18's whole mechanism |
| T3 → T10,11 | `ServiceError.to_response()`, `status_code` | FastAPI exception handler | agree |
| T4 → T5 | `OutboxMixin` columns + pending index | `OutboxRepository.get_pending` orders by `created_at`, filters `published` | agree |
| T4 → T6 | `ProcessedEventMixin` unique `(event_id, consumer_group)` | `on_conflict_do_update(index_elements=[...])` | agree |
| T4 → T11,12,13 | all three mixins | per-service concrete models | agree |
| T5 → T8 | `get_pending`, `mark_published` | `OutboxPublisher.publish_once` | agree |
| T5 → T11,12,13 | `save(session, stream, envelope)` no commit | called inside service/consumer transactions | agree |
| T6 → T12,13,14,15 | `is_processed`, `mark_processed` | every consumer handler | agree |
| T7 → T8 | `RedisStreamPublisher.publish` | publisher loop | agree |
| T7 → T12,13,14,15 | `RedisStreamConsumer(redis, name)`, `ensure_group(stream, group)`, `read`, `ack` | worker loops | agree after the `ensure_groups()` rename in self-review |
| T8 → T11,12,13 | `OutboxPublisher(session_factory=, repository=, publisher=, poll_interval_ms=, batch_size=)` | lifespan wiring | agree, keyword-only at every call site |
| T9 → T10,11 | `CorrelationIDMiddleware`, `CORRELATION_ID_HEADER` | `add_middleware`, `request.state.correlation_id` | agree |
| T9 → T12 | `ServiceClient(base_url, timeout, transport)` | routing's config client; `transport` is what lets T12 test with MockTransport | agree |
| T10 → T11,12,13 | `create_app(settings)`, `Database.session_factory/ping/dispose`, `get_db(request)`, `system.py` | copied per service | agree |
| T11 → T14 | `Notification`, `NotificationStatus`, `NotificationRepository` | `advance_status` added, consumers use the statuses | agree |
| T11 → T16 | `POST/GET /notifications` shapes | e2e assertions | agree |
| T12 → T15 | `Route`, `RouteStatus`, `RouteRepository.set_status(session, notification_id, status, fail_reason)` | called positionally in T15 | agree, arity and order match |
| T12 → T13,14,15 | `notification.routed` payload incl. `route_id` | email delivers from it; notification advances on it | agree |
| T13 → T14,15 | `delivery.completed` / `delivery.failed` payloads | both results consumers | agree; `reason` key present on both failure events |
| T10,11,12,13 → T16 | `docker-compose.yml` grown 2+3+2+2 | T16 expects 9 containers | agree, sums to 9 |
| T14 → T11 files | edits `app/main.py`, `app/repositories/notification.py`, `app/core/config.py` | sequential, after T11 | agree |
| T15 → T12 files | edits `app/main.py` | sequential, after T12 | agree |

### Per-task self-consistency

| Task | Tests vs code | Files created vs later touched | Finding |
|---|---|---|---|
| T1 | n/a (no tests) | creates `.gitignore` later never re-touched | **CONFLICT — see F1** |
| T2 | 8 tests cover every produced symbol | — | agree |
| T3 | 11 tests; sibling-type assertion present | — | agree |
| T4 | 7 tests incl. two-Base isolation | — | agree |
| T5 | 6 tests; rollback test pins the no-commit contract | creates `testing.py` | **CONFLICT — see F2** |
| T6 | 7 tests | — | `increment_fail_count` has no slice-1 caller — see F5 |
| T7 | 10 tests; offset-0 test pins spec 3.2 | — | agree |
| T8 | 6 tests; crash test pins at-least-once | — | agree |
| T9 | 12 tests across two files | — | agree |
| T10 | 11 tests; alembic test is sync for a stated reason | template copied by T11-13 | see F4 |
| T11 | 14 tests | `system.py` gains a Redis check | **see F7** |
| T12 | 10 tests; all four config responses covered | — | **CONFLICT — see F3** |
| T13 | 9 tests; filter + fail rule + replay | — | agree |
| T14 | 10 tests; the race test is the load-bearing one | — | agree |
| T15 | 7 tests; RoutingFailed-skip test present | — | agree |
| T16 | 7 e2e tests | reviews compose, adds nothing | **see F6** |
| T17 | no tests (docs + editor config) | Step 12 verifies docs against code | agree |

### Findings and rulings

**F1 — `.gitignore` omits `.superpowers/`.** Task 1 Step 1 prescribed a `.gitignore` without it, and the SDD workspace lives there, so briefs, reports and review packages would have been committed.
Ruling: corrected the plan's Task 1 `.gitignore` block to include `.superpowers/`, and wrote the file immediately so nothing leaks before Task 1 runs. Plan defect, not a spec conflict. Costs if wrong: nothing — the directory is pure scratch.

**F2 — `shared` ships `testing.py` importing undeclared dependencies.** `notification_shared.testing` imports `pytest` and `testcontainers`, which `shared/pyproject.toml` did not declare; the service images build `--no-dev`, which excludes both.
Ruling: added a `[project.optional-dependencies] testing` extra to the plan's shared manifest. Nothing changes operationally (the root dev group already installs them) but the dependency is now honest. Costs if wrong: none identified.

**F3 — misleading test name in T12.** `test_recovery_after_an_outage_routes_the_same_message` promised re-routing but asserted only a pending count, because slice 1 has no XCLAIM worker.
Ruling: renamed to `test_an_outage_leaves_the_message_pending_for_recovery` and rewrote the docstring to state the narrow guarantee. Costs if wrong: none — the assertion was always the honest one; only the name lied.

**F4 — the plan mandates duplication that the review rubric treats as a defect.** Four services each get their own `models/base.py`, `models/outbox.py`, `models/processed_event.py`, `create_app`, `Database`, `system.py`, and their own `consume_once`/`run_forever` loop. Reviewers will flag this as copy-paste.
Ruling: the duplication stands. Spec 3.12 requires per-service registries (a shared `Base` breaks test isolation), and the spec's stated priority is educational readability over abstraction — an explicit thirty-line consumer loop is the artefact a reader is meant to study. Only the outbox publisher, which is genuinely identical, is shared. Reviewers who raise it get this ruling, not a fix dispatch. Costs if wrong: four copies of ~40 lines drift independently; the per-service tests are what would catch that.

**F5 — `increment_fail_count` and `FAILED_PERMANENT` have no slice-1 caller.** Max-retry is slice 2.
Ruling: build them in T6 anyway, as the plan already states. `increment_fail_count` is the same upsert as `mark_processed`; splitting the repository across slices means writing and reviewing it twice. Costs if wrong: three tests covering code nothing calls until slice 2.

**F6 — weak assertion in T16's PROCESSING test.** `_observed[0] in {"CREATED", "PROCESSING"}` is true under almost any timing.
Ruling: stands as written. Asserting that `PROCESSING` was actually caught makes the test a race against a sub-second delivery; T14's guarded-transition integration test is what genuinely pins the behaviour, and the e2e test's real assertion is the terminal state. Costs if wrong: the e2e suite does not prove PROCESSING is observable to a real client — accepted, and stated in the plan.

**F7 — T11's `/health` returns 503 under its own test fixture.** The fixture builds the app without lifespan, so `app.state.redis` is absent and the Redis check reports DOWN. The test reads `.json()` without asserting the status code, so it passes.
Ruling: stands. The plan states this explicitly and the real lifespan path is covered by T16's e2e health test, which asserts `status == "UP"`. Costs if wrong: a reader of T11's test could mistake 503 for correct behaviour; the plan text mitigates.

**F8 — low-risk item to verify at implementation, not a conflict.** `NotificationRead` pairs `from_attributes=True` with `validation_alias="id"` to expose `notification_id`. Expected to work in Pydantic 2.13, but if attribute lookup uses the field name rather than the alias, T11's read-model test fails loudly on the first run. No pre-emptive change; the test is the detector.

Scan complete: 8 findings, 8 ruled, 3 plan corrections applied before dispatch.

## Task progress

Task 1: complete (commits 516c202..ad65f42, review clean)
Task 1: minor (deferred): shared declares starlette>=0.49.0 and uv resolved 1.6.0. Floor not ceiling, so FastAPI (added at Task 10) can pull it back down — no action now, but if Task 10's `uv sync` reports a resolution conflict, this is why.
Task 2: review found 1 Important (RED evidence reads as reconstructed, not captured) + 1 Minor labeled plan-mandated (EventEnvelope docstring claims immutability without frozen=True).
Task 2: Ruling: enforce the immutability the docstring claims by adding `model_config = ConfigDict(frozen=True)` — spec section 5.1 and "Design Principles" both state events are immutable facts, so the spec (binding authority) favours enforcement over softening the docstring. Nothing in slice 1 mutates an envelope (every consumer builds a new one via model_validate), so the risk is confined to a later task attempting mutation, which would now fail loudly instead of silently succeeding. Folded into fix round 1 rather than deferred, because it is one line in a file 15 tasks import. Costs if wrong: a future task that legitimately needs to amend an envelope must use model_copy(update=...) instead of assignment.
Task 2: fix round 1/5 (2 addressed, 0 open — RED evidence recaptured with real traceback; frozen=True + mutation test added; commits 9eb1bdc..c556a12)
Task 2: complete (commits ad65f42..c556a12, review clean)
Task 3: review spec ✅, implementation correct on every mandated point; 1 Important (RED evidence reformatted, closing line does not match real pytest output) + 2 Minor.
Task 3: minor (deferred): logging.py and config.py have "what" docstrings where context.py and exceptions.py have "why" ones — inconsistent depth, no action.
Task 3: minor (deferred): JSONFormatter coerces every optional field with str() unconditionally; harmless for current callers.
Task 3: Ruling: RED-evidence findings enter the fix loop rather than being adjudicated away, per the skill's rule that adjudication happens only at the round cap. But the recurrence across Tasks 2 and 3 is a method problem, not two implementer failures: from Task 4 onward every dispatch requires the RED run be captured with `2>&1 | tee <workspace>/task-N-red.txt` and the report to reference that file, so there is nothing to retype or compress. Costs if wrong: one extra file per task in a scratch directory that gets deleted at the end.
Task 3: fix round 1/5 — implementer captured task-3-red.txt (genuine, correct pytest formatting) then was killed by an API session rate limit mid-fix, leaving four modules moved out of the working tree and an unrequested move of prompt_microservice_notifier_v3.md into docs/.
Task 3: controller recovery (not a review fix): git restore of shared/notification_shared/ and prompt_microservice_notifier_v3.md from commit 310e8b5, removal of the identical docs/ copy after cmp verification. Working tree clean, 20/20 unit tests pass, ruff clean.
Task 3: Ruling: the RED-evidence finding is closed on direct controller verification instead of a scoped re-review. Two reasons: the fix produced zero code change (git status clean), so there is no diff for a re-reviewer to inspect; and I read task-3-red.txt myself and confirmed the markers the reviewer said were missing — real pytest header, collection count, two full tracebacks, and the `2 errors in 0.21s` closing line. Verifying the artifact first-hand is stronger than dispatching an agent to read the same file. Costs if wrong: Task 3's evidence rests on my reading rather than an independent reviewer's; the file is in the workspace and re-checkable until the workspace is deleted.
Task 3: complete (commits c556a12..310e8b5, review clean after ruling)
Task 3: BLOCKED ON EXTERNAL LIMIT — API session limit reached, resets 5:30pm Europe/Rome. No further subagent dispatch possible; Tasks 4-17 need ~28 dispatches.
Task 4: complete (commits 310e8b5..b917c85, review clean)
Task 4: minor (deferred): TimestampMixin lacks a docstring where the other two mixins have one.
Task 4: minor (deferred): test_mixins_declare_no_table_of_their_own checks absence of __tablename__ but not __abstract__; code is correct, coverage gap only.
Task 4: minor (deferred): report miscounts test file as 103 lines (actually 86) and its inline quotation of the pytest header drops a fragment; the tee capture files themselves are intact.
Note: the `tee` capture method introduced at Task 4 closed the recurring RED-evidence problem — Task 4's capture was genuine and correctly formatted on the first attempt, with no file relocation.
Task 5: Ruling: the plan's own code blocks are not isort-ordered, so implementers hit ruff I001 on transcription (Task 4 and Task 5 both). Applying `ruff check --fix` for import order only is the correct resolution and is NOT a deviation from the brief — the brief specifies behaviour, and ruff config from Task 1 specifies import order. Implementers from here on are told this up front. Costs if wrong: none; import order is mechanically checked and carries no semantics.
Task 5: review spec ✅, quality Approved, but 2 Important findings, both plan-mandated: the oldest-first test never asserted order, and the empty-list no-op test asserted nothing at all.
Task 5: Ruling: both findings are correct and both get fixed rather than parked. The spec's testing section calls the integration tier "the ones that have teeth", and a test that cannot distinguish a working implementation from a broken one has none. I then swept the whole plan for the same defect and found it in three places total (Tasks 5, 6, 7) plus the ordering gap — all four corrected in the plan text (commit 052dbb4) before Task 6 and 7 are dispatched, so the defect cannot recur. The oldest-first test additionally had to be restructured to one transaction per row: func.now() is transaction start time in Postgres, so three rows saved together share a created_at and the ORDER BY was not assertable at any strength. Costs if wrong: Task 5's test file diverges from the originally-reviewed brief; the corrected brief is regenerated so the implementer and re-reviewer both see the new text.
Task 5: fix round 1/5 (2 addressed, 0 open — ordering now asserted against insertion order with one txn per row; empty-list no-op now seeds a row and proves it stays pending; outbox.py untouched; commits 052dbb4..0479fde)
Task 5: complete (commits b917c85..0479fde, review clean)
Task 5: minor (deferred): save() has no method-level docstring; the no-commit contract is documented at module level only.
Task 6: complete (commits 0479fde..1c4cb39, review clean)
Task 6: minor (deferred): mark_processed's DO UPDATE sets status but does not reset fail_count to 0 when overwriting a FAILING row; untested either way, and nothing in slice 1 reads fail_count after a PROCESSED transition. Revisit when slice 2 adds max-retry, which is fail_count's first reader.
Task 6: minor (deferred): consumer_group is annotated str but callers pass a ConsumerGroup StrEnum; str() coercion makes it harmless and the signature is specified verbatim in the brief.
Task 7: complete (commits 1c4cb39..f9249b0, review clean)
Task 7: minor (deferred): redis_client fixture calls flushall() before yielding (correct) but carries no in-code comment explaining why before rather than after; the rationale lives only in the brief prose.
Task 8: Ruling: the plan's Task 8 test block imported `sqlalchemy.select` without using it (F401, not merely an import-order issue). The implementer removed it via `ruff check --fix` and flagged it as DONE_WITH_CONCERNS rather than deviating silently — correct on both counts. Plan text corrected so the brief no longer ships the dead import. Costs if wrong: none; the import was genuinely unused, the tests reach the database through OutboxRepository.get_pending.
Task 8: complete (commits f9249b0..5f5a62e, review clean)
Task 8: minor (deferred): set_correlation_id is never reset after a batch, so a stale id from the last published row persists into an unrelated failed iteration's logger.exception call. Plan-mandated, harmless in practice.
Task 8: minor (deferred): session_factory constructor parameter is unannotated where the other collaborators are typed. Plan-mandated, cosmetic.
Task 9: Ruling (pre-emptive, found by controller before dispatch): the plan's middleware test declared the probe route as `async def probe(request)` with no type annotation. FastAPI resolves parameters by annotation, so an unannotated `request` becomes a required query parameter rather than the injected Request object, and three of the four middleware tests would have failed with 422 before any implementation bug could surface. Corrected the plan to `async def probe(request: Request) -> dict:` with the reason stated inline, and imported Request from fastapi. Caught by reading the plan rather than by a fix loop. Costs if wrong: none; the annotation is how FastAPI's own documentation writes this.
Task 9: review not yet dispatched — controller verification caught three problems first: uv.lock left uncommitted (dirty tree), two manifests changed outside the declared four-file scope, and 2 deprecation warnings in the test output.
Task 9: Ruling on the warnings (plan-mandated): the plan specified fastapi.testclient.TestClient, which on starlette 1.x emits StarletteDeprecationWarning plus an anyio alias deprecation. I verified with a scratch probe that httpx.ASGITransport runs the middleware correctly and produces zero warnings, and that it is already the pattern every service test from Task 10 onward uses. Plan corrected to ASGITransport. Costs if wrong: ASGITransport does not run lifespan, which this test does not need; if a future middleware test needs lifespan it must use a different driver.
Task 9: Ruling on fastapi placement: the implementer added fastapi>=0.115.0 to shared/pyproject.toml runtime dependencies. Wrong on two counts — middleware.py imports only starlette so the shared library does not depend on FastAPI, and the bound contradicts the plan's global fastapi>=0.141.1. It belongs in the root dev group. Plan corrected to say so explicitly. Costs if wrong: none; the service manifests declare fastapi themselves from Task 10.
Task 9: Ruling on the notification-shared dev entry: unnecessary. Tasks 2-8 imported notification_shared from root-level tests with no such entry, because uv sync --all-packages installs every workspace member into the root .venv. Concluded from project history, not an experiment. Removing it. Costs if wrong: if root-level imports break after removal, the fix round's test run fails loudly and the entry goes back.
note: uv.lock in 7e9a15a reflects the incorrect manifests; the fix round regenerates it
Task 9: fix round 1/5 (3 addressed, 0 open — ASGITransport replaces TestClient, fastapi moved to root dev group at >=0.141.1, superfluous root manifest entries removed; commits 7e9a15a..3b6714d)
Task 9: complete (commits deca4e3..3b6714d, review clean) — SHARED LIBRARY DONE (11 modules: events, exceptions, context, logging, config, models, outbox, idempotency, streams, publisher, middleware, http_client, testing)
Task 9: minor (deferred): ServiceClient maps 404 and 5xx but lets any other 4xx fall through to response.json(), returning a non-dict body as if it were success. Slice 1 only ever issues GET /channels/{name} through it, so no caller can currently hit this; revisit if a future caller can receive a 401/403/422 from Configuration Service.
Task 9: minor (deferred): _send's local headers dict is unannotated. Cosmetic; ruff's selected rules do not require it.
Task 10: reported DONE_WITH_CONCERNS with four well-flagged items; controller verified all four before review. The implementer also caught and deleted a stray root alembic.ini that `alembic init` created while scaffolding script.py.mako, before it entered history — good discipline, no action needed.
Task 10: Ruling on ruff B008: the plan's global `select = [..., "B", ...]` is incompatible with the FastAPI endpoint code the same plan specifies, because bugbear's B008 flags Depends() as an argument default and FastAPI's dependency injection requires exactly that. A real plan defect. The implementer's remedy (extend-immutable-calls = ["fastapi.Depends"]) is better than ignoring B008 wholesale, but it was placed in the service manifest, which would mean four copies for four services. Moving it to the root [tool.ruff.lint.flake8-bugbear] and forbidding per-service [tool.ruff] blocks. Costs if wrong: if ruff ever runs from inside a service directory the root config would not apply; every invocation in this project runs from the root, and the plan now says so.
Task 10: Ruling on the Alembic warning: Alembic 1.20 emits "No path_separator found in configuration" on every command when the setting is absent, so the upgrade-head test could not have pristine output. Plan's alembic.ini template corrected to include `path_separator = os`, which propagates to Tasks 11-13 since they copy this file. Not silencing the warning with a filter — the setting is what Alembic actually asks for. Costs if wrong: `os` selects platform-native separators, which is right for a single-OS project; a cross-platform CI would want `:`.
Task 10: fix round 1/5 — implementer applied both edits then was killed by an API session rate limit (resets 00:50 Europe/Rome) before verifying or committing. Tree held a complete uncommitted fix, nothing half-written.
Task 10: Ruling: controller ran the remaining verification and commit rather than waiting for the limit to clear. This is not a controller-authored fix skipping review — the implementer authored both edits, the Task 10 review has not happened yet, so the entire diff including these config changes still goes to a fresh reviewer. Costs if wrong: none to correctness; the reviewer is explicitly told to scrutinise the config changes and the import re-sorting.
Task 10: Ruling: removing the per-service [tool.ruff] block had a consequence I did not foresee — ruff's isort no longer treats `app` as first-party inside a service directory, so 9 files reported I001. Tried known-first-party = ["app", "notification_shared"] at the root and REJECTED it: it changed grouping repository-wide and raised the count from 9 to 14, which would mean re-sorting every existing file. Took `ruff check --fix` instead, which is mechanical, stable, and already ruled a non-deviation. Costs if wrong: import order inside the four services is whatever root ruff decides rather than what the plan's code blocks show; import order carries no semantics.
Task 10: complete (commits 3b6714d..70d3105, review clean) — SERVICE TEMPLATE ESTABLISHED
Task 10: minor (deferred): ChannelService.set_enabled mutates the ORM entity and commits in the service layer, while ChannelRepository offers only reads. It satisfies the letter of the layering rule (no select/insert/update outside the repository) but the write path bypasses the repository entirely. Plan-mandated. Contained to this service: Task 12's RouteRepository has set_status and Task 14 adds NotificationRepository.advance_status, so the other services keep writes in the repository. Worth a ChannelRepository.set_enabled() in a later pass.
Task 10: minor (deferred): post-ruff import ordering groups app.* flat with third-party rather than in a first-party group. Consequence of the rejected known-first-party experiment; Tasks 11-13 inherit the same convention.
Task 10: minor (deferred): the runtime image copies the whole service directory including tests/. Plan-mandated; harmless since --no-dev means no test dependencies are installed.
Task 11: Ruling: the implementer hit a contradiction between the brief's Files list ("Modify: pyproject.toml (extend pythonpath)") and my dispatch constraint forbidding changes to the root manifest, and followed the stricter instruction while flagging it. Correct on both counts. The brief's line was stale — a leftover from the abandoned design where all tests shared one pytest process via a root pythonpath setting; that was superseded at Task 11's own Step 3 by per-service test directories invoked with PYTHONPATH on the command line. Plan line removed. Costs if wrong: none; no test command uses the setting, and the full suite passes without it.
Task 11: minor (deferred): the autogenerated migration needed hand-reformatting (quote style, line wrapping, explicit revision id) to satisfy ruff. Schema verified unchanged against a live Postgres with psql \d.
Task 11: complete (commits 70d3105..5c9ebb1, review clean)
Task 11: minor (deferred): the list_notifications query parameter is named `status`, shadowing the `status` imported from fastapi and used as status.HTTP_202_ACCEPTED elsewhere in the same file. Function-local, harmless, plan-mandated naming.
Task 12: complete (commits 5c9ebb1..87654c0, review clean)
Task 12: minor (deferred): test_a_configuration_timeout_behaves_the_same_as_a_5xx asserts only the Route count where its 5xx sibling checks all three tables plus XPENDING. The consume_once() == 0 assertion already proves no ack, so the critical path is covered.
Task 12: minor (deferred): test_the_route_is_marked_processed_for_its_own_group_only checks the single row's consumer_group rather than exercising a second group; the name overstates what it directly exercises. Group scoping is proven in the shared idempotency tests (Task 6).
Task 13: review spec ✅ but verdict "Needs fixes" on 2 Important findings, both plan-mandated.
Task 13: Ruling on the missing EmailDeliveryRepository: the reviewer is right and this gets fixed. The literal layering rule was not broken (no select/insert/update appears in the worker, only ORM session.add) but the convention was: Routing has RouteRepository, Notification has NotificationRepository, Email had none. The spec's stated priority is educational readability, and a layering that differs per service is exactly what a reader trips on. Added EmailDeliveryRepository to the plan with add() and get_by_notification(), and wired the worker to it. Costs if wrong: one more thin file per delivery service; the alternative was leaving the first future query of email_delivery with nowhere to live except the worker.
Task 13: Ruling on the TDD-order finding: no code fix, and severity lower than the reviewer assessed — for a reason the reviewer could not see from its brief. In THIS plan every task's implementation is given verbatim in the brief, so no implementer ever designs in response to red; they transcribe. The RED gate's real value here is proving the tests fail without the implementation, i.e. that they are not vacuous, and the capture delivers exactly that. What does stand is the practice concern: deleting files to force red is the same class of action that broke this repository at Task 3. Method change for Tasks 14-17 instead of a fix: the RED capture must be taken before any implementation file is written, and if that order is missed the implementer reports it rather than reconstructing red by deletion. Costs if wrong: Task 13's red capture evidences less than the others'; the code itself is verbatim from the brief and independently reviewed.
Task 13: NOTE for the final whole-branch review: Configuration Service has the mirror of finding 1 — ChannelService writes and commits in the service layer while ChannelRepository is read-only (deferred at Task 10). Triage both together; fixing only Email leaves 3 of 4 services consistent.
Task 13: fix round 1/5 (1 addressed, 1 closed by ruling — EmailDeliveryRepository added and wired, transaction boundary unchanged; commits d53b9e1..9046005)
Task 13: complete (commits 87654c0..9046005, review clean) — ALL FOUR SERVICES BUILT
Task 13: minor (deferred): _deliver accepts subject and body but reads neither; only recipient drives the failure check. Plan-mandated.
Task 13: minor (deferred): the model column is sent_at while the event payload field is delivered_at, for the same timestamp. Both spellings plan-mandated.
Task 14: complete (commits 9046005..f088d4f, review clean) — SAGA CLOSED, all three outcomes verified end-to-end against the running stack
Task 14: Ruling: the review found, by reasoning rather than by test failure, that the code admits CREATED → COMPLETED while the SPEC's legal-transitions table omits it. The code is right and the spec was wrong. In the race the correction exists to handle, the delivery result is consumed before notification.routed, so the row is still CREATED when the completion lands; a results guard admitting only PROCESSING would match zero rows, and because the handler still acks and still marks processed, the event would never be redelivered and the notification would sit at CREATED forever. Corrected the transition list in BOTH the spec and the plan, with the reasoning stated inline — Task 17 writes patterns.md and event-flows.md from these documents, so leaving it would have shipped documentation describing a guarantee narrower than the code's. Costs if wrong: none; the broader guard is what the passing race test exercises.
Task 14: minor (deferred): the results consumer reads its two streams sequentially, each with its own block_ms, so an empty iteration can block up to twice the poll interval. Matches the plan's code; latency only.
Task 14: minor (deferred): list(allowed_from) in advance_status is redundant when every caller already passes a list.
Task 15: complete (commits 3f65226..6312eb7, review clean)
Task 15: minor (deferred): the task-15 compose capture records summary lines emitted by the verification script rather than a raw transcript of each curl and poll. The claimed facts are all present and consistent; granularity only.
Task 15: minor (deferred): the skip branch logs "our own routing failure" for everything not in HANDLED, which today can only be RoutingFailed. Accurate now; revisit if a later slice puts a third event type on either result stream.
Task 16: Ruling (pre-emptive): the plan stated the unit tier at 38 in three places, stale since Task 2's review added a test proving EventEnvelope is frozen. Actual is 39. Corrected all three before dispatch so the implementer does not report a false mismatch against its own gate. Costs if wrong: none; the count is mechanically checkable.
Task 16: Ruling: the implementer found an arithmetic slip of mine — the plan's "Grand total 129" summed the six pre-e2e tiers and then presented the figure as if it included the e2e 7. True total is 136. Corrected. Costs if wrong: none; it was a number in prose, and the per-tier counts it was derived from were all correct.
Task 16: review spec ✅, Approved, 1 Important (plan-mandated): the health test hardcoded http://localhost:8002 and :8004 for routing and email while NOTIFICATION_URL and CONFIGURATION_URL were env-overridable. Half-overridable is the same as not overridable the moment anyone points the tier at a non-localhost stack.
Task 16: Ruling: fix it. Added ROUTING_URL and EMAIL_URL env-backed constants plus an ALL_SERVICE_URLS tuple, and rewrote the health test to iterate them. My first attempt at this edit reintroduced `from tests.e2e.conftest import ALL_SERVICE_URLS` — precisely the anti-pattern I had forbidden and the reviewer had credited the implementer for avoiding. Caught it myself before dispatching and replaced it with a `service_urls` fixture, consistent with how `settle` is exposed. Costs if wrong: one more fixture in the e2e conftest.
Task 16: fix round 1/5 (1 addressed, 0 open — four env-backed URLs, ALL_SERVICE_URLS, service_urls fixture, health test walks all four with one client; override path proven wired by an explicit-env run; commits 6712e51..71b4a9b)
Task 16: complete (commits 3c0f7c2..71b4a9b, review clean)
Task 16: minor (deferred): each e2e test opens a fresh httpx.AsyncClient rather than sharing one. Connection churn only, across seven tests.
Task 17: complete (commits 71b4a9b..9dcad79, review clean — zero findings at any severity)
Task 17: out-of-scope observation carried to the final review: RouteRepository.set_status has no SQL guard against out-of-order updates, unlike NotificationRepository.advance_status. Benign in slice 1 (routes reaches terminal state via a single routing-time write plus a one-shot result write, and nothing republishes a result once terminal), but a real gap the moment slice 2 introduces XCLAIM redelivery — idempotency should catch it before the repository, so the practical risk is low and the missing SQL guard is the asymmetry worth noting.
ALL 17 TASKS COMPLETE.
POST-TASK FINDING (controller, running the Definition of Done): `uv run ruff format --check .` fails — 24 Python source files and 4 markdown files would be reformatted. Root cause is a process gap of mine: the DoD demands both `ruff check` and `ruff format --check` clean, but no task's gate ever ran `ruff format`. Every task ran `ruff check` and `ruff check --fix`, which lint and autofix but do not format.
Ruling: fix it rather than relax the DoD, for two reasons. The committed .vscode/settings.json sets Ruff as the Python formatter with format-on-save, so the first edit anyone makes to those 24 files would reformat them and produce a diff unrelated to their change. And the DoD is part of the plan the user approved.
Ruling on the 4 markdown files: ruff 0.16 formats Python code blocks inside markdown. Three of the four are historical records — the spec, the plan, and prompt_microservice_notifier_v3.md — and rewriting them to satisfy a formatter would corrupt the record of what was decided. Excluding markdown from ruff entirely instead, which also makes `ruff format --check .` mean what the DoD intends: the Python sources are clean. Costs if wrong: code samples inside documentation are not formatter-checked, so a sample could drift from the style of the code it documents.
Formatter fix: complete (commit 26805bc) — 24 Python files reformatted by `ruff format`, markdown excluded via extend-exclude, all seven tiers re-verified at 39/29/11/24/17/9/7 = 136. Both ruff gates clean.
Fix wave: complete (commit 2e7e7e5, scoped re-review clean — all 9 findings ADDRESSED, no new breakage). Tests 136 -> 143.
Fix wave: parked — patterns.md's "Guarded monotonic transitions" section names only NotificationRepository.advance_status; after F7 added the guard to RouteRepository.set_status there are two instances of the pattern and the doc mentions one. Ruling: real and cosmetic. It is documentation consistency, not a guarantee gap, and the process allows no second fix wave. Costs if wrong: a reader studying the guard pattern sees one example where two exist.
Fix wave: parked — F2's residual flakiness. The e2e test now asserts "PROCESSING" in _observed, but email's simulated delivery is random.uniform(0, DELIVERY_LATENCY_MS_MAX), not a fixed sleep, so an unlucky short draw could in principle let the chain reach COMPLETED inside one 250ms poll window and fail a correctly-wired system. Ruling: park. The window between PROCESSING and COMPLETED is dominated by two 500ms worker poll intervals (email's outbox publisher, then notification's results consumer), not by the sleep, so missing it needs both polls to fire near-instantly AND a tiny random draw. The risk fell substantially when the ceiling went from 500ms to 2000ms. Costs if wrong: an occasional false e2e failure on correct code; the deterministic fix would be a latency floor rather than a ceiling, or a shorter poll interval, and that is the user's call.
