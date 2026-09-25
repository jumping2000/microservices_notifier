**English** | [Italiano](patterns.it.md)

# Patterns

Every pattern below answers three questions: what it does, which file it lives in, and what breaks
without it — with the test that proves it. A pattern described without its test is precisely the
kind of documentation this project exists to avoid.

## Outbox

**What it does.** Writes a domain state change and its outgoing event in one database
transaction, so the two can never disagree: either both the row and the event exist, or neither
does. A separate background loop then moves the event from the outbox table onto the Redis stream,
outside that transaction.

**Where it lives.** `shared/notification_shared/outbox.py` (`OutboxRepository.save`, which
deliberately does not commit — it enlists in the caller's transaction), and
`shared/notification_shared/publisher.py` (`OutboxPublisher`, the poll loop that reads pending
rows, calls `XADD`, and marks them published — identical in every service with an outbox: notification, routing, email and telegram). Each service
materializes its own `outbox` table from `OutboxMixin` (`shared/notification_shared/models.py`),
per ADR 0012.

**What breaks without it.** If the state change and the event publish were two separate operations
— say, `INSERT notifications` followed directly by `XADD` — a crash between them produces an
accepted notification with no event ever published: the client got `202 Accepted`, the row exists,
and nothing downstream will ever know it happened. The notification is silently stuck at `CREATED`
forever, with no error anywhere.

**The test.**
`tests/integration/test_outbox.py::test_a_rolled_back_transaction_leaves_no_outbox_row` proves the
other half of the same guarantee: it saves an outbox row, rolls back instead of committing, and
asserts the row does not exist. Because `save()` never commits on its own, a rollback of the
caller's transaction takes the outbox row with it — exactly as it must, so a failed domain write
can never leave an orphaned event behind either.

## Idempotent consumers

**What it does.** Before acting on an event, every consumer checks whether it has already
processed that `(event_id, consumer_group)` pair, and after acting, records that it has — in the
same transaction as the domain write. A consumer that sees the same event twice does nothing the
second time beyond acking it.

**Where it lives.** `shared/notification_shared/idempotency.py`
(`IdempotencyRepository.is_processed` / `.mark_processed`), backed by the `processed_events` table
materialized from `ProcessedEventMixin`. Every consumer handler in every service calls
`is_processed` before doing any work and `mark_processed` inside the same commit as the state
change — *except* where the handler must do slow I/O before it can decide what the state change
even is. Notification Service's two consumers
(`services/notification-service/app/workers/routed_consumer.py` and `results_consumer.py`) keep
all four — the check, the state change, the outbox row, and the mark — in one transaction, because
neither does anything slower than a local write between the check and the decision. Routing
Service's `notification_consumer.py` and Email Service's `routed_consumer.py` do not: each opens a
first, short transaction to run `is_processed`, closes it, then does the slow thing a transaction
must never be held open across — an HTTP call to Configuration Service, or the simulated delivery
latency sleep — before opening a second transaction for the state change, the outbox row and the
mark. See ADR 0023 for why this is correct rather than a bug, and the cost it carries forward.

**What breaks without it.** At-least-once delivery (the next pattern) guarantees duplicates will
happen — the outbox publisher itself can `XADD` the same event twice if it crashes between the
`XADD` and marking the row published. Without an idempotency check, a duplicate `notification.routed`
event would cause Email Service to send the email again, or Routing Service to insert a second
`routes` row (caught only by the `UNIQUE (notification_id)` backstop in ADR 0006, which turns a
silent bug into a loud `IntegrityError` rather than preventing the duplicate delivery attempt in
the first place).

**The test.**
`tests/integration/test_publisher.py::test_a_crash_between_xadd_and_the_mark_causes_a_duplicate` is
the proof that duplicates really happen: it forces `mark_published` to raise after the `XADD` has
already succeeded, confirms the event is still on the stream once, then lets the publisher run
again and confirms the *same* `event_id` now appears twice on the stream. That test is the
at-least-once proof; idempotency is what makes the duplicate it creates harmless downstream.

## At-least-once, not exactly-once

**What it does.** The platform guarantees every accepted notification's event is published *at
least* once — never zero times — but makes no attempt to guarantee it is published *exactly* once.
Duplicates are an accepted, expected possibility rather than a bug to eliminate at the source.

**Where it lives.** This is a property of the combination above: `OutboxPublisher.publish_once` in
`shared/notification_shared/publisher.py` marks rows published only *after* `XADD` succeeds, so a
crash in between leaves the row pending and it is republished on the next iteration — by design,
not by accident.

**Why this is acceptable here.** Exactly-once delivery across a network boundary is not achievable
without a distributed transaction spanning Redis and Postgres, which this platform does not have
and is not attempting to build — it is explicitly out of scope (see `README.md`'s known
limitations). At-least-once plus idempotent consumers gets the same *effective* guarantee — one
state change per event — without needing that distributed transaction, at the cost of consumers
having to do the idempotency work themselves rather than getting it for free from the broker.

**The test.** The same
`test_a_crash_between_xadd_and_the_mark_causes_a_duplicate` proves the "at least once" half
directly: the event is not lost by the crash, it is duplicated. There is no test that proves
exactly-once, because the platform does not claim it.

## Consumer groups at offset 0

**What it does.** Every consumer group is created with `XGROUP CREATE <stream> <group> 0
MKSTREAM` — offset `0`, never `$` — so a group created *after* an event was published still
receives that event.

**Where it lives.** `RedisStreamConsumer.ensure_group` in `shared/notification_shared/streams.py`.

**What breaks without it — the first-boot race.** Under `docker compose up`, all the services
start concurrently. If groups were created at `$` (only new messages from this point forward), a
notification-service that publishes `notification.created` a few milliseconds before
routing-service has finished creating its `routing-service` group would lose that event
permanently — the Outbox Pattern guarantees an event is *published*, not that anyone *hears* it.

**The volume-reset consequence.** Offset `0` means a newly created (or recreated) group replays the
stream's *entire* history, not just what's new. If Postgres volumes are reset while Redis's AOF
data survives, every consumer replays every event ever published against empty `processed_events`
tables — which is safe only because consumers are idempotent, but it does mean `docker compose
down -v` must reset both stores together (see ADR 0002 and the "Resetting" section of
`docs/local-development.md`).

**The test.**
`tests/integration/test_streams.py::test_a_group_created_at_offset_zero_sees_earlier_messages`
publishes an event, *then* creates the group, and asserts the group still reads it — the test's own
docstring notes that with `$` this test would read nothing.

## Guarded monotonic transitions

**What it does.** A notification's status only ever moves forward — `CREATED → PROCESSING →
COMPLETED/FAILED`, never backward, never reopened once terminal — and that rule is enforced in the
`WHERE` clause of the `UPDATE` statement itself, not by reading the current status in Python and
deciding whether to write.

**Where it lives.** Two implementations of the same shape, one per service that owns a status
column: `NotificationRepository.advance_status` in
`services/notification-service/app/repositories/notification.py`, called from both
`RoutedConsumer` (guard: `status = 'CREATED'`) and `ResultsConsumer` (guard: `status IN
('CREATED', 'PROCESSING')`); and `RouteRepository.set_status` in
`services/routing-service/app/repositories/route.py`, called from the results consumer with guard
`status = 'PROCESSING'` (only a route already in flight can be closed). This closes a slice 1
parked item: `docs/patterns.md` used to name only the notification-service implementation, as if
`routes` had no equivalent guard, when it always did.

**The race.** `notification-service-routed` (reads `notification.routed`) and
`notification-service-results` (reads `delivery.completed`/`delivery.failed`) are independent
consumer groups with no ordering guarantee between them. Fast delivery means the completion event
can arrive and be consumed *before* the routed event — the row is still `CREATED` when
`ResultsConsumer` sees `DeliveryCompleted`. See ADR 0016 for why `CREATED → COMPLETED` must
therefore be a legal transition even though it looks like a gap in the state machine: without it,
that early completion would match zero rows, and because the handler still acks and marks the
event processed regardless of `rowcount`, the notification would never be corrected and would sit
at `CREATED` forever despite having actually succeeded.

**What breaks without the guard.** A naive implementation — read the current status, decide the
new one in Python, write it — has a window between the read and the write in which the other
consumer group can act. The late-arriving `notification.routed` event would then unconditionally
overwrite an already-`COMPLETED` notification back to `PROCESSING`: a finished notification
silently reopened, visibly wrong to a polling client.

**The test.**
`services/notification-service/tests/test_status_transitions.py::test_a_result_arriving_before_the_routed_event_is_not_overwritten`
publishes both events, consumes the results event first (out of order), asserts the status is
`COMPLETED`, then consumes the late routed event and asserts the status is *still* `COMPLETED`. The
test's own docstring states directly: without the `status = 'CREATED'` guard on the routed handler,
this test ends at `PROCESSING`.

## Group-scoped idempotency

**What it does.** The idempotency ledger keys on `(event_id, consumer_group)`, not `event_id`
alone, so the same event is tracked independently by every group that reads it.

**Where it lives.** The `UNIQUE (event_id, consumer_group)` constraint on `ProcessedEventMixin`
(`shared/notification_shared/models.py`), and every call site that passes both values to
`IdempotencyRepository.is_processed` / `.mark_processed`.

**What breaks without it.** `delivery.failed` is read by two independent consumer groups:
`notification-service-results` (which fails the notification) and `routing-service-results`
(which fails the route). If idempotency were keyed on `event_id` alone, whichever group happened
to process a given `RoutingFailed` or `DeliveryFailed` event first would mark it processed for
*everyone* — the second group would see `is_processed() == True` and skip an event it has never
actually acted on. The notification or the route would silently never reach its terminal state.

**The test.**
`tests/integration/test_idempotency.py::test_the_same_event_is_tracked_per_consumer_group` marks
one event processed for `ConsumerGroup.ROUTING` and then asserts it is `True` for that group but
still `False` for `ConsumerGroup.EMAIL` — proving the same `event_id` is tracked independently per
group rather than globally.

## Recovery by idle claim

**What it does.** Finds a crashed consumer's pending stream entries by idle time rather than by
consumer name, and claims them onto a live consumer so they can be handled the normal way.

**Where it lives.** `shared/notification_shared/recovery.py` (`PendingRecoverer`), built on
`RedisStreamConsumer.get_pending` / `.claim` in `shared/notification_shared/streams.py`. One
instance per service, wired in `lifespan` next to `OutboxPublisher`; the per-service consumers stay
explicit and expose only `handle` and `give_up` for the recoverer to call.

**What breaks without it.** Consumer names are container hostnames, which change on every restart.
A consumer that crashes mid-handle leaves its entry pending under a name that no longer exists;
without idle-time claiming, nothing would ever find that entry again, and the notification behind it
would stay wherever the crash left it forever.

**The test.** `tests/integration/test_recovery.py::test_a_stranded_message_is_claimed_and_completed`
and the e2e
`tests/e2e/test_slice2.py::test_recovery_routes_a_notification_stranded_by_a_configuration_outage`.

## Max-retry and give-up

**What it does.** Caps how many times recovery retries a failing entry. Past `PENDING_MAX_RETRIES`,
the consumer gives up instead of retrying forever — it writes its own failure record and, where
there is someone to tell, a failure event, then the entry is acked and dropped.

**Where it lives.** `PendingRecoverer._recover` in `shared/notification_shared/recovery.py` decides
when to give up (`should_give_up`, comparing `processed_events.fail_count` to `max_retries`); each
consumer's own `give_up(session, envelope)` — `services/routing-service/app/workers/
notification_consumer.py`, `services/email-service/app/workers/routed_consumer.py`,
`services/telegram-service/app/workers/routed_consumer.py` — decides what to write.

**What breaks without it.** Without a cap, a poison message — one that can never succeed, such as a
malformed payload a consumer cannot act on — would be retried forever, forever occupying a slot in
the pending list. Without `give_up` writing something, an uncapped retry's alternative (silent
discard) would leave the notification stuck in whatever state the last attempt left it, with no
error anywhere a client or operator could see.

**The test.** `tests/integration/test_recovery.py::test_the_last_failed_attempt_gives_up_and_acks`
and
`services/routing-service/tests/test_notification_consumer.py::test_a_persistent_outage_ends_in_routing_failed_after_max_retries`.

## Stale-processing watchdog

**What it does.** A safety net independent of recovery: closes any notification that has sat
`PROCESSING` for longer than `PROCESSING_TIMEOUT_MINUTES`, whatever the reason it never received a
delivery result.

**Where it lives.** `services/notification-service/app/workers/watchdog.py` (`Watchdog`), calling
`NotificationRepository.fail_stale_processing` — a guarded `UPDATE` using the database's own clock,
publishing no event (ADR 0028).

**What breaks without it.** Recovery only ever revisits entries still pending in a consumer group.
A notification can end up `PROCESSING` with nothing pending anywhere — for example, Routing
Service's or Notification Service's own results consumer already gave up silently on the event that
would have closed it (spec 2.6). Without the watchdog, that notification is stuck `PROCESSING`
forever with no mechanism left that will ever look at it again.

**The test.**
`services/notification-service/tests/test_watchdog.py::test_a_stale_processing_notification_is_failed`,
`::test_a_recent_processing_notification_is_left_alone`, and
`::test_a_late_delivery_result_does_not_reopen_it`.

## Reserved recipients

**What it does.** Recognizes recipients reserved by convention for documentation and testing — RFC
2606 email domains and TLDs, and Telegram chat ids starting with `sim-` — and always delivers them
through the simulated sender, whatever the platform is configured to use for real delivery.

**Where it lives.** `shared/notification_shared/delivery.py`
(`is_failure_recipient`, `is_reserved_recipient`), checked in that order by both delivery
consumers before any network call, and by `tools/playground/loadtest.py::plan_notifications`, which
generates only reserved recipients.

**What breaks without it.** Real credentials in a developer's `.env` for manual playground testing
would leak into every other path that submits a notification — the e2e suite, the README's own
`curl` examples, the load test — turning an automated test run into a batch of real emails or
Telegram messages to whatever addresses those paths happen to use.

**The test.** `tests/unit/test_delivery.py`;
`services/email-service/tests/test_routed_consumer.py::test_a_reserved_recipient_never_reaches_the_configured_sender`;
`services/telegram-service/tests/test_bot_api.py::test_a_sim_chat_id_never_reaches_the_bot_api`.
