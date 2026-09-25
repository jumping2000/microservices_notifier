**English** | [Italiano](operations.it.md)

# Operations

Running, inspecting and troubleshooting the stack. All commands run from the repository root.
Redis and the databases are reached through `docker compose exec`, so you need no local client.

## Lifecycle

| Goal | Command |
|---|---|
| Build and start everything, wait until healthy | `docker compose up --build -d --wait` |
| Status of every container | `docker compose ps` |
| Follow one service's logs | `docker compose logs -f notification-service` |
| Restart one service | `docker compose restart email-service` |
| Recreate one service after changing its configuration | `docker compose up -d --wait email-service` |
| Stop everything, keep data | `docker compose stop` |
| Remove containers, keep data | `docker compose down` |
| Wipe all data | `docker compose down -v` — see [Resetting safely](#resetting-safely) |

## Health

```bash
curl -s http://localhost:8000/api/v1/health       # gateway: its own status only
for port in 8001 8002 8003 8004 8005; do curl -s http://localhost:$port/health; echo; done
```

A service answers `503` with `"status": "DOWN"` and the failing check (`database` or `redis`) when
a dependency is unreachable; Docker then marks it unhealthy.

## Trace one notification

Every log line is one JSON object with `service`, `correlation_id`, and in workers also `event_id`,
`event_type`, `notification_id` and `consumer_group`. The correlation id you send (or the one the
Gateway generates) follows the notification through every service:

```bash
curl -s -X POST http://localhost:8000/api/v1/notifications \
  -H 'Content-Type: application/json' -H 'X-Correlation-ID: trace-me' \
  -d '{"channel": "email", "recipient": "john@example.com", "body": "Hi"}'

docker compose logs --no-log-prefix | grep trace-me | sort
```

`--no-log-prefix` makes every line start with its JSON `timestamp`, so `sort` puts the whole saga in
time order: the Gateway forwarding the request, notification-service accepting it, routing-service
asking configuration-service and routing it, email-service delivering it, and notification-service
reaching `COMPLETED`.

## Inspect Redis Streams

```bash
docker compose exec redis redis-cli XLEN notification.created          # entries in a stream
docker compose exec redis redis-cli XINFO GROUPS notification.routed   # groups: pending, lag
docker compose exec redis redis-cli XPENDING notification.created routing-service   # summary
docker compose exec redis redis-cli XPENDING notification.created routing-service IDLE 30000 - + 10
docker compose exec redis redis-cli XINFO CONSUMERS notification.created routing-service
```

| Stream | Consumer groups |
|---|---|
| `notification.created` | `routing-service` |
| `notification.routed` | `email-service`, `telegram-service`, `notification-service-routed` |
| `delivery.completed` | `notification-service-results`, `routing-service-results` |
| `delivery.failed` | `notification-service-results`, `routing-service-results` |

In `XINFO GROUPS`, `pending` counts entries delivered to a consumer but not yet acknowledged, and
`lag` counts entries nobody has read yet. Both should return to `0` when the platform is idle. A
`pending` that stays above zero for longer than `PENDING_TIMEOUT_MS` is what recovery picks up.

`XINFO CONSUMERS` lists one consumer per container restart, because consumer names are container
hostnames. Old names with `pending 0` are harmless leftovers.

## Inspect the databases

User and password are `notif` / `notif`. One database per service:

| Container | Database | Main tables |
|---|---|---|
| `notification-db` | `notificationdb` | `notifications`, `outbox`, `processed_events` |
| `routing-db` | `routingdb` | `routes`, `outbox`, `processed_events` |
| `configuration-db` | `configurationdb` | `channels` |
| `email-db` | `emaildb` | `email_delivery`, `outbox`, `processed_events` |
| `telegram-db` | `telegramdb` | `telegram_delivery`, `outbox`, `processed_events` |

```bash
# notifications by outcome
docker compose exec notification-db psql -U notif -d notificationdb -c \
  "SELECT status, fail_reason, count(*) FROM notifications GROUP BY 1, 2 ORDER BY 3 DESC;"

# events not yet published by the outbox (should be 0 or close to it)
docker compose exec notification-db psql -U notif -d notificationdb -c \
  "SELECT count(*) FROM outbox WHERE NOT published;"

# the idempotency ledger: what each consumer group processed, gave up on, or is retrying
docker compose exec routing-db psql -U notif -d routingdb -c \
  "SELECT consumer_group, status, count(*), max(fail_count) FROM processed_events GROUP BY 1, 2;"

# one notification across services
docker compose exec email-db psql -U notif -d emaildb -c \
  "SELECT status, fail_reason, sent_at FROM email_delivery WHERE notification_id = '<id>';"
```

In `processed_events`, `status` is `PROCESSED`, `FAILING` (being retried; `fail_count` says how
many times) or `FAILED_PERMANENT` (given up). `fail_count` is kept after success as the history of
how many attempts an event needed.

## How recovery behaves

When a consumer cannot finish a message — its service crashed, or a dependency is down — the
message stays pending in Redis. Every service with consumers runs a recoverer that:

1. every `RECOVERY_POLL_INTERVAL_MS` (5 s) looks for entries idle longer than `PENDING_TIMEOUT_MS`
   (30 s), whichever consumer they belonged to, and claims them;
2. re-runs the same handler as the normal path; on success the entry is acknowledged;
3. on failure increments `fail_count`; after `PENDING_MAX_RETRIES` (3) failed attempts it **gives
   up**: routing, email and telegram record a failure with reason `max_retries_exceeded` and publish
   it, so the notification ends `FAILED` instead of hanging.

With the defaults, giving up takes about two minutes. Separately, the notification-service
**watchdog** fails any notification still `PROCESSING` after `PROCESSING_TIMEOUT_MINUTES` (5) with
`processing_timeout`. A delivery result that arrives after that does not reopen it.

## Troubleshooting

| Symptom | Likely cause | What to do |
|---|---|---|
| Notification stays `CREATED` | routing-service or configuration-service is down | Check `/health` on 8002 and 8003. Once they are back, recovery routes it within about 35 s; if they stay down, it ends `FAILED` / `max_retries_exceeded` |
| Notification stays `PROCESSING` | email-service or telegram-service is down or failing transiently | Check their health and logs. Recovery retries; the watchdog fails it after 5 minutes with `processing_timeout` |
| `FAILED` / `channel_disabled` | The channel was switched off | `PUT /api/v1/channels/<name>` with `{"enabled": true}`, then send again |
| `FAILED` / `simulated_failure` on a real address | The recipient contains `fail` | By design (see [Configuration](guides/configuration.md#reserved-recipients)); use another address |
| A real email or Telegram message never arrives but the notification is `COMPLETED` | The recipient is reserved (`@example.com`, `.test`, `sim-…`), so it was simulated | Use a real, non-reserved recipient |
| `FAILED` / `smtp_rejected` | The SMTP server refused the recipient or message | Check the address; look for the refusal in `docker compose logs email-service` |
| `FAILED` / `max_retries_exceeded` on email with real SMTP | Wrong credentials, or the server was unreachable for two minutes | Look for `SMTP authentication failed` in the email-service logs; fix `.env` and `docker compose up -d --wait` |
| `FAILED` / `telegram_rejected` | Wrong chat id, or you never sent your bot a message, or you blocked it | Send the bot a message, recheck the chat id with `getUpdates` |
| email-service unhealthy after editing `.env` | `SMTP_HOST` set without `SMTP_FROM` | `docker compose logs email-service` shows the validation error; add `SMTP_FROM` |
| Gateway answers `502 BAD_GATEWAY` | notification- or configuration-service is not reachable | `docker compose ps`; restart the service |
| Gateway answers `504 GATEWAY_TIMEOUT` | The service took longer than 10 s | Check its logs and database health |
| Load test verdict red with `UNSETTLED` | Settle timeout too short for the backlog | Raise it to about `email share × total × 2 s`, see the [playground guide](guides/playground.md#sizing-a-run) |
| Load test verdict red with `FAILED/processing_timeout` | A run large enough (over ~450) that queued notifications outlived the watchdog | Use a smaller total; this is the one-consumer compose stack's limit, not lost messages |
| Everything is reprocessed after a reset | Only one store was reset | Always reset with `docker compose down -v` |
| `uv sync` fails with `Accesso negato` / `Access is denied` on `.venv\Scripts` | VS Code's language servers hold the environment | Close VS Code, run `uv sync` from an external terminal |
| `port is already allocated` on `up` | Another program uses 8000–8005, 5433–5437 or 6379 | Stop it, or change the host port in `docker-compose.yml` |

## Resetting safely

```bash
docker compose down -v
docker compose up --build -d --wait
```

`down -v` deletes **every** volume: all five Postgres databases and Redis's data. Reset them
together, never one alone. Consumer groups start reading streams from the beginning (offset `0`,
[ADR 0002](adr/0002-consumer-groups-start-at-offset-zero.md)), so a wiped database next to a
surviving Redis stream would replay the stream's whole history into empty tables and reprocess
every notification ever sent — including, with real credentials, sending real messages again.
