**English** | [Italiano](configuration.it.md)

# Configuration

Every service reads its settings from environment variables. `docker-compose.yml` sets the values
the stack needs; the secrets for real delivery come from a local `.env` file next to it.

## Real delivery with `.env`

Both channels are **simulated by default**: nothing leaves your machine. To send for real:

```bash
cp .env.example .env      # .env is git-ignored; never commit it
# edit .env, then recreate the two delivery services:
docker compose up -d --wait
```

An empty value keeps that channel simulated. Check the result:

```bash
curl -s http://localhost:8004/version   # "delivery_mode": "smtp" or "simulated"
curl -s http://localhost:8005/version   # "delivery_mode": "bot_api" or "simulated"
```

The playground shows the same modes as badges.

### Telegram

1. In Telegram, talk to **@BotFather**, send `/newbot`, and copy the token it gives you.
2. Open a chat with your new bot and send it any message (a bot cannot write to you first).
3. Find your chat id: open `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser and read
   `result[0].message.chat.id` — a number such as `123456789` (group chats are negative, e.g.
   `-1001234567890`).
4. Put the token in `.env` as `TELEGRAM_BOT_TOKEN=...`, recreate the stack, then send a notification
   with `"channel": "telegram"` and your chat id as the `recipient`.

The recipient **is** the chat id; there is no separate chat id setting
([ADR 0026](../adr/0026-telegram-recipient-is-the-chat-id.md)). Treat the token as a password: the
services never write it to their logs.

### Email (SMTP)

Any SMTP server works. `SMTP_FROM` is required as soon as `SMTP_HOST` is set: without it
email-service refuses to start and never becomes healthy.

| Provider | `SMTP_HOST` | `SMTP_PORT` | `SMTP_SECURITY` | Notes |
|---|---|---|---|---|
| Gmail | `smtp.gmail.com` | `587` | `starttls` | Needs an **app password** (Google account → Security → 2-Step Verification → App passwords), not your normal password |
| Outlook / Microsoft 365 | `smtp.office365.com` | `587` | `starttls` | The account must allow SMTP AUTH |
| Any provider with implicit TLS | your host | `465` | `ssl` | |
| Local test server (Mailpit, MailHog) | `host.docker.internal` | `1025` | `none` | Catches mail in a web UI instead of delivering it; leave username and password empty |

```dotenv
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=you@gmail.com
SMTP_PASSWORD=abcd efgh ijkl mnop
SMTP_FROM=you@gmail.com
SMTP_SECURITY=starttls
```

What happens on failure ([ADR 0029](../adr/0029-real-email-through-generic-smtp.md)):

| SMTP result | Outcome |
|---|---|
| Message accepted | `COMPLETED` |
| Recipient refused or any other 5xx | `FAILED` / `smtp_rejected`, immediately |
| 4xx, timeout, connection error | Retried by recovery; `FAILED` / `max_retries_exceeded` after about two minutes |
| Authentication failure (535) | Same as above, and an ERROR in the logs: `SMTP authentication failed` — fix the credentials within the retry window and the message still goes out |

## Reserved recipients

Two rules run **before** any real send, whatever `.env` contains
([ADR 0030](../adr/0030-reserved-recipients-are-always-simulated.md)):

1. A recipient containing `fail` (any case) always ends `FAILED` / `simulated_failure`, and nothing
   is sent. This includes real-looking addresses such as `failla@gmail.com`.
2. A **reserved** recipient is always delivered by the simulated sender:
   - email at `example.com`, `example.org`, `example.net` or their subdomains, or at any domain
     ending in `.example`, `.invalid` or `.test` (RFC 2606 names that can never be real inboxes);
   - a Telegram chat id starting with `sim-`.

That is why every README example, every end-to-end test and every load test stays simulated even
on a stack with real credentials. To send for real, use a real address or chat id.

## All variables

`—` means required with no default. The compose file sets every required value.

### Common to every service

| Variable | Default | Meaning |
|---|---|---|
| `SERVICE_NAME` | the service's name | Appears in every log line and in `/health` |
| `SERVICE_VERSION` | `1.0.0` | Reported by `/version` |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Services with a database and Redis

notification, routing, email and telegram services.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — | `postgresql+asyncpg://user:password@host:port/db` (configuration-service needs it too) |
| `REDIS_URL` | `redis://redis:6379/0` | Redis Streams connection |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | How long a consumer blocks waiting for new stream entries |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | How often the outbox publisher looks for unpublished events |
| `OUTBOX_BATCH_SIZE` | `100` | Events published per outbox iteration |
| `PENDING_TIMEOUT_MS` | `30000` | How long an entry must sit unacknowledged before recovery claims it |
| `PENDING_MAX_RETRIES` | `3` | Recovery attempts before giving up with `max_retries_exceeded` |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | How often recovery looks for stranded entries |

### notification-service

| Variable | Default | Meaning |
|---|---|---|
| `PROCESSING_TIMEOUT_MINUTES` | `5` | The watchdog fails a notification stuck in `PROCESSING` longer than this |
| `WATCHDOG_INTERVAL_SECONDS` | `60` | How often the watchdog runs |

### routing-service

| Variable | Default | Meaning |
|---|---|---|
| `CONFIGURATION_SERVICE_URL` | `http://configuration-service:8000` | Where to ask whether a channel is enabled |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout of that call; a timeout counts as transient and is retried |

### email-service

| Variable | Default | Meaning |
|---|---|---|
| `DELIVERY_LATENCY_MS_MAX` | `500` (compose sets `2000`) | Upper bound of the simulated sender's random delay |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout of every SMTP operation |
| `SMTP_HOST` | empty | Empty means simulated delivery |
| `SMTP_PORT` | `587` | |
| `SMTP_USERNAME` | empty | Empty means no authentication |
| `SMTP_PASSWORD` | empty | Never logged |
| `SMTP_FROM` | empty | Required when `SMTP_HOST` is set |
| `SMTP_SECURITY` | `starttls` | `starttls`, `ssl` or `none` |

### telegram-service

| Variable | Default | Meaning |
|---|---|---|
| `DELIVERY_LATENCY_MS_MAX` | `500` (compose sets `2000`) | Upper bound of the simulated sender's random delay |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout of the Bot API call |
| `TELEGRAM_BOT_TOKEN` | empty | Empty means simulated delivery. Never logged |

### configuration-service

Only the common variables and `DATABASE_URL`. The two channels, `email` and `telegram`, are seeded
enabled by its first database migration.

### gateway

| Variable | Default | Meaning |
|---|---|---|
| `NOTIFICATION_SERVICE_URL` | `http://notification-service:8000` | Forward target for `/api/v1/notifications*` |
| `CONFIGURATION_SERVICE_URL` | `http://configuration-service:8000` | Forward target for `/api/v1/channels*` |
| `GATEWAY_TIMEOUT_SECONDS` | `10.0` | After this the Gateway answers `504` |

### Playground and end-to-end tests

These run on your machine, not in a container, and point at the published ports.

| Variable | Default |
|---|---|
| `GATEWAY_URL` | `http://localhost:8000` |
| `NOTIFICATION_URL` | `http://localhost:8001` |
| `ROUTING_URL` | `http://localhost:8002` |
| `CONFIGURATION_URL` | `http://localhost:8003` |
| `EMAIL_URL` | `http://localhost:8004` |
| `TELEGRAM_URL` | `http://localhost:8005` |

## Changing a value

Edit `docker-compose.yml` (or `.env` for the secrets) and recreate the affected service:
`docker compose up -d --wait <service>`. For the timing values, remember they interact: recovery
needs about `PENDING_TIMEOUT_MS × (PENDING_MAX_RETRIES + 1)` to give up, and that should stay well
below `PROCESSING_TIMEOUT_MINUTES` so a delivery gives up with its precise reason before the
watchdog fails it with the generic `processing_timeout`.
