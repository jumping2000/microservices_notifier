[English](README.md) | **Italiano**

# telegram-service

Consegna le notifiche via Telegram. Per impostazione predefinita la consegna è simulata; diventa
reale, tramite la Telegram Bot API, quando `TELEGRAM_BOT_TOKEN` è configurato (ADR 0026). Non espone **alcun endpoint REST di
dominio**: tutto il suo lavoro avviene in worker in background che reagiscono a
`notification.routed`.

## Tabelle

| Tabella | Scopo |
|---|---|
| `telegram_delivery` | Una riga per notifica, inserita già nel suo stato finale (ADR 0022): `notification_id` (univoco), `chat_id`, `status` (`DELIVERED`\|`FAILED`), `fail_reason`, `sent_at` |
| `outbox` | Eventi in attesa di pubblicazione (`OutboxMixin`) |
| `processed_events` | Registro di idempotenza (`ProcessedEventMixin`) |

## Stream consumati

| Stream | Consumer group | Comportamento |
|---|---|---|
| `notification.routed` | `telegram-service` | Filtra su `payload.channel == "telegram"` (altrimenti conferma e salta); consegna, poi scrive `telegram_delivery` già nel suo stato finale e pubblica `DeliveryCompleted` o `DeliveryFailed` |

## Stream pubblicati

| Stream | Tipo di evento |
|---|---|
| `delivery.completed` | `DeliveryCompleted` |
| `delivery.failed` | `DeliveryFailed` |

Il `recipient` della notifica è il chat id (ADR 0026) — non esiste una variabile
`TELEGRAM_CHAT_ID` separata. La consegna fallisce in modo deterministico, prima di qualsiasi chiamata di rete, quando
il chat id contiene la sottostringa `fail` (senza distinguere maiuscole/minuscole), e un chat id che inizia con `sim-`
è sempre simulato indipendentemente dalla configurazione (ADR 0030) — vedi "Consegna" più sotto per
l'ordine completo delle regole.

## Variabili d'ambiente

| Variabile | Predefinito | Note |
|---|---|---|
| `DATABASE_URL` | — (obbligatoria) | `postgresql+asyncpg://...` |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `SERVICE_NAME` | `telegram-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | |
| `OUTBOX_BATCH_SIZE` | `100` | |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | |
| `PENDING_TIMEOUT_MS` | `30000` | Per quanto tempo una voce deve restare inattiva prima che il recupero la rivendichi |
| `PENDING_MAX_RETRIES` | `3` | Tentativi di recupero prima del `give_up` |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | Intervallo della scansione di recupero |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout per le richieste alla Bot API |
| `DELIVERY_LATENCY_MS_MAX` | `500` | Limite massimo del ritardo casuale della consegna simulata |
| `TELEGRAM_BOT_TOKEN` | non impostato | Vuoto significa consegna simulata; impostato attiva la Bot API |

## Consegna

Regole verificate in quest'ordine, prima di qualsiasi chiamata di rete
(`shared/notification_shared/delivery.py`, condiviso con email-service perché i due non possano
divergere — ADR 0030):

1. Un chat id che contiene `fail` (senza distinguere maiuscole/minuscole) → `DeliveryFailed`, motivo
   `simulated_failure`.
2. Un chat id che inizia con `sim-` → il sender simulato, anche quando `TELEGRAM_BOT_TOKEN` è
   impostato.
3. Altrimenti → il sender configurato: simulato quando `TELEGRAM_BOT_TOKEN` non è impostato o è
   vuoto, la Bot API quando è impostato.

Esiti della Bot API (sezione 4.5 della spec), dopo le regole precedenti:

| Risposta | Esito |
|---|---|
| `200` con `ok: true` | `DeliveryCompleted` |
| `400`, `403` | permanente → `DeliveryFailed`, motivo `telegram_rejected` |
| `429`, `5xx`, timeout, errore di connessione, qualsiasi altro codice di stato | transitorio → `handle` solleva un'eccezione; nulla viene scritto o confermato; `PendingRecoverer` riprova e alla fine si arrende con `max_retries_exceeded` |

Il token fa parte dell'URL della richiesta, e httpx registra nei log gli URL delle richieste a
livello `INFO`; `silence_client_logs()` imposta i logger `httpx` e `httpcore` a `WARNING` così il token non
finisce mai nei log.

`GET /version` riporta `delivery_mode`: `simulated` o `bot_api`. Descrive il sender *configurato* —
un chat id `sim-` resta simulato anche quando `delivery_mode` riporta `bot_api`.

## Endpoint HTTP

**Nessuno per le operazioni di dominio** — non esiste un `POST /send` (correzione 3.21). Solo:

| Endpoint | Note |
|---|---|
| `GET /health` | Controlla Postgres e Redis; `503` se uno dei due è fermo |
| `GET /version` | Riporta `delivery_mode` |
| `GET /docs`, `/redoc` | |

## Worker

Tre task in background avviati nel `lifespan` di FastAPI: il consumer di `notification.routed`
(`telegram-service`), il publisher dell'outbox e il recoverer delle voci in sospeso.
