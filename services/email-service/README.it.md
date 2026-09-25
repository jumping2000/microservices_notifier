[English](README.md) | **Italiano**

# email-service

Consegna le notifiche via email. Per impostazione predefinita la consegna è simulata; diventa reale,
tramite SMTP generico, quando `SMTP_HOST` è configurato (ADR 0029). Non espone **alcun endpoint REST di dominio**: tutto il suo
lavoro avviene in worker in background che reagiscono a `notification.routed`.

## Tabelle

| Tabella | Scopo |
|---|---|
| `email_delivery` | Una riga per notifica, inserita già nel suo stato finale: `notification_id` (univoco), `recipient`, `status` (`DELIVERED`\|`FAILED`; `SENDING` è un membro dell'enum definito ma mai scritto — vedi ADR 0022), `fail_reason`, `sent_at` |
| `outbox` | Eventi in attesa di pubblicazione (`OutboxMixin`) |
| `processed_events` | Registro di idempotenza (`ProcessedEventMixin`) |

## Stream consumati

| Stream | Consumer group | Comportamento |
|---|---|---|
| `notification.routed` | `email-service` | Filtra su `payload.channel == "email"` (altrimenti conferma e salta); simula la consegna, poi scrive `email_delivery` già nel suo stato finale e pubblica `DeliveryCompleted` o `DeliveryFailed` |

## Stream pubblicati

| Stream | Tipo di evento |
|---|---|
| `delivery.completed` | `DeliveryCompleted` |
| `delivery.failed` | `DeliveryFailed` |

Non esiste un tasso di fallimento casuale — vedi "Consegna" più sotto per l'ordine esatto delle
regole.

## Variabili d'ambiente

| Variabile | Predefinito | Note |
|---|---|---|
| `DATABASE_URL` | — (obbligatoria) | `postgresql+asyncpg://...` |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `SERVICE_NAME` | `email-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | |
| `OUTBOX_BATCH_SIZE` | `100` | |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | |
| `PENDING_TIMEOUT_MS` | `30000` | Per quanto tempo una voce deve restare inattiva prima che il recupero la rivendichi |
| `PENDING_MAX_RETRIES` | `3` | Tentativi di recupero prima del `give_up` |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | Intervallo della scansione di recupero |
| `DELIVERY_LATENCY_MS_MAX` | `500` | Limite massimo del ritardo casuale della consegna simulata |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout di ogni operazione SMTP, riusato invece di una variabile separata |
| `SMTP_HOST` | non impostato | Vuoto significa consegna simulata; impostato attiva `SmtpSender` |
| `SMTP_PORT` | `587` | |
| `SMTP_USERNAME` | non impostato | Non impostato significa nessuna autenticazione |
| `SMTP_PASSWORD` | non impostato | Mai registrata nei log |
| `SMTP_FROM` | non impostato | Obbligatoria quando `SMTP_HOST` è impostato — altrimenti l'avvio fallisce |
| `SMTP_SECURITY` | `starttls` | `starttls` (587), `ssl` (465), oppure `none` (un server locale come Mailpit) |

## Consegna

Regole verificate in quest'ordine, prima di qualsiasi chiamata di rete
(`shared/notification_shared/delivery.py`, condiviso con telegram-service perché i due non possano
divergere — ADR 0030):

1. Un destinatario che contiene `fail` (senza distinguere maiuscole/minuscole) → `DeliveryFailed`, motivo
   `simulated_failure`.
2. Un destinatario riservato (`example.com`/`.org`/`.net`, o qualsiasi dominio sotto
   `.example`/`.invalid`/`.test`) → il sender simulato, anche quando `SMTP_HOST` è impostato.
3. Altrimenti → il sender configurato: simulato quando `SMTP_HOST` non è impostato o è vuoto, SMTP
   quando è impostato.

Esiti SMTP (sezione 6.2 della spec), dopo le regole precedenti:

| Condizione | Esito |
|---|---|
| Messaggio accettato | `DeliveryCompleted` |
| Destinatario rifiutato, o qualsiasi altra risposta `5xx` | permanente → `DeliveryFailed`, motivo `smtp_rejected` |
| Risposta `4xx`, timeout, errore di connessione | transitorio → `handle` solleva un'eccezione; `PendingRecoverer` riprova |
| Fallimento di autenticazione (`535`) | transitorio, registrato nei log a livello ERROR: un problema di configurazione, non del destinatario |

`GET /version` riporta `delivery_mode`: `simulated` o `smtp`. Descrive il sender *configurato* — un
destinatario riservato resta simulato anche quando `delivery_mode` riporta `smtp`.

## Endpoint HTTP

**Nessuno per le operazioni di dominio** — l'endpoint di debug `POST /send` della progettazione
originale è stato rimosso (ADR 0021). Solo:

| Endpoint | Note |
|---|---|
| `GET /health` | Controlla Postgres e Redis; `503` se uno dei due è fermo |
| `GET /version` | Riporta `delivery_mode` |
| `GET /docs`, `/redoc` | |

## Worker

Tre task in background avviati nel `lifespan` di FastAPI: il consumer di `notification.routed`
(`email-service`), il publisher dell'outbox e il recoverer delle voci in sospeso.
