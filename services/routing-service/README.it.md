[English](README.md) | **Italiano**

# routing-service

Decide su quale canale viene instradata una notifica. Non espone **alcun endpoint REST di
dominio**: tutto il suo lavoro avviene in consumer in background che reagiscono a
`notification.created`, e fa una sola chiamata REST sincrona verso l'esterno, a
Configuration Service, per ogni notifica che instrada.

## Tabelle

| Tabella | Scopo |
|---|---|
| `routes` | Una riga per notifica: `notification_id` (univoco), `channel`, `status` (`PROCESSING`\|`COMPLETED`\|`FAILED`), `fail_reason` |
| `outbox` | Eventi in attesa di pubblicazione (`OutboxMixin`) |
| `processed_events` | Registro di idempotenza (`ProcessedEventMixin`) |

## Stream consumati

| Stream | Consumer group | Comportamento |
|---|---|---|
| `notification.created` | `routing-service` | Chiama Configuration Service, scrive su `routes`, pubblica `NotificationRouted` o `RoutingFailed` |
| `delivery.completed` | `routing-service-results` | `SET routes.status='COMPLETED'` |
| `delivery.failed` | `routing-service-results` | `SET routes.status='FAILED', fail_reason=...` per `DeliveryFailed`; salta il proprio `RoutingFailed` (vedi ADR 0003) |

## Stream pubblicati

| Stream | Tipo di evento |
|---|---|
| `notification.routed` | `NotificationRouted` |
| `delivery.failed` | `RoutingFailed` |

## Variabili d'ambiente

| Variabile | Predefinito | Note |
|---|---|---|
| `DATABASE_URL` | — (obbligatoria) | `postgresql+asyncpg://...` |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `CONFIGURATION_SERVICE_URL` | `http://configuration-service:8000` | |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout per la chiamata a Configuration Service |
| `SERVICE_NAME` | `routing-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | |
| `OUTBOX_BATCH_SIZE` | `100` | |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | |
| `PENDING_TIMEOUT_MS` | `30000` | Per quanto tempo una voce deve restare inattiva prima che il recupero la rivendichi |
| `PENDING_MAX_RETRIES` | `3` | Tentativi di recupero prima del `give_up` |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | Intervallo della scansione di recupero |

## Endpoint HTTP

**Nessuno per le operazioni di dominio.** Solo:

| Endpoint | Note |
|---|---|
| `GET /health` | Controlla Postgres e Redis; `503` se uno dei due è fermo |
| `GET /version`, `/docs`, `/redoc` | |

## Worker

Quattro task in background avviati nel `lifespan` di FastAPI: il consumer di
`notification.created` (`routing-service`), il consumer `results` (`routing-service-results`), il
publisher dell'outbox e il recoverer delle voci in sospeso.

Un timeout o un `5xx` di Configuration Service è trattato come **transitorio**: il messaggio resta
in sospeso (nessuna `XACK`, nessuna riga in outbox) per il recupero via `XCLAIM` di `PendingRecoverer`,
invece di essere registrato come un fallimento di instradamento (ADR 0018). Dopo
`PENDING_MAX_RETRIES` tentativi di recupero falliti, `give_up` scrive la route come `FAILED` e
pubblica `RoutingFailed` con motivo `max_retries_exceeded`, in un'unica transazione con
`mark_failed_permanent` (ADR 0024).
