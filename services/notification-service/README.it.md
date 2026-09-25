[English](README.md) | **Italiano**

# notification-service

Possiede l'aggregato della notifica: cosa è stato richiesto e il suo stato attuale. I client lo
raggiungono tramite le rotte `/api/v1/notifications` del Gateway, ed è l'unico servizio il cui read
model viene interrogato dal client.

## Tabelle

| Tabella | Scopo |
|---|---|
| `notifications` | L'aggregato: `channel`, `recipient`, `subject`, `body`, `status` (`CREATED`\|`PROCESSING`\|`COMPLETED`\|`FAILED`), `fail_reason` |
| `outbox` | Eventi in attesa di pubblicazione (`OutboxMixin`) |
| `processed_events` | Registro di idempotenza (`ProcessedEventMixin`) |

## Stream consumati

| Stream | Consumer group | Comportamento |
|---|---|---|
| `notification.routed` | `notification-service-routed` | `SET status='PROCESSING' WHERE status='CREATED'` |
| `delivery.completed` | `notification-service-results` | `SET status='COMPLETED' WHERE status IN ('CREATED','PROCESSING')` |
| `delivery.failed` | `notification-service-results` | `SET status='FAILED', fail_reason=... WHERE status IN ('CREATED','PROCESSING')` (gestisce sia `DeliveryFailed` sia `RoutingFailed`) |

## Stream pubblicati

| Stream | Tipo di evento |
|---|---|
| `notification.created` | `NotificationCreated` |

## Variabili d'ambiente

| Variabile | Predefinito | Note |
|---|---|---|
| `DATABASE_URL` | — (obbligatoria) | `postgresql+asyncpg://...` |
| `REDIS_URL` | `redis://redis:6379/0` | |
| `SERVICE_NAME` | `notification-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | Intervallo di polling del publisher dell'outbox |
| `OUTBOX_BATCH_SIZE` | `100` | Dimensione del batch del publisher dell'outbox |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | Intervallo di polling per entrambi i consumer (`routed`, `results`) |
| `PENDING_TIMEOUT_MS` | `30000` | Per quanto tempo una voce deve restare inattiva prima che il recupero la rivendichi |
| `PENDING_MAX_RETRIES` | `3` | Tentativi di recupero prima del `give_up` |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | Intervallo della scansione di recupero |
| `PROCESSING_TIMEOUT_MINUTES` | `5` | Watchdog: per quanto tempo una notifica può restare in `PROCESSING` |
| `WATCHDOG_INTERVAL_SECONDS` | `60` | Intervallo della scansione del watchdog |

## Endpoint HTTP

| Endpoint | Note |
|---|---|
| `POST /notifications` | `202 Accepted`, body `{notification_id, status}` |
| `GET /notifications/{id}` | Read model: `notification_id`, `channel`, `status`, `fail_reason`, `created_at`, `updated_at`; `404` se sconosciuto |
| `GET /notifications` | `?limit=20&offset=0&status=&channel=` |
| `GET /health` | Controlla Postgres e Redis; `503` se uno dei due è fermo |
| `GET /version`, `/docs`, `/redoc` | |

## Worker

Cinque task in background avviati nel `lifespan` di FastAPI: il publisher dell'outbox, il consumer
`routed` (`notification-service-routed`), il consumer `results` (`notification-service-results`), il
recoverer delle voci in sospeso e il watchdog per lo stato `PROCESSING` bloccato
(`app/workers/watchdog.py`). Il watchdog chiude le notifiche rimaste `PROCESSING` per più di
`PROCESSING_TIMEOUT_MINUTES`; non pubblica alcun evento, e un risultato di consegna arrivato dopo
che ha già fatto fallire una notifica non la riapre (ADR 0028).
