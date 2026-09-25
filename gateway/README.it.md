[English](README.md) | **Italiano**

# gateway

L'unico punto di ingresso client della piattaforma: un reverse proxy FastAPI leggero davanti a
notification-service e configuration-service (sezione 5 della spec). Nessun database, nessun Redis,
nessuna logica di business: ogni richiesta viene inoltrata e ogni risposta torna indietro
invariata.

## Rotte

| Gateway | Inoltra a |
|---|---|
| `POST /api/v1/notifications` | notification-service `POST /notifications` |
| `GET /api/v1/notifications/{id}` | notification-service `GET /notifications/{id}` |
| `GET /api/v1/notifications` | notification-service `GET /notifications` |
| `GET /api/v1/channels` | configuration-service `GET /channels` |
| `PUT /api/v1/channels/{name}` | configuration-service `PUT /channels/{name}` |
| `GET /api/v1/health` | solo lo stato del Gateway stesso; non aggrega lo stato dei servizi a valle |

Gli URL di base provengono da `NOTIFICATION_SERVICE_URL` e `CONFIGURATION_SERVICE_URL`. La tabella
di instradamento è statica — rotte esplicite, non un percorso catch-all — così lo Swagger del
Gateway (`/docs`) elenca esattamente ciò che espone.

## Inoltro

- Metodo, query string, body e header della richiesta vengono inoltrati, esclusi gli header
  hop-by-hop (`host`, `content-length`, `connection`, `transfer-encoding`, `keep-alive`, `upgrade`,
  `te`, `trailer`, `proxy-authorization`, `proxy-authenticate`) e l'header di correlazione, che
  viene riaggiunto una sola volta in modo che non venga mai inviato due volte.
- `CorrelationIDMiddleware` (`shared/notification_shared/middleware.py`) genera
  `X-Correlation-ID` quando la richiesta non ne porta già uno; il Gateway lo invia sempre a valle.
- Lo status, il body e il `content-type` del servizio a valle vengono restituiti invariati, comprese le
  risposte `404` e `422` a valle. Nessuna trasformazione del payload, nessuna validazione, nessuna
  conoscenza dei canali.

## Fallimenti a valle

| Condizione | Risposta |
|---|---|
| Nessuna risposta entro `GATEWAY_TIMEOUT_SECONDS` | `504`, `ErrorCode.GATEWAY_TIMEOUT` |
| Connessione rifiutata / fallimento DNS | `502`, `ErrorCode.BAD_GATEWAY` (ADR 0027) |

Entrambi i casi usano il modello di errore comune (`shared/notification_shared/exceptions.py`).

## Variabili d'ambiente

| Variabile | Predefinito | Note |
|---|---|---|
| `NOTIFICATION_SERVICE_URL` | `http://notification-service:8000` | |
| `CONFIGURATION_SERVICE_URL` | `http://configuration-service:8000` | |
| `GATEWAY_TIMEOUT_SECONDS` | `10.0` | Timeout per richiesta sull'`httpx.AsyncClient` del Gateway |
| `SERVICE_NAME` | `gateway` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |

## Cosa volutamente non fa

Chiave API (slice 3), CORS (è qui che andrebbe, se mai un client browser ne avesse bisogno), nuovi
tentativi, caching, aggregazione delle risposte.
