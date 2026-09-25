**English** | [Italiano](README.it.md)

# gateway

The platform's single client entry point: a thin FastAPI reverse proxy over notification-service
and configuration-service (spec section 5). No database, no Redis, no business logic — every
request is forwarded and every response comes back unchanged.

## Routes

| Gateway | Forwards to |
|---|---|
| `POST /api/v1/notifications` | notification-service `POST /notifications` |
| `GET /api/v1/notifications/{id}` | notification-service `GET /notifications/{id}` |
| `GET /api/v1/notifications` | notification-service `GET /notifications` |
| `GET /api/v1/channels` | configuration-service `GET /channels` |
| `PUT /api/v1/channels/{name}` | configuration-service `PUT /channels/{name}` |
| `GET /api/v1/health` | the Gateway's own status only; does not aggregate downstream health |

Base URLs come from `NOTIFICATION_SERVICE_URL` and `CONFIGURATION_SERVICE_URL`. The routing table is
static — explicit routes, not a catch-all path — so the Gateway's Swagger (`/docs`) lists exactly
what it exposes.

## Forwarding

- Method, query string, body, and request headers are forwarded, minus hop-by-hop headers (`host`,
  `content-length`, `connection`, `transfer-encoding`, `keep-alive`, `upgrade`, `te`, `trailer`,
  `proxy-authorization`, `proxy-authenticate`) and the correlation header, which is re-added once so
  it is never sent twice.
- `CorrelationIDMiddleware` (`shared/notification_shared/middleware.py`) generates `X-Correlation-ID`
  when the request does not already carry one; the Gateway always sends it downstream.
- The downstream status, body, and `content-type` are returned unchanged, including downstream `404`
  and `422` responses. No payload transformation, no validation, no knowledge of channels.

## Downstream failures

| Condition | Response |
|---|---|
| No response within `GATEWAY_TIMEOUT_SECONDS` | `504`, `ErrorCode.GATEWAY_TIMEOUT` |
| Connection refused / DNS failure | `502`, `ErrorCode.BAD_GATEWAY` (ADR 0027) |

Both use the common error model (`shared/notification_shared/exceptions.py`).

## Environment variables

| Variable | Default | Notes |
|---|---|---|
| `NOTIFICATION_SERVICE_URL` | `http://notification-service:8000` | |
| `CONFIGURATION_SERVICE_URL` | `http://configuration-service:8000` | |
| `GATEWAY_TIMEOUT_SECONDS` | `10.0` | Per-request timeout on the Gateway's `httpx.AsyncClient` |
| `SERVICE_NAME` | `gateway` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |

## What it deliberately does not do

API key (slice 3), CORS (this is where it would go, if a browser client ever needs it), retries,
caching, response aggregation.
