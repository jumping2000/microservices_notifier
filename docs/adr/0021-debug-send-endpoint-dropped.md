# 0021: The debug `POST /send` endpoint is dropped

## Status

Accepted

## Context

An earlier design retained `POST /send` on Email and Telegram Services as a manual Swagger-trigger
endpoint, explicitly marked "debug endpoint — not part of the normal event-driven flow," and
required it to respect idempotency checks despite that.

## Decision

Drop it. It is a second entry path into delivery that no test and no documented flow uses, and
honouring idempotency on a request that carries no `event_id` is ill-defined — there is nothing to
key the idempotency check on. Manual triggering is better served by the end-to-end tests
(`tests/e2e/test_saga.py`) and, from slice 3 onward, by `redis-cli` and `curl` recipes in
`docs/operations.md`. Delivery services therefore expose only `GET /health`, `GET /version`,
`/docs`, and `/redoc` — confirmed in
`services/email-service/app/api/v1/router.py` and
`services/routing-service/app/api/v1/router.py`, both of which include only the `system` router.

## Consequences

There is now no way to trigger a delivery attempt directly against Email or Telegram Service via
HTTP; the only path into delivery is a `notification.routed` event on the stream. This is a
deliberate narrowing: it removes a manual testing convenience in exchange for there being exactly
one way delivery ever happens, which is also the one way this project's tests and documentation
describe. Anyone wanting to exercise Email Service in isolation must publish a `NotificationRouted`
event onto Redis by hand rather than `curl`ing the service directly.
