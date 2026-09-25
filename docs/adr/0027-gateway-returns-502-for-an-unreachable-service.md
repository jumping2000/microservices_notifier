# 0027: The Gateway returns 502 for an unreachable service

## Status

Accepted

## Context

The slice 1 spec defined only one Gateway failure status: `504` when a downstream service does not
respond within the configured timeout. It said nothing about the other way a downstream call can
fail — the connection is refused, or DNS resolution fails, because the service is not running at
all. Left unhandled, that case would fall through to FastAPI's default `500`, indistinguishable from
a bug in the Gateway itself. A client cannot tell "notification-service is slow" from
"notification-service is down" if both produce the same generic error.

## Decision

`gateway/app/api/v1/proxy.py::_forward` catches both cases from the single `httpx.AsyncClient` call
that forwards every request: `httpx.TimeoutException` raises `GatewayTimeoutError` (`504`,
`ErrorCode.GATEWAY_TIMEOUT`), and any other `httpx.TransportError` — connection refused, DNS
failure, and the like — raises `BadGatewayError` (`502`, `ErrorCode.BAD_GATEWAY`). Both are
`ServiceError` subclasses (`shared/notification_shared/exceptions.py`) and go through the same
`ServiceError` exception handler as every other error in the platform, so they render the same
`{"error": {"code": ..., "message": ...}}` body. Both new codes are added to the shared `ErrorCode`
enum rather than defined locally to the Gateway, so any future service can reuse them without
inventing a second vocabulary. Everything downstream returns unchanged: the Gateway's own `404`,
`422`, and other pass-through responses are not affected, because those come back as ordinary
`httpx.Response` objects, not exceptions.

## Consequences

Clients of the Gateway can now distinguish "slow" (`504`) from "down" (`502`) by status code alone,
without inspecting the error body. The Gateway never invents any other status: every downstream
failure is one of these two, and every downstream success or downstream-reported error passes
through with its original status, body, and `content-type` untouched (`gateway/app/api/v1/proxy.py`
sets no fallback status of its own). The distinction is coarse — a `502` covers everything from "DNS
cannot resolve the hostname" to "the TCP connection was actively refused" — but coarse is enough
here: the Gateway's job is to say a downstream call could not be completed, not to diagnose why.

Proof: `gateway/tests/test_proxy.py::test_a_downstream_timeout_is_a_504` and
`::test_an_unreachable_downstream_is_a_502`, against an `httpx.MockTransport` standing in for the
downstream service.
