# 0029: Real email through generic SMTP

## Status

Accepted

## Context

Slice 1 simulated every email: `SimulatedSender` slept a random delay and wrote a `DELIVERED` row,
with no message ever leaving the platform. That was enough to teach the saga, but the platform
cannot claim to demonstrate real delivery without it. The obvious alternatives — a provider-specific
API such as SendGrid or SES — would tie the education to one vendor's SDK and credentials model
instead of the protocol every mail server already speaks.

## Decision

`services/email-service/app/senders.py::SmtpSender` sends through `aiosmtplib`, a plain-text
`email.message.EmailMessage` from `SMTP_FROM`, to any standard SMTP server. It is the configured
sender whenever `SMTP_HOST` is set (`build_sender`); `SMTP_FROM` is required whenever `SMTP_HOST`
is set — `Settings._smtp_from_is_required_with_a_host` (`services/email-service/app/core/config.py`)
fails at startup otherwise, so a half-configured service never reaches `healthy` rather than failing
silently on the first send. `SMTP_SECURITY` selects the transport: `starttls` (default, port 587,
plain connect then STARTTLS), `ssl` (implicit TLS, port 465), or `none` (no TLS at all, for a local
server such as Mailpit). The subject is the notification's `subject`, or the literal string
`Notification` when it is null. The connection timeout reuses `HTTP_TIMEOUT_SECONDS` rather than
introducing a separate variable, since both express "how long this service waits on a network call."

Outcomes map onto the shared permanent/transient split (spec section 6.2): any `5xx` SMTP reply, or
`aiosmtplib.SMTPRecipientsRefused` where every refused code is `>= 500`, is permanent
(`smtp_rejected`); a `4xx` reply, a timeout, or a connection error is transient and left to
`PendingRecoverer`. An authentication failure (`535`) is deliberately classified **transient**
rather than permanent, logged at ERROR: it is a configuration problem, not a recipient problem, and
fixing the credentials inside the retry window lets the notification through instead of failing a
notification for a mistake in `.env`.

## Consequences

Any SMTP-speaking provider works without provider-specific code — no vendor SDK, no vendor-specific
error mapping beyond ordinary SMTP reply codes. `SMTP_PASSWORD` is never logged: `SmtpSender.__init__`
sets the `aiosmtplib` logger to `WARNING`, because it logs protocol lines, AUTH included, at `DEBUG`.
Real sending has no automated test that talks to a real mail server — spec section 10.3 covers it
against an in-process `aiosmtpd` server standing in for one, which exercises the same protocol and
the same `aiosmtplib` code path without any real network delivery or real credentials. Verifying
delivery to an actual mailbox is a manual step through the playground (spec section 12, gate 7), not
something the test suite can claim on its own.
