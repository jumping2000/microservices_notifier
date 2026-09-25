**English** | [Italiano](README.it.md)

# Documentation

Documentation of the Universal Notification Platform, an educational event-driven microservices
platform built on Redis Streams. Every document for readers exists in English and Italian; design
records (ADRs, specs, plans, execution ledgers) are English only, because they are dated records of
decisions.

## Using the platform

| Document | For |
|---|---|
| [Getting started](guides/getting-started.md) | From a fresh clone to a delivered notification |
| [Playground guide](guides/playground.md) | The manual test console and the simulated load test |
| [Configuration](guides/configuration.md) | Every environment variable; real email and Telegram delivery |
| [API reference](guides/api-reference.md) | Gateway endpoints, statuses, fail reasons, errors |
| [Operations](operations.md) | Inspecting streams and databases, recovery, troubleshooting, resetting |

## Understanding and developing it

| Document | For |
|---|---|
| [Project README](../README.md) | Overview, quick start, stream topology, known limitations |
| [Architecture](architecture.md) | Services, boundaries, why choreography, layering |
| [Event flows](event-flows.md) | Every stream, event payload and saga sequence diagram |
| [Patterns](patterns.md) | Outbox, idempotent consumers, recovery, guarded transitions — each with the test that proves it |
| [Local development](local-development.md) | uv workspace, running the tests, debugging in VS Code |
| Service READMEs | [notification](../services/notification-service/README.md) · [routing](../services/routing-service/README.md) · [configuration](../services/configuration-service/README.md) · [email](../services/email-service/README.md) · [telegram](../services/telegram-service/README.md) · [gateway](../gateway/README.md) |

## Design records (English only)

| Record | Content |
|---|---|
| [ADRs](adr/) | One file per design decision, 0001–0030 |
| [Slice 1 spec](superpowers/specs/2026-09-12-notification-platform-design.md) · [Slice 2 spec](superpowers/specs/2026-09-24-notification-platform-slice-2-design.md) | What each slice had to build |
| [Slice 1 plan](superpowers/plans/2026-09-12-notification-platform-slice-1.md) · [Slice 2 plan](superpowers/plans/2026-09-24-notification-platform-slice-2.md) | How it was built, task by task |
| [Slice 1 ledger](superpowers/slice-1-execution-ledger.md) · [Slice 2 ledger](superpowers/slice-2-execution-ledger.md) | Every ruling taken while building |
| [Original prompt](prompt_microservice_notifier_v3.md) | The requirements the project started from |

## Terminology

The Italian documents keep the established English term where Italian developers use it as is,
and translate the rest consistently:

| English | Italian documents | Meaning |
|---|---|---|
| stream, consumer group, outbox, worker, payload | kept in English | Redis Streams and pattern vocabulary |
| Gateway, watchdog, healthcheck, correlation id | kept in English | Component and header names |
| recovery | recupero | Claiming and retrying entries left pending |
| give up / give-up | resa (arrendersi) | Stopping after the last retry and publishing a failure |
| at-least-once delivery | consegna "almeno una volta" (at-least-once) | A message can arrive twice, never zero times |
| idempotent consumer | consumer idempotente | Processing the same event twice has one effect |
| reserved recipient | destinatario riservato | Always simulated, whatever is configured |
| load test | test di carico | The playground's second page |
| settle | arrivare allo stato finale | Reach `COMPLETED` or `FAILED` |
