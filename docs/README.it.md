[English](README.md) | **Italiano**

# Documentazione

Documentazione della Piattaforma di notifica universale, una piattaforma a microservizi
event-driven a scopo didattico costruita su Redis Streams. Ogni documento per i lettori esiste in
inglese e italiano; i documenti di progettazione (ADR, spec, piani, execution ledger) sono solo in
inglese, perché registrano decisioni datate.

## Usare la piattaforma

| Documento | Per |
|---|---|
| [Guida introduttiva](guides/getting-started.it.md) | Da un clone appena fatto a una notifica consegnata |
| [Guida al playground](guides/playground.it.md) | La console di test manuale e il test di carico simulato |
| [Configurazione](guides/configuration.it.md) | Ogni variabile d'ambiente; consegna reale via email e Telegram |
| [Riferimento API](guides/api-reference.it.md) | Endpoint del Gateway, stati, motivi di fallimento, errori |
| [Operatività](operations.it.md) | Ispezionare stream e database, recupero, troubleshooting, reset |

## Comprenderla e svilupparla

| Documento | Per |
|---|---|
| [README del progetto](../README.it.md) | Panoramica, avvio rapido, topologia degli stream, limiti noti |
| [Architettura](architecture.it.md) | Servizi, confini, perché la coreografia, stratificazione |
| [Flussi di eventi](event-flows.it.md) | Ogni stream, payload degli eventi e diagramma di sequenza della saga |
| [Pattern](patterns.it.md) | Outbox, consumer idempotenti, recupero, transizioni con condizione di guardia — ciascuno con il test che lo dimostra |
| [Sviluppo locale](local-development.it.md) | Workspace uv, esecuzione dei test, debug in VS Code |
| README dei servizi | [notification](../services/notification-service/README.it.md) · [routing](../services/routing-service/README.it.md) · [configuration](../services/configuration-service/README.it.md) · [email](../services/email-service/README.it.md) · [telegram](../services/telegram-service/README.it.md) · [gateway](../gateway/README.it.md) |

## Documenti di progettazione (solo in inglese)

| Documento | Contenuto |
|---|---|
| [ADR](adr/) | Un file per ogni decisione di design, 0001–0030 |
| [Spec slice 1](superpowers/specs/2026-09-12-notification-platform-design.md) · [Spec slice 2](superpowers/specs/2026-09-24-notification-platform-slice-2-design.md) | Cosa doveva costruire ciascuna slice |
| [Piano slice 1](superpowers/plans/2026-09-12-notification-platform-slice-1.md) · [Piano slice 2](superpowers/plans/2026-09-24-notification-platform-slice-2.md) | Come è stata costruita, attività per attività |
| [Ledger slice 1](superpowers/slice-1-execution-ledger.md) · [Ledger slice 2](superpowers/slice-2-execution-ledger.md) | Ogni decisione presa durante la costruzione |
| [Prompt originale](prompt_microservice_notifier_v3.md) | I requisiti da cui è partito il progetto |

## Terminologia

I documenti italiani mantengono il termine inglese consolidato dove gli sviluppatori italiani lo
usano così com'è, e traducono il resto in modo coerente:

| Inglese | Documenti italiani | Significato |
|---|---|---|
| stream, consumer group, outbox, worker, payload | restano in inglese | Vocabolario di Redis Streams e dei pattern |
| Gateway, watchdog, healthcheck, correlation id | restano in inglese | Nomi di componenti e header |
| recovery | recupero | Rivendicare (claim) e riprovare le voci rimaste in sospeso |
| give up / give-up | resa (arrendersi) | Fermarsi dopo l'ultimo tentativo e pubblicare un fallimento |
| at-least-once delivery | consegna "almeno una volta" (at-least-once) | Un messaggio può arrivare due volte, mai zero volte |
| idempotent consumer | consumer idempotente | Elaborare lo stesso evento due volte ha un solo effetto |
| reserved recipient | destinatario riservato | Sempre simulato, qualunque sia la configurazione |
| load test | test di carico | La seconda pagina del playground |
| settle | arrivare allo stato finale | Raggiungere lo stato `COMPLETED` o `FAILED` |
