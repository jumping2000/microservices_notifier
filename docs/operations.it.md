[English](operations.md) | **Italiano**

# Operatività

Avviare, ispezionare e risolvere i problemi dello stack. Tutti i comandi vengono eseguiti dalla
radice del repository. Redis e i database si raggiungono tramite `docker compose exec`, quindi non
serve alcun client locale.

## Ciclo di vita

| Obiettivo | Comando |
|---|---|
| Compilare e avviare tutto, attendere che sia healthy | `docker compose up --build -d --wait` |
| Stato di ogni container | `docker compose ps` |
| Seguire i log di un servizio | `docker compose logs -f notification-service` |
| Riavviare un servizio | `docker compose restart email-service` |
| Ricreare un servizio dopo averne modificato la configurazione | `docker compose up -d --wait email-service` |
| Fermare tutto, mantenendo i dati | `docker compose stop` |
| Rimuovere i container, mantenendo i dati | `docker compose down` |
| Cancellare tutti i dati | `docker compose down -v` — vedi [Reset in sicurezza](#reset-in-sicurezza) |

## Salute

```bash
curl -s http://localhost:8000/api/v1/health       # gateway: its own status only
for port in 8001 8002 8003 8004 8005; do curl -s http://localhost:$port/health; echo; done
```

Un servizio risponde `503` con `"status": "DOWN"` e il controllo fallito (`database` o `redis`)
quando una dipendenza non è raggiungibile; Docker allora lo segna come unhealthy.

## Seguire una notifica

Ogni riga di log è un oggetto JSON con `service`, `correlation_id`, e nei worker anche `event_id`,
`event_type`, `notification_id` e `consumer_group`. Il correlation id che invii (o quello generato
dal Gateway) segue la notifica attraverso ogni servizio:

```bash
curl -s -X POST http://localhost:8000/api/v1/notifications \
  -H 'Content-Type: application/json' -H 'X-Correlation-ID: trace-me' \
  -d '{"channel": "email", "recipient": "john@example.com", "body": "Hi"}'

docker compose logs --no-log-prefix | grep trace-me | sort
```

`--no-log-prefix` fa sì che ogni riga inizi con il proprio `timestamp` JSON, così `sort` mette
l'intera saga in ordine temporale: il Gateway che inoltra la richiesta, notification-service che la
accetta, routing-service che interroga configuration-service e la instrada, email-service che la
consegna, e notification-service che raggiunge `COMPLETED`.

## Ispezionare gli stream Redis

```bash
docker compose exec redis redis-cli XLEN notification.created          # entries in a stream
docker compose exec redis redis-cli XINFO GROUPS notification.routed   # groups: pending, lag
docker compose exec redis redis-cli XPENDING notification.created routing-service   # summary
docker compose exec redis redis-cli XPENDING notification.created routing-service IDLE 30000 - + 10
docker compose exec redis redis-cli XINFO CONSUMERS notification.created routing-service
```

| Stream | Consumer group |
|---|---|
| `notification.created` | `routing-service` |
| `notification.routed` | `email-service`, `telegram-service`, `notification-service-routed` |
| `delivery.completed` | `notification-service-results`, `routing-service-results` |
| `delivery.failed` | `notification-service-results`, `routing-service-results` |

In `XINFO GROUPS`, `pending` conta le voci consegnate a un consumer ma non ancora confermate,
mentre `lag` conta le voci che nessuno ha ancora letto. Entrambi dovrebbero tornare a `0` quando la
piattaforma è inattiva. Un `pending` che resta sopra zero più a lungo di `PENDING_TIMEOUT_MS` è
proprio ciò di cui si occupa il recupero.

`XINFO CONSUMERS` elenca un consumer per ogni riavvio del container, perché i nomi dei consumer
sono gli hostname dei container. I nomi vecchi con `pending 0` sono residui innocui.

## Ispezionare i database

Utente e password sono `notif` / `notif`. Un database per servizio:

| Container | Database | Tabelle principali |
|---|---|---|
| `notification-db` | `notificationdb` | `notifications`, `outbox`, `processed_events` |
| `routing-db` | `routingdb` | `routes`, `outbox`, `processed_events` |
| `configuration-db` | `configurationdb` | `channels` |
| `email-db` | `emaildb` | `email_delivery`, `outbox`, `processed_events` |
| `telegram-db` | `telegramdb` | `telegram_delivery`, `outbox`, `processed_events` |

```bash
# notifications by outcome
docker compose exec notification-db psql -U notif -d notificationdb -c \
  "SELECT status, fail_reason, count(*) FROM notifications GROUP BY 1, 2 ORDER BY 3 DESC;"

# events not yet published by the outbox (should be 0 or close to it)
docker compose exec notification-db psql -U notif -d notificationdb -c \
  "SELECT count(*) FROM outbox WHERE NOT published;"

# the idempotency ledger: what each consumer group processed, gave up on, or is retrying
docker compose exec routing-db psql -U notif -d routingdb -c \
  "SELECT consumer_group, status, count(*), max(fail_count) FROM processed_events GROUP BY 1, 2;"

# one notification across services
docker compose exec email-db psql -U notif -d emaildb -c \
  "SELECT status, fail_reason, sent_at FROM email_delivery WHERE notification_id = '<id>';"
```

In `processed_events`, `status` vale `PROCESSED`, `FAILING` (in corso di nuovi tentativi;
`fail_count` indica quante volte) oppure `FAILED_PERMANENT` (il consumer si è arreso). `fail_count` viene
mantenuto anche dopo il successo come storico di quanti tentativi un evento ha richiesto.

## Come funziona il recupero

Quando un consumer non riesce a completare un messaggio — il suo servizio è andato in crash, oppure
una dipendenza è ferma — il messaggio resta in sospeso (`pending`) in Redis. Ogni
servizio con consumer esegue un recoverer che:

1. ogni `RECOVERY_POLL_INTERVAL_MS` (5 s) cerca le voci inattive da più tempo di
   `PENDING_TIMEOUT_MS` (30 s), a prescindere dal consumer a cui appartenevano, e le rivendica (claim);
2. riesegue lo stesso handler del percorso normale; in caso di successo la voce viene confermata;
3. in caso di fallimento incrementa `fail_count`; dopo `PENDING_MAX_RETRIES` (3) tentativi falliti
   **si arrende**: routing, email e telegram registrano un fallimento con motivo
   `max_retries_exceeded` e lo pubblicano, così la notifica finisce in `FAILED` invece di restare
   bloccata.

Con i valori predefiniti, la resa arriva dopo circa due minuti. A parte questo, il **watchdog** di
notification-service fa fallire ogni notifica ancora `PROCESSING` dopo `PROCESSING_TIMEOUT_MINUTES`
(5) con `processing_timeout`. Un risultato di consegna che arriva dopo non la riapre.

## Risoluzione dei problemi

| Sintomo | Causa probabile | Cosa fare |
|---|---|---|
| La notifica resta `CREATED` | routing-service o configuration-service è fermo | Controlla `/health` su 8002 e 8003. Quando tornano attivi, il recupero la instrada entro circa 35 s; se restano fermi, finisce in `FAILED` / `max_retries_exceeded` |
| La notifica resta `PROCESSING` | email-service o telegram-service è fermo o fallisce in modo transitorio | Controlla la loro salute e i log. Il recupero riprova; il watchdog la fa fallire dopo 5 minuti con `processing_timeout` |
| `FAILED` / `channel_disabled` | Il canale è stato disattivato | `PUT /api/v1/channels/<name>` con `{"enabled": true}`, poi invia di nuovo |
| `FAILED` / `simulated_failure` su un indirizzo reale | Il destinatario contiene `fail` | È voluto (vedi [Configurazione](guides/configuration.it.md#destinatari-riservati)); usa un altro indirizzo |
| Un messaggio email o Telegram reale non arriva mai ma la notifica è `COMPLETED` | Il destinatario è riservato (`@example.com`, `.test`, `sim-…`), quindi è stato simulato | Usa un destinatario reale e non riservato |
| `FAILED` / `smtp_rejected` | Il server SMTP ha rifiutato il destinatario o il messaggio | Controlla l'indirizzo; cerca il rifiuto in `docker compose logs email-service` |
| `FAILED` / `max_retries_exceeded` su email con SMTP reale | Credenziali errate, oppure il server è stato irraggiungibile per due minuti | Cerca `SMTP authentication failed` nei log di email-service; correggi `.env` ed esegui `docker compose up -d --wait` |
| `FAILED` / `telegram_rejected` | Chat id errato, oppure non hai mai inviato un messaggio al bot, oppure lo hai bloccato | Invia un messaggio al bot, ricontrolla il chat id con `getUpdates` |
| email-service unhealthy dopo aver modificato `.env` | `SMTP_HOST` impostato senza `SMTP_FROM` | `docker compose logs email-service` mostra l'errore di validazione; aggiungi `SMTP_FROM` |
| Il Gateway risponde `502 BAD_GATEWAY` | notification-service o configuration-service non è raggiungibile | `docker compose ps`; riavvia il servizio |
| Il Gateway risponde `504 GATEWAY_TIMEOUT` | Il servizio ha impiegato più di 10 s | Controlla i suoi log e la salute del database |
| Verdetto rosso del test di carico con `UNSETTLED` | **Settle timeout** troppo breve per il backlog | Alzalo a circa `email share × total × 2 s`, vedi la [guida al playground](guides/playground.it.md#dimensionare-un-test) |
| Verdetto rosso del test di carico con `FAILED/processing_timeout` | Un test così grande (oltre ~450) che le notifiche in coda superano il timeout del watchdog | Usa un totale più piccolo; è il limite dello stack compose a consumer singolo, non una perdita di messaggi |
| Tutto viene rielaborato dopo un reset | È stato azzerato un solo archivio | Fai sempre il reset con `docker compose down -v` |
| `uv sync` fallisce con `Accesso negato` / `Access is denied` su `.venv\Scripts` | I language server di VS Code tengono occupato l'ambiente | Chiudi VS Code, esegui `uv sync` da un terminale esterno |
| `port is already allocated` su `up` | Un altro programma usa 8000–8005, 5433–5437 o 6379 | Fermalo, oppure cambia la porta host in `docker-compose.yml` |

## Reset in sicurezza

```bash
docker compose down -v
docker compose up --build -d --wait
```

`down -v` elimina **ogni** volume: tutti e cinque i database Postgres e i dati di Redis. Azzerali
insieme, mai uno solo. I consumer group iniziano a leggere gli stream dall'inizio (offset `0`,
[ADR 0002](adr/0002-consumer-groups-start-at-offset-zero.md)), quindi un database svuotato accanto a
uno stream Redis sopravvissuto riprodurrebbe l'intera storia dello stream in tabelle vuote e
rielaborerebbe ogni notifica mai inviata — compreso, con credenziali reali, il nuovo invio di messaggi
reali.
