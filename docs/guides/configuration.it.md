[English](configuration.md) | **Italiano**

# Configurazione

Ogni servizio legge le proprie impostazioni dalle variabili d'ambiente. `docker-compose.yml`
imposta i valori di cui lo stack ha bisogno; i segreti per la consegna reale provengono da un file
`.env` locale accanto a esso.

## Consegna reale con `.env`

Entrambi i canali sono **simulati per impostazione predefinita**: nulla esce dalla tua
macchina. Per inviare
realmente:

```bash
cp .env.example .env      # .env is git-ignored; never commit it
# edit .env, then recreate the two delivery services:
docker compose up -d --wait
```

Un valore vuoto mantiene quel canale simulato. Verifica il risultato:

```bash
curl -s http://localhost:8004/version   # "delivery_mode": "smtp" or "simulated"
curl -s http://localhost:8005/version   # "delivery_mode": "bot_api" or "simulated"
```

Il playground mostra le stesse modalità come badge.

### Telegram

1. Su Telegram, scrivi a **@BotFather**, invia `/newbot` e copia il token che ti fornisce.
2. Apri una chat con il tuo nuovo bot e inviagli un messaggio qualsiasi (un bot non può scriverti
   per primo).
3. Trova il tuo chat id: apri `https://api.telegram.org/bot<TOKEN>/getUpdates` in un browser e
   leggi `result[0].message.chat.id` — un numero come `123456789` (le chat di gruppo sono negative,
   ad es. `-1001234567890`).
4. Metti il token in `.env` come `TELEGRAM_BOT_TOKEN=...`, ricrea lo stack, quindi invia una
   notifica con `"channel": "telegram"` e il tuo chat id come `recipient`.

Il `recipient` **è** il chat id; non esiste un'impostazione separata per il chat id
([ADR 0026](../adr/0026-telegram-recipient-is-the-chat-id.md)). Tratta il token come una password:
i servizi non lo scrivono mai nei loro log.

### Email (SMTP)

Funziona con qualsiasi server SMTP. `SMTP_FROM` è obbligatorio non appena viene impostato
`SMTP_HOST`: senza di esso email-service rifiuta di avviarsi e non diventa mai healthy.

| Provider | `SMTP_HOST` | `SMTP_PORT` | `SMTP_SECURITY` | Note |
|---|---|---|---|---|
| Gmail | `smtp.gmail.com` | `587` | `starttls` | Richiede una **password per le app** (account Google → Sicurezza → Verifica in due passaggi → Password per le app), non la password normale |
| Outlook / Microsoft 365 | `smtp.office365.com` | `587` | `starttls` | L'account deve consentire SMTP AUTH |
| Qualsiasi provider con TLS implicito | il tuo host | `465` | `ssl` | |
| Server di test locale (Mailpit, MailHog) | `host.docker.internal` | `1025` | `none` | Cattura la posta in un'interfaccia web invece di consegnarla; lascia vuoti nome utente e password |

```dotenv
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=you@gmail.com
SMTP_PASSWORD=abcd efgh ijkl mnop
SMTP_FROM=you@gmail.com
SMTP_SECURITY=starttls
```

Cosa succede in caso di fallimento ([ADR 0029](../adr/0029-real-email-through-generic-smtp.md)):

| Risultato SMTP | Esito |
|---|---|
| Messaggio accettato | `COMPLETED` |
| Destinatario rifiutato o qualsiasi altro 5xx | `FAILED` / `smtp_rejected`, immediatamente |
| 4xx, timeout, errore di connessione | Riprovato dal recupero; `FAILED` / `max_retries_exceeded` dopo circa due minuti |
| Errore di autenticazione (535) | Come sopra, più un ERROR nei log: `SMTP authentication failed` — correggi le credenziali entro la finestra dei tentativi e il messaggio viene comunque inviato |

## Destinatari riservati

Due regole si applicano **prima** di qualsiasi invio reale, indipendentemente dal contenuto di
`.env` ([ADR 0030](../adr/0030-reserved-recipients-are-always-simulated.md)):

1. Un destinatario che contiene `fail` (in qualsiasi combinazione di maiuscole/minuscole) finisce
   sempre in `FAILED` / `simulated_failure`, e non viene inviato nulla. Questo include indirizzi
   dall'aspetto reale come `failla@gmail.com`.
2. Un destinatario **riservato** viene sempre consegnato dal sender simulato:
   - email a `example.com`, `example.org`, `example.net` o i loro sottodomini, oppure a qualsiasi
     dominio che termina in `.example`, `.invalid` o `.test` (nomi RFC 2606 che non possono mai
     essere caselle reali);
   - un chat id di Telegram che inizia con `sim-`.

Per questo ogni esempio del README, ogni test end-to-end e ogni test di carico resta simulato anche
su uno stack con credenziali reali. Per inviare realmente, usa un indirizzo o un chat id reale.

## Tutte le variabili

`—` significa obbligatorio senza valore predefinito. Il file compose imposta ogni valore obbligatorio.

### Comuni a ogni servizio

| Variabile | Predefinito | Significato |
|---|---|---|
| `SERVICE_NAME` | il nome del servizio | Compare in ogni riga di log e in `/health` |
| `SERVICE_VERSION` | `1.0.0` | Riportato da `/version` |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Servizi con database e Redis

I servizi notification, routing, email e telegram.

| Variabile | Predefinito | Significato |
|---|---|---|
| `DATABASE_URL` | — | `postgresql+asyncpg://user:password@host:port/db` (serve anche a configuration-service) |
| `REDIS_URL` | `redis://redis:6379/0` | Connessione a Redis Streams |
| `CONSUMER_POLL_INTERVAL_MS` | `500` | Per quanto tempo un consumer resta bloccato in attesa di nuove voci nello stream |
| `OUTBOX_POLL_INTERVAL_MS` | `500` | Ogni quanto l'outbox publisher cerca eventi non ancora pubblicati |
| `OUTBOX_BATCH_SIZE` | `100` | Eventi pubblicati per ogni iterazione dell'outbox |
| `PENDING_TIMEOUT_MS` | `30000` | Per quanto tempo una voce deve restare senza conferma prima che il recupero la rivendichi |
| `PENDING_MAX_RETRIES` | `3` | Tentativi di recupero prima di arrendersi con `max_retries_exceeded` |
| `RECOVERY_POLL_INTERVAL_MS` | `5000` | Ogni quanto il recupero cerca voci rimaste bloccate |

### notification-service

| Variabile | Predefinito | Significato |
|---|---|---|
| `PROCESSING_TIMEOUT_MINUTES` | `5` | Il watchdog fa fallire una notifica bloccata in `PROCESSING` più a lungo di questo valore |
| `WATCHDOG_INTERVAL_SECONDS` | `60` | Ogni quanto viene eseguito il watchdog |

### routing-service

| Variabile | Predefinito | Significato |
|---|---|---|
| `CONFIGURATION_SERVICE_URL` | `http://configuration-service:8000` | Dove chiedere se un canale è abilitato |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout di quella chiamata; un timeout è considerato transitorio e viene riprovato |

### email-service

| Variabile | Predefinito | Significato |
|---|---|---|
| `DELIVERY_LATENCY_MS_MAX` | `500` (compose imposta `2000`) | Limite massimo del ritardo casuale del sender simulato |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout di ogni operazione SMTP |
| `SMTP_HOST` | vuoto | Vuoto significa consegna simulata |
| `SMTP_PORT` | `587` | |
| `SMTP_USERNAME` | vuoto | Vuoto significa nessuna autenticazione |
| `SMTP_PASSWORD` | vuoto | Mai registrato nei log |
| `SMTP_FROM` | vuoto | Obbligatorio quando `SMTP_HOST` è impostato |
| `SMTP_SECURITY` | `starttls` | `starttls`, `ssl` o `none` |

### telegram-service

| Variabile | Predefinito | Significato |
|---|---|---|
| `DELIVERY_LATENCY_MS_MAX` | `500` (compose imposta `2000`) | Limite massimo del ritardo casuale del sender simulato |
| `HTTP_TIMEOUT_SECONDS` | `5.0` | Timeout della chiamata alla Bot API |
| `TELEGRAM_BOT_TOKEN` | vuoto | Vuoto significa consegna simulata. Mai registrato nei log |

### configuration-service

Solo le variabili comuni e `DATABASE_URL`. I due canali, `email` e `telegram`, vengono creati
già abilitati dalla sua prima migrazione del database.

### gateway

| Variabile | Predefinito | Significato |
|---|---|---|
| `NOTIFICATION_SERVICE_URL` | `http://notification-service:8000` | Destinazione di inoltro per `/api/v1/notifications*` |
| `CONFIGURATION_SERVICE_URL` | `http://configuration-service:8000` | Destinazione di inoltro per `/api/v1/channels*` |
| `GATEWAY_TIMEOUT_SECONDS` | `10.0` | Trascorso questo tempo il Gateway risponde `504` |

### Playground e test end-to-end

Girano sulla tua macchina, non in un container, e puntano alle porte pubblicate.

| Variabile | Predefinito |
|---|---|
| `GATEWAY_URL` | `http://localhost:8000` |
| `NOTIFICATION_URL` | `http://localhost:8001` |
| `ROUTING_URL` | `http://localhost:8002` |
| `CONFIGURATION_URL` | `http://localhost:8003` |
| `EMAIL_URL` | `http://localhost:8004` |
| `TELEGRAM_URL` | `http://localhost:8005` |

## Modificare un valore

Modifica `docker-compose.yml` (o `.env` per i segreti) e ricrea il servizio interessato:
`docker compose up -d --wait <service>`. Per i valori di temporizzazione, ricorda che interagiscono
tra loro: il recupero impiega circa `PENDING_TIMEOUT_MS × (PENDING_MAX_RETRIES + 1)` per arrendersi,
e questo valore dovrebbe restare ben al di sotto di `PROCESSING_TIMEOUT_MINUTES`, così una consegna
si arrende con il suo motivo preciso prima che il watchdog la faccia fallire con il generico
`processing_timeout`.
