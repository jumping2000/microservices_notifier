[English](playground.md) | **Italiano**

# Guida al playground

Il playground è un'app Streamlit per usare a mano la piattaforma in esecuzione. Ha due pagine: la
**Console**, per inviare notifiche e osservarle muoversi lungo la saga, e il **Load test**, per
lanciare centinaia di notifiche simulate e verificare che ognuna termini nello stato giusto.

## Avvialo

Lo stack deve essere in esecuzione (vedi [Guida introduttiva](getting-started.it.md)).

```bash
uv sync --all-packages --group playground     # once, or after pulling new code
uv run --group playground streamlit run tools/playground/app.py
```

Apri http://localhost:8501. Le due pagine sono nella barra di navigazione a sinistra: **app** è la
Console, **Load test** è la seconda pagina. Il playground comunica con il Gateway su `GATEWAY_URL`
(predefinito `http://localhost:8000`) e con la porta di ciascun servizio per l'health check; vedi
[Configurazione](configuration.it.md#playground-e-test-end-to-end) per puntarlo altrove.

## Console

### Barra laterale

- **Services** — una riga per servizio: verde quando `/health` risponde `UP`, con la versione e i
  controlli su database/Redis; rossa con `unreachable` quando il servizio non risponde. La riga del
  Gateway mostra solo la salute del Gateway stesso.
- **Delivery mode** — per email e telegram: 🟢 **SIMULATED**, oppure 🟠 **REAL (smtp)** /
  **REAL (bot_api)** quando sono configurate credenziali reali (lette dal `/version` di ciascun
  servizio).
- **Channels** — un toggle per canale. Azionare un toggle chiama `PUT /api/v1/channels/{name}`; ha
  effetto sulla prossima notifica instradata.

### Invia una notifica

Compila **Channel**, **Recipient** (un indirizzo email, oppure un chat id Telegram), **Subject** e
**Body**, poi premi **Send**.

- Se il canale è in modalità REAL e il destinatario non è riservato né contiene `fail`, il modulo
  si blocca con un avviso: spunta **I understand this may send a real message** e premi di nuovo
  **Send**. La casella si svuota dopo ogni invio, quindi ogni invio reale richiede una conferma
  separata. Se il playground non riesce a leggere la modalità di un canale, la tratta come reale.
- Un destinatario che contiene `fail` mostra una nota: terminerà in `FAILED` /
  `simulated_failure` per scelta progettuale, e non viene inviato nulla.
- I destinatari riservati (`@example.com`, `.test`, `sim-…`, vedi
  [Configurazione](configuration.it.md#destinatari-riservati)) sono sempre simulati, anche in
  modalità REAL.

### Scenari

Cinque pulsanti inviano ciascuno una notifica già pronta, sempre a destinatari riservati, quindi
non inviano mai nulla di reale:

| Pulsante | Timeline attesa |
|---|---|
| Email: happy path | `CREATED → PROCESSING → COMPLETED` |
| Email: channel disabled | Disabilita l'email, invia, `CREATED → FAILED` (`channel_disabled`), poi riabilita l'email una volta che la notifica è arrivata allo stato finale |
| Email: delivery failure | `CREATED → PROCESSING → FAILED` (`simulated_failure`) |
| Telegram: happy path | `CREATED → PROCESSING → COMPLETED` |
| Telegram: delivery failure | `CREATED → PROCESSING → FAILED` (`simulated_failure`) |

### Timeline della saga

Dopo ogni invio, la timeline interroga la notifica ogni mezzo secondo ed elenca ogni stato
osservato con il tempo trascorso e il `fail_reason`, finché lo stato non diventa finale. Se non
arriva nulla di finale entro 30 secondi smette di fare polling e lo segnala: il recupero riprova un
messaggio bloccato dopo 30 s, e il watchdog fa fallire una notifica ferma in `PROCESSING` dopo 5
minuti (vedi [Operatività](../operations.it.md#come-funziona-il-recupero)).

### Tabella delle notifiche

Le 50 notifiche più recenti, aggiornate ogni due secondi, con filtri per stato e canale.

## Test di carico

Il test di carico invia molte notifiche insieme attraverso il Gateway, segue ognuna fino al suo
stato finale, e ti dice se ognuna è terminata dove doveva. **Non invia mai nulla di reale**: genera
da sé i propri destinatari, tutti riservati — `load-N@example.com` per l'email e `sim-load-N` per
Telegram, con `-fail` aggiunto alla quota di fallimento — e non accetta nessun destinatario da te.

### Parametri

| Campo | Predefinito | Significato |
|---|---|---|
| Notifications | 200 | Quante inviarne (1–2000) |
| Concurrency | 10 | Richieste in corso contemporaneamente (1–50) |
| Telegram share | 0.3 | Frazione inviata a Telegram; il resto va all'email |
| Failing share | 0.1 | Frazione il cui destinatario contiene `fail` e deve terminare in `FAILED` / `simulated_failure` |
| Settle timeout (s) | 300 | Per quanto tempo continuare il polling in cerca di stati finali (10–600) |

Un test ha due fasi, ciascuna con la propria barra di avanzamento: **submit** (tutte le `POST`) e
**settle** (polling di ogni notifica ogni 250 ms finché non è finale o scade il timeout). Non
toccare la pagina durante un test: qualunque interazione lo riavvia.

### Leggere il report

- **Verdict** — verde quando ogni notifica è terminata nello stato atteso: `FAILED` /
  `simulated_failure` per la quota di fallimento, `COMPLETED` per il resto. Se è rosso, elenca quante sono
  terminate nello stato sbagliato, quante non sono mai arrivate allo stato finale, e quante sono
  state rifiutate in fase di submit.
- **Accepted / s** — quanto velocemente il Gateway ha accettato le `POST`.
- **POST latency** e **End-to-end latency** (p50, p95, max) — la latenza end-to-end va dalla `POST`
  al primo polling che ha visto uno stato finale, quindi la sua risoluzione è 250 ms.
- **Outcomes** — conteggi per stato finale, per esempio `COMPLETED`, `FAILED/simulated_failure`, e
  `UNSETTLED` per le notifiche non ancora finali al timeout.
- **Submit errors** — ogni `POST` che non ha restituito `202`, per codice di stato o tipo di
  errore.
- **Chart** — completamenti al secondo durante il test.

### Dimensionare un test

Lo stack compose esegue **un consumer per canale**, e la consegna simulata attende un tempo
casuale di 0–2 s per messaggio, quindi ogni canale consegna circa un messaggio al secondo. I numeri
misurano questa configurazione didattica deliberata, non il limite massimo del codice. Un test di
riferimento con i valori predefiniti: 200/200 nello stato atteso, circa 70 accettate/s, end-to-end p50
circa 41 s e p95 circa 120 s.

- Il timeout di attesa (Settle timeout) deve coprire il backlog dell'email: circa **quota email × totale × 2 s**. Con
  un timeout breve le notifiche rimaste compaiono come `UNSETTLED` — ancora in coda, non persi.
- Oltre circa **450 notifiche** il backlog supera la durata di `PROCESSING_TIMEOUT_MINUTES` (5
  minuti): il watchdog fa fallire le notifiche ancora in coda con `processing_timeout`, e il verdetto
  diventa rosso. La pagina avvisa prima di un test del genere e spiega il risultato dopo. È il limite
  dello stack a un consumer, non una perdita di messaggi né un errore di instradamento.

## Invio di un messaggio reale

1. Configura il canale in `.env` (vedi
   [Configurazione](configuration.it.md#consegna-reale-con-env)) e ricrea lo stack.
2. Controlla la barra laterale: il canale mostra 🟠 **REAL**.
3. In **Send a notification**, inserisci **il tuo** indirizzo o chat id, spunta la conferma e
   invia.
4. Seguila nella timeline e controlla la tua casella di posta o Telegram.

Ricorda che la consegna è "almeno una volta" (at-least-once): se un servizio va in crash, o scatta
un timeout dopo che il server ha già accettato il messaggio, lo stesso messaggio può essere inviato
due volte.
