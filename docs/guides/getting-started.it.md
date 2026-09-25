[English](getting-started.md) | **Italiano**

# Guida introduttiva

Da un clone appena fatto a una notifica consegnata in circa dieci minuti. Tutto gira in locale in
Docker; non viene inviato nulla di reale finché non aggiungi tu stesso le credenziali (vedi
[Configurazione](configuration.it.md)).

## Prerequisiti

| Strumento | Versione | Perché |
|---|---|---|
| Docker con Compose v2 | Docker Desktop su Windows/macOS, oppure Docker Engine su Linux | Esegue i dodici container |
| `uv` | 0.11 o successivo | Ambiente Python per il playground e i test |
| Git | qualsiasi | Per clonare il repository |

Non serve un'installazione locale di Python: `uv` scarica Python 3.14 al primo utilizzo.

Gli esempi di shell qui sotto usano Bash (Git Bash su Windows). In Windows PowerShell 5.1, `curl` è
un alias di `Invoke-WebRequest` e non accetta questi argomenti: chiama invece `curl.exe`, oppure
usa la variante con `Invoke-RestMethod` mostrata dopo ogni esempio.

## 1. Avvia lo stack

```bash
git clone https://github.com/jumping2000/microservices_notifier.git
cd microservices_notifier
docker compose up --build -d --wait
```

La prima build richiede qualche minuto. `--wait` restituisce il controllo solo quando ogni
healthcheck ha esito positivo. Controlla il risultato:

```bash
docker compose ps --format "{{.Service}} {{.Status}}"
```

Tutte e dodici le righe dovrebbero terminare con `(healthy)`:

| Container | Porta host | Ruolo |
|---|---|---|
| `gateway` | 8000 | Unico punto di ingresso, `/api/v1/*` |
| `notification-service` | 8001 | Possiede le notifiche e il loro stato |
| `routing-service` | 8002 | Decide se e dove instradare una notifica |
| `configuration-service` | 8003 | Interruttori on/off dei canali |
| `email-service` | 8004 | Consegna email (simulata, oppure SMTP reale) |
| `telegram-service` | 8005 | Consegna Telegram (simulata, oppure Bot API reale) |
| `notification-db` … `telegram-db` | 5433–5437 | Un database Postgres per servizio |
| `redis` | 6379 | Redis Streams, l'unico message broker |

## 2. Invia la tua prima notifica

```bash
curl -s -X POST http://localhost:8000/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{"channel": "email", "recipient": "john@example.com", "subject": "Welcome", "body": "Hello John!"}'
```

```json
{"notification_id": "f0dee1d5-2c10-405f-8fa5-731a8e082de4", "status": "CREATED"}
```

PowerShell:

```powershell
$r = Invoke-RestMethod -Method Post -Uri http://localhost:8000/api/v1/notifications `
  -ContentType 'application/json' `
  -Body '{"channel":"email","recipient":"john@example.com","subject":"Welcome","body":"Hello John!"}'
$r.notification_id
```

Il `202 Accepted` significa che la notifica è stata salvata e il suo evento `NotificationCreated` è
stato accodato. La consegna avviene in modo asincrono.

## 3. Seguila fino alla fine

Interroga la notifica in polling con l'id che hai ricevuto:

```bash
curl -s http://localhost:8000/api/v1/notifications/f0dee1d5-2c10-405f-8fa5-731a8e082de4
```

PowerShell: `Invoke-RestMethod http://localhost:8000/api/v1/notifications/$($r.notification_id)`

Ripeti la richiesta qualche volta. In pochi secondi lo `status` segue la sequenza
`CREATED → PROCESSING → COMPLETED`.
`john@example.com` è un indirizzo **riservato**, quindi l'email viene simulata anche se in seguito
configuri SMTP reale — vedi [Configurazione](configuration.it.md#destinatari-riservati).

Prova anche i due esiti di fallimento:

- Un destinatario che contiene `fail`, per esempio `fail@example.com`, termina in `FAILED` con
  `fail_reason: "simulated_failure"`.
- Disabilita prima il canale, poi invia: la notifica termina in `FAILED` con
  `fail_reason: "channel_disabled"` senza mai raggiungere `PROCESSING`.

  ```bash
  curl -s -X PUT http://localhost:8000/api/v1/channels/email -H 'Content-Type: application/json' -d '{"enabled": false}'
  # ...send, poll...
  curl -s -X PUT http://localhost:8000/api/v1/channels/email -H 'Content-Type: application/json' -d '{"enabled": true}'
  ```

Il [riferimento API](api-reference.it.md) elenca ogni endpoint, campo ed errore. Ogni servizio
espone anche una Swagger UI interattiva su `/docs`; quella del Gateway è su
http://localhost:8000/docs.

## 4. Apri il playground

Il playground è una console Streamlit per usare la piattaforma senza `curl`:

```bash
uv sync --all-packages --group playground
uv run --group playground streamlit run tools/playground/app.py
```

Si apre su http://localhost:8501. La [guida al playground](playground.it.md) illustra entrambe le
pagine.

Su Windows, se `uv sync` fallisce con `Accesso negato` / `Access is denied` su `.venv\Scripts`,
chiudi VS Code (i suoi language server Python e Ruff tengono aperto l'ambiente) ed eseguilo di nuovo
da un terminale esterno.

## 5. Ferma e riavvia

| Obiettivo | Comando |
|---|---|
| Ferma i container, mantieni tutti i dati | `docker compose stop` (riavvia con `docker compose start`) |
| Rimuovi i container, mantieni tutti i dati | `docker compose down` |
| Cancella **tutti** i dati e riparti da zero | `docker compose down -v` |

Cancella sempre Postgres e Redis insieme con `down -v`, mai un volume da solo: vedi
[Operatività](../operations.it.md#reset-in-sicurezza) per il motivo.

## Prossimi passi

- [Guida al playground](playground.it.md) — la console e il test di carico
- [Configurazione](configuration.it.md) — ogni variabile, e come inviare email e messaggi Telegram reali
- [Riferimento API](api-reference.it.md) — endpoint, stati, motivi di fallimento, errori
- [Operatività](../operations.it.md) — ispezionare stream e database, troubleshooting
- [Architettura](../architecture.it.md) — come si incastrano i servizi tra loro, e perché
