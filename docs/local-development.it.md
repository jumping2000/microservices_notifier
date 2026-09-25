[English](local-development.md) | **Italiano**

# Sviluppo locale

## Workspace uv

L'intera piattaforma è un unico workspace uv, dichiarato alla radice del repository:

```toml
[tool.uv.workspace]
members = ["gateway", "services/*", "shared"]
```

`uv sync --all-packages --group playground`, eseguito dalla radice del repository, produce un
**unico `.venv` alla radice** che contiene tutti i servizi, il Gateway e la libreria condivisa
(`notification-shared`) insieme. È l'interprete configurato in VS Code
(`.vscode/settings.json`) e l'ambiente in cui girano tutti i livelli di test.

**Se `uv sync` fallisce con `failed to remove directory ...\.venv\Scripts: Accesso negato (os error
5)`** su Windows, i language server Python e Ruff di VS Code tengono aperto il venv. Chiudi
VS Code, riesegui `uv sync` da un terminale esterno, poi riapri VS Code. Non eliminare `.venv` a
mano mentre VS Code è in esecuzione.

**`uv pip install` non funziona qui.** La versione di `uv` installata è la 0.11.7, che rifiuta
`uv pip install` come interfaccia legacy compatibile con pip. Usa solo:

- `uv add <package>` — aggiunge una dipendenza e aggiorna il `pyproject.toml` corrispondente
- `uv sync` / `uv sync --all-packages` — installa/aggiorna l'ambiente fissato dal lockfile
- `uv run <command>` — esegue qualunque cosa (pytest, ruff, alembic, uvicorn) dentro il `.venv` del
  workspace

Mai `pip`, `venv`, `virtualenv`, `conda` o `poetry` in questo progetto.

## Eseguire i test

Otto invocazioni, eseguite dalla radice del repository (alcuni test fanno riferimento a percorsi
relativi al repository, come l'`alembic.ini` di un servizio):

```bash
uv run pytest tests/unit
uv run pytest tests/integration
PYTHONPATH=services/configuration-service uv run pytest services/configuration-service/tests
PYTHONPATH=services/notification-service  uv run pytest services/notification-service/tests
PYTHONPATH=services/routing-service       uv run pytest services/routing-service/tests
PYTHONPATH=services/email-service         uv run pytest services/email-service/tests
PYTHONPATH=services/telegram-service      uv run pytest services/telegram-service/tests
PYTHONPATH=gateway                        uv run pytest gateway/tests
```

A questi si aggiunge la suite del playground, che richiede il suo gruppo di dipendenze
`playground` al posto dell'ambiente predefinito:

```bash
PYTHONPATH=tools/playground uv run --group playground pytest tools/playground/tests
```

**Perché ogni servizio ha bisogno del proprio processo.** Ogni servizio definisce un modulo
chiamato `app` (`services/<name>/app/`), e lo stesso vale per il Gateway (`gateway/app/`). Se due
suite girassero in un unico processo pytest, l'`import app...` della seconda punterebbe al
pacchetto `app` che Python ha importato per primo, qualunque sia — quindi ognuna gira come processo a sé,
con `PYTHONPATH` puntato su quella singola directory. I due livelli alla radice del repository
(`tests/unit`, `tests/integration`) non hanno bisogno di alcun override di `PYTHONPATH` perché
importano solo `notification_shared`, che si trova comunque nel path del `.venv` del workspace.

`tests/unit` non richiede alcun I/O. `tests/integration` e ogni suite `services/*/tests` richiedono
Docker in esecuzione — avviano veri container Postgres e Redis tramite testcontainers. La suite del
gateway no: non ha database né Redis, quindi `httpx.MockTransport` basta a sostituire i servizi a
valle. `tests/e2e` richiede l'intero stack già avviato (vedi sotto).

`.vscode/tasks.json` incapsula questi comandi (più lint e il ciclo di vita di compose) come task di
VS Code; "test: integration (all services)" è quello che itera su tutte le suite dei servizi in un
unico task, e usa la sintassi della shell POSIX — su Windows è vincolato a `bash.exe` tramite
l'`options.shell.executable` di quel task.

## Debug di un servizio da VS Code

`.vscode/launch.json` ha una configurazione di debug Uvicorn per ogni servizio, ciascuna puntata sui
corrispondenti Postgres e Redis **containerizzati**, sulle rispettive porte localhost pubblicate,
invece che sugli hostname interni ai container usati da `docker-compose.yml`. Per usarne una:

1. Avvia solo l'infrastruttura, non i container dell'applicazione:

   ```bash
   docker compose up -d redis notification-db routing-db configuration-db email-db telegram-db
   ```

   (Questo è il task "compose up (infra only)".)

2. **Applica le migrazioni a mano.** Un servizio avviato in questo modo esegue Uvicorn direttamente
   tramite il debugger, quindi l'`entrypoint.sh` del container (che normalmente esegue `alembic
   upgrade head` prima di avviare Uvicorn) non viene mai eseguito. Applica prima tu stesso le
   migrazioni, con `DATABASE_URL` impostata sulla stessa porta localhost usata dalla configurazione
   di avvio:

   ```bash
   uv run alembic -c services/notification-service/alembic.ini upgrade head
   ```

   (sostituisci il nome del servizio e il suo `alembic.ini`; per telegram-service vale lo
   stesso. Il Gateway non ha database né migrazioni.)

3. Avvia il servizio dal pannello Run and Debug (es. "notification-service"). Resta in ascolto
   sulla propria porta host (`8000` gateway, `8001` notification, `8002` routing, `8003`
   configuration, `8004` email, `8005` telegram) con `--reload` attivo.

**La configurazione di avvio non esegue Alembic.** È l'unica cosa da ricordare: se sotto il debugger un
servizio non parte e dà l'errore "relation does not exist", le migrazioni non sono state applicate
prima al database di quella porta.

## Playground

`tools/playground/` è la console di test manuale della piattaforma, due pagine Streamlit. Le sue
dipendenze stanno in un gruppo a parte, `playground`, così l'ambiente di test non le porta con sé:

```bash
docker compose up --build -d --wait
uv run --group playground streamlit run tools/playground/app.py   # http://localhost:8501
```

**Console** (`app.py`) pilota a mano lo stack in esecuzione: badge di salute e modalità di consegna
per ogni servizio, gli interruttori dei canali, un modulo di invio, pulsanti a un clic per gli
scenari della saga su entrambi i canali, una timeline live dello stato per l'ultima notifica
inviata, e una tabella delle notifiche filtrabile. Le chiamate su notifiche e canali passano
attraverso il Gateway (`GATEWAY_URL`, predefinito `http://localhost:8000`); salute e `/version`
restano per singolo servizio, perché il Gateway non li aggrega — la console legge le stesse variabili
`NOTIFICATION_URL` / `ROUTING_URL` / `CONFIGURATION_URL` / `EMAIL_URL` / `TELEGRAM_URL` usate da
`tests/e2e`. Quando la modalità configurata del canale selezionato è reale (SMTP o la Bot API) e il
destinatario non è riservato, il modulo avvisa che questo invierà un messaggio reale e chiede di
spuntare una casella di conferma esplicita prima dell'invio. I pulsanti degli scenari e ogni esempio del
README/e2e usano destinatari riservati, quindi restano sempre simulati.

**Test di carico** (`pages/2_Load_test.py`), basato su `loadtest.py` (nessun import di Streamlit,
quindi testabile in modo indipendente): invia un numero configurabile di notifiche attraverso il
Gateway con concorrenza limitata, poi fa polling su ciascuna fino allo stato finale, e riporta
accettate/s, percentili di latenza e un verdetto di correttezza — quante notifiche sono arrivate
allo stato previsto dal loro destinatario, quante sono arrivate allo stato sbagliato, quante non
sono mai arrivate allo stato finale. Ogni destinatario che genera è riservato
(`load-{i}@example.com`, `sim-load-{i}`) — il test di carico non può mai inviare un messaggio reale,
nemmeno contro uno stack con credenziali reali configurate, perché non accetta mai un destinatario
fornito dal chiamante. Il timeout di attesa predefinito della pagina è 300 s (il valore predefinito del motore
stesso, `LoadTestConfig`, è 120 s); un test di grandi dimensioni richiede un timeout di attesa di
circa `email share × total × 2s`, poiché compose esegue un consumer per gruppo e la consegna
simulata attende fino a 2 s.

## Reset

```bash
docker compose down -v
```

azzera **Postgres e Redis insieme** — ogni volume Postgres con nome e il volume dati di Redis
basato su AOF. È obbligatorio, non facoltativo: azzerarne uno senza l'altro provoca un replay. I
consumer group vengono creati a offset `0` (ADR 0002), quindi un gruppo creato su un database
Postgres azzerato, mentre lo stream Redis è sopravvissuto, riproduce l'intera storia di quello
stream su tabelle `processed_events`/di dominio vuote. Ogni controllo di
idempotenza riparte da zero, e ogni notifica, route e consegna mai creata da quando lo stream è
iniziato viene rielaborata. Azzera sempre entrambi gli archivi nello stesso
`docker compose down -v`, mai `docker volume rm` su uno solo.
