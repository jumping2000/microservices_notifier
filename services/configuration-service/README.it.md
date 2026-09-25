[English](README.md) | **Italiano**

# configuration-service

Contiene lo stato di abilitazione/disabilitazione dei canali a runtime, letto da Routing Service
una volta per ogni notifica. Non pubblica eventi e non esegue worker in background: la
configurazione è stato del read model, non uno stream di eventi di dominio.

## Tabelle

| Tabella | Scopo |
|---|---|
| `channels` | `name` (chiave primaria), `enabled` — popolata da una migrazione dati Alembic con `email = true`, `telegram = true` (ADR 0009). Nessun `outbox`, nessun `processed_events`, nessun timestamp. |

## Stream consumati

Nessuno. Configuration Service non tocca affatto Redis.

## Stream pubblicati

Nessuno.

## Variabili d'ambiente

| Variabile | Predefinito | Note |
|---|---|---|
| `DATABASE_URL` | — (obbligatoria) | `postgresql+asyncpg://...` |
| `SERVICE_NAME` | `configuration-service` | |
| `SERVICE_VERSION` | `1.0.0` | |
| `LOG_LEVEL` | `INFO` | |

## Endpoint HTTP

| Endpoint | Note |
|---|---|
| `GET /channels` | Ogni canale configurato e se è abilitato |
| `GET /channels/{name}` | `404` se il canale non è configurato |
| `PUT /channels/{name}` | Body `{enabled: bool}`; ha effetto dalla prossima decisione di instradamento — Routing Service non mantiene alcuna cache locale |
| `GET /health` | Controlla solo Postgres (nessuna dipendenza da Redis); `503` se è fermo |
| `GET /version`, `/docs`, `/redoc` | |

## Note sui livelli

`ChannelService` (`app/services/channel.py`) scrive e fa il commit direttamente dopo aver
modificato una riga letta tramite `ChannelRepository`, che è di sola lettura (`list_all`, `get`).
Questo è l'unico servizio della piattaforma in cui il livello di repository non possiede anche la
scrittura — vedi la sezione delle conseguenze dell'ADR 0012 per capire perché questo viene
registrato come un'incoerenza nota anziché presentato come un pattern.
