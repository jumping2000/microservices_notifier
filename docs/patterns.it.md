[English](patterns.md) | **Italiano**

# Pattern

Ogni pattern qui sotto risponde a tre domande: cosa fa, in quale file si trova e cosa si rompe senza
di esso — con il test che lo dimostra. Un pattern descritto senza il suo test è esattamente il tipo
di documentazione che questo progetto vuole evitare.

## Outbox

**Cosa fa.** Scrive un cambiamento di stato di dominio e il suo evento in uscita in un'unica
transazione di database, così i due non possono mai essere in disaccordo: o esistono entrambi, la
riga e l'evento, o non esiste nessuno dei due. Un ciclo separato in background sposta poi l'evento
dalla tabella outbox allo stream Redis, al di fuori di quella transazione.

**Dove si trova.** `shared/notification_shared/outbox.py` (`OutboxRepository.save`, che
deliberatamente non fa commit — partecipa alla transazione del chiamante), e
`shared/notification_shared/publisher.py` (`OutboxPublisher`, il poll loop che legge le righe in
sospeso, chiama `XADD` e le segna come pubblicate — identico in ogni servizio con un outbox: notification, routing, email e telegram). Ogni
servizio materializza la propria tabella `outbox` a partire da `OutboxMixin`
(`shared/notification_shared/models.py`), come da ADR 0012.

**Cosa si rompe senza di esso.** Se il cambiamento di stato e la pubblicazione dell'evento fossero
due operazioni separate — ad esempio un `INSERT notifications` seguito direttamente da `XADD` — un
crash tra le due produce una notifica accettata senza che nessun evento venga mai pubblicato: il
client ha ricevuto `202 Accepted`, la riga esiste, e nessun sistema a valle saprà mai che è
successo. La notifica resta bloccata silenziosamente a `CREATED` per sempre, senza alcun errore da
nessuna parte.

**Il test.**
`tests/integration/test_outbox.py::test_a_rolled_back_transaction_leaves_no_outbox_row` dimostra
l'altra metà della stessa garanzia: salva una riga outbox, fa rollback invece di commit, e verifica
che la riga non esista. Poiché `save()` non fa mai commit da sola, un rollback della transazione del
chiamante si porta via anche la riga outbox — esattamente come deve essere, così una scrittura di
dominio fallita non può mai lasciarsi dietro un evento orfano.

## Consumer idempotenti

**Cosa fa.** Prima di agire su un evento, ogni consumer controlla se ha già elaborato quella coppia
`(event_id, consumer_group)`, e dopo aver agito, registra di averlo fatto — nella stessa transazione
della scrittura di dominio. Un consumer che vede lo stesso evento due volte non fa nulla la seconda
volta, a parte confermarlo (ack).

**Dove si trova.** `shared/notification_shared/idempotency.py` (`IdempotencyRepository.is_processed`
/ `.mark_processed`), basato sulla tabella `processed_events` materializzata da
`ProcessedEventMixin`. Ogni consumer handler di ogni servizio chiama `is_processed` prima di fare
qualunque lavoro e `mark_processed` nello stesso commit del cambiamento di stato — *tranne* dove
l'handler deve fare I/O lento prima di poter decidere quale sia il cambiamento di stato stesso.
I due consumer di Notification Service (`services/notification-service/app/workers/routed_consumer.py`
e `results_consumer.py`) tengono tutti e quattro gli elementi — il controllo, il cambiamento di
stato, la riga outbox e il marcaggio — in un'unica transazione, perché nessuno dei due fa nulla di
più lento di una scrittura locale tra il controllo e la decisione. `notification_consumer.py` di
Routing Service e `routed_consumer.py` di Email Service no: ciascuno apre una prima transazione,
breve, per eseguire `is_processed`, la chiude, poi esegue l'operazione lenta durante la quale
nessuna transazione deve restare aperta — una chiamata HTTP a Configuration Service, o la sleep
che simula la latenza di consegna — prima di aprire una seconda transazione per il cambiamento di
stato, la riga outbox e il marcaggio. Vedi ADR 0023 per capire perché questo è corretto e non un
bug, e il costo che si porta dietro.

**Cosa si rompe senza di esso.** La consegna "almeno una volta" (at-least-once, il pattern
successivo) garantisce che ci saranno duplicati — l'outbox publisher stesso può fare due volte la
`XADD` dello stesso evento se va in crash tra la `XADD` e il marcaggio della riga come pubblicata.
Senza un controllo di idempotenza, un evento `notification.routed` duplicato farebbe inviare di
nuovo l'email a Email Service, oppure farebbe inserire a Routing Service una seconda riga `routes`
(intercettata solo dal vincolo di sicurezza `UNIQUE (notification_id)` dell'ADR 0006, che trasforma
un bug silenzioso in un rumoroso `IntegrityError` invece di impedire a monte il tentativo di
consegna duplicato).

**Il test.**
`tests/integration/test_publisher.py::test_a_crash_between_xadd_and_the_mark_causes_a_duplicate` è
la prova che i duplicati accadono davvero: forza `mark_published` a sollevare un'eccezione dopo che
la `XADD` è già riuscita, conferma che l'evento è comunque presente una sola volta sullo
stream, poi lascia ripartire il publisher e conferma che lo *stesso* `event_id` ora appare due volte
sullo stream. Quel test è la prova dell'at-least-once; l'idempotenza è ciò che rende innocuo, a
valle, il duplicato che esso crea.

## "Almeno una volta", non "esattamente una volta"

**Cosa fa.** La piattaforma garantisce che l'evento di ogni notifica accettata venga pubblicato
*almeno* una volta — mai zero volte — ma non tenta di garantire che venga pubblicato *esattamente*
una volta. I duplicati sono una possibilità accettata e prevista, non un bug da eliminare alla
fonte.

**Dove si trova.** È una proprietà della combinazione descritta sopra: `OutboxPublisher.publish_once`
in `shared/notification_shared/publisher.py` segna le righe come pubblicate solo *dopo* che `XADD`
ha avuto successo, quindi un crash nel mezzo lascia la riga in sospeso e viene ripubblicata
all'iterazione successiva — per progetto, non per caso.

**Perché è accettabile qui.** La consegna "esattamente una volta" (exactly-once) attraverso un confine di rete non è
raggiungibile senza una transazione distribuita che copra Redis e Postgres, che questa piattaforma
non ha e non sta cercando di costruire — è esplicitamente fuori perimetro (vedi i limiti noti in
`README.md`). La combinazione di at-least-once e consumer idempotenti ottiene la stessa garanzia *effettiva* — un
cambiamento di stato per evento — senza bisogno di quella transazione distribuita, al costo di far
svolgere ai consumer il lavoro di idempotenza da soli invece di ottenerlo gratuitamente dal broker.

**Il test.** Lo stesso `test_a_crash_between_xadd_and_the_mark_causes_a_duplicate` dimostra
direttamente la metà "almeno una volta": l'evento non viene perso dal crash, viene duplicato. Non
esiste un test che dimostri l'exactly-once, perché la piattaforma non lo promette.

## Consumer group a offset 0

**Cosa fa.** Ogni consumer group viene creato con `XGROUP CREATE <stream> <group> 0 MKSTREAM` —
offset `0`, mai `$` — così un gruppo creato *dopo* la pubblicazione di un evento riceve comunque
quell'evento.

**Dove si trova.** `RedisStreamConsumer.ensure_group` in `shared/notification_shared/streams.py`.

**Cosa si rompe senza di esso — la race condition al primo avvio.** Con `docker compose up`, tutti
i servizi partono contemporaneamente. Se i gruppi venissero creati a `$` (solo i nuovi
messaggi da quel momento in poi), un notification-service che pubblica `notification.created` pochi
millisecondi prima che routing-service abbia finito di creare il proprio gruppo `routing-service`
perderebbe quell'evento in modo permanente — il pattern Outbox garantisce che un evento venga
*pubblicato*, non che qualcuno lo *ascolti*.

**La conseguenza del reset dei volumi.** L'offset `0` significa che un gruppo appena creato (o
ricreato) riproduce *l'intera* storia dello stream, non solo le novità. Se i volumi Postgres vengono
azzerati mentre i dati AOF di Redis sopravvivono, ogni consumer riesegue ogni evento mai pubblicato
contro tabelle `processed_events` vuote — cosa sicura solo perché i consumer sono idempotenti, ma
significa che `docker compose down -v` deve azzerare entrambi gli archivi insieme (vedi ADR 0002 e
la sezione "Reset" di `docs/local-development.md`).

**Il test.**
`tests/integration/test_streams.py::test_a_group_created_at_offset_zero_sees_earlier_messages`
pubblica un evento, *poi* crea il gruppo, e verifica che il gruppo lo legga comunque — il docstring
del test stesso nota che con `$` questo test non leggerebbe nulla.

## Transizioni monotone con condizione di guardia

**Cosa fa.** Lo stato di una notifica si muove solo in avanti — `CREATED → PROCESSING →
COMPLETED/FAILED`, mai all'indietro e, una volta terminale, non si riapre più — e quella regola è imposta
nella clausola `WHERE` dell'istruzione `UPDATE` stessa, non leggendo lo stato corrente in Python e
decidendo se scrivere.

**Dove si trova.** Due implementazioni dello stesso schema, una per ogni servizio che possiede una
colonna di stato: `NotificationRepository.advance_status` in
`services/notification-service/app/repositories/notification.py`, chiamato sia da `RoutedConsumer`
(guardia: `status = 'CREATED'`) sia da `ResultsConsumer` (guardia: `status IN ('CREATED',
'PROCESSING')`); e `RouteRepository.set_status` in
`services/routing-service/app/repositories/route.py`, chiamato dal results consumer con guardia
`status = 'PROCESSING'` (solo una route già in corso può essere chiusa). Questo chiude una questione
accantonata nella slice 1: `docs/patterns.md` un tempo citava solo l'implementazione di
notification-service, come se `routes` non avesse una guardia equivalente, quando invece l'ha
sempre avuta.

**La race condition.** `notification-service-routed` (legge `notification.routed`) e
`notification-service-results` (legge `delivery.completed`/`delivery.failed`) sono consumer group
indipendenti senza garanzia di ordinamento tra loro. Una consegna veloce fa sì che l'evento di
completamento possa arrivare ed essere consumato *prima* dell'evento routed — la riga è ancora
`CREATED` quando `ResultsConsumer` vede `DeliveryCompleted`. Vedi ADR 0016 per capire perché
`CREATED → COMPLETED` deve quindi essere una transizione lecita anche se sembra una lacuna nella
macchina a stati: senza di essa, quel completamento anticipato non corrisponderebbe a nessuna riga,
e poiché l'handler comunque conferma (ack) e segna l'evento come elaborato indipendentemente da
`rowcount`, la notifica non verrebbe mai corretta e resterebbe a `CREATED` per sempre nonostante
fosse in realtà riuscita.

**Cosa si rompe senza la guardia.** Un'implementazione ingenua — leggere lo stato corrente, decidere
il nuovo in Python, scriverlo — ha una finestra tra la lettura e la scrittura in cui l'altro
consumer group può agire. L'evento `notification.routed` arrivato in ritardo sovrascriverebbe
allora incondizionatamente una notifica già `COMPLETED` riportandola a `PROCESSING`: una notifica
conclusa riaperta silenziosamente, visibilmente sbagliata per un client che fa polling.

**Il test.**
`services/notification-service/tests/test_status_transitions.py::test_a_result_arriving_before_the_routed_event_is_not_overwritten`
pubblica entrambi gli eventi, consuma prima l'evento dei risultati (fuori ordine), verifica che lo
stato sia `COMPLETED`, poi consuma l'evento routed arrivato in ritardo e verifica che lo stato sia
*ancora* `COMPLETED`. Il docstring del test stesso dichiara direttamente: senza la guardia
`status = 'CREATED'` sull'handler routed, questo test finirebbe a `PROCESSING`.

## Idempotenza per consumer group

**Cosa fa.** Il registro di idempotenza usa come chiave `(event_id, consumer_group)`, non
`event_id` da solo, così lo stesso evento viene tracciato in modo indipendente da ogni gruppo che
lo legge.

**Dove si trova.** Il vincolo `UNIQUE (event_id, consumer_group)` su `ProcessedEventMixin`
(`shared/notification_shared/models.py`), e ogni punto di chiamata che passa entrambi i valori a
`IdempotencyRepository.is_processed` / `.mark_processed`.

**Cosa si rompe senza di esso.** `delivery.failed` viene letto da due consumer group indipendenti:
`notification-service-results` (che fa fallire la notifica) e `routing-service-results` (che fa
fallire la route). Se l'idempotenza fosse basata sulla sola chiave `event_id`, qualunque gruppo
elaborasse per primo un dato evento `RoutingFailed` o `DeliveryFailed` lo segnerebbe come elaborato
per *tutti* — il secondo gruppo vedrebbe `is_processed() == True` e salterebbe un evento su cui in
realtà non ha mai agito. La notifica o la route non raggiungerebbero mai, silenziosamente, il
proprio stato terminale.

**Il test.**
`tests/integration/test_idempotency.py::test_the_same_event_is_tracked_per_consumer_group` segna
un evento come elaborato per `ConsumerGroup.ROUTING` e poi verifica che sia `True` per quel gruppo
ma ancora `False` per `ConsumerGroup.EMAIL` — dimostrando che lo stesso `event_id` viene tracciato
in modo indipendente per gruppo, non globalmente.

## Recupero tramite claim su inattività

**Cosa fa.** Trova le voci in sospeso nello stream di un consumer andato in crash in base al tempo di
inattività anziché al nome del consumer, e le rivendica (claim) su un consumer attivo così da
poterle gestire nel modo consueto.

**Dove si trova.** `shared/notification_shared/recovery.py` (`PendingRecoverer`), costruito su
`RedisStreamConsumer.get_pending` / `.claim` in `shared/notification_shared/streams.py`.
Un'istanza per servizio, collegata nel `lifespan` accanto a `OutboxPublisher`; i consumer di ogni
servizio restano espliciti ed espongono solo `handle` e `give_up` perché il recoverer li chiami.

**Cosa si rompe senza di esso.** I nomi dei consumer sono gli hostname dei container, che cambiano
a ogni riavvio. Un consumer che va in crash a metà di `handle` lascia la propria voce in sospeso
sotto un nome che non esiste più; senza il claim basato sul tempo di inattività, nulla troverebbe
mai più quella voce, e la notifica dietro di essa resterebbe per sempre dove l'ha lasciata il crash.

**Il test.** `tests/integration/test_recovery.py::test_a_stranded_message_is_claimed_and_completed`
e l'e2e
`tests/e2e/test_slice2.py::test_recovery_routes_a_notification_stranded_by_a_configuration_outage`.

## Numero massimo di tentativi e resa

**Cosa fa.** Limita quante volte il recupero riprova una voce che continua a fallire. Superato
`PENDING_MAX_RETRIES`, il consumer si arrende invece di riprovare all'infinito — scrive il proprio
record di fallimento e, dove c'è qualcuno da avvisare, un evento di fallimento, poi la voce viene
confermata (ack) e scartata.

**Dove si trova.** `PendingRecoverer._recover` in `shared/notification_shared/recovery.py` decide
quando arrendersi (`should_give_up`, confrontando `processed_events.fail_count` con
`max_retries`); il `give_up(session, envelope)` proprio di ogni consumer —
`services/routing-service/app/workers/notification_consumer.py`,
`services/email-service/app/workers/routed_consumer.py`,
`services/telegram-service/app/workers/routed_consumer.py` — decide cosa scrivere.

**Cosa si rompe senza di esso.** Senza un limite, un poison message — un messaggio che non può mai
riuscire, come un payload malformato su cui un consumer non può agire — verrebbe riprovato
all'infinito, occupando per sempre un posto nell'elenco delle voci in sospeso. Senza che `give_up` scriva
qualcosa, l'alternativa ai tentativi illimitati (lo scarto silenzioso) lascerebbe la notifica
bloccata nello stato in cui l'ha lasciata l'ultimo tentativo, senza alcun errore visibile da nessuna
parte a un client o a un operatore.

**Il test.** `tests/integration/test_recovery.py::test_the_last_failed_attempt_gives_up_and_acks`
e
`services/routing-service/tests/test_notification_consumer.py::test_a_persistent_outage_ends_in_routing_failed_after_max_retries`.

## Watchdog per il `PROCESSING` bloccato

**Cosa fa.** Una rete di sicurezza indipendente dal recupero: chiude qualunque notifica rimasta
`PROCESSING` più a lungo di `PROCESSING_TIMEOUT_MINUTES`, qualunque sia il motivo per cui non ha
mai ricevuto un risultato di consegna.

**Dove si trova.** `services/notification-service/app/workers/watchdog.py` (`Watchdog`), che chiama
`NotificationRepository.fail_stale_processing` — un `UPDATE` con condizione di guardia che usa l'orologio del
database stesso, senza pubblicare alcun evento (ADR 0028).

**Cosa si rompe senza di esso.** Il recupero rivisita solo le voci ancora in sospeso in un consumer
group. Una notifica può finire in `PROCESSING` senza nulla in sospeso da nessuna parte — per
esempio, il results consumer di Routing Service o di Notification Service si è già arreso
silenziosamente sull'evento che l'avrebbe chiusa (spec 2.6). Senza il watchdog, quella notifica
resta bloccata in `PROCESSING` per sempre, senza più alcun meccanismo che la riesamini.

**Il test.**
`services/notification-service/tests/test_watchdog.py::test_a_stale_processing_notification_is_failed`,
`::test_a_recent_processing_notification_is_left_alone`, e
`::test_a_late_delivery_result_does_not_reopen_it`.

## Destinatari riservati

**Cosa fa.** Riconosce i destinatari riservati per convenzione a documentazione e test — i domini e
i TLD email dell'RFC 2606, e i chat id Telegram che iniziano con `sim-` — e li consegna sempre
attraverso il sender simulato, qualunque sia la configurazione della piattaforma per la consegna
reale.

**Dove si trova.** `shared/notification_shared/delivery.py` (`is_failure_recipient`,
`is_reserved_recipient`), controllati in quest'ordine da entrambi i consumer di consegna prima di
qualunque chiamata di rete, e da `tools/playground/loadtest.py::plan_notifications`, che genera
solo destinatari riservati.

**Cosa si rompe senza di esso.** Credenziali reali nel file `.env` di uno sviluppatore per i test
manuali nel playground finirebbero per trapelare in ogni altro percorso che invia una notifica — la
suite e2e, gli esempi `curl` dello stesso README, il test di carico — trasformando un'esecuzione
automatica di test in una raffica di email o messaggi Telegram reali verso qualunque indirizzo quei
percorsi usino.

**Il test.** `tests/unit/test_delivery.py`;
`services/email-service/tests/test_routed_consumer.py::test_a_reserved_recipient_never_reaches_the_configured_sender`;
`services/telegram-service/tests/test_bot_api.py::test_a_sim_chat_id_never_reaches_the_bot_api`.
