# Cluster e topic

Questo capitolo tratta la panoramica del cluster, l’elenco dei topic, la creazione e l’eliminazione dei topic, la schermata del topic e la sua configurazione. Sotto ogni connessione del pannello [[ui:sidebar.connections]] ci sono i collegamenti [[ui:sidebar.cluster]] e [[ui:sidebar.topics]]. Ognuno si apre in una scheda propria.

## Panoramica del cluster {#cluster-overview}

[[ui:cluster.overview.title]] mostra lo stato del cluster al momento del caricamento. Fai clic su [[ui:cluster.overview.refresh]] per ricaricarla.

- `Cluster ID` — l’identificatore del cluster.
- [[ui:cluster.overview.controller]] — l’ID del broker che il cluster indica come controller. Nei cluster KRaft i client non comunicano direttamente con i controller: qui il cluster indica un broker casuale, non il controller attivo, e il broker può cambiare da un aggiornamento all’altro.
- [[ui:cluster.overview.brokers]] — il numero di broker.
- [[ui:cluster.overview.topicsInternal]] — tutti i topic, con il numero di topic interni tra parentesi.
- [[ui:cluster.overview.partitions]] — il numero di partizioni di tutti i topic.
- `Under-replicated` — le partizioni in cui non tutte le repliche sono sincronizzate (nell’ISR). Di solito un broker è inattivo o non riesce a tenere il passo.
- `Offline` — le partizioni senza leader. Nessuno può scriverci né leggerle.

Un valore diverso da zero in `Under-replicated` o `Offline` viene evidenziato.

Sotto si trova la tabella dei broker con `ID` e [[ui:cluster.overview.address]]. Il broker indicato nel campo [[ui:cluster.overview.controller]] è contrassegnato da [[ui:cluster.overview.controllerBadge]]. Nei cluster KRaft si tratta dello stesso broker casuale, non del controller attivo. Fai clic su un broker per vederne la configurazione sotto la tabella. La configurazione dei broker è di sola consultazione. Usa [[ui:common.config.searchParam]] per trovare un parametro per nome e [[ui:common.config.onlyChanged]] per nascondere i parametri con valori predefiniti. Un’icona a forma di lucchetto contrassegna i parametri di sola lettura.

## Elenco dei topic {#topic-list}

[[ui:sidebar.topics]] elenca i topic del cluster. Fai clic su una riga per aprire il topic in una scheda propria.

Le colonne sono [[ui:topics.list.colName]], [[ui:topics.list.colPartitions]], `RF` (fattore di replica), [[ui:topics.list.colMessages]], `cleanup.policy` e [[ui:topics.list.colUnderRepl]] (il numero di partizioni sotto-replicate). Il numero di messaggi è approssimativo: è la differenza tra gli offset finali e iniziali delle partizioni. La compattazione e i marcatori di transazione rendono il numero reale più piccolo. Il numero di messaggi e `cleanup.policy` vengono caricati solo per le righe che raggiungi scorrendo.

Controlli della barra degli strumenti:

- [[ui:topics.list.search]] — digita parte di un nome per trovare i topic; maiuscole e minuscole non contano. Per cercare con un’espressione regolare, racchiudila tra barre, per esempio `/^orders\..*/`.
- [[ui:topics.list.internal]] — mostra i topic interni, come `__consumer_offsets`. Sono contrassegnati da [[ui:topics.list.internalBadge]].
- L’elenco dell’aggiornamento automatico — [[ui:topics.list.autoRefreshOff]], [[ui:topics.list.every10]] o [[ui:topics.list.every30]].
- [[ui:topics.list.refresh]] — ricarica subito l’elenco e il numero di messaggi.

Accanto alla barra degli strumenti, l’app mostra quanti topic corrispondono alla ricerca rispetto al totale. Sulle connessioni con l’opzione [[ui:common.readOnly]] attiva, i pulsanti [[ui:topics.list.create]] e [[ui:topics.list.select]] sono nascosti.

## Creare un topic {#create-topic}

1. Nell’elenco dei topic, fai clic su [[ui:topics.list.create]]. Sopra l’elenco si apre il modulo [[ui:topics.create.title]].
2. In [[ui:topics.create.name]] inserisci il nome. Può contenere lettere latine, cifre, `.`, `_` e `-`, fino a 249 caratteri. Il modulo avvisa se il nome contiene sia `.` sia `_`: i nomi delle metriche Kafka di questi topic possono entrare in conflitto con quelli di altri topic.
3. Imposta il numero di [[ui:topics.create.partitions]]. Le partizioni vengono lette in parallelo e l’ordine è garantito solo all’interno di una partizione. Il numero può essere aumentato in seguito, ma mai ridotto.
4. Imposta il [[ui:topics.create.rf]], cioè quante copie di ogni partizione conservano i broker. Non può superare il numero di broker, che il modulo mostra sotto il campo. Il valore predefinito è 3, oppure il numero di broker se sono meno. `-1` usa il valore predefinito del broker.
5. Scegli un profilo in [[ui:topics.create.preset]]:
   - [[ui:topics.create.presetDelete]] — i messaggi vecchi vengono eliminati in base al tempo.
   - `compact` — viene conservato solo l’ultimo valore per chiave.
   - `compact + delete` — entrambe le cose.
   - [[ui:topics.create.presetTest]] — i messaggi vengono conservati per un’ora.
6. Facoltativamente, aggiungi [[ui:topics.create.extraConfigs]], una per riga, per esempio `retention.ms=86400000`. Hanno la precedenza sul profilo. Le righe che iniziano con `#` vengono ignorate.
7. Fai clic su [[ui:topics.create.submit]].

Quando il modulo è valido, mostra il comando `kafka-topics.sh` equivalente. Puoi copiarlo, per esempio per creare lo stesso topic in un altro cluster.

Su una connessione prod confermi la creazione digitando il nome della connessione. Una volta creato il topic, si apre la sua scheda.

## Eliminare i topic {#delete-topics}

Per eliminare un topic, aprilo e fai clic su [[ui:topics.actions.deleteTopic]]. Digita il nome del topic per confermare e fai clic su [[ui:topics.delete.confirm]]. La scheda del topic si chiude.

Per eliminare più topic contemporaneamente:

1. Nell’elenco dei topic, fai clic su [[ui:topics.list.select]]. Accanto ai nomi compaiono le caselle di controllo.
2. Seleziona i topic. I topic interni non si possono selezionare.
3. Fai clic sul pulsante di eliminazione, che mostra il numero di topic selezionati.
4. Controlla l’elenco dei nomi, digita il loro numero per confermare e fai clic su [[ui:topics.delete.confirm]].

[[ui:topics.list.cancelSelect]] nasconde le caselle di controllo.

Su una connessione prod, entrambi i modi di eliminazione chiedono anche di digitare il nome della connessione.

L’eliminazione non si può annullare: i dati vanno persi. Se alcuni topic non vengono eliminati, per esempio per mancanza di diritti, l’app li elenca con il motivo. Gli altri topic vengono eliminati.

## Schermata del topic {#topic-details}

La scheda del topic mostra il numero di partizioni, il fattore di replica e il numero approssimativo di messaggi. I pulsanti in alto:

- [[ui:topics.details.refresh]] — ricarica i dati del topic.
- [[ui:topics.details.produce]] — apre la schermata [[ui:sidebar.producer]] con questo topic. Vedi [Producer](producer.md).
- [[ui:topics.details.consume]] — avvia una sessione consumer per questo topic. Richiede Pro. Vedi [Sessioni consumer](consumer-sessions.md).

Sulle connessioni con l’opzione [[ui:common.readOnly]] attiva, entrambi i pulsanti sono disattivati.

La seconda riga contiene le azioni che modificano il topic. È nascosta sulle connessioni in sola lettura e per i topic interni.

- [[ui:topics.actions.addPartitions]] — inserisci il nuovo totale e fai clic su [[ui:topics.actions.addSubmit]]. Il numero di partizioni non può essere ridotto. Dopo un aumento, i messaggi con la stessa chiave possono finire in un’altra partizione, quindi l’ordine per chiave non è più garantito.
- [[ui:topics.actions.purge]] — elimina tutti i messaggi fino alla fine attuale delle partizioni selezionate. Il topic resta. Seleziona le partizioni, digita il nome del topic e fai clic su [[ui:topics.actions.purgeConfirm]]. Lo svuotamento funziona solo se `cleanup.policy` include `delete`; altrimenti il pulsante è disattivato e viene indicato il motivo. Per un topic `compact,delete`, lo svuotamento elimina anche gli ultimi valori delle chiavi.
- [[ui:topics.actions.deleteTopic]] — vedi [Eliminare i topic](topics.md#delete-topics).

Su una connessione prod, ciascuna di queste azioni chiede anche il nome della connessione.

Le schede sottostanti:

- [[ui:topics.details.tabMessages]] — la consultazione dei messaggi. Vedi [Consultare i messaggi](browse.md).
- [[ui:topics.details.tabPartitions]] — una tabella con ogni partizione: [[ui:topics.details.colLeader]], [[ui:topics.details.colReplicas]], `ISR` (repliche sincronizzate), gli offset iniziale (`Low`) e finale (`High`) e il numero di messaggi. Una partizione senza leader mostra [[ui:topics.details.noLeader]].
- [[ui:topics.details.tabConfigs]] — i parametri del topic. Vedi [Configurazione del topic](topics.md#topic-config).
- [[ui:topics.details.tabGroups]] — i consumer group che hanno offset sottoposti a commit per questo topic, con [[ui:topics.groups.colState]], [[ui:topics.groups.colPartitions]] e [[ui:topics.groups.colLag]]. Fai clic su un gruppo per aprirlo. Vedi [Consumer group](groups.md).

## Configurazione del topic {#topic-config}

La scheda [[ui:topics.details.tabConfigs]] elenca tutti i parametri del topic in tre colonne:

- [[ui:common.config.colParam]] — il nome del parametro. I nomi in grassetto sono impostati esplicitamente: sul topic o nella configurazione del broker.
- [[ui:common.config.colValue]] — il valore attuale. I valori sensibili sono nascosti come `••••••`.
- [[ui:common.config.colSource]] — da dove proviene il valore. Per esempio, `DynamicTopic` è impostato su questo topic, `StaticBroker` e `DynamicBroker` provengono dalla configurazione del broker e `Default` è il valore predefinito di Kafka.

Usa [[ui:common.config.searchParam]] per trovare un parametro per nome e [[ui:common.config.onlyChanged]] per mostrare solo i parametri in grassetto. Un’icona a forma di lucchetto contrassegna i parametri di sola lettura. L’icona `?` accanto a un nome spiega il parametro.

## Modificare la configurazione {#edit-config}

1. Nella scheda [[ui:topics.details.tabConfigs]], fai clic su [[ui:topics.details.edit]]. Sulle connessioni con l’opzione [[ui:common.readOnly]] attiva, questo pulsante è nascosto.
2. Modifica i valori nella colonna [[ui:common.config.colValue]]. I parametri di sola lettura e quelli sensibili non si possono modificare. Per rimuovere il valore proprio del topic, fai clic su [[ui:common.config.reset]]: si applica allora il valore del broker. Un valore vuoto non è ammesso.
3. Fai clic su [[ui:common.config.review]]. L’app mostra ogni modifica come valore vecchio → valore nuovo; [[ui:common.config.defaultValue]] significa che il valore viene ripristinato.
4. Fai clic su [[ui:common.config.apply]], oppure su [[ui:common.config.back]] per continuare a modificare.

Nel cluster non cambia nulla finché non applichi le modifiche. [[ui:common.config.discard]] annulla tutte le modifiche. Finché ci sono modifiche non salvate, il pulsante [[ui:topics.details.closeEditor]] è disattivato. Su una connessione prod confermi le modifiche digitando il nome della connessione.

Vengono inviati solo i parametri che hai modificato. Gli altri parametri del topic restano invariati. Il broker controlla i valori e rifiuta con un errore quelli non validi.

## Parametri del topic {#config-parameters}

I topic Kafka hanno decine di parametri. Le icone `?` nella tabella della configurazione li spiegano tutti. Questi sono i più usati:

- `cleanup.policy` — che cosa succede ai dati vecchi. `delete` rimuove i segmenti vecchi in base al tempo o alla dimensione. `compact` conserva solo l’ultimo valore per chiave. `compact,delete` fa entrambe le cose.
- `retention.ms` — per quanto tempo vengono conservati i messaggi, in millisecondi. Il valore predefinito è `604800000` (7 giorni); `-1` li conserva per sempre. Funziona se `cleanup.policy` include `delete`.
- `retention.bytes` — la dimensione massima di ogni partizione prima che i segmenti vecchi vengano eliminati. Il valore predefinito `-1` significa nessun limite di dimensione.
- `segment.ms` e `segment.bytes` — quando il broker inizia un nuovo segmento del log. Conservazione e compattazione agiscono su segmenti interi, quindi un messaggio può restare più a lungo di `retention.ms` finché il suo segmento non viene chiuso.
- `min.insync.replicas` — quante repliche devono confermare una scrittura con `acks=all`. Di solito 2 con fattore di replica 3.
- `max.message.bytes` — il batch più grande che il broker accetta per questo topic. Il valore predefinito è circa 1 MB. I producer devono consentire la stessa dimensione (`max.request.size`).
- `compression.type` — come il broker memorizza i messaggi. `producer` mantiene la compressione del producer.
- `message.timestamp.type` — `CreateTime` (l’ora impostata dal producer) o `LogAppendTime` (l’ora in cui il broker ha scritto il messaggio). È l’ora mostrata durante la consultazione dei messaggi.
- `delete.retention.ms` — per quanto tempo i marcatori di eliminazione (messaggi con valore null) restano in un topic compattato. Il valore predefinito è 1 giorno.

Alcuni parametri esistono solo nelle versioni più recenti di Kafka, altri sono deprecati. L’icona `?` lo segnala dove conta. I parametri del tiered storage, come `remote.storage.enable`, funzionano solo se il cluster lo supporta.
