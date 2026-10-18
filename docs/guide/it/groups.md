# Consumer group

Un consumer group è un insieme di consumer che si dividono la lettura dei topic. Kafka assegna ogni partizione a un membro del gruppo e conserva, per ogni partizione, l’offset di cui il gruppo ha eseguito il commit. Dopo un riavvio, le applicazioni riprendono a leggere da quell’offset.

Apri [[ui:sidebar.groups]] sotto la connessione nel pannello [[ui:sidebar.connections]]. I gruppi di un singolo topic si trovano anche nella scheda [[ui:topics.details.tabGroups]] del topic. Vedi [Schermata del topic](topics.md#topic-details).

## Elenco dei gruppi {#group-list}

L’elenco mostra tutti i consumer group del cluster. Le colonne:

- [[ui:groups.list.colGroup]] — l’ID del gruppo (`group.id`).
- [[ui:groups.list.colState]] — lo stato riportato da Kafka, per esempio `Stable` (i membri stanno leggendo), `Empty` (nessun membro), `PreparingRebalance` o `CompletingRebalance` (le partizioni vengono ridistribuite) e `Dead` (il gruppo non esiste più).
- [[ui:groups.list.colMembers]] — il numero di membri attivi.
- `Assignor` — la strategia usata per distribuire le partizioni, per esempio `range`.
- [[ui:groups.list.colLag]] — il lag totale del gruppo. Vedi [Lag](groups.md#lag).
- [[ui:groups.list.colTopics]] — i topic per cui il gruppo ha offset sottoposti a commit.

Digita in [[ui:groups.list.search]] per mostrare i gruppi il cui ID contiene il testo; maiuscole e minuscole non contano. L’elenco degli stati accanto mostra solo i gruppi in un determinato stato; [[ui:groups.list.allStates]] rimuove questo filtro. Il contatore indica quanti gruppi sono mostrati e quanti ce ne sono in totale.

Membri, assignor e lag vengono caricati solo per le righe visibili, man mano che scorri. Finché non arrivano, la cella mostra `…`. Se non si possono caricare, la cella mostra `—`; passaci sopra con il puntatore per vedere il motivo.

[[ui:groups.list.refresh]] ricarica l’elenco e i suoi dati. Se alcuni broker non hanno risposto, un avviso indica che l’elenco potrebbe essere incompleto.

## Lag {#lag}

Il lag è il numero di messaggi che il gruppo non ha ancora letto. Per ogni partizione è la fine del log meno l’offset sottoposto a commit. Il lag non viene mai mostrato sotto lo zero.

Il lag dipende solo dagli offset sottoposti a commit. Un consumer attivo può aver letto oltre l’ultimo commit, quindi il suo lag reale può essere più piccolo. Quando il gruppo esegue di nuovo il commit, il lag diminuisce.

Una partizione senza offset sottoposto a commit mostra [[ui:groups.lag.noCommit]] e non rientra nel totale. Il lag totale di un gruppo mostra `—` quando il gruppo non ha alcun commit ([[ui:groups.lag.noCommits]]) o quando non è stato possibile leggere gli offset di una partizione ([[ui:groups.lag.unknown]]).

Il lag conta gli offset, non i messaggi. Nei topic con compattazione o transazioni, alcuni offset non contengono messaggi visibili, quindi al gruppo possono restare da leggere meno messaggi di quanto indichi il lag.

I valori sono un’istantanea al momento del caricamento. Per seguire il lag, apri il gruppo e attiva l’aggiornamento automatico.

## Dettagli del gruppo {#group-details}

Fai clic su un gruppo nell’elenco per aprirne la scheda. In alto, un riepilogo mostra lo stato, il numero di membri e l’assignor.

- [[ui:groups.details.autoRefresh]] — [[ui:groups.details.refreshOff]], [[ui:groups.details.refresh5]] o [[ui:groups.details.refresh10]].
- [[ui:groups.details.refresh]] — ricarica subito.
- [[ui:groups.details.join]] — avvia una [sessione consumer](consumer-sessions.md) in questo gruppo. Il modulo della sessione riceve l’ID del gruppo e i topic per cui il gruppo ha commit. Richiede Pro, e il pulsante è disattivato sulle connessioni con l’opzione [[ui:common.readOnly]] attiva; passa il puntatore sul pulsante per vedere il motivo.

[[ui:groups.details.partitions]] elenca le partizioni per cui il gruppo ha commit e quelle assegnate ai suoi membri. Il lag totale del gruppo è mostrato sopra la tabella. Le colonne sono [[ui:groups.details.colTopic]], [[ui:groups.details.colPartition]], `Committed` (l’offset sottoposto a commit), `Log end` (l’offset finale della partizione), [[ui:groups.details.colLag]] e [[ui:groups.details.colMember]] (il client ID del membro che legge la partizione).

[[ui:groups.details.members]] elenca i membri attivi: `Client ID`, [[ui:groups.details.colHost]], `Member ID` e [[ui:groups.details.colAssignment]] (le partizioni del membro nella forma `topic:partition`). Un gruppo senza membri mostra [[ui:groups.details.noMembers]].

La seconda riga contiene [[ui:groups.details.resetOffsets]] e [[ui:groups.details.deleteGroup]]. È nascosta sulle connessioni in sola lettura.

## Reimpostare gli offset {#reset-offsets}

La reimpostazione sposta gli offset sottoposti a commit di un gruppo. Il gruppo rilegge così dei messaggi oppure li salta. Gli offset si possono reimpostare solo per un gruppo senza membri attivi (stato `Empty` o `Dead`). Altrimenti i membri sovrascriverebbero i nuovi offset con i propri commit. Arresta prima le applicazioni del gruppo.

1. Nella scheda del gruppo, fai clic su [[ui:groups.details.resetOffsets]]. Finché il gruppo ha membri, il pulsante è disattivato; passa il puntatore sul pulsante per vedere il motivo.
2. Scegli la strategia in [[ui:groups.reset.strategy]]. Vedi [Strategie di reimpostazione](groups.md#reset-strategies).
3. Scegli l’ambito in [[ui:groups.reset.scope]]. Vedi [Ambito della reimpostazione](groups.md#reset-scope).
4. Fai clic su [[ui:groups.reset.preview]].
5. Controlla la tabella e fai clic sul pulsante di applicazione, che mostra il numero di partizioni che cambieranno.

L’anteprima non modifica nulla. Per ogni partizione mostra il topic, la partizione, [[ui:groups.reset.colCurrent]] (l’offset sottoposto a commit), [[ui:groups.reset.colNew]] (l’offset di destinazione) e `Low / High` (il primo offset e l’offset finale della partizione). Una partizione per cui non si può calcolare una destinazione è contrassegnata da [[ui:groups.reset.noTarget]], con il motivo. Queste partizioni vengono saltate.

Viene applicato solo il piano mostrato nell’anteprima. Qualsiasi modifica nel modulo nasconde l’anteprima e occorre fare di nuovo clic su [[ui:groups.reset.preview]]. Su una connessione prod, l’applicazione chiede il nome della connessione; sulle altre connessioni avviene subito.

Prima di applicare, l’app verifica di nuovo che il gruppo non abbia membri. Dopo l’applicazione, [[ui:groups.reset.result]] elenca ogni partizione con il nuovo offset o un errore.

Puoi anche preparare una reimpostazione a partire da un messaggio: vedi [Reimpostare un gruppo su un messaggio](browse.md#reset-group-here). [[ui:common.close]] chiude il pannello di reimpostazione.

## Strategie di reimpostazione {#reset-strategies}

[[ui:groups.reset.strategy]] stabilisce l’offset di destinazione per ogni partizione dell’ambito:

- [[ui:groups.reset.strategyEarliest]] — il primo offset ancora conservato. Il gruppo rilegge tutto ciò che il topic conserva.
- [[ui:groups.reset.strategyLatest]] — la fine della partizione. Il gruppo salta tutto ciò che si è accumulato e legge solo i nuovi messaggi.
- [[ui:groups.reset.strategyTimestamp]] — il primo messaggio scritto alla data e ora indicate in [[ui:groups.reset.timestamp]] o successivamente. Nelle partizioni senza messaggi più recenti, la destinazione è la fine.
- [[ui:groups.reset.strategyOffset]] — il valore indicato in [[ui:groups.reset.offset]], un intero pari o superiore a 0, in ogni partizione. Se una partizione non ha questo offset, la destinazione è il suo primo offset o il suo offset finale.
- [[ui:groups.reset.strategyShift]] — sposta l’offset sottoposto a commit del valore di [[ui:groups.reset.shift]]. Un numero negativo fa rileggere N messaggi indietro, uno positivo fa saltare N messaggi. Il risultato resta compreso tra il primo offset e l’offset finale. Le partizioni senza offset sottoposto a commit non si possono spostare e vengono saltate.

L’ora è la tua ora locale, con i secondi. Kafka trova l’offset in base ai timestamp dei messaggi.

## Ambito della reimpostazione {#reset-scope}

[[ui:groups.reset.scope]] stabilisce quali partizioni vengono reimpostate. Le altre partizioni mantengono i propri offset.

- [[ui:groups.reset.scopeAll]] — tutte le partizioni per cui il gruppo ha offset sottoposti a commit. Se il gruppo non ha ancora commit, scegli invece i topic.
- [[ui:groups.reset.scopeTopics]] — tutte le partizioni dei topic elencati, comprese quelle senza commit. Separa i nomi con virgole, spazi o a capo. Usalo per far iniziare a un gruppo la lettura di un nuovo topic da un punto scelto.
- [[ui:groups.reset.scopePartitions]] — solo le partizioni elencate. Inseriscile in [[ui:groups.reset.partitionsLabel]] nella forma `topic:partition`, separate da virgole o spazi, per esempio `orders:0, orders:3`.

## Eliminare un gruppo {#delete-group}

L’eliminazione di un gruppo rimuove tutti i suoi offset sottoposti a commit. Si può eliminare solo un gruppo senza membri, nello stato `Empty`.

1. Nella scheda del gruppo, fai clic su [[ui:groups.details.deleteGroup]]. Finché il gruppo ha membri, il pulsante è disattivato; passa il puntatore sul pulsante per vedere il motivo.
2. Digita l’ID del gruppo per confermare. Su una connessione prod, digita anche il nome della connessione.
3. Fai clic su [[ui:common.delete]].

Dopo l’eliminazione, la scheda del gruppo si chiude. Se un’applicazione con questo `group.id` si avvia di nuovo, Kafka ricrea il gruppo. Senza offset sottoposti a commit, l’applicazione inizia dal punto indicato dalla sua impostazione `auto.offset.reset`: dall’inizio o dalla fine.

Sulle connessioni con l’opzione [[ui:common.readOnly]] attiva, il pulsante è nascosto.
