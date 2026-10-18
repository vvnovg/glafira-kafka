# Sessioni consumer

Una sessione consumer legge i topic come un vero membro di un consumer group. Le sessioni consumer sono una funzione Pro.

## Che cos’è una sessione {#what-is}

La [consultazione](browse.md) legge i messaggi senza entrare in un gruppo e non modifica nulla nel cluster. Una sessione è diversa: entra in un gruppo con il `group.id` indicato, riceve le partizioni da Kafka e può eseguire il commit degli offset. Usala per leggere ciò che leggerebbe un gruppo, per spostare gli offset di un gruppo con i commit o per osservare il ribilanciamento di un gruppo.

Una sessione influisce sul cluster. Quando entra o esce, Kafka ridistribuisce le partizioni del gruppo e gli altri membri eseguono un ribilanciamento. I commit modificano gli offset del gruppo, che le sue applicazioni useranno.

In Free, [[ui:sidebar.consumer]], [[ui:topics.details.consume]] e [[ui:groups.details.join]] sono disattivati e contrassegnati da un lucchetto e da `Pro`. Se una scheda di sessione si apre senza una licenza Pro attiva, mostra [[ui:consumer.proOnly]] e il collegamento [[ui:common.openLicense]]. Vedi [Attivare una chiave](license-updates.md#activate).

Le sessioni non sono disponibili sulle connessioni con l’opzione [[ui:common.readOnly]] attiva: la scheda mostra [[ui:consumer.readOnlyBlocked]]. Se una connessione passa in sola lettura mentre una sessione è in corso, la sessione esce dal gruppo.

## Avviare una sessione {#start-session}

Una sessione si apre in tre modi:

- [[ui:sidebar.consumer]] sotto la connessione nel pannello [[ui:sidebar.connections]] — un modulo vuoto;
- [[ui:topics.details.consume]] nella scheda di un topic — il topic è già compilato;
- [[ui:groups.details.join]] nella scheda di un gruppo — sono già compilati il gruppo e i topic per cui ha commit.

Ogni volta si apre una nuova scheda, quindi più sessioni possono essere in corso contemporaneamente.

In [[ui:consumer.form.groupTopics]]:

- `group.id` — il gruppo in cui entrare. Se non ne inserisci uno, l’app propone un nuovo nome come `glafira-kafka-anna-1a2b3c`. Un nuovo gruppo non ha commit.
- [[ui:consumer.form.topicsLabel]] — i nomi dei topic separati da virgole o spazi. In alternativa, seleziona [[ui:consumer.form.useRegex]] e inserisci un’espressione in [[ui:consumer.form.patternLabel]], per esempio `orders\..*`. L’espressione viene confrontata dall’inizio del nome: se manca, viene aggiunto `^`. I topic corrispondenti creati in seguito vengono aggiunti entro qualche minuto (all’aggiornamento dei metadati); questo causa un ribilanciamento.
- `auto.offset.reset` — da dove iniziare nelle partizioni per cui il gruppo non ha un offset sottoposto a commit: [[ui:consumer.form.resetLatest]] (il valore predefinito) o [[ui:consumer.form.resetEarliest]]. Dove il gruppo ha un offset sottoposto a commit, la lettura riprende da quello.
- [[ui:consumer.form.autoCommit]] — esegue automaticamente il commit degli offset letti. Disattivato per impostazione predefinita. Vedi [Commit](consumer-sessions.md#commits).
- `isolation.level` — `read_committed` (il valore predefinito) mostra solo i messaggi delle transazioni confermate. `read_uncommitted` mostra anche i messaggi delle transazioni aperte e annullate.

In [[ui:consumer.form.formatLegend]] scegli [[ui:consumer.form.keyFormat]] e [[ui:consumer.form.valueFormat]], come nella consultazione. Vedi [Formati](browse.md#formats). [[ui:consumer.form.buffer]] stabilisce quanti messaggi conserva la tabella, da 1 a 100000; il valore predefinito è 5000.

I filtri funzionano come nella consultazione. Vedi [Filtri](browse.md#filters).

Fai clic su [[ui:consumer.actions.connect]]. Su una connessione prod, l’app chiede di digitare il nome della connessione e di confermare con [[ui:consumer.dialog.confirm]]. Mentre la sessione è in corso, il modulo non si può modificare.

## Lettura {#reading}

La riga di stato accanto ai pulsanti mostra [[ui:consumer.status.connecting]], poi [[ui:consumer.status.inGroup]]. Mostra anche quanti messaggi sono stati letti, quanti corrispondono ai filtri e quanti sono in tabella. Se la connessione al broker si interrompe, la sessione si riconnette da sola e lo segnala nella riga di stato.

I messaggi compaiono nella tabella man mano che arrivano, e la sessione continua a leggere i nuovi messaggi finché non la interrompi. Quando la tabella è piena, i messaggi più vecchi vengono rimossi. Fai clic su una riga per vedere i dettagli del messaggio, come nella consultazione. Vedi [Dettagli del messaggio](browse.md#message-details). Seleziona `UTC` per mostrare le ore in UTC.

La sessione legge solo le partizioni che Kafka le ha assegnato. Se il gruppo ha altri membri, questi leggono le altre partizioni. Se il gruppo ha più membri che partizioni, la sessione potrebbe non ricevere alcuna partizione.

[[ui:consumer.disconnect]] fa uscire la sessione dal gruppo, e gli altri membri ricevono le sue partizioni. Anche la chiusura della scheda della sessione fa uscire dal gruppo. Quando la sessione è terminata, la riga di stato mostra [[ui:consumer.status.disconnected]].

Se un topic non esiste, la sessione si arresta con un errore e mostra i nomi da controllare.

## Commit {#commits}

Un commit salva un offset per il gruppo. Il membro successivo che legge la partizione, o la stessa applicazione dopo un riavvio, inizia da quell’offset.

Con l’opzione [[ui:consumer.form.autoCommit]] attiva, la sessione esegue il commit degli offset letti in background a intervalli di pochi secondi, e un’ultima volta quando esce dal gruppo. I messaggi nascosti dai filtri contano come letti.

Senza commit automatico, la sessione non esegue alcun commit da sola. Usa i pulsanti:

- [[ui:consumer.actions.commitSelected]] — esegue il commit dell’offset successivo al messaggio selezionato, solo nella sua partizione. Il gruppo riprenderà dal messaggio seguente. Se il messaggio selezionato è più vecchio del commit attuale, l’offset torna indietro.
- [[ui:consumer.actions.commitAll]] — esegue il commit della posizione attuale in ogni partizione assegnata. Sono compresi i messaggi nascosti dai filtri e quelli già rimossi dalla tabella.

Dopo un commit, una riga mostra gli offset sottoposti a commit nella forma `topic:partition → offset`. Su una connessione prod, ogni commit chiede il nome della connessione.

Se la sessione non risponde in tempo, l’app avvisa che non è noto se il commit sia stato applicato. Controlla gli offset nella scheda del gruppo. Vedi [Dettagli del gruppo](groups.md#group-details).

## Mettere in pausa le partizioni {#pause-partitions}

Il pannello a destra della tabella elenca le partizioni assegnate alla sessione nella forma `topic:partition`. Fai clic su [[ui:consumer.assignment.pause]] accanto a una partizione per interromperne la lettura e su [[ui:consumer.assignment.resume]] per continuare. Una partizione in pausa è contrassegnata da [[ui:consumer.assignment.paused]].

La pausa non fa uscire dal gruppo, quindi non c’è ribilanciamento. La partizione resta assegnata alla sessione e, finché è in pausa, gli altri membri non la leggono. La pausa non modifica nulla nel cluster e non richiede conferma.

Dopo un ribilanciamento, le partizioni appena assegnate non sono in pausa. Quando la sessione esce dal gruppo, tutte le pause terminano.

## Registro dei ribilanciamenti {#rebalance-log}

[[ui:consumer.assignment.logTitle]] sotto l’elenco delle partizioni registra ogni cambiamento dell’assegnazione, dal più recente. Ogni voce ha un’ora e uno di tre tipi: partizioni assegnate alla sessione, partizioni revocate alla sessione o un errore di ribilanciamento con il relativo testo.

Un ribilanciamento avviene quando un membro entra nel gruppo o ne esce, quando cambiano i topic sottoscritti o quando un membro smette di rispondere in tempo. Per osservarlo, avvia in un’altra scheda una seconda sessione con lo stesso `group.id`: le partizioni vengono suddivise tra le due sessioni, ed entrambi i registri lo mostrano.

Il registro conserva le ultime 200 voci. Prima del primo ribilanciamento mostra [[ui:consumer.assignment.logEmpty]].

## Limiti {#limits}

- Le sessioni richiedono Pro. Se Pro smette di essere attivo mentre una sessione è in corso, la sessione continua a leggere, ma commit, pause e riprese vengono rifiutati e non si può avviare una nuova sessione.
- Le sessioni non sono disponibili sulle connessioni con l’opzione [[ui:common.readOnly]] attiva.
- Una sessione non può iniziare da un offset scelto. Per impostare il punto di partenza, reimposta prima gli offset del gruppo: vedi [Reimpostare gli offset](groups.md#reset-offsets). La reimpostazione richiede un gruppo senza membri, quindi disconnetti le sessioni di questo gruppo prima di reimpostare.
- La tabella conserva al massimo 100000 messaggi. I messaggi più vecchi vengono rimossi, ma contano comunque come letti.
- Chiavi e valori molto grandi vengono mostrati solo in parte, come nella consultazione.
- Non entrare nel gruppo di un’applicazione in esecuzione se non è ciò che vuoi. La sessione sottrae partizioni all’applicazione, che non le elabora finché la sessione le detiene. I commit della sessione, compresi quelli automatici, spostano gli offset dell’applicazione.
