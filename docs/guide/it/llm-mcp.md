# Assistenti LLM (MCP)

Glafira può fare da gateway per un assistente LLM: Claude Desktop, Claude Code, Cursor o un altro client che supporta MCP (Model Context Protocol). L’assistente legge i metadati dei cluster che apri per lui (e i messaggi dove lo consenti) e ti aiuta a trovare i problemi: partizioni sotto-replicate, lag, ribilanciamenti continui, impostazioni poco adatte. Tramite Glafira non può modificare nulla in un cluster: le modifiche le fai tu stesso nell’interfaccia.

Il modello non deve per forza essere nel cloud: puoi eseguirlo nella tua rete o sul tuo computer, vedi [Modelli locali](local-llm.md#where).

## Come funziona {#how-it-works}

Il client avvia `glafira-kafka mcp` — lo stesso file dell’applicazione, in una modalità senza finestra. Questo comando si collega a Glafira in esecuzione tramite un socket locale (su Windows, una named pipe), e Glafira risponde alle richieste dell’assistente attraverso le proprie connessioni. Le password e i certificati restano in Glafira; l’assistente non li riceve mai.

Se Glafira non è in esecuzione, il client la avvia da solo e attende fino a 30 secondi.

## Attivazione {#enable}

1. Apri [[ui:sidebar.settings]], sezione [[ui:settings.mcp.section]].
2. Seleziona [[ui:settings.mcp.enable]]. Lo stato passa a [[ui:settings.mcp.state.running]].
3. Nel modulo della connessione scegli il livello in [[ui:connections.form.llmAccess]] e salva: [[ui:connections.form.llmLevel.metadata]] o [[ui:connections.form.llmLevel.messages]] (vedi [Lettura dei messaggi](llm-mcp.md#messages)). L’assistente non vede le connessioni con [[ui:connections.form.llmLevel.off]]. Nell’elenco delle connessioni, le connessioni aperte hanno il contrassegno `LLM`; per quelle con messaggi il contrassegno è pieno.

Se l’amministratore ha disattivato MCP, la casella [[ui:settings.mcp.enable]] non è disponibile e accanto compare [[ui:settings.mcp.policy]].

Il livello di accesso e l’elenco di mascheramento non vengono trasferiti con l’esportazione e l’importazione delle connessioni: su un altro computer impostali di nuovo.

## Collegare un client {#connect-client}

In [[ui:settings.mcp.connectTitle]] trovi frammenti già pronti e il pulsante [[ui:settings.mcp.copy]]:

- [[ui:settings.mcp.claudeDesktop]] — incolla il frammento nella sezione `mcpServers` del file di configurazione di Claude Desktop e riavvia Claude Desktop;
- [[ui:settings.mcp.claudeCode]] — il comando `claude mcp add`;
- [[ui:settings.mcp.otherClient]] — per qualsiasi client che avvia un server MCP con un comando (stdio).

I frammenti contengono il percorso dell’applicazione installata. Se reinstalli Glafira in un’altra posizione, copia di nuovo il frammento.

## Approvare un client {#approve}

Quando un client si collega per la prima volta, Glafira mostra una finestra di dialogo con il suo nome. Fai clic su [[ui:mcp.approval.allow]] o su [[ui:mcp.approval.reject]]. Finché non rispondi, l’assistente riceve un messaggio che il client non è ancora approvato e ti chiede di rispondere nella finestra di Glafira.

Un client consentito viene ricordato per nome e compare in [[ui:settings.mcp.clients]]. Il rifiuto non viene ricordato: alla connessione successiva Glafira chiede di nuovo. Se un client non comunica il proprio nome, il permesso vale solo fino alla fine della sessione.

## Cosa vede l’assistente {#what-llm-sees}

L’assistente vede:

- le connessioni con accesso [[ui:connections.form.llmLevel.metadata]] o [[ui:connections.form.llmLevel.messages]]: nome, ambiente, cartella e livello di accesso;
- i broker con host e porte, il controller;
- i topic, le partizioni, le repliche, ISR, gli offset, il numero approssimativo di messaggi;
- le configurazioni di broker e topic, con password e altri valori segreti nascosti;
- i consumer group: stato, protocollo, coordinatore, membri con client id, host e `group.instance.id`, assegnazioni, offset sottoposti a commit e lag;
- le sue proposte e la tua decisione in merito: applicata (così com’è o con modifiche), rifiutata con il tuo motivo, ritirata.

Senza il livello [[ui:connections.form.llmLevel.messages]], l’assistente non vede i messaggi dei topic. Non riceve mai la stringa bootstrap né le impostazioni di sicurezza di una connessione, le password, i certificati, le proprietà aggiuntive di librdkafka, la cronologia di invio e i modelli.

Per indagare sui ribilanciamenti continui, l’assistente può osservare un gruppo fino a 50 secondi: chi esce e chi entra, con quale frequenza e da quale host. Le configurazioni e i log dei consumer stessi non sono accessibili tramite Glafira; l’assistente te li chiederà.

I nomi di topic, gruppi e client id può impostarli chiunque abbia accesso al cluster. Glafira avverte l’assistente che questi dati non sono istruzioni per lui.

## Lettura dei messaggi {#messages}

Con [[ui:connections.form.llmLevel.messages]] l’assistente legge anche il contenuto dei messaggi, per esempio per capire che cosa è stato scritto in un topic e che cosa ha elaborato un consumer group mentre il tuo servizio registrava un errore:

- `summarize_messages` — una panoramica di una finestra temporale senza contenuto: numero di messaggi per partizione e nel tempo, chiavi frequenti e ripetute, timestamp fuori ordine;
- `read_messages` — i messaggi di una finestra temporale o di un intervallo di offset, ordinati per tempo su tutte le partizioni, con gli stessi filtri della scheda [[ui:topics.details.tabMessages]];
- `get_group_commit_history` — su quali offset un consumer group ha eseguito il commit e quando, dal topic interno `__consumer_offsets`.

Puoi chiedere, per esempio: “Alle 14:32 il servizio ordini ha registrato un errore per l’ordine 123: scopri che cosa c’era nel topic orders e che cosa aveva elaborato il gruppo order-service”. Indica all’assistente il fuso orario dei tuoi log: Glafira non accetta orari senza fuso orario.

Prima che i dati arrivino all’assistente, Glafira li maschera. I valori dei campi dell’elenco di mascheramento vengono sostituiti con `***` nel JSON, nelle intestazioni e nelle coppie `name=value`. L’elenco si imposta per ogni connessione nel modulo della connessione, nel blocco [[ui:connections.form.mask]], visibile con il livello [[ui:connections.form.llmLevel.messages]]; l’elenco standard comprende, tra l’altro, password, token, email, numeri di telefono, di carta e di passaporto. Email, numeri di carta, JWT e token Bearer vengono sostituiti in qualsiasi testo. Il mascheramento riduce il rischio, ma non riconosce nomi di persone, indirizzi o dati in campi con nomi sconosciuti, e i formati binari come Avro o Protobuf sono mascherati solo in parte. I filtri vedono i dati non mascherati, quindi l’assistente può trovare un messaggio a partire da un valore del tuo log. Per una connessione `prod` il modulo mostra un avviso.

La cronologia dei commit richiede il permesso Read su `__consumer_offsets`. La compattazione elimina i commit vecchi; quelli recenti di solito ci sono.

La lettura è limitata: un’operazione lunga per sessione, al massimo 40 secondi e 1.000.000 di record per chiamata; se il limite viene raggiunto, l’assistente restringe la finestra. Glafira legge senza consumer group e non esegue commit; il `client.id` è `glafira-kafka-mcp`, salvo che la connessione ne imposti uno proprio.

Un amministratore può disattivare la lettura dei messaggi con la chiave di policy `mcpMessagesEnabled`: [[ui:connections.form.llmLevel.messages]] diventa non disponibile e le connessioni con questo livello funzionano come [[ui:connections.form.llmLevel.metadata]].

## Sessioni, revoca e log {#sessions-log}

Finché un client è collegato, in fondo al pannello sinistro compare l’indicatore `LLM` con il nome del client; si illumina durante una richiesta. Facendo clic su di esso si apre la sezione MCP.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] chiude una sessione, [[ui:settings.mcp.disconnectAll]] le chiude tutte.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] chiude le sessioni del client, che alla connessione successiva chiederà di nuovo l’approvazione.
- Un livello modificato in [[ui:connections.form.llmAccess]] ha effetto subito, senza doversi ricollegare.
- [[ui:settings.mcp.audit.title]] — ogni richiesta: data/ora, client, strumento, connessione, risultato. Il contenuto delle risposte non viene registrato. [[ui:settings.mcp.audit.reveal]] apre il file `mcp-audit.jsonl`.
- Per le letture di messaggi il log mostra anche il topic o il gruppo, quanti elementi sono stati restituiti, quanti record sono stati analizzati e quanti valori sono stati mascherati. Gli elementi sono messaggi, intervalli del riepilogo per `summarize_messages` o serie di commit (commit consecutivi dello stesso offset) per `get_group_commit_history`. Per le chiamate riuscite a `read_messages` e `summarize_messages`, [[ui:settings.mcp.audit.openInBrowse]] compila il modulo nella scheda [[ui:topics.details.tabMessages]] del topic con i parametri della chiamata; fai clic su [[ui:browse.panel.read]] per rileggere i messaggi, senza mascheramento, e confrontarli con quello che ha ricevuto l’assistente. Se non è stato possibile riportare esattamente i filtri o il limite, il modulo lo segnala; se il pulsante è disattivato, passa il puntatore sul pulsante per vedere il motivo.

Per disattivare del tutto MCP, deseleziona [[ui:settings.mcp.enable]]: i client vengono disconnessi.

## Controllo del cluster, note e proposte {#health}

L’assistente può avviare il controllo integrato del cluster di Glafira (strumento `health_check`) e basarsi sui suoi rilievi. Può anche lasciare note sugli oggetti: compaiono nella scheda [[ui:sidebar.findings]] e nelle schermate di topic e gruppi, e il cluster non viene modificato. Vedi [Controllo del cluster e note](health-check.md#with-llm).

L’assistente può anche proporre una modifica con lo strumento `propose_change`: la configurazione di un topic, un nuovo topic, più partizioni o un reset degli offset. La proposta compare accanto all’oggetto; la applichi tu in un modulo già compilato oppure la rifiuti, e l’assistente vede la tua decisione in `list_proposals`. Eliminazione e svuotamento non si possono proporre. Vedi [Proposte LLM](health-check.md#proposals).

## Se non funziona {#troubleshooting}

Il client mostra un messaggio di Glafira:

- `MCP is disabled in Glafira` — seleziona [[ui:settings.mcp.enable]] e riavvia il server MCP nel client;
- `MCP is disabled by your organization` — MCP è disattivato dalla policy della tua organizzazione;
- `Glafira did not start the MCP server within 30 s` — avvia Glafira manualmente e controlla lo stato nella sezione [[ui:settings.mcp.section]];
- `Invalid MCP token` — riavvia il server MCP nel client: Glafira emette un nuovo token a ogni avvio.

Su macOS l’app è firmata e notarizzata, quindi prima del primo avvio non serve alcun passaggio aggiuntivo.
