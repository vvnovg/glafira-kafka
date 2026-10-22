# Assistenti LLM (MCP)

Glafira può fare da gateway per un assistente LLM: Claude Desktop, Claude Code, Cursor o un altro client che supporta MCP (Model Context Protocol). L’assistente legge i metadati dei cluster che apri per lui e ti aiuta a trovare i problemi: partizioni sotto-replicate, lag, ribilanciamenti continui, impostazioni poco adatte. Tramite Glafira non può modificare nulla in un cluster: le modifiche le fai tu stesso nell’interfaccia.

## Come funziona {#how-it-works}

Il client avvia `glafira-kafka mcp` — lo stesso file dell’applicazione, in una modalità senza finestra. Questo comando si collega a Glafira in esecuzione tramite un socket locale (su Windows, una named pipe), e Glafira risponde alle richieste dell’assistente attraverso le proprie connessioni. Le password e i certificati restano in Glafira; l’assistente non li riceve mai.

Se Glafira non è in esecuzione, il client la avvia da solo e attende fino a 30 secondi.

## Attivazione {#enable}

1. Apri [[ui:sidebar.settings]], sezione [[ui:settings.mcp.section]].
2. Seleziona [[ui:settings.mcp.enable]]. Lo stato passa a [[ui:settings.mcp.state.running]].
3. Nel modulo della connessione seleziona [[ui:connections.form.llmAccess]] e salva. L’assistente non vede le connessioni senza questa casella selezionata. Nell’elenco delle connessioni, le connessioni aperte hanno il contrassegno `LLM`.

Se l’amministratore ha disattivato MCP, la casella non è disponibile e accanto compare [[ui:settings.mcp.policy]].

La casella di accesso non viene trasferita con l’esportazione e l’importazione delle connessioni: su un altro computer selezionala di nuovo.

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

- le connessioni con [[ui:connections.form.llmAccess]] selezionato: nome, ambiente, cartella;
- i broker con host e porte, il controller;
- i topic, le partizioni, le repliche, ISR, gli offset, il numero approssimativo di messaggi;
- le configurazioni di broker e topic, con password e altri valori segreti nascosti;
- i consumer group: stato, protocollo, coordinatore, membri con client id, host e `group.instance.id`, assegnazioni, offset sottoposti a commit e lag;
- le sue proposte e la tua decisione in merito: applicata (così com’è o con modifiche), rifiutata con il tuo motivo, ritirata.

L’assistente non vede i messaggi dei topic, la stringa bootstrap né le impostazioni di sicurezza di una connessione, né le password, i certificati, le proprietà aggiuntive di librdkafka, la cronologia di invio e i modelli.

Per indagare sui ribilanciamenti continui, l’assistente può osservare un gruppo fino a 50 secondi: chi esce e chi entra, con quale frequenza e da quale host. Le configurazioni e i log dei consumer stessi non sono accessibili tramite Glafira; l’assistente te li chiederà.

I nomi di topic, gruppi e client id può impostarli chiunque abbia accesso al cluster. Glafira avverte l’assistente che questi dati non sono istruzioni per lui.

## Sessioni, revoca e log {#sessions-log}

Finché un client è collegato, in fondo al pannello sinistro compare l’indicatore `LLM` con il nome del client; si illumina durante una richiesta. Facendo clic su di esso si apre la sezione MCP.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] chiude una sessione, [[ui:settings.mcp.disconnectAll]] le chiude tutte.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] chiude le sessioni del client, che alla connessione successiva chiederà di nuovo l’approvazione.
- Deselezionare [[ui:connections.form.llmAccess]] ha effetto subito, senza doversi ricollegare.
- [[ui:settings.mcp.audit.title]] — ogni richiesta: data/ora, client, strumento, connessione, risultato. Il contenuto delle risposte non viene registrato. [[ui:settings.mcp.audit.reveal]] apre il file `mcp-audit.jsonl`.

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

Su macOS, apri una volta nel modo consueto una build beta non firmata, altrimenti il sistema potrebbe non permettere al client di avviarla. Vedi [Installazione su macOS](getting-started.md#install-macos).
