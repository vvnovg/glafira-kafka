# Controllo del cluster e note

Glafira controlla un cluster con un insieme di regole integrate e mostra i problemi trovati accanto agli oggetti: nell’albero delle connessioni, negli elenchi di topic e gruppi e nelle schermate di un topic o di un gruppo. Un assistente LLM può aggiungere a questi rilievi le proprie note. Il controllo legge soltanto il cluster e non vi cambia nulla.

## Avviare un controllo {#run}

Apri [[ui:sidebar.cluster]] o [[ui:sidebar.findings]] di una connessione e fai clic su [[ui:health.check]]. Un controllo dura fino a 45 secondi: Glafira legge i broker, le partizioni, le configurazioni dei topic (fino a 2000 topic) e i consumer group (fino a 500). Se i topic o i gruppi sono di più, se non si riesce a leggere qualcosa o se scadono i 45 secondi, il risultato è contrassegnato come [[ui:health.partial]] e il rilievo [[ui:health.ruleNames.check.partial]] mostra cosa non è stato letto.

Glafira non interroga il cluster da sola: un controllo parte solo quando fai clic sul pulsante o quando lo chiede un assistente LLM. Il risultato resta salvato fino al controllo successivo.

## Cosa viene controllato {#rules}

- Broker e partizioni: nessun controller, partizioni non disponibili e sotto-replicate, cluster con un solo broker, squilibrio dei leader, leader non sulla replica preferita.
- Durabilità dei topic: fattore di replica 1, `min.insync.replicas` maggiore del fattore di replica, `min.insync.replicas` uguale al fattore di replica, `min.insync.replicas` uguale a 1, elezione unclean del leader.
- Configurazione dei broker: creazione automatica dei topic, fattore di replica predefinito 1, `min.insync.replicas` predefinito 1, impostazioni importanti diverse tra i broker.
- Consumer group: gruppi vuoti con lag, gruppi in ribilanciamento, membri senza partizioni, lag cresciuto dal controllo precedente.

## Gravità e ambiente {#severity}

Ogni rilievo ha una gravità: [[ui:health.severity.critical]], [[ui:health.severity.warning]] o [[ui:health.severity.info]]. Dipende dall’ambiente della connessione. Per esempio, il fattore di replica 1 è critico in prod, un avviso in stage e non viene mostrato in dev, test e local. L’ambiente si imposta nel modulo della connessione.

## La scheda “Rilievi” {#tab}

Nella scheda [[ui:sidebar.findings]] i rilievi sono raggruppati per regola; ognuno spiega il problema e cosa fare. Filtrali per [[ui:health.filterSeverity]], [[ui:health.filterSource]] e [[ui:health.filterKind]] oppure cerca per nome. Il nome dell’oggetto apre la sua schermata.

Un badge con un numero nell’albero delle connessioni e negli elenchi di topic e gruppi mostra quanti rilievi e note LLM aperte con gravità [[ui:health.severity.critical]] o [[ui:health.severity.warning]] riguardano l’oggetto. Passa il puntatore sul badge per vederne i titoli.

## Nascondere i rilievi {#hide}

Se un rilievo è previsto per un oggetto specifico, per esempio un topic di test con fattore di replica 1 in prod, fai clic su [[ui:health.hide]]. Un rilievo nascosto non conta nei badge e resta nascosto anche nei controlli successivi. Per vedere i rilievi nascosti attiva “Mostra nascosti”; lì trovi [[ui:health.restore]].

## Note LLM {#notes}

Un assistente LLM collegato tramite MCP (vedi [Assistenti LLM (MCP)](llm-mcp.md#how-it-works)) può lasciare una nota sul cluster, su un broker, un topic o un gruppo. Le note compaiono in [[ui:health.notes]] e nelle schermate degli oggetti, firmate con il nome del client. Il testo è mostrato così com’è, senza formattazione né link.

- [[ui:health.resolve]] e [[ui:health.dismiss]] chiudono una nota; quelle chiuse restano in una sezione compressa.
- [[ui:common.delete]] elimina una nota e [[ui:health.deleteAll]] elimina tutte le note della connessione.
- Una nota viene eliminata automaticamente 7 giorni dopo la creazione. Una connessione può avere al massimo 200 note aperte.

## Proposte LLM {#proposals}

Oltre alle note, un assistente LLM può proporre una modifica: la configurazione di un topic, un nuovo topic, più partizioni o il reset degli offset di un consumer group vuoto. La proposta compare in [[ui:health.proposals.title]] nella scheda [[ui:sidebar.findings]] e nella schermata del topic o del gruppo, con la motivazione dell’assistente, una descrizione della modifica e il nome del client. Nulla viene applicato da solo; eliminazione e svuotamento non si possono proporre affatto.

- [[ui:health.proposals.openForm]] apre la scheda dell’oggetto con il modulo consueto già compilato: l’editor di configurazione con il confronto “prima → dopo”, il modulo di creazione del topic, la finestra delle partizioni o il reset degli offset con anteprima. Modifica i valori se vuoi e conferma come al solito; su una connessione prod Glafira chiede il nome della connessione. Dopo una modifica riuscita la proposta viene segnata come applicata (con modifiche, se hai cambiato qualcosa), e l’assistente lo vede in `list_proposals`.
- [[ui:health.proposals.reject]] chiude una proposta; puoi indicare un motivo, che l’assistente vedrà. [[ui:health.reopen]] ripristina una proposta rifiutata.
- Su una connessione in sola lettura la proposta resta, ma [[ui:health.proposals.openForm]] non è disponibile finché non togli la sola lettura nel modulo della connessione; anche l’assistente ne viene informato.
- [[ui:common.delete]] elimina una proposta, [[ui:health.proposals.deleteAll]] elimina tutte le proposte della connessione. Una proposta viene eliminata automaticamente 7 giorni dopo la creazione; una connessione può avere al massimo 50 proposte aperte.

## Con un assistente LLM {#with-llm}

L’assistente avvia lo stesso controllo con lo strumento `health_check` e vede gli stessi rilievi, compresi quelli nascosti, che sono contrassegnati. Se l’ultimo controllo ha meno di 5 minuti, l’assistente ne riceve il risultato senza interrogare di nuovo il cluster. Chiedi per esempio: “Controlla lo stato del cluster e lascia note sui problemi più importanti”.
