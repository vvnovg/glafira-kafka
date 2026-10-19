# Controllo del cluster e note

Glafira controlla un cluster con un insieme di regole integrate e mostra i problemi trovati accanto agli oggetti: nell’albero delle connessioni, negli elenchi di topic e gruppi e nelle schermate di un topic o di un gruppo. Con Glafira Pro un assistente LLM può aggiungere a questi rilievi le proprie note. Il controllo legge soltanto il cluster e non vi cambia nulla.

## Avviare un controllo {#run}

Apri [[ui:sidebar.cluster]] o [[ui:sidebar.findings]] di una connessione e premi [[ui:health.check]]. Un controllo dura fino a 45 secondi: Glafira legge i broker, le partizioni, le configurazioni dei topic (fino a 2000 topic) e i consumer group (fino a 500). Sui cluster grandi il risultato è contrassegnato come [[ui:health.partial]] e il rilievo [[ui:health.ruleNames.check.partial]] mostra cosa non è stato letto.

Glafira non interroga il cluster da sola: un controllo parte solo quando premi il pulsante o quando lo chiede un assistente LLM. Il risultato resta salvato fino al controllo successivo.

## Cosa viene controllato {#rules}

- Broker e partizioni: nessun controller, partizioni non disponibili e sotto-replicate, cluster con un solo broker, squilibrio dei leader, leader non sulla replica preferita.
- Durabilità dei topic: fattore di replica 1, `min.insync.replicas` maggiore o uguale al fattore di replica o uguale a 1, elezione unclean del leader.
- Configurazione dei broker: creazione automatica dei topic, fattore di replica e `min.insync.replicas` predefiniti, impostazioni importanti diverse tra i broker.
- Consumer group: gruppi vuoti con lag, gruppi in ribilanciamento, membri senza partizioni, lag cresciuto dal controllo precedente.

## Gravità e ambiente {#severity}

Ogni rilievo ha una gravità: [[ui:health.severity.critical]], [[ui:health.severity.warning]] o [[ui:health.severity.info]]. Dipende dall’ambiente della connessione. Per esempio, il fattore di replica 1 è critico in prod, un avviso in stage e non viene mostrato in dev, test e local. L’ambiente si imposta nel modulo della connessione.

## La scheda «Rilievi» {#tab}

Nella scheda [[ui:sidebar.findings]] i rilievi sono raggruppati per regola; ognuno spiega il problema e cosa fare. Filtrali per [[ui:health.filterSeverity]], [[ui:health.filterSource]] e [[ui:health.filterKind]] oppure cerca per nome. Il nome dell’oggetto apre la sua schermata.

Un badge con un numero nell’albero delle connessioni e negli elenchi di topic e gruppi mostra quanti rilievi critici e avvisi riguardano l’oggetto.

## Nascondere i rilievi {#hide}

Se un rilievo è previsto per un oggetto specifico, per esempio un topic di test con fattore di replica 1 in prod, premi [[ui:health.hide]]. Un rilievo nascosto non conta nei badge e resta nascosto anche nei controlli successivi. Per vedere i rilievi nascosti attiva «Mostra nascosti»; lì trovi [[ui:health.restore]].

## Note LLM {#notes}

Con Glafira Pro un assistente LLM collegato tramite MCP (vedi [Assistenti LLM (MCP)](llm-mcp.md#how-it-works)) può lasciare una nota sul cluster, su un broker, un topic o un gruppo. Le note compaiono in [[ui:health.notes]] e nelle schermate degli oggetti, firmate con il nome del client. Il testo è mostrato così com’è, senza formattazione né link.

- [[ui:health.resolve]] e [[ui:health.dismiss]] chiudono una nota; quelle chiuse restano in una sezione compressa.
- [[ui:common.delete]] elimina una nota e [[ui:health.deleteAll]] elimina tutte le note della connessione.
- Una nota viene eliminata automaticamente 7 giorni dopo la creazione. Una connessione può avere al massimo 200 note aperte.

Senza Pro un assistente può comunque avviare il controllo e leggere i rilievi, ma non può lasciare note.

## Con un assistente LLM {#with-llm}

L’assistente avvia lo stesso controllo con lo strumento `health_check` e vede gli stessi rilievi, compresi quelli nascosti, che sono contrassegnati. Se l’ultimo controllo ha meno di 5 minuti, l’assistente ne riceve il risultato senza interrogare di nuovo il cluster. Chiedi per esempio: «Controlla lo stato del cluster e lascia note sui problemi più importanti».
