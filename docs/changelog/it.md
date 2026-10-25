# Registro delle modifiche

Versione italiana di [CHANGELOG.md](../../CHANGELOG.md), dove si trovano anche le versioni precedenti. La sezione di una versione viene mostrata in “Informazioni” come note dell’aggiornamento.

## 0.5.0
- Lettura dei messaggi da parte di un assistente LLM. Una connessione ha tre livelli di accesso per gli LLM: “Disattivato”, “Solo metadati”, “Metadati e messaggi”. Al terzo livello l’assistente, tramite MCP, può:
  - leggere i messaggi in una finestra temporale o per offset, con gli stessi filtri della scheda “Messaggi” (`read_messages`);
  - vedere una panoramica di una finestra senza il contenuto: volume per partizione e nel tempo, chiavi frequenti, timestamp fuori ordine (`summarize_messages`);
  - vedere quali offset un consumer group ha sottoposto a commit e quando, da `__consumer_offsets` (`get_group_commit_history`).
- Gli orari vengono passati solo con il fuso orario. Una chiamata dura al massimo 40 secondi, esamina al massimo un milione di record e restituisce al massimo 100 KB.
- Mascheramento prima dell’invio dei dati all’assistente:
  - i valori dei campi di un elenco di nomi — quello standard o quello proprio della connessione — vengono sostituiti con `***`;
  - email, numeri di carta, JWT e token di autorizzazione vengono mascherati in qualsiasi testo;
  - i filtri lavorano sui dati non mascherati, quindi un messaggio si può trovare tramite un valore preso da un log.
- Il livello di accesso e l’elenco di mascheramento non vengono esportati né importati.
- La chiave di policy `mcpMessagesEnabled` vieta la lettura dei messaggi: il livello “Metadati e messaggi” funziona allora come “Solo metadati”.
- Log MCP: per le letture di messaggi mostra il topic o il gruppo, quanto è stato restituito rispetto a quanto esaminato e quanti valori sono stati mascherati. Il pulsante “Apri in ‘Messaggi’” compila il modulo della schermata “Messaggi” con la stessa finestra e gli stessi filtri; se non è possibile riportarli esattamente, il modulo lo segnala.
- MCP: sono supportati i client del protocollo MCP 2026-07-28 (per esempio Claude Code 2.1.310): nome del client e approvazione arrivano subito alla connessione. Una sessione in attesa ha il pulsante “Approva”. Tutti gli strumenti sono contrassegnati esplicitamente come di sola lettura (`idempotentHint`).
- Il server catalogo `glafira-mcp-catalog` per i registri MCP (Glama): un’immagine Docker, l’elenco reale degli strumenti, e le chiamate rispondono che serve l’applicazione desktop. Il README è in inglese (in russo — `README.ru.md`).
- Reimpostazione degli offset proposta da un LLM: prima della scrittura la composizione del gruppo viene verificata di nuovo. Se il broker ha rifiutato perché nel frattempo sono entrati membri nel gruppo, viene mostrato un errore chiaro, “il gruppo non è vuoto”, e il modulo di reimpostazione mostra subito lo stato attuale del gruppo.
- Interfaccia:
  - i testi segnaposto nei campi vuoti sono in corsivo, per non confonderli con i valori inseriti;
  - i campi della posizione iniziale e delle partizioni nella schermata “Messaggi” mostrano un suggerimento al passaggio del puntatore;
  - sopra i contrassegni di ambiente e LLM nell’albero delle connessioni compare la freccia normale.
- Guida: la sezione “Lettura dei messaggi” nel capitolo “Assistenti LLM (MCP)”, in 12 lingue.
