# Registro delle modifiche

Versione italiana di [CHANGELOG.md](../../CHANGELOG.md), dove si trovano anche le versioni precedenti. La sezione di una versione viene mostrata in “Informazioni” come note dell’aggiornamento.

## 0.5.3
- Errori TLS: quando il broker chiude la connessione durante l’handshake TLS, il messaggio non afferma più che la porta non ha TLS. Indica che la porta potrebbe non avere TLS o che il certificato client non è stato accettato, che è il caso abituale con mTLS.
- MCP funziona meglio con i modelli locali (Qwen, gpt-oss e altri):
  - gli schemi dei parametri degli strumenti non contengono più riferimenti `$ref`, che alcuni server (vLLM con il parser `qwen3_coder`) passavano agli strumenti come stringhe;
  - un oggetto annidato inviato come stringa JSON viene accettato, mentre i valori `null` e le chiavi vuote vengono ignorati; se gli argomenti continuano a non andare bene, l’errore mostra gli argomenti attesi;
  - quando una finestra temporale non contiene messaggi, `read_messages` consiglia di controllare il fuso orario e di ampliare la finestra; le descrizioni degli strumenti spiegano come passare un’ora con il fuso orario e come filtrare per una sola chiave esatta.
- “Impostazioni” → “Assistenti LLM (MCP)”:
  - “Limite dimensione risposta”: 100 KB (predefinito), 48 KB o 24 KB; per un modello locale con un contesto ridotto, scegli un valore più basso;
  - “Set compatto” per ogni client approvato: senza gli strumenti per note e proposte di modifica, il che fa risparmiare circa 1000 token di contesto.
- Guida: un nuovo capitolo, “Modelli locali”: dove può girare il modello, risultati di Qwen3.6 e Qwen3.8 su un Mac, hardware, Ollama, configurazione di Qwen Code e Claude Code e verifica del risultato.

## 0.5.2
- Mascheramento più accurato dei messaggi letti da un assistente LLM:
  - nel testo, i valori dei cookie e delle intestazioni `Authorization` (compresa `Proxy-Authorization`) vengono mascherati fino alla fine della riga, quindi tutti i cookie, i parametri Digest e le firme AWS restano nascosti;
  - nelle coppie `name=value` vengono riconosciuti nomi in qualsiasi alfabeto, per esempio un nome di campo dell’elenco proprio della connessione scritto in cirillico;
  - vengono trovati i numeri di carta attaccati a lettere (`id4111…`) o scritti con raggruppamenti insoliti, e anche i numeri di carta negativi e decimali in JSON.
- Topic senza il permesso Describe: la scheda “Messaggi” e gli strumenti `read_messages`, `summarize_messages` e `get_group_commit_history` indicano “Non sei autorizzato a eseguire l’operazione” anziché “non trovato”.
- `get_group_commit_history`: se la cronologia dei commit non sta in 100 KB nemmeno senza serie di commit, l’assistente riceve, anziché una cronologia vuota, l’errore “risposta troppo grande” con il consiglio di restringere la richiesta. `summarize_messages` usa meno memoria con chiavi grandi.
- Modulo di connessione:
  - quando erano aperti due moduli di connessione, il livello di accesso LLM scelto in uno di essi non veniva mostrato, anche se veniva salvato correttamente; ora ogni modulo mostra il proprio;
  - i lettori di schermo annunciano l’avviso che il contenuto dei messaggi di un cluster di produzione sarà inviato al fornitore LLM;
  - se non è stato possibile caricare lo stato MCP, una nota spiega perché non si può scegliere “Metadati e messaggi”;
  - un elenco di mascheramento nascosto ai livelli “Disattivato” e “Solo metadati” non impedisce più il salvataggio.
- Contrassegni `LLM` nell’albero delle connessioni: i lettori di schermo li leggono per intero, per esempio “Accesso LLM: metadati e messaggi”, anziché “LLM”; il testo ha più contrasto.
- Log MCP: il pulsante “Apri in ‘Messaggi’” spiega che non è disponibile quando non è stato possibile caricare l’elenco delle connessioni.

## 0.5.1
- Le note dell’aggiornamento in “Informazioni” → “Aggiornamenti” vengono mostrate nella lingua dell’interfaccia (in inglese se manca la traduzione) e come testo formattato invece che come Markdown grezzo. Vale per gli aggiornamenti proposti alla versione 0.5.1 e successive; le versioni precedenti mostrano le note come semplice testo in inglese.
- Il registro delle modifiche ora è in inglese (`CHANGELOG.md`); le sue versioni nelle altre lingue dell’interfaccia si trovano in `docs/changelog/`.

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
