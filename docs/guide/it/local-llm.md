# Modelli locali

L’assistente non deve per forza essere eseguito nel cloud: Glafira risponde allo stesso modo a ogni client MCP, e il client può comunicare con un modello nella tua rete o sul tuo computer. Questo capitolo spiega dove può essere eseguito il modello, di quale hardware ha bisogno un modello locale, come collegarlo a Glafira e come verificare il risultato. Come funziona l’accesso dell’assistente ai cluster è descritto in [Assistenti LLM (MCP)](llm-mcp.md#how-it-works).

## Dove viene eseguito il modello {#where}

1. **Un modello cloud** (Claude, GPT e altri): l’opzione più potente, ma le richieste con i metadati del cluster — e, con il livello [[ui:connections.form.llmLevel.messages]], anche con il contenuto dei messaggi — escono dalla tua rete. Molte aziende lo vietano.
2. **Un server di modelli aziendale** all’interno della tua rete: i dati restano nel perimetro, e gli amministratori scelgono il modello e l’hardware. Se un server del genere non esiste ancora, è il passo successivo.
3. **Un modello sul computer dell’ingegnere**: per quando il cloud è vietato e un server aziendale non esiste ancora o è in via di configurazione. I dati non lasciano mai il computer, ma il modello è più debole di uno cloud e risponde più lentamente.

Il resto del capitolo riguarda la terza opzione; un client per la seconda si configura allo stesso modo, cambia solo l’indirizzo del server.

## Cosa può fare un modello locale {#capabilities}

Testato su un Mac con M4 Pro e 48 GB di memoria, un contesto di 64k token e un cluster di prova con un broker e tre topic (7 partizioni, circa 3000 messaggi). Tre attività tipiche: un controllo del cluster con spiegazione dei risultati, il lag di un gruppo senza membri e la ricerca di un messaggio in base all’ora indicata nel log di un servizio. Ogni modello è stato eseguito tre volte.

| Modello (4 bit) | Server | Risposte corrette | Generazione | Controllo del cluster | Lag del gruppo | Ricerca del messaggio |
|---|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | Ollama | 9 su 9 | 70–80 token/s | 23–29 s | 8–31 s | 34–43 s |
| Qwen3.6-35B-A3B | vllm-metal | 9 su 9 | 25–30 token/s | 33–52 s | 43–60 s | circa 2 min |
| Qwen3.8-27B | vllm-metal | 9 su 9 | 8–10 token/s | 8–16 min | 10–12 min | 28–46 min |

Qwen3.8-27B dà risposte più dettagliate, ma è un modello denso: per ogni token usa tutti i 27 miliardi di parametri, mentre Qwen3.6-35B-A3B ne usa circa 3 miliardi. Per il lavoro quotidiano su un Mac scegli Qwen3.6-35B-A3B in Ollama: lì lo stesso modello risponde da 1,5 a 3 volte più velocemente che su vllm-metal.

Un modello locale gestisce peggio di uno cloud le indagini lunghe e articolate in molti passaggi: sbaglia di più gli argomenti degli strumenti e può trarre una conclusione errata da dati incompleti. Verifica le sue conclusioni con il registro MCP.

## Hardware e modelli {#hardware}

Su hardware comune funzionano meglio i modelli MoE con pochi parametri attivi (circa 3 miliardi, come Qwen3.6-35B-A3B): sono più veloci dei modelli densi della stessa dimensione. Il contesto deve essere di almeno 32k token, meglio 64k.

| Piattaforma | Server di modelli | Cosa ci sta |
|---|---|---|
| macOS, Apple Silicon, 48 GB o più | vllm-metal od Ollama | Qwen3.6-35B-A3B (testato) |
| macOS, Apple Silicon, 32 GB | Ollama | gpt-oss-20b; 35B-A3B è al limite |
| Linux, NVIDIA con 24 GB di memoria video | vLLM | 35B-A3B a 4 bit con un contesto più piccolo |
| Windows, NVIDIA | Ollama; vLLM solo in WSL2 | come su Linux |
| x86 senza GPU, 32–64 GB di DDR5 | Ollama | solo modelli MoE, lentamente |

Un Mac con processore Intel non è adatto ai modelli locali.

## Server di modelli {#server}

Il server deve offrire un’API compatibile con OpenAI con chiamata agli strumenti. Su un Mac, [Ollama](https://ollama.com) è l’opzione più semplice: installa l’app, scarica il modello e crea una sua variante con un contesto di 64k token — per impostazione predefinita Ollama usa un contesto più piccolo e le descrizioni degli strumenti di Glafira non ci stanno:

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

Il client ha bisogno dell’indirizzo `http://127.0.0.1:11434/v1`, del modello `qwen3.6-64k:35b-a3b` e di una chiave API qualsiasi, purché non vuota. Ollama ascolta solo su `127.0.0.1`, quindi gli altri computer non possono collegarsi al modello.

Dopo 5 minuti di inattività Ollama scarica il modello dalla memoria e la domanda successiva attende che venga ricaricato. Per tenere il modello in memoria più a lungo, imposta la variabile d’ambiente `OLLAMA_KEEP_ALIVE`, ad esempio a `1h`.

Su Linux con NVIDIA la scelta abituale è vLLM. Interpreta le chiamate agli strumenti con il flag `--tool-call-parser`, impostato per la famiglia del modello (`qwen3_coder` per Qwen3.6); in Ollama se ne occupa il template del modello.

## Impostazioni di Glafira {#glafira-settings}

Nella sezione [[ui:settings.mcp.section]]:

- [[ui:settings.mcp.responseLimit]] — la risposta più grande di una chiamata a uno strumento. Una risposta da 100 KB occupa circa 30k token, quindi per un contesto da 64k scegli 48 o 24 KB: Glafira tronca gli elenchi lunghi e dice al modello di restringere la richiesta.
- [[ui:settings.mcp.colCompact]] nell’elenco [[ui:settings.mcp.clients]] — un insieme di strumenti senza note e proposte di modifica: le loro descrizioni occupano circa 1000 token di contesto. La casella compare quando il client si collega e lo approvi; il client vede il nuovo elenco dopo essersi ricollegato.

## Verifica {#check}

1. Collega il client e approvalo nella finestra di dialogo di Glafira.
2. Apri una connessione per l’assistente, meglio se di prova, con il livello [[ui:connections.form.llmLevel.metadata]].
3. Fai tre o quattro domande tipiche: “Controlla il cluster e spiega i risultati”, “Perché il gruppo … ha un lag elevato?”, “Quali topic non hanno replica?”.
4. In [[ui:settings.mcp.audit.title]] controlla quali strumenti ha chiamato il modello e con quale risultato, e confronta la sua risposta con i dati in Glafira.
5. Se apri i messaggi, controlla il mascheramento: il pulsante [[ui:settings.mcp.audit.openInBrowse]] mostra gli stessi messaggi senza mascheramento.
6. Disattiva la rete e ripeti una domanda: la risposta deve arrivare anche senza accesso a internet.

## Limitazioni {#limits}

- Un modello locale risponde diverse volte più lentamente di uno cloud, e i modelli densi come Qwen3.8-27B sono ancora più lenti su un Mac (vedi la [tabella](local-llm.md#capabilities)).
- Mentre il modello è in esecuzione, al computer resta meno memoria: su un Mac da 48 GB un modello da 35 miliardi di parametri occupa circa 20 GB.
- Il modello può trarre una conclusione errata. Glafira non gli permette di modificare nulla nel cluster, ma le decisioni spettano a te.
