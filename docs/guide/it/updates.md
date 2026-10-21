# Aggiornamenti e diagnostica

Questo capitolo tratta come aggiornare l’app, la schermata [[ui:sidebar.about]], l’archivio diagnostico e come sostenere il progetto.

## Aggiornamenti {#updates}

All’avvio, l’app verifica una volta se è disponibile una nuova versione. Se c’è, un avviso sopra le schede ne indica la versione. [[ui:about.notice.more]] apre la schermata [[ui:sidebar.about]]. Se la verifica non riesce, per esempio senza accesso a internet, non compare alcun messaggio.

Per verificare manualmente:

1. Apri [[ui:sidebar.about]] in fondo al pannello sinistro.
2. In [[ui:about.updates.title]], fai clic su [[ui:about.updates.check]].
3. Se è disponibile una nuova versione, la schermata ne mostra il numero e le note di rilascio. Fai clic su [[ui:about.updates.install]].

L’app scarica l’aggiornamento, ne verifica la firma, lo installa e si riavvia. Un download interrotto da un problema di rete viene ripetuto automaticamente, fino a tre tentativi in totale. Se tutti e tre i tentativi falliscono, l’app segnala [[ui:errors.updates.downloadInterrupted]]. Se la verifica della firma non riesce, l’app segnala [[ui:errors.updates.badSignature]].

L’installazione può chiedere una password di amministratore, perché l’app è installata per tutti gli utenti del computer: dal programma di installazione `.pkg` su macOS, dal programma di installazione `.msi` su Windows o da un pacchetto `.deb` o `.rpm` su Linux. Gli utenti senza diritti di amministratore non possono aggiornare un’installazione di questo tipo.

Se è in corso la lettura di messaggi, una sessione consumer o un invio in batch, l’app chiede prima conferma e indica quante sessioni sono attive. Fai clic su [[ui:about.updates.stopAndUpdate]] per interromperle e continuare, oppure su [[ui:common.cancel]]. Connessioni, impostazioni e altri dati vengono conservati dopo un aggiornamento.

Gli aggiornamenti vengono scaricati dalla pagina delle release su GitHub. Se la verifica non riesce con [[ui:errors.updates.checkFailed]], controlla la rete o il proxy. [[ui:errors.updates.notConfigured]] significa che questa build è stata creata senza la chiave del programma di aggiornamento, per esempio una build di sviluppo o di test. In tal caso scarica le nuove versioni dalla pagina delle release: https://github.com/vvnovg/glafira-kafka-releases/releases

Un’organizzazione può disattivare la verifica degli aggiornamenti tramite una policy, oppure indirizzarla a un mirror interno. Quando la verifica è disattivata, la sezione [[ui:about.updates.title]] indica [[ui:errors.updates.managed]] dopo il clic su [[ui:about.updates.check]]. Come [[ui:errors.updates.notConfigured]], questo messaggio compare solo dopo il clic; in questi casi la verifica all’avvio non mostra nulla.

## Informazioni {#about}

Apri [[ui:sidebar.about]] in fondo al pannello sinistro. Su macOS la schermata si trova anche nel menu Aiuto come [[ui:menu.about]].

La schermata mostra una breve descrizione dell’app e:

- [[ui:about.version]] — il numero di versione, e [[ui:about.build]] — l’identificatore del codice sorgente da cui è stata compilata l’app. Indica entrambi in una richiesta di assistenza.
- [[ui:about.system]] — il sistema operativo e l’architettura del processore.
- [[ui:about.license]] — Apache 2.0; il link apre il testo della licenza nel browser.
- [[ui:about.policy]] — [[ui:about.policySet]] o [[ui:about.policyNone]]. Gli errori della policy sono elencati sotto.
- [[ui:about.dataDir]] — la cartella con i profili delle connessioni, i modelli, la cronologia e le impostazioni.
- [[ui:about.logDir]] — la cartella con i file di log. Vedi [Log](troubleshooting.md#logs).

In questa schermata si trovano anche le sezioni [[ui:about.updates.title]] e [[ui:about.diagnostics.title]].

## Archivio diagnostico {#diagnostics}

L’archivio diagnostico raccoglie ciò che serve all’assistenza per capire un problema. Per crearlo:

1. Apri [[ui:sidebar.about]].
2. In [[ui:about.diagnostics.title]], fai clic su [[ui:about.diagnostics.collect]]. Su macOS puoi anche scegliere [[ui:menu.diagnostics]] nel menu Aiuto.
3. Scegli dove salvare il file. Il nome proposto è `diagnostics-<date>.zip`.

Una volta salvato l’archivio, la schermata ne mostra il percorso. Il file ZIP contiene:

- `info.json` — la versione e la build dell’app, il sistema operativo, i percorsi delle cartelle dei dati e dei log, e lo stato della policy;
- `connections.json` — i profili delle connessioni: nomi, ambienti, server bootstrap, protocolli, nomi utente SASL, percorsi dei file dei certificati, proprietà aggiuntive e timeout;
- `logs/` — i file di log dell’app.

L’archivio non contiene password: sono conservate nell’archivio dei segreti di sistema, e le proprietà aggiuntive con password e segreti vengono rifiutate al salvataggio di una connessione. Non sono inclusi nemmeno i file dei certificati. Nomi host, nomi utente e percorsi dei file sono memorizzati come testo in chiaro, e i log contengono gli indirizzi dei broker presenti nei messaggi di errore di Kafka. Esamina l’archivio prima di inviarlo fuori dalla tua azienda.

L’app non invia nulla da sola. Allega tu l’archivio alla richiesta di assistenza, vedi [Contattare l’assistenza](troubleshooting.md#support).

## Sostieni il progetto {#support}

Glafira è gratuita e open source con Apache License 2.0. Le donazioni la fanno vivere. [[ui:support.title]] in fondo al pannello sinistro, nella schermata [[ui:sidebar.about]] e, su macOS, nel menu Aiuto apre la pagina di sostegno nel browser: https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md
