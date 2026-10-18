# Licenza e aggiornamenti

Questo capitolo tratta che cosa aggiunge Pro, come attivare una chiave di licenza, che cosa succede quando la licenza scade, come aggiornare l’app, la schermata [[ui:sidebar.about]] e l’archivio diagnostico.

## Free e Pro {#free-and-pro}

Free copre il lavoro quotidiano con Kafka: tutti i protocolli di sicurezza, mTLS compreso, la panoramica del cluster e i topic, la consultazione dei messaggi, il Producer con l’invio in batch, i consumer group e la reimpostazione degli offset. L’interfaccia è disponibile in inglese e russo.

Pro aggiunge:

- le sessioni consumer: lettura come membro di un gruppo, commit degli offset, pausa delle partizioni. Vedi [Sessioni consumer](consumer-sessions.md#what-is);
- la modalità prod: creazione di connessioni prod, passaggio di connessioni a prod e disattivazione dell’opzione [[ui:common.readOnly]] su una connessione prod. Vedi [Ambienti e prod](connections.md#environments);
- altre sei lingue dell’interfaccia: tedesco, francese, spagnolo, italiano, polacco e portoghese (Brasile). Vedi [Lingua dell’interfaccia](getting-started.md#language).

In Free queste funzioni sono visibili ma non disponibili. I comandi delle sessioni consumer sono disattivati e contrassegnati da un lucchetto e da `Pro`. L’opzione prod compare come [[ui:connections.form.prodPro]]. Le lingue Pro sono contrassegnate da `Pro` nell’elenco delle lingue. Se un’azione richiede Pro, l’errore indica [[ui:errors.license.proRequired]] e offre il pulsante [[ui:common.openLicense]].

La schermata [[ui:tabs.license]] elenca le sessioni consumer e la modalità prod in [[ui:license.whatProGives]]. Le lingue aggiuntive non sono in quell’elenco.

## Attivare una chiave {#activate}

Una chiave di licenza è una riga di testo emessa per una persona o per un’organizzazione. L’app verifica la chiave sul tuo computer e non contatta alcun server.

1. Fai clic su [[ui:tabs.license]] in fondo al pannello sinistro.
2. Incolla l’intera chiave in [[ui:license.keyLabel]].
3. Fai clic su [[ui:license.activate]].

Le funzioni Pro diventano subito disponibili. La schermata mostra [[ui:license.tier]] (Pro), [[ui:license.licensee]], [[ui:license.kind]] ( [[ui:license.kindPersonal]] o [[ui:license.kindOrganization]] ) e [[ui:license.expires]]: una data, oppure [[ui:license.perpetual]]. La chiave viene salvata nella cartella dei dati dell’app, quindi resta dopo un riavvio o un aggiornamento. Una nuova chiave sostituisce quella precedente.

Se la chiave non viene accettata, il motivo compare sotto il campo:

- [[ui:errors.license.malformed]] — una parte della chiave manca o è stata modificata. Copia di nuovo la chiave come un’unica riga, senza caratteri in più.
- [[ui:errors.license.badSignature]] — la chiave non è stata emessa per Glafira, oppure è stata modificata.
- La chiave è scaduta — il messaggio ne indica la data di scadenza. Richiedi una nuova chiave.
- La chiave richiede una versione più recente dell’app — il messaggio indica la versione della chiave. Aggiorna prima l’app, vedi [Aggiornamenti](license-updates.md#updates).

Per rimuovere la chiave, fai clic su [[ui:license.removeKey]]. L’app chiede: [[ui:license.removeQuestion]] Fai clic su [[ui:license.removeYes]] per confermare. I tuoi dati restano.

Un’organizzazione può attivare Pro per tutti gli utenti tramite una policy. Il campo della chiave e i pulsanti sono allora nascosti, e la schermata indica: [[ui:license.managed]] Una chiave inserita da te non viene usata finché è impostata la chiave della policy. La sezione [[ui:license.policyTitle]] mostra da dove proviene la policy ed elenca i suoi errori. Se la chiave dell’organizzazione non è valida, il motivo compare nella schermata [[ui:tabs.license]]. In tal caso rivolgiti ai tuoi amministratori. Gli amministratori trovano le impostazioni della policy in `docs/admin.md` nel codice sorgente di Glafira.

## Scadenza {#expiry}

Una chiave può avere una data di scadenza, indicata in [[ui:license.expires]]. La licenza è valida fino alla fine di quel giorno.

Quando mancano meno di 14 giorni, un avviso sopra le schede indica quanti giorni restano, e la schermata [[ui:tabs.license]] mostra la data di scadenza. [[ui:license.notice.more]] nell’avviso apre la schermata [[ui:tabs.license]]. Il pulsante `×` nasconde l’avviso fino al riavvio dell’app.

Dopo la data di scadenza, il piano passa a Free. L’avviso e la schermata [[ui:tabs.license]] indicano che la licenza è scaduta e che le funzioni Pro sono disattivate. Che cosa cambia:

- Non si possono avviare nuove sessioni consumer. In una sessione già in corso, i commit e la pausa delle partizioni vengono rifiutati con un errore che indica la data di scadenza.
- Le connessioni prod continuano a funzionare con le stesse conferme. Non si possono creare nuove connessioni prod, e su una connessione prod non si può disattivare l’opzione [[ui:common.readOnly]].
- Se era selezionata una lingua Pro, l’interfaccia passa all’inglese. La tua scelta viene conservata.

Connessioni, password, modelli e cronologia degli invii vengono conservati. Per continuare con Pro, attiva una nuova chiave come descritto in [Attivare una chiave](license-updates.md#activate).

## Aggiornamenti {#updates}

All’avvio, l’app verifica una volta se è disponibile una nuova versione. Se c’è, un avviso sopra le schede ne indica la versione. [[ui:license.notice.more]] apre la schermata [[ui:sidebar.about]]. Se la verifica non riesce, per esempio senza accesso a internet, non compare alcun messaggio.

Per verificare manualmente:

1. Apri [[ui:sidebar.about]] in fondo al pannello sinistro.
2. In [[ui:about.updates.title]], fai clic su [[ui:about.updates.check]].
3. Se è disponibile una nuova versione, la schermata ne mostra il numero e le note di rilascio. Fai clic su [[ui:about.updates.install]].

L’app scarica l’aggiornamento, ne verifica la firma, lo installa e si riavvia. Un download interrotto da un problema di rete viene ripetuto automaticamente, fino a tre tentativi in totale. Se tutti e tre i tentativi falliscono, l’app segnala [[ui:errors.updates.downloadInterrupted]]. Se la verifica della firma non riesce, l’app segnala [[ui:errors.updates.badSignature]].

L’installazione può chiedere una password di amministratore, perché l’app è installata per tutti gli utenti del computer: dal programma di installazione `.pkg` su macOS, dal programma di installazione `.msi` su Windows o da un pacchetto `.deb` o `.rpm` su Linux. Gli utenti senza diritti di amministratore non possono aggiornare un’installazione di questo tipo.

Se è in corso la lettura di messaggi, una sessione consumer o un invio in batch, l’app chiede prima conferma e indica quante sessioni sono attive. Fai clic su [[ui:about.updates.stopAndUpdate]] per interromperle e continuare, oppure su [[ui:common.cancel]]. Connessioni, impostazioni, chiave di licenza e altri dati vengono conservati dopo un aggiornamento.

Gli aggiornamenti vengono scaricati dalla pagina delle release su GitHub. Se la verifica non riesce con [[ui:errors.updates.checkFailed]], controlla la rete o il proxy. [[ui:errors.updates.notConfigured]] significa che questa build è stata creata senza la chiave del programma di aggiornamento, per esempio una build di sviluppo o di test. In tal caso scarica le nuove versioni dalla pagina delle release: https://github.com/vvnovg/glafira-kafka-releases/releases

Un’organizzazione può disattivare la verifica degli aggiornamenti tramite una policy, oppure indirizzarla a un mirror interno. Quando la verifica è disattivata, la sezione [[ui:about.updates.title]] indica [[ui:errors.updates.managed]] dopo il clic su [[ui:about.updates.check]]. Come [[ui:errors.updates.notConfigured]], questo messaggio compare solo dopo il clic; in questi casi la verifica all’avvio non mostra nulla.

## Informazioni {#about}

Apri [[ui:sidebar.about]] in fondo al pannello sinistro. Su macOS la schermata si trova anche nel menu Aiuto come [[ui:menu.about]].

La schermata mostra una breve descrizione dell’app e:

- [[ui:about.version]] — il numero di versione, e [[ui:about.build]] — l’identificatore del codice sorgente da cui è stata compilata l’app. Indica entrambi in una richiesta di assistenza.
- [[ui:about.system]] — il sistema operativo e l’architettura del processore.
- [[ui:about.tier]] — Free o Pro.
- [[ui:about.policy]] — [[ui:about.policySet]] o [[ui:about.policyNone]]. Gli errori della policy sono elencati sotto.
- [[ui:about.dataDir]] — la cartella con i profili delle connessioni, i modelli, la cronologia, le impostazioni e la chiave di licenza.
- [[ui:about.logDir]] — la cartella con i file di log. Vedi [Log](troubleshooting.md#logs).

In questa schermata si trovano anche le sezioni [[ui:about.updates.title]] e [[ui:about.diagnostics.title]].

## Archivio diagnostico {#diagnostics}

L’archivio diagnostico raccoglie ciò che serve all’assistenza per capire un problema. Per crearlo:

1. Apri [[ui:sidebar.about]].
2. In [[ui:about.diagnostics.title]], fai clic su [[ui:about.diagnostics.collect]]. Su macOS puoi anche scegliere [[ui:menu.diagnostics]] nel menu Aiuto.
3. Scegli dove salvare il file. Il nome proposto è `diagnostics-<date>.zip`.

Una volta salvato l’archivio, la schermata ne mostra il percorso. Il file ZIP contiene:

- `info.json` — la versione e la build dell’app, il sistema operativo, i percorsi delle cartelle dei dati e dei log, lo stato della policy e lo stato della licenza (piano, intestatario, tipo, data di scadenza);
- `connections.json` — i profili delle connessioni: nomi, ambienti, server bootstrap, protocolli, nomi utente SASL, percorsi dei file dei certificati, proprietà aggiuntive e timeout;
- `logs/` — i file di log dell’app.

L’archivio non contiene password: sono conservate nell’archivio dei segreti di sistema, e le proprietà aggiuntive con password e segreti vengono rifiutate al salvataggio di una connessione. Non sono inclusi nemmeno la chiave di licenza e i file dei certificati. Nomi host, nomi utente e percorsi dei file sono memorizzati come testo in chiaro, e i log contengono gli indirizzi dei broker presenti nei messaggi di errore di Kafka. Esamina l’archivio prima di inviarlo fuori dalla tua azienda.

L’app non invia nulla da sola. Allega tu l’archivio alla richiesta di assistenza, vedi [Contattare l’assistenza](troubleshooting.md#support).
