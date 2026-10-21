# Connessioni

Una connessione è il profilo salvato di un cluster Kafka: indirizzi, impostazioni di sicurezza e password. Le connessioni sono elencate nel pannello [[ui:sidebar.connections]] a sinistra. Puoi organizzarle in cartelle e trasferirle su un altro computer con l’esportazione e l’importazione.

## Creare una connessione {#create}

1. Fai clic su `+` ( [[ui:sidebar.newConnection]] ) nell’intestazione del pannello [[ui:sidebar.connections]]. Per creare la connessione direttamente in una cartella, apri il menu della cartella e scegli [[ui:sidebar.folder.newConnection]].
2. Inserisci il [[ui:connections.form.name]]. Compare nel pannello e nei titoli delle schede.
3. In [[ui:connections.form.bootstrap]] inserisci uno o più indirizzi nel formato `host:port`, separati da virgole, per esempio `kafka1:9092,kafka2:9092`. Basta un broker raggiungibile: il client trova gli altri da solo.
4. Scegli il [[ui:connections.form.protocol]] e compila i campi che aggiunge. Vedi [Protocolli di sicurezza](connections.md#security).
5. Facoltativamente, scegli la [[ui:connections.form.folder]], un [[ui:connections.form.environment]] e il [[ui:connections.form.color]] del pallino accanto al nome.
6. Fai clic su [[ui:connections.form.test]], poi su [[ui:common.save]].

Dopo il salvataggio il modulo si chiude e si apre la panoramica del cluster.

Altri due campi servono per casi particolari. [[ui:connections.form.timeout]] indica quanto attendere la risposta di un broker. Il valore predefinito è `30000` ms, il minimo `1000`. Aumentalo per reti lente e VPN. [[ui:connections.form.props]] accetta qualsiasi impostazione di librdkafka nella forma `key=value`, una per riga. Le righe che iniziano con `#` vengono ignorate. Password e chiavi non sono ammesse: le proprietà il cui nome contiene `password` o `secret`, `sasl.jaas.config`, `ssl.key.pem` e `ssl.keystore.key` vengono rifiutate al salvataggio.

Per modificare una connessione in seguito, fai clic su `✎` ( [[ui:sidebar.editConnection]] ) accanto al nome. Per eliminarla, apri il modulo, fai clic su [[ui:common.delete]] e conferma con [[ui:connections.form.deleteYes]].

## Provare la connessione {#test}

[[ui:connections.form.test]] verifica passo per passo le impostazioni del modulo e si ferma al primo passaggio non riuscito:

1. [[ui:connections.form.step.config]] — gli indirizzi, i file dei certificati, le password e il nome utente SASL.
2. [[ui:connections.form.step.tcp]] — i nomi host vengono risolti e si apre una connessione TCP verso almeno un server bootstrap.
3. [[ui:connections.form.step.kafka]] — l’handshake TLS e l’accesso SASL riescono e il broker restituisce i metadati. In caso di successo, il passaggio mostra il numero di broker e di topic.

La prova usa i valori presenti nel modulo, comprese le password digitate ma non ancora salvate. Non viene salvato nulla e nel cluster non cambia nulla. Se un passaggio non riesce, sotto di esso compaiono il testo dell’errore e i dettagli. Gli errori più comuni sono spiegati in [Impossibile connettersi](troubleshooting.md#cannot-connect).

## Protocolli di sicurezza {#security}

Il protocollo scelto in [[ui:connections.form.protocol]] deve corrispondere al listener del broker a cui ti connetti. I tuoi amministratori Kafka sanno quale usare.

- `PLAINTEXT` — nessuna cifratura e nessun accesso. Tipico dei cluster locali e di test.
- `SSL` — il traffico è cifrato con TLS. Con un certificato client si tratta di mTLS.
- `SASL_PLAINTEXT` — accesso con nome utente e password, senza cifratura.
- `SASL_SSL` — accesso con nome utente e password su TLS.

Quando cambi protocollo, il modulo mostra solo i campi che servono a quel protocollo.

## SASL {#sasl}

Per `SASL_PLAINTEXT` e `SASL_SSL` compila tre campi:

- [[ui:connections.form.saslMechanism]] — `PLAIN`, `SCRAM-SHA-256` o `SCRAM-SHA-512`. È il cluster a stabilire quale funziona.
- [[ui:connections.form.username]].
- [[ui:connections.form.password]]. Viene salvata nell’archivio dei segreti di sistema, non nel profilo. Vedi [Password](connections.md#passwords).

SCRAM non invia mai la password vera e propria. `PLAIN` la invia così com’è, quindi con `SASL_PLAINTEXT` la password viaggia in chiaro. Quando possibile, usa `SASL_SSL` per i cluster esterni al tuo computer.

## SSL e la CA attendibile {#tls}

Per `SSL` e `SASL_SSL` il modulo aggiunge le impostazioni TLS.

[[ui:connections.tls.ca]] è il certificato dell’autorità (CA) che ha firmato i certificati dei broker. Lascia il campo vuoto se il sistema operativo considera già attendibile questa CA: in tal caso vengono usati i certificati radice di sistema. Altrimenti inserisci il percorso completo del file o fai clic su [[ui:connections.tls.pick]]. Dopo aver scelto un file compare il campo [[ui:connections.tls.caFormat]]. Per PKCS#12 e JKS c’è anche il campo [[ui:connections.tls.truststorePassword]].

L’opzione [[ui:connections.tls.verifyHostname]] è attiva per impostazione predefinita. Verifica che il certificato del broker sia stato emesso per il nome host a cui ti connetti. Disattivala solo negli ambienti di test, per esempio quando i broker usano certificati autofirmati con altri nomi.

## Certificato client (mTLS) {#mtls}

Se i broker autenticano i client tramite certificato, attiva [[ui:connections.tls.mtls]]. Il certificato e la chiave vengono emessi dai tuoi amministratori Kafka. Poi compila:

- [[ui:connections.tls.cert]] — il percorso del file del certificato.
- [[ui:connections.tls.certFormat]] — PEM, PKCS#12 o JKS.
- Per PEM: [[ui:connections.tls.key]] — il percorso del file della chiave privata, e [[ui:connections.tls.keyPasswordPem]] se la chiave è cifrata.
- Per PKCS#12: [[ui:connections.tls.keystorePassword]].
- Per JKS: [[ui:connections.tls.keystorePassword]], [[ui:connections.tls.keyPasswordJks]] se la chiave ha una password propria, e [[ui:connections.tls.alias]] se l’archivio contiene più chiavi.

I percorsi devono essere assoluti. I file vengono letti a ogni connessione, quindi dopo il rinnovo del certificato basta riconnettersi. L’app verifica che la chiave corrisponda al certificato e che il certificato non sia scaduto.

Un caso comune è un cluster in Kubernetes, dove il secret del client contiene `tls.crt`, `tls.key` e `ca.crt`. Salvali in file, scegli il protocollo `SSL`, imposta [[ui:connections.tls.ca]] su `ca.crt` e usa `tls.crt` e `tls.key` come certificato e chiave client PEM con password vuote.

## Formati dei certificati {#certificate-formats}

Glafira legge tre formati. Il formato viene scelto automaticamente in base all’estensione del file; controllalo nel campo del formato e cambialo se necessario.

- PEM — un file di testo che inizia con `-----BEGIN`. Estensioni `.pem`, `.crt`, `.cer`, `.key`. Per un certificato client, la chiave è un file separato. La CA attendibile può essere anche un certificato DER binario.
- PKCS#12 — un archivio binario con certificato e chiave. Estensioni `.p12`, `.pfx`.
- JKS — un keystore Java. Estensioni `.jks`, `.keystore`, `.truststore`.

Due tipi di file vanno prima convertiti:

- Gli archivi JCEKS non sono supportati. Convertili in PKCS#12:

```bash
keytool -importkeystore -srcstoretype JCEKS -srckeystore client.jceks -deststoretype PKCS12 -destkeystore client.p12
```

- Gli archivi PKCS#12 cifrati con algoritmi obsoleti (RC2, 3DES) potrebbero non aprirsi. Se l’app segnala un algoritmo obsoleto, ricrea il file con AES:

```bash
openssl pkcs12 -export -keypbe AES-256-CBC -certpbe AES-256-CBC -in tls.crt -inkey tls.key -out client.p12
```

## Password {#passwords}

La password SASL e le password di chiavi e archivi vengono salvate nell’archivio dei segreti di sistema: Portachiavi su macOS, Gestione credenziali su Windows, Secret Service su Linux. Non vengono mai scritte nel profilo della connessione, nei file di esportazione né nell’archivio diagnostico.

Quando modifichi una connessione salvata, i campi delle password sono vuoti e mostrano [[ui:connections.form.passwordKeep]]. Lascia un campo vuoto per mantenere la password salvata, oppure digitane una nuova per sostituirla. Per eliminare la password salvata di un certificato, seleziona [[ui:connections.tls.clear]] accanto al campo. Al salvataggio vengono eliminate le password che la connessione non usa più, per esempio dopo il passaggio del protocollo a `PLAINTEXT`.

Se l’app non riesce ad accedere all’archivio dei segreti, al salvataggio compare un errore. Su Linux verifica che GNOME Keyring, KWallet o un altro provider Secret Service sia in esecuzione nella tua sessione.

## Ambienti e prod {#environments}

[[ui:connections.form.environment]] contrassegna la connessione: `local`, `dev`, `test`, `stage` o `prod`. L’ambiente compare come etichetta colorata accanto al nome nel pannello, così vedi sempre con quale cluster stai lavorando.

`prod` aggiunge una protezione. Qualsiasi modifica su una connessione prod, come creare un topic, inviare un messaggio, reimpostare gli offset o avviare una sessione consumer, richiede di digitare il nome della connessione. L’eliminazione e lo svuotamento chiedono il nome dell’oggetto su qualsiasi connessione (l’eliminazione di più topic ne chiede il numero), e su prod vanno digitati entrambi. Queste conferme compaiono all’interno della finestra dell’app.

Una nuova connessione prod nasce con l’opzione [[ui:common.readOnly]] attiva. Deseleziona la casella se devi apportare modifiche.

## Sola lettura {#read-only}

L’opzione [[ui:common.readOnly]] blocca tutto ciò che modifica il cluster: creazione, modifica, svuotamento ed eliminazione dei topic, invio di messaggi, reimpostazione degli offset, eliminazione dei gruppi e avvio di sessioni consumer. I pulsanti di queste azioni sono nascosti o disattivati, e anche il core dell’app rifiuta tali operazioni. La consultazione e la lettura restano disponibili. Un’icona a forma di lucchetto accanto al nome nel pannello contrassegna le connessioni in sola lettura.

Usa questa modalità quando devi solo guardare i dati, soprattutto sui cluster condivisi. La sola lettura è una protezione interna all’app: non sostituisce i diritti di accesso (ACL) in Kafka.

## Cartelle {#folders}

Le cartelle aiutano a organizzare le connessioni, per esempio per progetto o per ambiente. Le cartelle possono essere annidate.

- Per creare una cartella al primo livello, fai clic su `+📁` ( [[ui:sidebar.newFolder]] ) nell’intestazione del pannello. Per creare una sottocartella, apri il menu della cartella e scegli [[ui:sidebar.newFolder]]. Digita il nome e premi `Enter`, oppure premi `Esc` per annullare.
- Per aprire il menu della cartella, fai clic su `⋯` ( [[ui:sidebar.menu]] ) nella riga della cartella o fai clic con il tasto destro sulla cartella. Il menu contiene [[ui:sidebar.folder.rename]], [[ui:sidebar.folder.delete]] e i comandi di esportazione e importazione.
- Si può eliminare solo una cartella vuota. Sposta o elimina prima le sue connessioni e sottocartelle.
- I nomi delle cartelle devono essere univoci all’interno di uno stesso livello e lunghi al massimo 100 caratteri.

Per spostare una connessione, trascinala su una cartella, oppure su un’altra connessione per posizionarla prima di quella. Per spostarla al primo livello, rilasciala sull’intestazione del pannello o nello spazio vuoto sotto l’elenco. Puoi anche scegliere la cartella in [[ui:connections.form.folder]] nel modulo della connessione; [[ui:connections.form.folderRoot]] indica il primo livello. Le cartelle si trascinano allo stesso modo. Rilascia una cartella sul bordo superiore di un’altra per posizionarla prima di quella. Una cartella non può essere spostata in una propria sottocartella.

Fai clic sul nome di una cartella per comprimerla o espanderla. L’app ricorda quali cartelle sono compresse. Usa [[ui:sidebar.filter]] sopra l’albero per trovare connessioni e cartelle per nome. Le cartelle corrispondenti vengono mostrate con tutto il loro contenuto.

## Esportazione {#export}

L’esportazione salva connessioni e cartelle in un file, per esempio per condividerle con i colleghi o trasferirle su un altro computer.

1. Per esportare tutto, apri il menu `⋯` dell’intestazione del pannello e scegli [[ui:sidebar.exportAll]]. Per esportare una cartella con le sue sottocartelle, scegli [[ui:sidebar.folder.export]] nel suo menu.
2. Scegli dove salvare il file. Il nome proposto è `glafira-connections-<date>.glafira-connections.json` o `<folder>.glafira-connections.json`.

Una volta salvato il file, il pannello mostra quante connessioni e cartelle contiene.

Il file contiene le impostazioni delle connessioni e la struttura delle cartelle. Non contiene password né proprietà segrete. Non include nemmeno i file dei certificati: ne conserva solo i percorsi. Host, nomi utente e percorsi sono memorizzati come testo in chiaro, quindi condividi il file solo con chi è autorizzato a vederli.

## Importazione {#import}

1. Apri il menu `⋯` dell’intestazione del pannello e scegli [[ui:sidebar.import]]. Per importare direttamente in una cartella, scegli [[ui:sidebar.folder.importHere]] nel suo menu.
2. Scegli un file `.glafira-connections.json`. Si apre la finestra [[ui:exchange.import.title]].
3. In [[ui:exchange.import.target]] scegli dove vanno le nuove cartelle e connessioni. [[ui:connections.form.folderRoot]] è il primo livello.
4. Lascia attiva l’opzione [[ui:exchange.import.updateExisting]] per aggiornare dal file le connessioni che esistono già. Disattivala per lasciarle invariate.
5. Controlla l’anteprima e fai clic su [[ui:exchange.import.apply]], oppure su [[ui:common.cancel]] per chiudere la finestra senza modifiche.

L’anteprima si aggiorna a ogni modifica nella finestra. Mostra i totali ed elenchi espandibili: [[ui:exchange.import.groupAdded]], [[ui:exchange.import.groupUpdated]], [[ui:exchange.import.groupUnchanged]] e [[ui:exchange.import.groupSkipped]], con il motivo per ogni connessione saltata.

Come l’app abbina il file alle tue connessioni:

- Le connessioni vengono abbinate tramite un ID interno, non per nome. Una connessione è considerata esistente se il file contiene la stessa connessione: per esempio, è stata esportata da questo computer o importata in precedenza dalla stessa origine.
- Una connessione aggiornata riceve le impostazioni dal file. Le tue password e la sua posizione nell’albero vengono mantenute.
- Le cartelle esistenti non vengono rinominate né spostate. Le nuove connessioni del file vengono comunque inserite al loro interno.
- Se una nuova connessione ha lo stesso nome di un’altra nella stessa cartella, viene aggiunto un numero, per esempio `kafka-dev (2)`.
- Le connessioni che questa versione di Glafira non supporta, o che contengono segreti nelle proprietà aggiuntive, vengono saltate.

L’anteprima avvisa anche quando i file dei certificati non si trovano su questo computer. Le password non sono nel file, quindi dopo l’importazione il pannello indica quante connessioni ne hanno bisogno. Apri ciascuna di queste connessioni, inserisci le password e salva.
