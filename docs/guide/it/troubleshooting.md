# Risoluzione dei problemi

Questo capitolo spiega gli errori di connessione più comuni e che cosa controllare per ciascuno. Indica anche dove si trovano i log e che cosa includere in una richiesta di assistenza. Su macOS, il menu Aiuto apre questo capitolo con [[ui:menu.troubleshooting]].

Un messaggio di errore ha un breve testo e, nella maggior parte delle schermate, il codice dell’errore tra parentesi, per esempio `TIMEOUT`. Espandi [[ui:common.details]] per vedere il messaggio originale del client Kafka (librdkafka). Spesso indica l’indirizzo esatto del broker e la causa.

## Impossibile connettersi {#cannot-connect}

Inizia con [[ui:connections.form.test]] nel modulo della connessione, vedi [Provare la connessione](connections.md#test). La prova si ferma al primo passaggio non riuscito, e il passaggio indica dove cercare:

- [[ui:connections.form.step.config]] — il problema è nel modulo stesso: un indirizzo senza porta, un file di certificato mancante o illeggibile, una password del certificato errata. L’errore indica il campo. Per i file dei certificati, vedi [Errori TLS](troubleshooting.md#tls-errors).
- [[ui:connections.form.step.tcp]] — il computer non riesce a raggiungere i server bootstrap. Vedi [DNS e TCP](troubleshooting.md#dns-tcp).
- [[ui:connections.form.step.kafka]] — la rete funziona, ma l’handshake TLS, l’accesso SASL o la richiesta dei metadati non sono riusciti. Vedi [Errori TLS](troubleshooting.md#tls-errors), [Errori SASL](troubleshooting.md#sasl-errors) e [Timeout](troubleshooting.md#timeouts).

La causa più comune è un protocollo che non corrisponde alla porta. Ogni listener del broker ha una porta e un protocollo propri. Chiedi ai tuoi amministratori Kafka quale porta corrisponde a quale protocollo, vedi [Protocolli di sicurezza](connections.md#security).

Se la prova riesce ma la lettura o l’invio non funzionano, la causa sono spesso gli indirizzi dei broker. Il client ottiene l’elenco dei broker dal server bootstrap e poi si connette a ciascun broker all’indirizzo che il broker pubblica (`advertised.listeners`). Se questi indirizzi sono interni, per esempio nomi host di Docker o Kubernetes, non sono raggiungibili dal tuo computer. Gli indirizzi sono elencati nella tabella dei broker della panoramica del cluster, vedi [Panoramica del cluster](topics.md#cluster-overview). Chiedi ai tuoi amministratori un listener raggiungibile dall’esterno.

## DNS e TCP {#dns-tcp}

[[ui:errors.kafka.tcpUnreachable]] significa che l’app non è riuscita ad aprire una connessione TCP verso nessuno degli indirizzi in [[ui:connections.form.bootstrap]]. Per ogni indirizzo l’attesa massima è di 5 secondi. I dettagli mostrano l’ultimo indirizzo provato e il motivo. `DNS` nei dettagli significa che il nome host non è stato risolto.

Controlla:

- il nome host e la porta, senza un prefisso di protocollo come `https://`;
- che la VPN sia attiva, se il cluster si trova in una rete privata;
- che un firewall non blocchi la porta;
- per un broker in Docker, che la porta sia pubblicata sull’host, per esempio `-p 9092:9092`.

Per verificare la porta al di fuori dell’app, esegui `nc -vz <host> <port>` su macOS o Linux, oppure `Test-NetConnection <host> -Port <port>` in PowerShell su Windows.

[[ui:errors.kafka.connectionFailed]] dopo che il passaggio TCP è riuscito significa di solito che un broker ha chiuso la connessione o non è stato raggiungibile in seguito. Controlla anche gli indirizzi dei broker, vedi [Impossibile connettersi](troubleshooting.md#cannot-connect).

## Errori TLS {#tls-errors}

Errori nel passaggio [[ui:connections.form.step.kafka]] durante l’handshake TLS:

- [[ui:errors.kafka.handshakeClosed]]. Cause frequenti: la porta non ha TLS — controlla la porta e il protocollo; il broker ha rifiutato il certificato client senza indicarne il motivo — controlla la data di scadenza e l’emittente del certificato; un proxy davanti ai broker, come Istio o Envoy, non ha una route per questo indirizzo — connettiti con il nome usato dalle applicazioni del cluster. La causa esatta è nel log del broker.
- [[ui:errors.kafka.caUntrusted]]. In [[ui:connections.tls.ca]] indica il file della CA che ha firmato i certificati dei broker. I tuoi amministratori possono fornirti questo file. Con il campo vuoto, sono considerati attendibili solo i certificati radice di sistema.
- [[ui:errors.kafka.hostnameMismatch]]. Connettiti con il nome per cui è stato emesso il certificato, non con un indirizzo IP o un alias. Solo negli ambienti di test puoi disattivare [[ui:connections.tls.verifyHostname]].
- [[ui:errors.kafka.clientCertRejected]]. Il broker richiede un certificato client, oppure non considera attendibile quello che usi. Verifica che [[ui:connections.tls.mtls]] sia attivo e che il certificato sia stato emesso per questo cluster. Vedi [Certificato client (mTLS)](connections.md#mtls).
- [[ui:errors.kafka.tlsError]] — un altro problema TLS. Espandi [[ui:common.details]] per conoscerne la causa.

Se hai scelto un protocollo senza SSL e vedi comunque un errore TLS, probabilmente la porta appartiene a un listener TLS. In tal caso i dettagli riportano `connecting to a SSL listener?`. Scegli `SSL` o `SASL_SSL`, oppure usa la porta di un listener senza TLS.

I problemi con i file dei certificati vengono segnalati nel passaggio [[ui:connections.form.step.config]], prima che l’app si connetta. Il messaggio inizia con il nome del campo: CA attendibile, certificato client o chiave client. Casi comuni:

- Il percorso non è assoluto o il file non si può leggere. Inserisci il percorso completo o scegli il file con [[ui:connections.tls.pick]].
- [[ui:errors.tls.keyEncrypted]], [[ui:errors.tls.keyBadPassword]], [[ui:errors.tls.clientPkcs12BadPassword]] o [[ui:errors.tls.jksBadStorePassword]] — inserisci la password corretta nel modulo.
- [[ui:errors.tls.keyMismatch]] — il file della chiave appartiene a un altro certificato.
- Il certificato client è scaduto o non è ancora valido — il messaggio indica la data. Richiedi un nuovo certificato.
- L’archivio PKCS#12 usa un algoritmo obsoleto, oppure l’archivio è in formato JCEKS. Converti il file come indicato in [Formati dei certificati](connections.md#certificate-formats).

## Errori SASL {#sasl-errors}

[[ui:errors.kafka.authFailed]] significa che il broker non ha accettato l’accesso SASL. Controlla:

- il nome utente e la password. Quando modifichi una connessione salvata, un campo password vuoto mantiene la password salvata. Digita di nuovo la password se non sei sicuro che sia corretta;
- [[ui:connections.form.saslMechanism]]. Deve essere il meccanismo per cui è stato creato il tuo utente. Le credenziali SCRAM vengono create separatamente per `SCRAM-SHA-256` e `SCRAM-SHA-512`, e gli utenti `PLAIN` vengono configurati a loro volta separatamente;
- il protocollo: `SASL_SSL` e `SASL_PLAINTEXT` usano di solito porte diverse.

[[ui:errors.connections.saslPasswordMissing]] — per questa connessione non è salvata alcuna password, per esempio dopo un’importazione. Apri la connessione, inserisci la password e salva. Vedi [Password](connections.md#passwords).

[[ui:errors.secrets.keychain]] — l’app non riesce a leggere la password dall’archivio dei segreti di sistema. Su macOS, consenti l’accesso quando il Portachiavi lo chiede. Su Linux, verifica che GNOME Keyring, KWallet o un altro provider Secret Service sia in esecuzione nella tua sessione.

[[ui:errors.kafka.notAuthorized]] è un caso diverso: l’accesso è riuscito, ma l’utente Kafka non ha i diritti (ACL) per questa operazione, per esempio per leggere un topic o un gruppo. Chiedi i diritti ai tuoi amministratori. Tieni presente che Kafka non segnala alcun errore per i topic che non hai il permesso di descrivere: semplicemente non compaiono nell’elenco dei topic.

## Timeout {#timeouts}

[[ui:errors.kafka.timeout]] significa che il broker non ha risposto in tempo. Le cause tipiche sono una rete o una VPN lenta, un cluster sovraccarico o indirizzi dei broker non raggiungibili, vedi [Impossibile connettersi](troubleshooting.md#cannot-connect).

Un problema TLS o SASL può sembrare un timeout. L’app cerca di trovare la causa reale e di mostrarla al suo posto, ma non sempre è possibile. Espandi [[ui:common.details]]: messaggi come `SSL handshake failed` o `SASL authentication error` indicano la causa.

Se la rete è semplicemente lenta, aumenta [[ui:connections.form.timeout]] nel modulo della connessione. Il valore predefinito è `30000` ms. Questo valore si applica alle richieste ai broker, all’invio dei messaggi e alla lettura. La prova della connessione usa limiti propri (5 secondi per ogni indirizzo TCP e 10 secondi per il passaggio Kafka), quindi un valore più alto non cambia la prova.

## Un’azione non è disponibile o viene rifiutata {#read-only-blocked}

Se un pulsante è nascosto o disattivato, o un’azione viene rifiutata, controlla queste cause:

- La connessione è in sola lettura. La contrassegna un’icona a forma di lucchetto accanto al nome. I pulsanti che modificano il cluster sono nascosti o disattivati. Se una richiesta di questo tipo arriva comunque al core dell’app, per esempio dopo che la modalità è stata attivata mentre una schermata era aperta, l’errore indica che la connessione è in sola lettura. Per apportare modifiche, apri la connessione, disattiva l’opzione [[ui:common.readOnly]] e salva. Vedi [Sola lettura](connections.md#read-only).
- La connessione è prod. Le modifiche chiedono di digitare il nome della connessione. Finché il nome non corrisponde, l’azione non viene eseguita. Vedi [Ambienti e prod](connections.md#environments).
- Il consumer group ha membri attivi. Gli offset si possono reimpostare e un gruppo si può eliminare solo quando non ha membri. Vedi [Reimpostare gli offset](groups.md#reset-offsets).
- L’impostazione è gestita dalla tua organizzazione. Una policy può impostare la lingua dell’interfaccia e la verifica degli aggiornamenti, e queste impostazioni non si possono cambiare nell’app. Vedi [Impostazioni definite dalla tua organizzazione](getting-started.md#managed-settings).
- Kafka ha rifiutato l’operazione con [[ui:errors.kafka.notAuthorized]]. È una questione di diritti nel cluster, non di impostazioni di Glafira. Vedi [Errori SASL](troubleshooting.md#sasl-errors).

## Log {#logs}

L’app scrive un log della propria attività e degli errori del client Kafka. La cartella esatta è indicata nella schermata [[ui:sidebar.about]], alla voce [[ui:about.logDir]]. Per impostazione predefinita:

- macOS: `~/Library/Logs/io.github.vvnovg.glafira.kafka`
- Windows: `%LOCALAPPDATA%\io.github.vvnovg.glafira.kafka\logs`
- Linux: `~/.local/share/io.github.vvnovg.glafira.kafka/logs`

Il file corrente è `app.log`. Quando raggiunge 5 MB, viene rinominato in `app.1.log`, e i file più vecchi passano a `app.2.log` e così via. L’app conserva fino a cinque file ed elimina il più vecchio.

Le password non vengono scritte nei log. Vengono invece scritti gli indirizzi dei broker e i testi degli errori di Kafka. Non serve raccogliere i file a mano: l’archivio diagnostico li include, vedi [Archivio diagnostico](updates.md#diagnostics).

## Contattare l’assistenza {#support}

Prima di contattare l’assistenza, esegui [[ui:connections.form.test]] e annota quale passaggio non riesce. Poi invia la richiesta a chi si occupa dell’assistenza su Glafira per te, per esempio i tuoi amministratori. Includi:

- l’archivio diagnostico, vedi [Archivio diagnostico](updates.md#diagnostics). Contiene la versione, la build, i log e i profili delle connessioni senza password;
- che cosa hai fatto, che cosa ti aspettavi e che cosa è successo, passo per passo;
- il testo completo dell’errore, insieme al contenuto di [[ui:common.details]]. Puoi selezionare e copiare questo testo;
- uno screenshot, se il problema riguarda l’interfaccia;
- l’ora in cui si è verificato l’errore, per trovarlo nei log.

Se non riesci a creare l’archivio, indica i valori di [[ui:about.version]] e [[ui:about.build]] della schermata [[ui:sidebar.about]] e allega i file di log dalla cartella dei log.
