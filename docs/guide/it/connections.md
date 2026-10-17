# Connections

A connection is a saved profile of one Kafka cluster: addresses, security settings and passwords. Connections are listed in the [[ui:sidebar.connections]] panel on the left. You can sort them into folders and move them to another computer with export and import.

## Create a connection {#create}

1. Click `+` ( [[ui:sidebar.newConnection]] ) in the header of the [[ui:sidebar.connections]] panel. To create the connection right in a folder, open the folder menu and choose [[ui:sidebar.folder.newConnection]].
2. Enter a [[ui:connections.form.name]]. It is shown in the panel and in tab titles.
3. In [[ui:connections.form.bootstrap]], enter one or more addresses in `host:port` format, separated by commas, for example `kafka1:9092,kafka2:9092`. One reachable broker is enough: the client discovers the others by itself.
4. Choose a [[ui:connections.form.protocol]] and fill in the fields it adds. See [Security protocols](connections.md#security).
5. Optionally, choose a [[ui:connections.form.folder]], an [[ui:connections.form.environment]] and a [[ui:connections.form.color]] for the dot next to the name.
6. Click [[ui:connections.form.test]], then [[ui:common.save]].

After saving, the form closes and the cluster overview opens.

Two more fields are for special cases. [[ui:connections.form.timeout]] is how long to wait for a broker response. The default is `30000` ms, the minimum is `1000`. Increase it for slow networks and VPNs. [[ui:connections.form.props]] takes any librdkafka setting as `key=value`, one per line. Lines that start with `#` are ignored. Passwords and keys are not allowed there: properties whose names contain `password` or `secret`, `sasl.jaas.config`, `ssl.key.pem` and `ssl.keystore.key` are rejected on save.

To change a connection later, click `✎` ( [[ui:sidebar.editConnection]] ) next to its name. To delete it, open the form, click [[ui:common.delete]] and confirm with [[ui:connections.form.deleteYes]].

## Test the connection {#test}

[[ui:connections.form.test]] checks the settings in the form step by step and stops at the first failed step:

1. [[ui:connections.form.step.config]] — the addresses, certificate files, passwords and the SASL username.
2. [[ui:connections.form.step.tcp]] — the host names resolve, and a TCP connection to at least one bootstrap server opens.
3. [[ui:connections.form.step.kafka]] — the TLS handshake and SASL login succeed, and the broker returns metadata. On success, the step shows the number of brokers and topics.

The test uses the values currently in the form, including passwords you typed but have not saved. Nothing is saved or changed in the cluster. If a step fails, its error text and details are shown under it. Common errors are explained in [Troubleshooting](troubleshooting.md).

## Security protocols {#security}

The [[ui:connections.form.protocol]] must match the broker listener you connect to. Your Kafka administrators know which one to use.

- `PLAINTEXT` — no encryption and no login. Typical for local and test clusters.
- `SSL` — traffic is encrypted with TLS. With a client certificate, this is mTLS.
- `SASL_PLAINTEXT` — login with a username and password, without encryption.
- `SASL_SSL` — login with a username and password over TLS.

When you change the protocol, the form shows only the fields that the protocol needs. All protocols, including mTLS, are available in Free.

## SASL {#sasl}

For `SASL_PLAINTEXT` and `SASL_SSL`, fill in three fields:

- [[ui:connections.form.saslMechanism]] — `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`. The cluster defines which one works.
- [[ui:connections.form.username]].
- [[ui:connections.form.password]]. It is saved in the system secret store, not in the profile. See [Passwords](connections.md#passwords).

SCRAM never sends the password itself. `PLAIN` sends it as is, so over `SASL_PLAINTEXT` the password travels in clear text. Use `SASL_SSL` for clusters outside your computer when possible.

## SSL and the trusted CA {#tls}

For `SSL` and `SASL_SSL`, the form adds TLS settings.

[[ui:connections.tls.ca]] is the certificate of the authority (CA) that signed the broker certificates. Leave it empty if the operating system already trusts this CA: the system root certificates are then used. Otherwise enter the full path to the file or click [[ui:connections.tls.pick]]. After you choose a file, the [[ui:connections.tls.caFormat]] field appears. For PKCS#12 and JKS, there is also a [[ui:connections.tls.truststorePassword]] field.

[[ui:connections.tls.verifyHostname]] is on by default. It checks that the broker certificate was issued for the host name you connect to. Turn it off only in test environments, for example when brokers use self-signed certificates with other names.

## Client certificate (mTLS) {#mtls}

If brokers authenticate clients by certificate, turn on [[ui:connections.tls.mtls]]. Your Kafka administrators issue the certificate and key. Then fill in:

- [[ui:connections.tls.cert]] — the path to the certificate file.
- [[ui:connections.tls.certFormat]] — PEM, PKCS#12 or JKS.
- For PEM: [[ui:connections.tls.key]] — the path to the private key file, and [[ui:connections.tls.keyPasswordPem]] if the key is encrypted.
- For PKCS#12: [[ui:connections.tls.keystorePassword]].
- For JKS: [[ui:connections.tls.keystorePassword]], [[ui:connections.tls.keyPasswordJks]] if the key has its own password, and [[ui:connections.tls.alias]] if the store has several keys.

Paths must be absolute. The files are read every time the app connects, so after the certificate is renewed you only need to reconnect. The app checks that the key matches the certificate and that the certificate has not expired.

A common case is a cluster in Kubernetes, where the client secret holds `tls.crt`, `tls.key` and `ca.crt`. Save them to files, choose protocol `SSL`, set [[ui:connections.tls.ca]] to `ca.crt`, and use `tls.crt` and `tls.key` as a PEM client certificate and key with empty passwords.

## Certificate formats {#certificate-formats}

Glafira reads three formats. The format is chosen automatically by the file extension; check it in the format field and change it if needed.

- PEM — a text file that starts with `-----BEGIN`. Extensions `.pem`, `.crt`, `.cer`, `.key`. For a client certificate, the key is a separate file. The trusted CA can also be a binary DER certificate.
- PKCS#12 — a binary store with a certificate and key. Extensions `.p12`, `.pfx`.
- JKS — a Java keystore. Extensions `.jks`, `.keystore`, `.truststore`.

Two kinds of files need converting first:

- JCEKS stores are not supported. Convert them to PKCS#12:

```bash
keytool -importkeystore -srcstoretype JCEKS -srckeystore client.jceks -deststoretype PKCS12 -destkeystore client.p12
```

- PKCS#12 stores encrypted with legacy algorithms (RC2, 3DES) cannot be read. Recreate them with AES:

```bash
openssl pkcs12 -export -keypbe AES-256-CBC -certpbe AES-256-CBC -in tls.crt -inkey tls.key -out client.p12
```

## Passwords {#passwords}

The SASL password and the passwords of keys and stores are saved in the system secret store: Keychain on macOS, Credential Manager on Windows, Secret Service on Linux. They are never written to the connection profile, export files or the diagnostics archive.

When you edit a saved connection, password fields are empty and show [[ui:connections.form.passwordKeep]]. Leave a field empty to keep the saved password, or type a new one to replace it. To delete a saved certificate password, select [[ui:connections.tls.clear]] next to the field. When you save, passwords that the connection no longer uses are deleted, for example after switching the protocol to `PLAINTEXT`.

If the app cannot access the secret store, an error is shown when you save. On Linux, check that GNOME Keyring, KWallet or another Secret Service provider runs in your session.

## Environments and prod {#environments}

[[ui:connections.form.environment]] marks the connection: `local`, `dev`, `test`, `stage` or `prod`. The environment is shown as a colored badge next to the name in the panel, so you always see which cluster you work with.

`prod` adds protection. Any change on a prod connection, such as creating a topic, sending a message or resetting offsets, asks you to type the connection name. Deleting and purging ask for the name of the object on any connection, and on prod you type both. These confirmations appear inside the app window.

Creating prod connections and switching connections to prod need Pro. In Free, this option is shown as [[ui:connections.form.prodPro]] and cannot be selected. Prod connections created earlier keep working in Free, with the same confirmations.

A new prod connection starts as [[ui:common.readOnly]]. Clear the box if you need to make changes.

## Read-only {#read-only}

[[ui:common.readOnly]] blocks everything that changes the cluster: creating, changing, purging and deleting topics, sending messages, resetting offsets, deleting groups and consumer sessions. The buttons for these actions are hidden, and the app core rejects such operations as well. Browsing and reading stay available. A lock icon next to the name in the panel marks read-only connections.

Use this mode when you only need to look at the data, especially on shared clusters. Kafka permissions for writing are not needed in this mode.

In Free, the box cannot be cleared on a read-only prod connection: lifting this protection needs Pro.

## Folders {#folders}

Folders help to sort connections, for example by project or by environment. Folders can be nested.

- To create a folder at the top level, click `+📁` ( [[ui:sidebar.newFolder]] ) in the panel header. To create a subfolder, open the folder menu and choose [[ui:sidebar.newFolder]]. Type the name and press `Enter`, or press `Esc` to cancel.
- To open the folder menu, click `⋯` ( [[ui:sidebar.menu]] ) on the folder row or right-click the folder. The menu has [[ui:sidebar.folder.rename]], [[ui:sidebar.folder.delete]] and the export and import commands.
- Only an empty folder can be deleted. Move or delete its connections and subfolders first.
- Folder names must be unique within one level and at most 100 characters long.

To move a connection, drag it onto a folder, or onto another connection to place it before that one. To move it to the top level, drop it onto the panel header or the empty space below the list. You can also choose a [[ui:connections.form.folder]] in the connection form; [[ui:connections.form.folderRoot]] means the top level. Folders are dragged the same way. Drop a folder onto the upper edge of another folder to place it before that folder. A folder cannot be moved into its own subfolder.

Click a folder name to collapse or expand it. The app remembers which folders are collapsed. Use [[ui:sidebar.filter]] above the tree to find connections and folders by name. Matching folders are shown with all their contents.

## Export {#export}

Export saves connections and folders to a file, for example to share them with colleagues or move them to another computer.

1. To export everything, open the panel header menu `⋯` and choose [[ui:sidebar.exportAll]]. To export one folder with its subfolders, choose [[ui:sidebar.folder.export]] in its menu.
2. Choose where to save the file. The suggested name is `glafira-connections-<date>.glafira-connections.json` or `<folder>.glafira-connections.json`.

When the file is saved, the panel shows how many connections and folders it contains.

The file has the connection settings and the folder structure. It does not contain passwords or secret properties. Certificate files are not included either: the file only keeps their paths. Hosts, usernames and paths are stored as plain text, so share the file only with people who may see them.

## Import {#import}

1. Open the panel header menu `⋯` and choose [[ui:sidebar.import]]. To import straight into a folder, choose [[ui:sidebar.folder.importHere]] in its menu.
2. Choose a `.glafira-connections.json` file. The [[ui:exchange.import.title]] dialog opens.
3. In [[ui:exchange.import.target]], choose where new folders and connections go. [[ui:connections.form.folderRoot]] is the top level.
4. Leave [[ui:exchange.import.updateExisting]] on to update connections that already exist from the file. Turn it off to keep them as they are.
5. Check the preview and click [[ui:exchange.import.apply]], or [[ui:common.cancel]] to close the dialog without changes.

The preview updates after each change in the dialog. It shows the totals and expandable lists: [[ui:exchange.import.groupAdded]], [[ui:exchange.import.groupUpdated]], [[ui:exchange.import.groupUnchanged]] and [[ui:exchange.import.groupSkipped]] with the reason for each skipped connection.

How the app matches the file with your connections:

- Connections are matched by an internal ID, not by name. A connection counts as existing if the file contains the same connection: for example, it was exported from this computer, or imported from the same source before.
- An updated connection gets the settings from the file. Your passwords and its place in the tree are kept.
- Existing folders are not renamed or moved. New connections from the file still go into them.
- If a new connection has the same name as another one in the same folder, a number is added, for example `kafka-dev (2)`.
- Connections that this version of Glafira does not support, or that contain secrets in extra properties, are skipped.

The preview also warns when certificate files are not found on this computer, and when prod connections will become read-only because Pro is not active. Passwords are not in the file, so after import the panel says how many connections need them. Open each of these connections, enter the passwords and save.
