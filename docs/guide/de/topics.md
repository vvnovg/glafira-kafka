# Cluster und Topics

Dieses Kapitel behandelt die Cluster-Übersicht, die Topic-Liste, das Erstellen und Löschen von Topics, den Topic-Bildschirm und die Topic-Konfiguration. Unter jeder Verbindung im Bereich [[ui:sidebar.connections]] gibt es Links zu [[ui:sidebar.cluster]] und [[ui:sidebar.topics]]. Beide öffnen sich jeweils in einem eigenen Tab.

## Cluster-Übersicht {#cluster-overview}

[[ui:cluster.overview.title]] zeigt den Zustand des Clusters zum Zeitpunkt des Ladens. Klicken Sie auf [[ui:cluster.overview.refresh]], um ihn neu zu laden.

- `Cluster ID` — die Kennung des Clusters.
- [[ui:cluster.overview.controller]] — die ID des Brokers, den der Cluster als Controller meldet. In KRaft-Clustern sprechen Clients nicht direkt mit den Controllern: Der Cluster meldet hier einen zufälligen Broker, nicht den aktiven Controller, und dieser kann sich zwischen zwei Aktualisierungen ändern.
- [[ui:cluster.overview.brokers]] — die Anzahl der Broker.
- [[ui:cluster.overview.topicsInternal]] — alle Topics, in Klammern die Anzahl der internen Topics.
- [[ui:cluster.overview.partitions]] — die Anzahl der Partitionen aller Topics.
- `Under-replicated` — Partitionen, bei denen nicht alle Replikate synchron sind (im ISR). Meist ist ein Broker ausgefallen oder kommt nicht hinterher.
- `Offline` — Partitionen ohne Leader. In sie kann niemand schreiben und aus ihnen niemand lesen.

Ein Wert ungleich null bei `Under-replicated` oder `Offline` wird hervorgehoben.

Darunter steht die Broker-Tabelle mit `ID` und [[ui:cluster.overview.address]]. Der Broker aus dem Feld [[ui:cluster.overview.controller]] ist mit [[ui:cluster.overview.controllerBadge]] gekennzeichnet. In KRaft-Clustern ist das derselbe zufällige Broker, nicht der aktive Controller. Klicken Sie auf einen Broker, um seine Konfiguration unter der Tabelle zu sehen. Die Broker-Konfiguration lässt sich nur ansehen. Mit [[ui:common.config.searchParam]] finden Sie einen Parameter nach Namen, mit [[ui:common.config.onlyChanged]] blenden Sie Parameter mit Standardwerten aus. Ein Schloss-Symbol kennzeichnet schreibgeschützte Parameter.

## Topic-Liste {#topic-list}

[[ui:sidebar.topics]] listet die Topics des Clusters auf. Klicken Sie auf eine Zeile, um das Topic in einem eigenen Tab zu öffnen.

Die Spalten sind [[ui:topics.list.colName]], [[ui:topics.list.colPartitions]], `RF` (Replikationsfaktor), [[ui:topics.list.colMessages]], `cleanup.policy` und [[ui:topics.list.colUnderRepl]] (die Anzahl unterreplizierter Partitionen). Die Nachrichtenanzahl ist ein Näherungswert: die Differenz zwischen End- und Start-Offsets der Partitionen. Durch Compaction und Transaktionsmarker ist die tatsächliche Zahl kleiner. Nachrichtenanzahl und `cleanup.policy` werden nur für die Zeilen geladen, zu denen Sie scrollen.

Elemente der Symbolleiste:

- [[ui:topics.list.search]] — geben Sie einen Teil eines Namens ein, um Topics zu finden; Groß- und Kleinschreibung spielen keine Rolle. Für die Suche mit einem regulären Ausdruck setzen Sie ihn zwischen Schrägstriche, zum Beispiel `/^orders\..*/`.
- [[ui:topics.list.internal]] — interne Topics wie `__consumer_offsets` anzeigen. Sie sind mit [[ui:topics.list.internalBadge]] gekennzeichnet.
- Die Liste für die automatische Aktualisierung — [[ui:topics.list.autoRefreshOff]], [[ui:topics.list.every10]] oder [[ui:topics.list.every30]].
- [[ui:topics.list.refresh]] — Liste und Nachrichtenanzahl sofort neu laden.

Neben der Symbolleiste zeigt die App, wie viele Topics von der Gesamtzahl zur Suche passen. Bei Verbindungen mit [[ui:common.readOnly]] sind die Schaltflächen [[ui:topics.list.create]] und [[ui:topics.list.select]] ausgeblendet.

## Topic erstellen {#create-topic}

1. Klicken Sie in der Topic-Liste auf [[ui:topics.list.create]]. Das Formular [[ui:topics.create.title]] öffnet sich über der Liste.
2. Geben Sie den [[ui:topics.create.name]] ein. Er darf lateinische Buchstaben, Ziffern, `.`, `_` und `-` enthalten, bis zu 249 Zeichen. Das Formular warnt, wenn der Name sowohl `.` als auch `_` enthält: Die Kafka-Metriknamen solcher Topics können mit denen anderer Topics kollidieren.
3. Legen Sie die Anzahl der [[ui:topics.create.partitions]] fest. Partitionen werden parallel gelesen, die Reihenfolge ist nur innerhalb einer Partition garantiert. Die Anzahl lässt sich später erhöhen, aber nie verringern.
4. Legen Sie den [[ui:topics.create.rf]] fest — wie viele Kopien jeder Partition die Broker vorhalten. Er darf die Anzahl der Broker nicht überschreiten, die das Formular unter dem Feld anzeigt. Standard ist 3 oder die Anzahl der Broker, wenn es weniger sind. `-1` übernimmt den Standardwert des Brokers.
5. Wählen Sie eine [[ui:topics.create.preset]]:
   - [[ui:topics.create.presetDelete]] — alte Nachrichten werden nach Zeit gelöscht.
   - `compact` — pro Schlüssel bleibt nur der neueste Wert erhalten.
   - `compact + delete` — beides.
   - [[ui:topics.create.presetTest]] — Nachrichten werden eine Stunde lang aufbewahrt.
6. Optional fügen Sie unter [[ui:topics.create.extraConfigs]] weitere Parameter hinzu, einen pro Zeile, zum Beispiel `retention.ms=86400000`. Sie überschreiben die Voreinstellung. Zeilen, die mit `#` beginnen, werden ignoriert.
7. Klicken Sie auf [[ui:topics.create.submit]].

Ist das Formular gültig, zeigt es den entsprechenden `kafka-topics.sh`-Befehl. Sie können ihn kopieren, etwa um dasselbe Topic in einem anderen Cluster anzulegen.

Bei einer Prod-Verbindung bestätigen Sie das Erstellen durch Eingabe des Verbindungsnamens. Nach dem Erstellen öffnet sich der Tab des Topics.

## Topics löschen {#delete-topics}

Um ein einzelnes Topic zu löschen, öffnen Sie es und klicken auf [[ui:topics.actions.deleteTopic]]. Geben Sie zur Bestätigung den Topic-Namen ein und klicken Sie auf [[ui:topics.delete.confirm]]. Der Tab des Topics schließt sich.

So löschen Sie mehrere Topics auf einmal:

1. Klicken Sie in der Topic-Liste auf [[ui:topics.list.select]]. Neben den Namen erscheinen Kontrollkästchen.
2. Wählen Sie die Topics aus. Interne Topics lassen sich nicht auswählen.
3. Klicken Sie auf die Schaltfläche zum Löschen. Sie zeigt die Anzahl der ausgewählten Topics.
4. Prüfen Sie die Liste der Namen, geben Sie zur Bestätigung deren Anzahl ein und klicken Sie auf [[ui:topics.delete.confirm]].

[[ui:topics.list.cancelSelect]] blendet die Kontrollkästchen aus.

Bei einer Prod-Verbindung verlangen beide Wege zusätzlich die Eingabe des Verbindungsnamens.

Das Löschen lässt sich nicht rückgängig machen: Die Daten gehen verloren. Werden einige Topics nicht gelöscht, etwa wegen fehlender Rechte, listet die App sie mit dem Grund auf. Die übrigen Topics werden gelöscht.

## Topic-Bildschirm {#topic-details}

Der Tab eines Topics zeigt die Anzahl der Partitionen, den Replikationsfaktor und die ungefähre Nachrichtenanzahl. Die Schaltflächen oben:

- [[ui:topics.details.refresh]] — die Daten des Topics neu laden.
- [[ui:topics.details.produce]] — den [[ui:sidebar.producer]] mit diesem Topic öffnen. Siehe [Producer](producer.md).
- [[ui:topics.details.consume]] — eine Consumer-Session für dieses Topic starten. Dafür ist Pro erforderlich. Siehe [Consumer-Sessions](consumer-sessions.md).

Bei Verbindungen mit [[ui:common.readOnly]] sind beide Schaltflächen deaktiviert.

Die zweite Zeile enthält Aktionen, die das Topic verändern. Sie ist bei schreibgeschützten Verbindungen und für interne Topics ausgeblendet.

- [[ui:topics.actions.addPartitions]] — geben Sie die neue Gesamtzahl ein und klicken Sie auf [[ui:topics.actions.addSubmit]]. Die Anzahl der Partitionen lässt sich nicht verringern. Nach einer Erhöhung können Nachrichten mit demselben Schlüssel in einer anderen Partition landen, die Reihenfolge nach Schlüssel geht dann verloren.
- [[ui:topics.actions.purge]] — alle Nachrichten bis zum aktuellen Ende der ausgewählten Partitionen löschen. Das Topic selbst bleibt bestehen. Wählen Sie die Partitionen aus, geben Sie den Topic-Namen ein und klicken Sie auf [[ui:topics.actions.purgeConfirm]]. Leeren funktioniert nur, wenn `cleanup.policy` den Wert `delete` enthält; andernfalls ist die Schaltfläche deaktiviert und der Grund wird angezeigt. Bei einem Topic mit `compact,delete` löscht das Leeren auch die neuesten Werte der Schlüssel.
- [[ui:topics.actions.deleteTopic]] — siehe [Topics löschen](topics.md#delete-topics).

Bei einer Prod-Verbindung verlangt jede dieser Aktionen zusätzlich den Verbindungsnamen.

Die Tabs darunter:

- [[ui:topics.details.tabMessages]] — Nachrichten durchsuchen. Siehe [Nachrichten durchsuchen](browse.md).
- [[ui:topics.details.tabPartitions]] — eine Tabelle mit allen Partitionen: [[ui:topics.details.colLeader]], [[ui:topics.details.colReplicas]], `ISR` (synchrone Replikate), Start-Offset (`Low`), End-Offset (`High`) und Anzahl der Nachrichten. Eine Partition ohne Leader zeigt [[ui:topics.details.noLeader]].
- [[ui:topics.details.tabConfigs]] — die Parameter des Topics. Siehe [Topic-Konfiguration](topics.md#topic-config).
- [[ui:topics.details.tabGroups]] — Consumer Groups mit committeten Offsets für dieses Topic, mit [[ui:topics.groups.colState]], [[ui:topics.groups.colPartitions]] und [[ui:topics.groups.colLag]]. Klicken Sie auf eine Gruppe, um sie zu öffnen. Siehe [Consumer Groups](groups.md).

## Topic-Konfiguration {#topic-config}

Der Tab [[ui:topics.details.tabConfigs]] listet alle Parameter des Topics in drei Spalten auf:

- [[ui:common.config.colParam]] — der Name des Parameters. Fett gedruckte Namen sind explizit gesetzt: am Topic oder in der Broker-Konfiguration.
- [[ui:common.config.colValue]] — der aktuelle Wert. Vertrauliche Werte werden als `••••••` verborgen.
- [[ui:common.config.colSource]] — woher der Wert stammt. `DynamicTopic` etwa ist an diesem Topic gesetzt, `StaticBroker` und `DynamicBroker` stammen aus der Broker-Konfiguration, und `Default` ist der Kafka-Standardwert.

Mit [[ui:common.config.searchParam]] finden Sie einen Parameter nach Namen, mit [[ui:common.config.onlyChanged]] zeigen Sie nur die fett gedruckten Parameter. Ein Schloss-Symbol kennzeichnet schreibgeschützte Parameter. Das Symbol `?` neben einem Namen erklärt den Parameter.

## Konfiguration ändern {#edit-config}

1. Klicken Sie im Tab [[ui:topics.details.tabConfigs]] auf [[ui:topics.details.edit]]. Bei Verbindungen mit [[ui:common.readOnly]] ist diese Schaltfläche ausgeblendet.
2. Ändern Sie die Werte in der Spalte [[ui:common.config.colValue]]. Schreibgeschützte und vertrauliche Parameter lassen sich nicht bearbeiten. Um den eigenen Wert des Topics zu entfernen, klicken Sie auf [[ui:common.config.reset]]: Dann gilt der Wert des Brokers. Ein leerer Wert ist nicht zulässig.
3. Klicken Sie auf [[ui:common.config.review]]. Die App zeigt jede Änderung als alter Wert → neuer Wert; [[ui:common.config.defaultValue]] bedeutet, dass der Wert zurückgesetzt wird.
4. Klicken Sie auf [[ui:common.config.apply]] oder auf [[ui:common.config.back]], um weiter zu bearbeiten.

Bis zum Anwenden ändert sich im Cluster nichts. [[ui:common.config.discard]] verwirft alle Änderungen. Solange ungespeicherte Änderungen vorliegen, ist [[ui:topics.details.closeEditor]] deaktiviert. Bei einer Prod-Verbindung bestätigen Sie die Änderungen durch Eingabe des Verbindungsnamens.

Es werden nur die geänderten Parameter gesendet. Die übrigen Parameter des Topics bleiben unverändert. Der Broker prüft die Werte und lehnt ungültige mit einem Fehler ab.

## Topic-Parameter {#config-parameters}

Kafka-Topics haben Dutzende Parameter. Die `?`-Symbole in der Konfigurationstabelle erklären jeden davon. Am häufigsten brauchen Sie diese:

- `cleanup.policy` — was mit alten Daten geschieht. `delete` entfernt alte Segmente nach Zeit oder Größe. `compact` behält pro Schlüssel nur den neuesten Wert. `compact,delete` macht beides.
- `retention.ms` — wie lange Nachrichten aufbewahrt werden, in Millisekunden. Standard ist `604800000` (7 Tage); `-1` bewahrt sie unbegrenzt auf. Wirkt, wenn `cleanup.policy` den Wert `delete` enthält.
- `retention.bytes` — die maximale Größe jeder Partition, bevor alte Segmente gelöscht werden. Der Standardwert `-1` bedeutet keine Größenbegrenzung.
- `segment.ms` und `segment.bytes` — wann der Broker ein neues Log-Segment beginnt. Aufbewahrung und Compaction arbeiten mit ganzen Segmenten, eine Nachricht kann also länger als `retention.ms` erhalten bleiben, bis ihr Segment geschlossen ist.
- `min.insync.replicas` — wie viele Replikate einen Schreibvorgang mit `acks=all` bestätigen müssen. Meist 2 bei Replikationsfaktor 3.
- `max.message.bytes` — der größte Batch, den der Broker für dieses Topic annimmt. Standard ist etwa 1 MB. Producer müssen dieselbe Größe zulassen (`max.request.size`).
- `compression.type` — wie der Broker Nachrichten speichert. `producer` übernimmt die Komprimierung des Producers.
- `message.timestamp.type` — `CreateTime` (vom Producer gesetzte Zeit) oder `LogAppendTime` (Zeitpunkt, zu dem der Broker die Nachricht geschrieben hat). Diese Zeit wird beim Durchsuchen von Nachrichten angezeigt.
- `delete.retention.ms` — wie lange Löschmarker (Nachrichten mit Null-Wert) in einem kompaktierten Topic erhalten bleiben. Standard ist 1 Tag.

Manche Parameter gibt es nur in neueren Kafka-Versionen, manche sind veraltet. Das `?`-Symbol weist darauf hin, wo es wichtig ist. Parameter für Tiered Storage wie `remote.storage.enable` funktionieren nur, wenn der Cluster es unterstützt.
