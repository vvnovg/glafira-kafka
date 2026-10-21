# Consumer-Sessions

Eine Consumer-Session liest Topics als echtes Mitglied einer Consumer Group.

## Was eine Session ist {#what-is}

Das [Durchsuchen](browse.md) liest Nachrichten, ohne einer Gruppe beizutreten, und ändert nichts im Cluster. Eine Session ist anders: Sie tritt einer Gruppe mit der angegebenen `group.id` bei, erhält Partitionen von Kafka und kann Offsets committen. Verwenden Sie sie, um zu lesen, was eine Gruppe lesen würde, um die Offsets einer Gruppe durch Committen zu verschieben oder um zu beobachten, wie eine Gruppe ein Rebalance durchläuft.

Eine Session wirkt sich auf den Cluster aus. Wenn sie beitritt oder austritt, verteilt Kafka die Partitionen der Gruppe neu, und die übrigen Mitglieder durchlaufen ein Rebalance. Commits ändern die Offsets der Gruppe, die deren Anwendungen verwenden.

Bei Verbindungen mit aktivierter Option [[ui:common.readOnly]] sind Sessions nicht verfügbar: Der Tab zeigt [[ui:consumer.readOnlyBlocked]]. Wird eine Verbindung schreibgeschützt, während eine Session läuft, verlässt die Session die Gruppe.

## Session starten {#start-session}

Eine Session lässt sich auf drei Arten öffnen:

- [[ui:sidebar.consumer]] unter der Verbindung im Bereich [[ui:sidebar.connections]] — ein leeres Formular;
- [[ui:topics.details.consume]] im Tab eines Topics — das Topic ist eingetragen;
- [[ui:groups.details.join]] im Tab einer Gruppe — die Gruppe und die Topics, für die sie Commits hat, sind eingetragen.

Jedes Mal öffnet sich ein neuer Tab, sodass mehrere Sessions gleichzeitig laufen können.

Unter [[ui:consumer.form.groupTopics]]:

- `group.id` — die Gruppe, der beigetreten wird. Geben Sie keine ein, schlägt die App einen neuen Namen wie `glafira-kafka-anna-1a2b3c` vor. Eine neue Gruppe hat keine Commits.
- [[ui:consumer.form.topicsLabel]] — Topic-Namen, getrennt durch Kommas oder Leerzeichen. Oder wählen Sie [[ui:consumer.form.useRegex]] und geben unter [[ui:consumer.form.patternLabel]] einen Ausdruck ein, zum Beispiel `orders\..*`. Der Ausdruck wird ab dem Anfang des Namens abgeglichen: Fehlt `^`, wird es ergänzt. Passende Topics, die später angelegt werden, kommen innerhalb weniger Minuten hinzu (bei der Aktualisierung der Metadaten); das löst ein Rebalance aus.
- `auto.offset.reset` — wo in Partitionen begonnen wird, für die die Gruppe keinen committeten Offset hat: [[ui:consumer.form.resetLatest]] (Standard) oder [[ui:consumer.form.resetEarliest]]. Wo die Gruppe einen committeten Offset hat, wird ab diesem weitergelesen.
- [[ui:consumer.form.autoCommit]] — gelesene Offsets automatisch committen. Standardmäßig deaktiviert. Siehe [Commits](consumer-sessions.md#commits).
- `isolation.level` — `read_committed` (Standard) zeigt nur Nachrichten abgeschlossener Transaktionen. `read_uncommitted` zeigt auch Nachrichten offener und abgebrochener Transaktionen.

Wählen Sie unter [[ui:consumer.form.formatLegend]] das [[ui:consumer.form.keyFormat]] und das [[ui:consumer.form.valueFormat]], wie beim Durchsuchen. Siehe [Formate](browse.md#formats). [[ui:consumer.form.buffer]] legt fest, wie viele Nachrichten die Tabelle behält, von 1 bis 100000; Standard ist 5000.

Die Filter funktionieren wie beim Durchsuchen. Siehe [Filter](browse.md#filters).

Klicken Sie auf [[ui:consumer.actions.connect]]. Bei einer Prod-Verbindung verlangt die App die Eingabe des Verbindungsnamens und eine Bestätigung mit [[ui:consumer.dialog.confirm]]. Solange die Session läuft, lässt sich das Formular nicht ändern.

## Lesen {#reading}

Die Statuszeile neben den Schaltflächen zeigt [[ui:consumer.status.connecting]], dann [[ui:consumer.status.inGroup]]. Außerdem zeigt sie, wie viele Nachrichten gelesen wurden, wie viele zu den Filtern passen und wie viele in der Tabelle stehen. Bricht die Verbindung zum Broker ab, verbindet sich die Session selbst neu und zeigt das in der Statuszeile an.

Die Nachrichten erscheinen in der Tabelle, sobald sie eintreffen, und die Session liest neue Nachrichten, bis Sie sie beenden. Ist die Tabelle voll, werden die ältesten Nachrichten entfernt. Klicken Sie auf eine Zeile, um die Details der Nachricht zu sehen, wie beim Durchsuchen. Siehe [Nachrichtendetails](browse.md#message-details). Wählen Sie `UTC`, um Zeiten in UTC anzuzeigen.

Die Session liest nur die Partitionen, die Kafka ihr zugewiesen hat. Hat die Gruppe weitere Mitglieder, lesen diese die übrigen Partitionen. Hat die Gruppe mehr Mitglieder als Partitionen, erhält die Session möglicherweise gar keine Partition.

[[ui:consumer.disconnect]] verlässt die Gruppe, und die übrigen Mitglieder übernehmen ihre Partitionen. Auch das Schließen des Session-Tabs verlässt die Gruppe. Ist die Session beendet, zeigt die Statuszeile [[ui:consumer.status.disconnected]].

Existiert ein Topic nicht, hält die Session mit einem Fehler an und zeigt die zu prüfenden Namen.

## Commits {#commits}

Ein Commit speichert einen Offset für die Gruppe. Das nächste Mitglied, das die Partition liest, oder dieselbe Anwendung nach einem Neustart beginnt ab diesem Offset.

Mit aktiviertem [[ui:consumer.form.autoCommit]] committet die Session gelesene Offsets alle paar Sekunden im Hintergrund und noch einmal beim Verlassen der Gruppe. Durch Filter ausgeblendete Nachrichten zählen als gelesen.

Ohne Auto-Commit committet die Session nichts von sich aus. Verwenden Sie die Schaltflächen:

- [[ui:consumer.actions.commitSelected]] — committet den Offset nach der ausgewählten Nachricht, nur in deren Partition. Die Gruppe setzt mit der nächsten Nachricht fort. Ist die ausgewählte Nachricht älter als der aktuelle Commit, wird der Offset zurückgesetzt.
- [[ui:consumer.actions.commitAll]] — committet die aktuelle Position in jeder zugewiesenen Partition. Das schließt durch Filter ausgeblendete und bereits aus der Tabelle entfernte Nachrichten ein.

Nach einem Commit zeigt eine Zeile die committeten Offsets als `topic:partition → offset`. Bei einer Prod-Verbindung verlangt jeder Commit den Verbindungsnamen.

Antwortet die Session nicht rechtzeitig, warnt die App, dass unbekannt ist, ob der Commit angewendet wurde. Prüfen Sie die Offsets im Tab der Gruppe. Siehe [Gruppendetails](groups.md#group-details).

## Partitionen pausieren {#pause-partitions}

Der Bereich rechts neben der Tabelle listet die der Session zugewiesenen Partitionen als `topic:partition` auf. Klicken Sie neben einer Partition auf [[ui:consumer.assignment.pause]], um das Lesen anzuhalten, und auf [[ui:consumer.assignment.resume]], um es fortzusetzen. Eine pausierte Partition ist mit [[ui:consumer.assignment.paused]] gekennzeichnet.

Pausieren verlässt die Gruppe nicht, es gibt also kein Rebalance. Die Partition bleibt der Session zugewiesen, und andere Mitglieder lesen sie nicht, solange sie pausiert ist. Pausieren ändert nichts im Cluster und erfordert keine Bestätigung.

Nach einem Rebalance sind neu zugewiesene Partitionen nicht pausiert. Verlässt die Session die Gruppe, enden alle Pausen.

## Rebalance-Protokoll {#rebalance-log}

[[ui:consumer.assignment.logTitle]] unter der Partitionsliste verzeichnet jede Änderung der Zuweisung, die neueste zuerst. Jeder Eintrag hat eine Zeit und eine von drei Arten: der Session zugewiesene Partitionen, ihr entzogene Partitionen oder ein Rebalance-Fehler mit seinem Text.

Ein Rebalance findet statt, wenn ein Mitglied der Gruppe beitritt oder sie verlässt, wenn sich die abonnierten Topics ändern oder wenn ein Mitglied nicht rechtzeitig antwortet. Um es zu beobachten, starten Sie in einem anderen Tab eine zweite Session mit derselben `group.id`: Die Partitionen werden auf beide Sessions aufgeteilt, und beide Protokolle zeigen das.

Das Protokoll behält die letzten 200 Einträge. Vor dem ersten Rebalance zeigt es [[ui:consumer.assignment.logEmpty]].

## Einschränkungen {#limits}

- Bei Verbindungen mit aktivierter Option [[ui:common.readOnly]] sind Sessions nicht verfügbar.
- Eine Session kann nicht ab einem gewählten Offset beginnen. Um den Start festzulegen, setzen Sie zuerst die Offsets der Gruppe zurück: siehe [Offsets zurücksetzen](groups.md#reset-offsets). Das Zurücksetzen erfordert eine Gruppe ohne Mitglieder, trennen Sie also vorher die Sessions dieser Gruppe.
- Die Tabelle behält höchstens 100000 Nachrichten. Ältere Nachrichten werden entfernt, zählen aber weiterhin als gelesen.
- Sehr große Schlüssel und Werte werden nur teilweise angezeigt, wie beim Durchsuchen.
- Treten Sie der Gruppe einer laufenden Anwendung nicht ohne Absicht bei. Die Session nimmt der Anwendung Partitionen weg, und die Anwendung verarbeitet sie nicht, solange die Session sie hält. Commits der Session, auch Auto-Commits, verschieben die Offsets der Anwendung.
