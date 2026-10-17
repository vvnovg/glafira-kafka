# Consumer Groups

Eine Consumer Group ist eine Menge von Consumern, die sich das Lesen von Topics teilen. Kafka weist jede Partition einem Mitglied der Gruppe zu und speichert für jede Partition den Offset, den die Gruppe committet hat. Anwendungen lesen nach einem Neustart ab diesem Offset weiter.

Öffnen Sie [[ui:sidebar.groups]] unter der Verbindung im Bereich [[ui:sidebar.connections]]. Die Gruppen eines einzelnen Topics finden Sie auch im Tab [[ui:topics.details.tabGroups]] des Topics. Siehe [Topic-Bildschirm](topics.md#topic-details).

## Gruppenliste {#group-list}

Die Liste zeigt alle Consumer Groups des Clusters. Die Spalten:

- [[ui:groups.list.colGroup]] — die Gruppen-ID (`group.id`).
- [[ui:groups.list.colState]] — der von Kafka gemeldete Status, zum Beispiel `Stable` (Mitglieder lesen), `Empty` (keine Mitglieder), `PreparingRebalance` oder `CompletingRebalance` (Partitionen werden neu verteilt) und `Dead` (die Gruppe existiert nicht mehr).
- [[ui:groups.list.colMembers]] — die Anzahl der aktiven Mitglieder.
- `Assignor` — die Strategie, nach der Partitionen verteilt werden, zum Beispiel `range`.
- [[ui:groups.list.colLag]] — der gesamte Lag der Gruppe. Siehe [Lag](groups.md#lag).
- [[ui:groups.list.colTopics]] — die Topics, für die die Gruppe Offsets committet hat.

Geben Sie unter [[ui:groups.list.search]] Text ein, um die Gruppen anzuzeigen, deren ID diesen Text enthält; Groß- und Kleinschreibung spielen keine Rolle. Die Statusliste daneben zeigt nur Gruppen in einem bestimmten Status; [[ui:groups.list.allStates]] hebt diesen Filter auf. Der Zähler zeigt, wie viele Gruppen angezeigt werden und wie viele es insgesamt gibt.

Mitglieder, Assignor und Lag werden beim Scrollen nur für die sichtbaren Zeilen geladen. Bis sie da sind, zeigt die Zelle `…`. Lassen sie sich nicht laden, zeigt die Zelle `—`; zeigen Sie mit der Maus darauf, um den Grund zu sehen.

[[ui:groups.list.refresh]] lädt die Liste und ihre Daten neu. Haben einige Broker nicht geantwortet, weist eine Warnung darauf hin, dass die Liste unvollständig sein kann.

## Lag {#lag}

Der Lag gibt an, wie viele Nachrichten die Gruppe noch nicht gelesen hat. Für jede Partition ist er das Ende des Logs minus den committeten Offset. Der Lag wird nie unter null angezeigt.

Der Lag hängt nur von committeten Offsets ab. Ein aktiver Consumer hat möglicherweise schon weiter gelesen, als er committet hat, sein tatsächlicher Lag kann also kleiner sein. Sobald die Gruppe erneut committet, sinkt der Lag.

Eine Partition ohne committeten Offset zeigt [[ui:groups.lag.noCommit]] und zählt nicht zur Summe. Der gesamte Lag einer Gruppe zeigt `—`, wenn die Gruppe überhaupt keine Commits hat ([[ui:groups.lag.noCommits]]) oder wenn die Offsets einer Partition nicht gelesen werden konnten ([[ui:groups.lag.unknown]]).

Der Lag zählt Offsets, nicht Nachrichten. In Topics mit Compaction oder Transaktionen enthalten manche Offsets keine sichtbaren Nachrichten, der Gruppe bleiben also möglicherweise weniger Nachrichten zu lesen, als der Lag anzeigt.

Die Werte sind eine Momentaufnahme zum Zeitpunkt des Ladens. Um den Lag zu verfolgen, öffnen Sie die Gruppe und aktivieren die automatische Aktualisierung.

## Gruppendetails {#group-details}

Klicken Sie in der Liste auf eine Gruppe, um ihren Tab zu öffnen. Oben zeigt eine Zusammenfassung Status, Anzahl der Mitglieder und Assignor.

- [[ui:groups.details.autoRefresh]] — [[ui:groups.details.refreshOff]], [[ui:groups.details.refresh5]] oder [[ui:groups.details.refresh10]].
- [[ui:groups.details.refresh]] — sofort neu laden.
- [[ui:groups.details.join]] — eine [Consumer-Session](consumer-sessions.md) in dieser Gruppe starten. Das Session-Formular übernimmt die Gruppen-ID und die Topics, für die die Gruppe Commits hat. Dafür ist Pro erforderlich, und bei Verbindungen mit [[ui:common.readOnly]] ist die Schaltfläche deaktiviert; zeigen Sie mit der Maus darauf, um den Grund zu sehen.

[[ui:groups.details.partitions]] listet die Partitionen auf, für die die Gruppe Commits hat, sowie die ihren Mitgliedern zugewiesenen Partitionen. Der gesamte Lag der Gruppe steht über der Tabelle. Die Spalten sind [[ui:groups.details.colTopic]], [[ui:groups.details.colPartition]], `Committed` (der committete Offset), `Log end` (der End-Offset der Partition), [[ui:groups.details.colLag]] und [[ui:groups.details.colMember]] (die Client-ID des Mitglieds, das die Partition liest).

[[ui:groups.details.members]] listet die aktiven Mitglieder auf: `Client ID`, [[ui:groups.details.colHost]], `Member ID` und [[ui:groups.details.colAssignment]] (die Partitionen des Mitglieds als `topic:partition`). Eine Gruppe ohne Mitglieder zeigt [[ui:groups.details.noMembers]].

Die zweite Zeile enthält [[ui:groups.details.resetOffsets]] und [[ui:groups.details.deleteGroup]]. Bei schreibgeschützten Verbindungen ist sie ausgeblendet.

## Offsets zurücksetzen {#reset-offsets}

Beim Zurücksetzen werden die committeten Offsets einer Gruppe verschoben. Die Gruppe liest dann Nachrichten erneut oder überspringt sie. Offsets lassen sich nur für eine Gruppe ohne aktive Mitglieder zurücksetzen (Status `Empty` oder `Dead`). Andernfalls würden die Mitglieder die neuen Offsets mit ihren eigenen Commits überschreiben. Beenden Sie zuerst die Anwendungen der Gruppe.

1. Klicken Sie im Tab der Gruppe auf [[ui:groups.details.resetOffsets]]. Solange die Gruppe Mitglieder hat, ist die Schaltfläche deaktiviert; zeigen Sie mit der Maus darauf, um den Grund zu sehen.
2. Wählen Sie die [[ui:groups.reset.strategy]]. Siehe [Strategien zum Zurücksetzen](groups.md#reset-strategies).
3. Wählen Sie den [[ui:groups.reset.scope]]. Siehe [Umfang des Zurücksetzens](groups.md#reset-scope).
4. Klicken Sie auf [[ui:groups.reset.preview]].
5. Prüfen Sie die Tabelle und klicken Sie auf die Schaltfläche zum Anwenden. Sie zeigt die Anzahl der Partitionen, die sich ändern.

Die Vorschau ändert nichts. Für jede Partition zeigt sie Topic, Partition, [[ui:groups.reset.colCurrent]] (den committeten Offset), [[ui:groups.reset.colNew]] (den Ziel-Offset) und `Low / High` (den ersten und den End-Offset der Partition). Eine Partition, für die sich kein Ziel berechnen lässt, ist mit [[ui:groups.reset.noTarget]] und dem Grund gekennzeichnet. Solche Partitionen werden übersprungen.

Angewendet wird nur der in der Vorschau gezeigte Plan. Jede Änderung im Formular blendet die Vorschau aus, und Sie müssen erneut auf [[ui:groups.reset.preview]] klicken. Bei einer Prod-Verbindung verlangt das Anwenden den Verbindungsnamen; bei anderen Verbindungen wird sofort angewendet.

Vor dem Anwenden prüft die App erneut, ob die Gruppe keine Mitglieder hat. Nach dem Anwenden listet [[ui:groups.reset.result]] jede Partition mit ihrem neuen Offset oder einem Fehler auf.

Sie können ein Zurücksetzen auch von einer Nachricht aus vorbereiten: siehe [Gruppe auf eine Nachricht zurücksetzen](browse.md#reset-group-here). [[ui:common.close]] schließt den Bereich zum Zurücksetzen.

## Strategien zum Zurücksetzen {#reset-strategies}

[[ui:groups.reset.strategy]] legt den Ziel-Offset für jede Partition im Umfang fest:

- [[ui:groups.reset.strategyEarliest]] — der erste noch gespeicherte Offset. Die Gruppe liest alles erneut, was das Topic vorhält.
- [[ui:groups.reset.strategyLatest]] — das Ende der Partition. Die Gruppe überspringt alles Angesammelte und liest nur neue Nachrichten.
- [[ui:groups.reset.strategyTimestamp]] — die erste Nachricht, die zum Zeitpunkt unter [[ui:groups.reset.timestamp]] oder danach geschrieben wurde. In Partitionen ohne neuere Nachrichten ist das Ziel das Ende.
- [[ui:groups.reset.strategyOffset]] — der angegebene [[ui:groups.reset.offset]], eine ganze Zahl ab 0, in jeder Partition. Hat eine Partition diesen Offset nicht, ist das Ziel ihr erster bzw. ihr End-Offset.
- [[ui:groups.reset.strategyShift]] — verschiebt den committeten Offset um den Wert unter [[ui:groups.reset.shift]]. Eine negative Zahl liest N Nachrichten zurück erneut, eine positive überspringt N Nachrichten. Das Ergebnis bleibt zwischen erstem und End-Offset. Partitionen ohne committeten Offset lassen sich nicht verschieben und werden übersprungen.

Die Zeit ist Ihre Ortszeit, mit Sekunden. Kafka ermittelt den Offset anhand der Zeitstempel der Nachrichten.

## Umfang des Zurücksetzens {#reset-scope}

[[ui:groups.reset.scope]] legt fest, welche Partitionen zurückgesetzt werden. Die übrigen Partitionen behalten ihre Offsets.

- [[ui:groups.reset.scopeAll]] — alle Partitionen, für die die Gruppe Offsets committet hat. Hat die Gruppe noch keine Commits, wählen Sie stattdessen Topics.
- [[ui:groups.reset.scopeTopics]] — alle Partitionen der aufgeführten Topics, auch Partitionen ohne Commits. Trennen Sie die Namen durch Kommas, Leerzeichen oder Zeilenumbrüche. So lässt sich eine Gruppe ab einer gewählten Stelle mit dem Lesen eines neuen Topics beginnen.
- [[ui:groups.reset.scopePartitions]] — nur die aufgeführten Partitionen. Geben Sie sie unter [[ui:groups.reset.partitionsLabel]] als `topic:partition` ein, getrennt durch Kommas oder Leerzeichen, zum Beispiel `orders:0, orders:3`.

## Gruppe löschen {#delete-group}

Das Löschen einer Gruppe entfernt alle ihre committeten Offsets. Nur eine Gruppe ohne Mitglieder im Status `Empty` lässt sich löschen.

1. Klicken Sie im Tab der Gruppe auf [[ui:groups.details.deleteGroup]]. Solange die Gruppe Mitglieder hat, ist die Schaltfläche deaktiviert; zeigen Sie mit der Maus darauf, um den Grund zu sehen.
2. Geben Sie zur Bestätigung die Gruppen-ID ein. Bei einer Prod-Verbindung geben Sie zusätzlich den Verbindungsnamen ein.
3. Klicken Sie auf [[ui:common.delete]].

Nach dem Löschen schließt sich der Tab der Gruppe. Startet eine Anwendung mit dieser `group.id` erneut, legt Kafka die Gruppe neu an. Ohne committete Offsets beginnt die Anwendung dort, wo ihre Einstellung `auto.offset.reset` es vorgibt: am Anfang oder am Ende.

Bei Verbindungen mit [[ui:common.readOnly]] ist die Schaltfläche ausgeblendet.
