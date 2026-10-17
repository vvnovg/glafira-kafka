# Producer

Der [[ui:sidebar.producer]] sendet Nachrichten an Kafka: einzeln aus einem Formular oder viele auf einmal. Öffnen Sie ihn über die Verbindung im Bereich [[ui:sidebar.connections]] oder mit [[ui:topics.details.produce]] im Tab eines Topics; dann ist das Topic bereits eingetragen. Jede Verbindung hat einen eigenen Producer-Tab.

Bei Verbindungen mit [[ui:common.readOnly]] öffnet sich der Producer mit der Warnung [[ui:producer.readOnly]]. Die Schaltflächen zum Senden sind ausgeblendet, Vorlagen und Verlauf lassen sich aber weiterhin ansehen.

## Nachricht senden {#send-message}

1. Geben Sie das [[ui:producer.topic]] ein. Das Feld schlägt die Topics des Clusters vor, ohne interne Topics.
2. Geben Sie optional eine Nummer unter [[ui:producer.partition]] ein. Zeigt das Feld [[ui:producer.partitionAuto]], wird die Partition anhand des Schlüssels gewählt: Nachrichten mit demselben Schlüssel landen in derselben Partition, wie beim Java-Client. Nachrichten ohne Schlüssel landen in einer zufälligen Partition.
3. Füllen Sie [[ui:producer.key]] und [[ui:producer.value]] aus und wählen Sie deren Formate. Siehe [Formate für Schlüssel und Wert](producer.md#key-value-formats).
4. Fügen Sie optional Header hinzu.
5. Klicken Sie auf [[ui:producer.send]].

Nach erfolgreichem Senden zeigt eine Zeile unter dem Formular Partition, Offset und Zeit der Nachricht. Ist `acks` auf `0` gesetzt, liefert der Broker keinen Offset, und stattdessen wird `—` angezeigt.

Ist die Partitionsnummer zu groß oder bestätigt der Broker die Nachricht nicht innerhalb des Verbindungs-Timeouts, zeigt die App einen Fehler. Auch ein nicht vorhandenes Topic führt zu einem Fehler, es sei denn, der Broker legt Topics automatisch an (`auto.create.topics.enable`). Ist das automatische Anlegen deaktiviert, kann es bis zu etwa 30 s (dem Verbindungs-Timeout) dauern, bis dieser Fehler erscheint. Fehler im Formular, etwa ungültiges JSON oder ein unbekannter Platzhalter, werden angezeigt, bevor etwas gesendet wird.

Bei einer Prod-Verbindung verlangt das Senden die Eingabe des Verbindungsnamens und eine Bestätigung mit [[ui:producer.confirmLabel]]. Siehe [Umgebungen und Prod](connections.md#environments).

[[ui:producer.clearForm]] leert das Formular und setzt die Producer-Einstellungen auf die Standardwerte zurück. Das Topic bleibt erhalten.

## Formate für Schlüssel und Wert {#key-value-formats}

[[ui:producer.keyFormat]] und [[ui:producer.valueFormat]] legen fest, wie der Text im Feld in Bytes umgewandelt wird:

- `string` — der Text unverändert, in UTF-8.
- `json` — der Text wird auf gültiges JSON geprüft und so gesendet, wie er eingegeben wurde, ohne Neuformatierung.
- `hex` — zwei Hex-Ziffern pro Byte, zum Beispiel `48656c6c6f`. Leerzeichen und Zeilenumbrüche werden ignoriert.
- `base64` — Bytes in Standard-Base64.
- `null` — überhaupt kein Schlüssel bzw. kein Wert. Das Textfeld ist deaktiviert.

Standard ist `string` für den Schlüssel und `json` für den Wert. Ein leeres Schlüsselfeld mit `string` sendet einen leeren Schlüssel, keinen fehlenden: Alle solchen Nachrichten erhalten denselben Hash und landen in einer Partition. Um eine Nachricht ohne Schlüssel zu senden, wählen Sie `null`.

Eine Nachricht mit dem Wert `null` ist ein Tombstone. In einem Topic mit `cleanup.policy=compact` weist sie Kafka an, ältere Nachrichten mit demselben Schlüssel zu entfernen.

## Header {#headers}

Header sind Paare aus Name und Wert, die mit der Nachricht gesendet werden. Klicken Sie auf [[ui:producer.headers.add]], um eine Zeile hinzuzufügen, und füllen Sie dann [[ui:producer.headers.colName]], [[ui:producer.headers.colValue]] und [[ui:producer.headers.colFormat]] aus. Die Schaltfläche `×` entfernt die Zeile.

Das Format eines Header-Werts ist `string` (Text in UTF-8) oder `hex` (Bytes als Hex-Ziffern). Zeilen mit leerem Namen werden nicht gesendet. Kafka erlaubt mehrere Header mit demselben Namen, und alle werden gesendet.

Platzhalter funktionieren in Header-Werten, aber nicht in Header-Namen.

## Platzhalter {#placeholders}

Platzhalter werden bei jedem Senden durch Werte ersetzt. Sie funktionieren im Schlüssel, im Wert und in Header-Werten. Die Liste steht unter den Headern:

- `{{uuid}}` — eine neue zufällige UUID, zum Beispiel `0b6f6f2c-3c1e-4a8e-9a54-2f0d6f1d7c21`.
- `{{now}}` — die aktuelle Zeit in UTC im Format ISO 8601 mit Millisekunden, zum Beispiel `2026-10-15T12:30:00.000Z`.
- `{{nowMillis}}` — die aktuelle Zeit in Millisekunden seit 1970-01-01 UTC.
- `{{seq}}` — die Nummer der Kopie in einem Stapel, beginnend bei 0. Beim einzelnen Senden ist sie `0`.
- `{{randomInt 1 100}}` — eine zufällige ganze Zahl zwischen den beiden Zahlen, beide eingeschlossen.
- `{{randomString 8}}` — eine zufällige Zeichenkette aus lateinischen Buchstaben und Ziffern der angegebenen Länge, bis zu 100000.
- `{{env NAME}}` — der Wert der Umgebungsvariablen `NAME`. Ist sie nicht gesetzt, wird nichts gesendet und ein Fehler angezeigt.

Leerzeichen innerhalb der Klammern sind erlaubt. Ein unbekannter Platzhalter oder ein fehlendes `}}` bricht das Senden mit einem Fehler ab. Um `{{` als Text zu senden, schreiben Sie `\{{`.

Beim Format `json` wird das JSON nach dem Ersetzen der Platzhalter geprüft. Platzhalter, die Text erzeugen, brauchen Anführungszeichen: `{"id": "{{uuid}}"}`. Zahlen nicht: `{"n": {{seq}}}`.

`{{env NAME}}` liest die Umgebung des Glafira-Prozesses. Starten Sie Glafira über das Dock oder das Startmenü, fehlen möglicherweise Variablen, die nur in Ihrem Terminal-Profil gesetzt sind.

Kommt eine Nachricht aus [Im Producer öffnen](browse.md#open-in-producer) oder aus dem Verlauf, wird jedes `{{` darin als `\{{` geschrieben. Die Nachricht wird dann genau so gesendet, wie sie war, ohne dass etwas ersetzt wird.

## Vorlagen {#templates}

Eine Vorlage speichert das ganze Formular: Topic, Partition, Schlüssel, Wert, Header, Formate und Producer-Einstellungen. Platzhalter werden so gespeichert, wie sie geschrieben sind, und bei jedem Senden ersetzt.

Um eine Vorlage zu speichern, klicken Sie auf [[ui:producer.templates.saveAs]], geben den [[ui:producer.templates.nameLabel]] ein und klicken auf [[ui:common.save]]. Das Topic darf leer sein. Standardmäßig gehört die Vorlage zu dieser Verbindung. Wählen Sie [[ui:producer.templates.global]], um sie bei jeder Verbindung anzuzeigen; solche Vorlagen sind in der Liste mit [[ui:producer.templates.allSuffix]] gekennzeichnet.

Um eine Vorlage zu verwenden, wählen Sie sie in der Liste [[ui:producer.templates.label]]. Das Formular wird durch die Vorlage ersetzt. Sie können das Formular vor dem Senden bearbeiten; die Vorlage ändert sich dabei nicht.

Beim Speichern entsteht immer eine neue Vorlage. Um eine Vorlage zu ändern, laden Sie sie, bearbeiten das Formular, speichern es unter einem neuen Namen und löschen die alte. Um die ausgewählte Vorlage zu löschen, klicken Sie auf [[ui:producer.templates.delete]] und bestätigen mit [[ui:common.delete]].

Die Vorlagen einer Verbindung werden zusammen mit der Verbindung gelöscht.

## Verlauf {#history}

[[ui:producer.history.title]] listet die letzten 100 Einzelsendungen dieser Verbindung auf, die neuesten zuerst. Nachrichten aus Stapelsendungen werden nicht aufgenommen. Jede Zeile zeigt Zeit, Topic, Partition und Offset sowie den Anfang des Schlüssels.

Der Verlauf speichert die Nachricht so, wie sie gesendet wurde, mit bereits ersetzten Platzhaltern. [[ui:producer.history.repeat]] lädt sie zusammen mit ihren Producer-Einstellungen ins Formular. Gesendet wird erst, wenn Sie auf [[ui:producer.send]] klicken. Die Kopie wird genau so gesendet, wie sie gespeichert ist: Beispielsweise wird dieselbe UUID erneut gesendet.

Ein Schlüssel, Wert oder Header-Wert über 64 KB wird nur teilweise gespeichert. Für solche Zeilen ist [[ui:producer.history.repeat]] deaktiviert.

Der Verlauf wird auf diesem Computer im Klartext gespeichert, einschließlich der Werte von `{{env NAME}}`. Senden Sie keine Geheimnisse über den Producer, wenn das eine Rolle spielt. Der Verlauf einer Verbindung wird zusammen mit der Verbindung gelöscht.

## N Kopien des Formulars {#batch-copies}

[[ui:producer.batch.legend]] unter dem Formular sendet viele Nachrichten in einem Durchlauf. Im Modus [[ui:producer.batch.modeCopies]] wird das Formular so oft wie angegeben gesendet.

1. Füllen Sie das Formular wie für ein einzelnes Senden aus.
2. Wählen Sie [[ui:producer.batch.modeCopies]] und geben Sie unter [[ui:producer.batch.count]] die Anzahl ein, von 1 bis 1000000.
3. Legen Sie optional die [[ui:producer.batch.rate]] fest. Siehe [Senderate](producer.md#batch-rate).
4. Klicken Sie auf [[ui:producer.batch.start]].

Platzhalter werden für jede Kopie separat ersetzt. `{{seq}}` ist die Nummer der Kopie, von 0 bis N−1, sodass jede Nachricht einen eigenen Schlüssel erhalten kann, zum Beispiel `order-{{seq}}`. Fehler in Platzhaltern werden vor dem Start an der ersten Kopie geprüft.

Während des Durchlaufs zeigt der Bereich, wie viele Nachrichten gesendet wurden und wie viele fehlgeschlagen sind. Am Ende zeigt er [[ui:producer.batch.done]]. Fehler werden mit der Nummer der Kopie aufgeführt, gezählt ab 1. Die Liste zeigt die ersten 20 Fehler; gibt es mehr, nennt ein Hinweis darunter die Gesamtzahl.

[[ui:producer.batch.cancel]] beendet den Durchlauf. Nachrichten, die die App noch nicht verlassen haben, werden nicht gesendet, und der Bereich zeigt ihre Anzahl. Die App wartet bis zu 2 Sekunden, bis der Broker bereits gesendete Nachrichten bestätigt. Danach noch unbestätigte Nachrichten zählen als Fehler, da unbekannt ist, ob sie geschrieben wurden. Auch das Schließen des Producer-Tabs beendet den Durchlauf.

Bei einer Prod-Verbindung verlangt der Durchlauf den Verbindungsnamen und wird mit [[ui:producer.batch.confirmLabel]] bestätigt.

## Aus einer Datei senden {#batch-file}

Im Modus [[ui:producer.batch.modeFile]] wird jede Zeile bzw. jeder Datensatz einer Datei zu einer Nachricht. Unterstützt werden JSON Lines (`.jsonl`, `.ndjson`) und CSV (`.csv`).

1. Geben Sie im Formular oben das [[ui:producer.topic]] ein. Alle Nachrichten gehen an dieses Topic.
2. Wählen Sie [[ui:producer.batch.modeFile]] und klicken Sie auf [[ui:producer.batch.chooseFile]].
3. Klicken Sie auf [[ui:producer.batch.start]].

Schlüssel, Wert, Header und Partition aus dem Formular werden nicht verwendet, und Platzhalter werden nicht ersetzt. Die Producer-Einstellungen des Formulars gelten.

In JSON Lines ist jede Zeile ein JSON-Objekt, zum Beispiel `{"key": "k1", "value": {"id": 1}, "headers": {"source": "test"}, "partition": 0}`. Alle Felder sind optional:

- `key` und `value` — eine JSON-Zeichenkette wird als ihr Text gesendet, ohne Anführungszeichen. Jeder andere JSON-Wert, etwa ein Objekt oder eine Zahl, wird als JSON-Text gesendet. `null` oder ein fehlendes Feld bedeutet kein Schlüssel bzw. kein Wert.
- `headers` — ein Objekt mit Zeichenketten als Werten.
- `partition` — eine Partitionsnummer. Fehlt sie, wird die Partition anhand des Schlüssels gewählt.

Leere Zeilen werden übersprungen. Die Datei muss in UTF-8 vorliegen.

In CSV enthält die erste Zeile die Spaltennamen. Die Spalte `value` ist erforderlich; die Spalten `key`, `partition` und `header.NAME` sind optional, zum Beispiel `header.source`. Eine leere `key`-Zelle bedeutet kein Schlüssel, eine leere Header-Zelle bedeutet, dass dieser Header fehlt. Eine leere `value`-Zelle sendet einen leeren Wert, nicht `null`; aus CSV lässt sich also kein Tombstone senden.

Die Datei wird als Stream gelesen, daher ist die Gesamtzahl vorab nicht bekannt und wird als `—` angezeigt. Eine Zeile oder ein Datensatz über 16 MB, ungültiges JSON oder eine negative Partition zählt als Fehler für diese Zeile, und der Durchlauf fährt mit der nächsten Zeile fort. Fehler werden mit der Zeilennummer angezeigt.

## Senderate {#batch-rate}

[[ui:producer.batch.rate]] begrenzt einen Stapel auf die angegebene Anzahl Nachrichten pro Sekunde. Geben Sie eine ganze Zahl ab 1 ein. Ein leeres Feld bedeutet [[ui:producer.batch.rateUnlimited]].

Die Nachrichten werden gleichmäßig über die Zeit verteilt: Bei 10 Nachrichten pro Sekunde geht etwa alle 100 ms eine Nachricht hinaus. Nutzen Sie die Begrenzung, um einen gemeinsam genutzten Cluster nicht zu überlasten oder eine gleichmäßige Last nachzubilden.

Ohne Begrenzung sendet die App so schnell, wie der Broker annimmt. Die Senderate gilt für beide Stapelmodi.

## Producer-Einstellungen {#producer-settings}

[[ui:producer.settings.legend]] gelten für Einzelsendungen und Stapel. Sie werden in Vorlagen und im Verlauf gespeichert.

`acks` legt fest, wie viele Replikate einen Schreibvorgang bestätigen müssen:

- `all` — der Standard. Der Leader wartet auf alle synchronen Replikate. Das ist die sicherste Wahl. Sind weniger Replikate synchron, als die Topic-Einstellung `min.insync.replicas` verlangt, schlägt der Schreibvorgang fehl.
- `1` — nur der Leader bestätigt. Schneller, aber eine Nachricht kann verloren gehen, wenn der Leader ausfällt, bevor die Follower sie kopiert haben.
- `0` — der Producer wartet auf keine Bestätigung. Nachrichten können verloren gehen, und es wird kein Offset zurückgegeben.

[[ui:producer.settings.compression]] komprimiert Nachrichten-Batches: `none` (Standard), `gzip`, `snappy`, `lz4` oder `zstd`. Komprimierung spart Netzwerkverkehr und Speicherplatz auf Kosten etwas CPU. `zstd` und `lz4` sind gängige Optionen. Bei einer einzelnen kleinen Nachricht ist der Gewinn gering.

[[ui:producer.settings.idempotence]] setzt `enable.idempotence`. Sie verhindert Duplikate, wenn der Client ein Senden nach einem Netzwerkfehler wiederholt. Sie erfordert `acks` mit dem Wert `all`; bei anderen Werten zeigt die App einen Fehler und sendet nichts.
