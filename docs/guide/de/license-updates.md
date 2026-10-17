# Lizenz und Updates

Dieses Kapitel beschreibt, was Pro bietet, wie Sie einen Lizenzschlüssel aktivieren, was beim Ablauf der Lizenz geschieht, wie Sie die App aktualisieren, den Bildschirm [[ui:sidebar.about]] und das Diagnosearchiv.

## Free und Pro {#free-and-pro}

Free deckt die tägliche Arbeit mit Kafka ab: alle Sicherheitsprotokolle einschließlich mTLS, Cluster-Übersicht und Topics, Durchsuchen von Nachrichten, den Producer mit Stapelversand, Consumer Groups und das Zurücksetzen von Offsets. Die Oberfläche ist auf Englisch und Russisch verfügbar.

Pro bietet zusätzlich:

- Consumer-Sessions: Lesen als Gruppenmitglied, Committen von Offsets, Pausieren von Partitionen. Siehe [Consumer-Sessions](consumer-sessions.md#what-is);
- den Prod-Modus: Prod-Verbindungen anlegen, Verbindungen auf Prod umstellen und [[ui:common.readOnly]] bei einer Prod-Verbindung aufheben. Siehe [Umgebungen und Prod](connections.md#environments);
- sechs weitere Sprachen der Oberfläche: Deutsch, Französisch, Spanisch, Italienisch, Polnisch und Portugiesisch (Brasilien). Siehe [Sprache der Oberfläche](getting-started.md#language).

In Free sind diese Funktionen sichtbar, aber nicht verfügbar. Die Befehle für Consumer-Sessions sind deaktiviert und mit einem Schloss und `Pro` gekennzeichnet. Die Prod-Option wird als [[ui:connections.form.prodPro]] angezeigt. Pro-Sprachen sind in der Sprachliste mit `Pro` gekennzeichnet. Erfordert eine Aktion Pro, lautet der Fehler [[ui:errors.license.proRequired]], und die Schaltfläche [[ui:common.openLicense]] wird angeboten.

Der Bildschirm [[ui:tabs.license]] führt Consumer-Sessions und den Prod-Modus unter [[ui:license.whatProGives]] auf. Die zusätzlichen Sprachen stehen nicht in dieser Liste.

## Schlüssel aktivieren {#activate}

Ein Lizenzschlüssel ist eine Textzeile, die für eine Person oder eine Organisation ausgestellt wird. Die App prüft den Schlüssel auf Ihrem Computer und kontaktiert keinen Server.

1. Klicken Sie unten im linken Bereich auf [[ui:tabs.license]].
2. Fügen Sie den vollständigen Schlüssel unter [[ui:license.keyLabel]] ein.
3. Klicken Sie auf [[ui:license.activate]].

Die Pro-Funktionen sind sofort verfügbar. Der Bildschirm zeigt [[ui:license.tier]] (Pro), [[ui:license.licensee]], [[ui:license.kind]] ( [[ui:license.kindPersonal]] oder [[ui:license.kindOrganization]] ) und [[ui:license.expires]]: ein Datum oder [[ui:license.perpetual]]. Der Schlüssel wird im Datenordner der App gespeichert und bleibt daher nach einem Neustart oder Update erhalten. Ein neuer Schlüssel ersetzt den vorherigen.

Wird der Schlüssel nicht akzeptiert, steht der Grund unter dem Feld:

- [[ui:errors.license.malformed]] — ein Teil des Schlüssels fehlt oder wurde verändert. Kopieren Sie den Schlüssel erneut als eine Zeile, ohne zusätzliche Zeichen.
- [[ui:errors.license.badSignature]] — der Schlüssel wurde nicht für Glafira ausgestellt oder wurde bearbeitet.
- Der Schlüssel ist abgelaufen — die Meldung nennt sein Enddatum. Fordern Sie einen neuen Schlüssel an.
- Der Schlüssel erfordert eine neuere Version der App — die Meldung nennt die Schlüsselversion. Aktualisieren Sie zuerst die App, siehe [Updates](license-updates.md#updates).

Um den Schlüssel zu entfernen, klicken Sie auf [[ui:license.removeKey]]. Die App fragt: [[ui:license.removeQuestion]] Klicken Sie zur Bestätigung auf [[ui:license.removeYes]]. Ihre Daten bleiben erhalten.

Eine Organisation kann Pro per Richtlinie für alle Benutzer aktivieren. Das Schlüsselfeld und die Schaltflächen sind dann ausgeblendet, und der Bildschirm zeigt: [[ui:license.managed]] Ein selbst eingegebener Schlüssel wird nicht verwendet, solange der Richtlinienschlüssel gesetzt ist. Der Abschnitt [[ui:license.policyTitle]] zeigt, woher die Richtlinie stammt, und listet ihre Fehler auf. Ist der Organisationsschlüssel ungültig, wird der Grund auf dem Bildschirm [[ui:tabs.license]] angezeigt. Wenden Sie sich in diesem Fall an Ihre Administratoren. Administratoren finden die Richtlinieneinstellungen in `docs/admin.md` im Quellcode von Glafira.

## Ablauf {#expiry}

Ein Schlüssel kann ein Enddatum haben, das unter [[ui:license.expires]] angezeigt wird. Die Lizenz gilt bis zum Ende dieses Tages.

Sind es weniger als 14 Tage, zeigt ein Hinweis über den Tabs, wie viele Tage verbleiben, und der Bildschirm [[ui:tabs.license]] zeigt das Enddatum. [[ui:license.notice.more]] im Hinweis öffnet den Bildschirm [[ui:tabs.license]]. Die Schaltfläche `×` blendet den Hinweis bis zum nächsten Start der App aus.

Nach dem Enddatum wechselt der Tarif zu Free. Der Hinweis und der Bildschirm [[ui:tabs.license]] melden, dass die Lizenz abgelaufen ist und die Pro-Funktionen deaktiviert sind. Was sich ändert:

- Neue Consumer-Sessions lassen sich nicht starten. In einer bereits laufenden Session werden Commits und das Pausieren von Partitionen mit einem Fehler abgelehnt, der das Enddatum nennt.
- Prod-Verbindungen funktionieren mit denselben Bestätigungen weiter. Neue Prod-Verbindungen lassen sich nicht anlegen, und [[ui:common.readOnly]] lässt sich bei einer Prod-Verbindung nicht aufheben.
- War eine Pro-Sprache ausgewählt, wechselt die Oberfläche auf Englisch. Ihre Auswahl bleibt erhalten.

Verbindungen, Passwörter, Vorlagen und Sendeverlauf bleiben erhalten. Um mit Pro weiterzuarbeiten, aktivieren Sie einen neuen Schlüssel wie unter [Schlüssel aktivieren](license-updates.md#activate) beschrieben.

## Updates {#updates}

Beim Start prüft die App einmal, ob eine neue Version verfügbar ist. Gibt es eine, nennt ein Hinweis über den Tabs ihre Version. [[ui:license.notice.more]] öffnet den Bildschirm [[ui:sidebar.about]]. Schlägt die Prüfung fehl, etwa ohne Internetzugang, erscheint keine Meldung.

So suchen Sie manuell:

1. Öffnen Sie unten im linken Bereich [[ui:sidebar.about]].
2. Klicken Sie unter [[ui:about.updates.title]] auf [[ui:about.updates.check]].
3. Ist eine neue Version verfügbar, zeigt der Bildschirm ihre Nummer und die Versionshinweise. Klicken Sie auf [[ui:about.updates.install]].

Die App lädt das Update herunter, prüft seine Signatur, installiert es und startet neu. Ein durch ein Netzwerkproblem unterbrochener Download wird automatisch wiederholt, insgesamt bis zu drei Versuche. Schlagen alle drei Versuche fehl, meldet die App [[ui:errors.updates.downloadInterrupted]]. Schlägt die Signaturprüfung fehl, meldet die App [[ui:errors.updates.badSignature]].

Die Installation kann ein Administratorpasswort verlangen, weil die App für alle Benutzer des Computers installiert ist: über das `.pkg`-Installationsprogramm unter macOS, das `.msi`-Installationsprogramm unter Windows oder ein `.deb`- bzw. `.rpm`-Paket unter Linux. Benutzer ohne Administratorrechte können eine solche Installation nicht aktualisieren.

Werden gerade Nachrichten gelesen, läuft eine Consumer-Session oder wird ein Stapel gesendet, fragt die App zuerst nach und nennt die Anzahl der aktiven Sessions. Klicken Sie auf [[ui:about.updates.stopAndUpdate]], um sie zu beenden und fortzufahren, oder auf [[ui:common.cancel]]. Verbindungen, Einstellungen, Lizenzschlüssel und andere Daten bleiben nach einem Update erhalten.

Updates werden von der Release-Seite auf GitHub heruntergeladen. Schlägt die Prüfung mit [[ui:errors.updates.checkFailed]] fehl, prüfen Sie Ihr Netzwerk oder Ihren Proxy. [[ui:errors.updates.notConfigured]] bedeutet, dass dieser Build ohne den Updater-Schlüssel erstellt wurde, etwa ein Entwicklungs- oder Test-Build. Laden Sie neue Versionen in diesem Fall von der Release-Seite herunter: https://github.com/vvnovg/glafira-kafka-releases/releases

Eine Organisation kann die Suche nach Updates per Richtlinie abschalten oder auf einen internen Mirror umleiten. Ist die Suche abgeschaltet, zeigt der Abschnitt [[ui:about.updates.title]] nach einem Klick auf [[ui:about.updates.check]] die Meldung [[ui:errors.updates.managed]]. Wie [[ui:errors.updates.notConfigured]] erscheint diese Meldung erst nach dem Klick; die Prüfung beim Start zeigt in diesen Fällen nichts an.

## Über Glafira {#about}

Öffnen Sie unten im linken Bereich [[ui:sidebar.about]]. Unter macOS finden Sie den Bildschirm auch im Menü „Hilfe“ als [[ui:menu.about]].

Der Bildschirm zeigt eine kurze Beschreibung der App sowie:

- [[ui:about.version]] — die Versionsnummer, und [[ui:about.build]] — die Kennung des Quellcodes, aus dem die App gebaut wurde. Geben Sie beides in einer Supportanfrage an.
- [[ui:about.system]] — Betriebssystem und Prozessorarchitektur.
- [[ui:about.tier]] — Free oder Pro.
- [[ui:about.policy]] — [[ui:about.policySet]] oder [[ui:about.policyNone]]. Fehler in der Richtlinie werden darunter aufgeführt.
- [[ui:about.dataDir]] — der Ordner mit Verbindungsprofilen, Vorlagen, Verlauf, Einstellungen und Lizenzschlüssel.
- [[ui:about.logDir]] — der Ordner mit den Protokolldateien. Siehe [Protokolle](troubleshooting.md#logs).

Auch die Abschnitte [[ui:about.updates.title]] und [[ui:about.diagnostics.title]] befinden sich auf diesem Bildschirm.

## Diagnosearchiv {#diagnostics}

Das Diagnosearchiv sammelt, was der Support braucht, um ein Problem zu verstehen. So erstellen Sie es:

1. Öffnen Sie [[ui:sidebar.about]].
2. Klicken Sie unter [[ui:about.diagnostics.title]] auf [[ui:about.diagnostics.collect]]. Unter macOS können Sie auch im Menü „Hilfe“ [[ui:menu.diagnostics]] wählen.
3. Wählen Sie den Speicherort. Vorgeschlagen wird `diagnostics-<date>.zip`.

Nach dem Speichern zeigt der Bildschirm den Pfad des Archivs. Die ZIP-Datei enthält:

- `info.json` — Version und Build der App, Betriebssystem, Pfade des Daten- und Protokollordners, Status der Richtlinie und der Lizenz (Tarif, Lizenznehmer, Typ, Enddatum);
- `connections.json` — Verbindungsprofile: Namen, Umgebungen, Bootstrap-Server, Protokolle, SASL-Benutzernamen, Pfade der Zertifikatsdateien, zusätzliche Eigenschaften und Timeouts;
- `logs/` — die Protokolldateien der App.

Das Archiv enthält keine Passwörter: Sie liegen im Schlüsselbund des Systems, und zusätzliche Eigenschaften mit Passwörtern und Geheimnissen werden beim Speichern einer Verbindung abgelehnt. Auch Lizenzschlüssel und Zertifikatsdateien sind nicht enthalten. Hostnamen, Benutzernamen und Dateipfade stehen im Klartext darin, und die Protokolle enthalten Broker-Adressen aus Kafka-Fehlermeldungen. Sehen Sie das Archiv durch, bevor Sie es außerhalb Ihres Unternehmens versenden.

Die App sendet nichts von sich aus. Hängen Sie das Archiv selbst an Ihre Supportanfrage an, siehe [Support kontaktieren](troubleshooting.md#support).
