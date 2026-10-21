# Updates und Diagnose

Dieses Kapitel beschreibt, wie Sie die App aktualisieren, den Bildschirm [[ui:sidebar.about]], das Diagnosearchiv und wie Sie das Projekt unterstützen können.

## Updates {#updates}

Beim Start prüft die App einmal, ob eine neue Version verfügbar ist. Gibt es eine, nennt ein Hinweis über den Tabs ihre Version. [[ui:about.notice.more]] öffnet den Bildschirm [[ui:sidebar.about]]. Schlägt die Prüfung fehl, etwa ohne Internetzugang, erscheint keine Meldung.

So suchen Sie manuell:

1. Öffnen Sie unten im linken Bereich [[ui:sidebar.about]].
2. Klicken Sie unter [[ui:about.updates.title]] auf [[ui:about.updates.check]].
3. Ist eine neue Version verfügbar, zeigt der Bildschirm ihre Nummer und die Versionshinweise. Klicken Sie auf [[ui:about.updates.install]].

Die App lädt das Update herunter, prüft seine Signatur, installiert es und startet neu. Ein durch ein Netzwerkproblem unterbrochener Download wird automatisch wiederholt, insgesamt bis zu drei Versuche. Schlagen alle drei Versuche fehl, meldet die App [[ui:errors.updates.downloadInterrupted]]. Schlägt die Signaturprüfung fehl, meldet die App [[ui:errors.updates.badSignature]].

Die Installation kann ein Administratorpasswort verlangen, weil die App für alle Benutzer des Computers installiert ist: über das `.pkg`-Installationsprogramm unter macOS, das `.msi`-Installationsprogramm unter Windows oder ein `.deb`- bzw. `.rpm`-Paket unter Linux. Benutzer ohne Administratorrechte können eine solche Installation nicht aktualisieren.

Werden gerade Nachrichten gelesen, läuft eine Consumer-Session oder wird ein Stapel gesendet, fragt die App zuerst nach und nennt die Anzahl der aktiven Sessions. Klicken Sie auf [[ui:about.updates.stopAndUpdate]], um sie zu beenden und fortzufahren, oder auf [[ui:common.cancel]]. Verbindungen, Einstellungen und andere Daten bleiben nach einem Update erhalten.

Updates werden von der Release-Seite auf GitHub heruntergeladen. Schlägt die Prüfung mit [[ui:errors.updates.checkFailed]] fehl, prüfen Sie Ihr Netzwerk oder Ihren Proxy. [[ui:errors.updates.notConfigured]] bedeutet, dass dieser Build ohne den Updater-Schlüssel erstellt wurde, etwa ein Entwicklungs- oder Test-Build. Laden Sie neue Versionen in diesem Fall von der Release-Seite herunter: https://github.com/vvnovg/glafira-kafka-releases/releases

Eine Organisation kann die Suche nach Updates per Richtlinie abschalten oder auf einen internen Mirror umleiten. Ist die Suche abgeschaltet, zeigt der Abschnitt [[ui:about.updates.title]] nach einem Klick auf [[ui:about.updates.check]] die Meldung [[ui:errors.updates.managed]]. Wie [[ui:errors.updates.notConfigured]] erscheint diese Meldung erst nach dem Klick; die Prüfung beim Start zeigt in diesen Fällen nichts an.

## Über Glafira {#about}

Öffnen Sie unten im linken Bereich [[ui:sidebar.about]]. Unter macOS finden Sie den Bildschirm auch im Menü „Hilfe“ als [[ui:menu.about]].

Der Bildschirm zeigt eine kurze Beschreibung der App sowie:

- [[ui:about.version]] — die Versionsnummer, und [[ui:about.build]] — die Kennung des Quellcodes, aus dem die App gebaut wurde. Geben Sie beides in einer Supportanfrage an.
- [[ui:about.system]] — Betriebssystem und Prozessorarchitektur.
- [[ui:about.license]] — Apache 2.0; der Link öffnet den Lizenztext im Browser.
- [[ui:about.policy]] — [[ui:about.policySet]] oder [[ui:about.policyNone]]. Fehler in der Richtlinie werden darunter aufgeführt.
- [[ui:about.dataDir]] — der Ordner mit Verbindungsprofilen, Vorlagen, Verlauf, Einstellungen.
- [[ui:about.logDir]] — der Ordner mit den Protokolldateien. Siehe [Protokolle](troubleshooting.md#logs).

Auch die Abschnitte [[ui:about.updates.title]] und [[ui:about.diagnostics.title]] befinden sich auf diesem Bildschirm.

## Diagnosearchiv {#diagnostics}

Das Diagnosearchiv sammelt, was der Support braucht, um ein Problem zu verstehen. So erstellen Sie es:

1. Öffnen Sie [[ui:sidebar.about]].
2. Klicken Sie unter [[ui:about.diagnostics.title]] auf [[ui:about.diagnostics.collect]]. Unter macOS können Sie auch im Menü „Hilfe“ [[ui:menu.diagnostics]] wählen.
3. Wählen Sie den Speicherort. Vorgeschlagen wird `diagnostics-<date>.zip`.

Nach dem Speichern zeigt der Bildschirm den Pfad des Archivs. Die ZIP-Datei enthält:

- `info.json` — Version und Build der App, Betriebssystem, Pfade des Daten- und Protokollordners, Status der Richtlinie;
- `connections.json` — Verbindungsprofile: Namen, Umgebungen, Bootstrap-Server, Protokolle, SASL-Benutzernamen, Pfade der Zertifikatsdateien, zusätzliche Eigenschaften und Timeouts;
- `logs/` — die Protokolldateien der App.

Das Archiv enthält keine Passwörter: Sie liegen im Schlüsselbund des Systems, und zusätzliche Eigenschaften mit Passwörtern und Geheimnissen werden beim Speichern einer Verbindung abgelehnt. Auch Zertifikatsdateien sind nicht enthalten. Hostnamen, Benutzernamen und Dateipfade stehen im Klartext darin, und die Protokolle enthalten Broker-Adressen aus Kafka-Fehlermeldungen. Sehen Sie das Archiv durch, bevor Sie es außerhalb Ihres Unternehmens versenden.

Die App sendet nichts von sich aus. Hängen Sie das Archiv selbst an Ihre Supportanfrage an, siehe [Support kontaktieren](troubleshooting.md#support).

## Projekt unterstützen {#support}

Glafira ist kostenlos und quelloffen unter der Apache License 2.0. Spenden halten das Projekt am Leben. [[ui:support.title]] unten im linken Bereich, auf dem Bildschirm [[ui:sidebar.about]] und unter macOS im Menü „Hilfe“ öffnet die Seite zur Unterstützung im Browser: https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md
