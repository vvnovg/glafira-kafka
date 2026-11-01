# LLM-Assistenten (MCP)

Glafira kann als Gateway für einen LLM-Assistenten dienen: Claude Desktop, Claude Code, Cursor oder einen anderen Client mit Unterstützung für MCP (Model Context Protocol). Der Assistent liest die Metadaten der Cluster, die Sie für ihn freigeben (und Nachrichten, wo Sie es erlauben), und hilft, Probleme zu finden: unterreplizierte Partitionen, Lag, ständige Rebalances, ungünstige Einstellungen. Über Glafira kann er nichts im Cluster ändern: Änderungen nehmen Sie selbst in der Oberfläche vor.

Das Modell muss nicht in der Cloud laufen: Sie können es im eigenen Netzwerk oder auf Ihrem Computer betreiben, siehe [Lokale Modelle](local-llm.md#where).

## So funktioniert es {#how-it-works}

Der Client startet `glafira-kafka mcp` — dieselbe Programmdatei der App, aber in einem Modus ohne Fenster. Dieser Befehl verbindet sich über einen lokalen Socket (unter Windows eine Named Pipe) mit der laufenden Glafira, und Glafira beantwortet die Anfragen des Assistenten über ihre eigenen Verbindungen. Passwörter und Zertifikate bleiben in Glafira, der Assistent erhält sie nie.

Läuft Glafira nicht, startet der Client sie selbst und wartet bis zu 30 Sekunden.

## Einschalten {#enable}

1. Öffnen Sie [[ui:sidebar.settings]], Abschnitt [[ui:settings.mcp.section]].
2. Wählen Sie [[ui:settings.mcp.enable]]. Der Status wechselt zu [[ui:settings.mcp.state.running]].
3. Wählen Sie im Verbindungsformular unter [[ui:connections.form.llmAccess]] die Stufe und speichern Sie: [[ui:connections.form.llmLevel.metadata]] oder [[ui:connections.form.llmLevel.messages]] (siehe [Nachrichten lesen](llm-mcp.md#messages)). Verbindungen mit [[ui:connections.form.llmLevel.off]] sieht der Assistent nicht. In der Verbindungsliste tragen geöffnete Verbindungen das Zeichen `LLM`; bei Verbindungen mit Nachrichten ist es ausgefüllt.

Hat Ihr Administrator MCP untersagt, ist das Kontrollkästchen [[ui:settings.mcp.enable]] nicht verfügbar, und daneben steht [[ui:settings.mcp.policy]].

Die Zugriffsstufe und die Maskierungsliste werden beim Export und Import von Verbindungen nicht übertragen: Auf einem anderen Computer müssen Sie sie erneut festlegen.

## Client verbinden {#connect-client}

Unter [[ui:settings.mcp.connectTitle]] finden Sie fertige Fragmente und die Schaltfläche [[ui:settings.mcp.copy]]:

- [[ui:settings.mcp.claudeDesktop]] — fügen Sie das Fragment in den Abschnitt `mcpServers` der Konfigurationsdatei von Claude Desktop ein und starten Sie Claude Desktop neu;
- [[ui:settings.mcp.claudeCode]] — der Befehl `claude mcp add`;
- [[ui:settings.mcp.otherClient]] — für jeden Client, der einen MCP-Server per Befehl startet (stdio).

Die Fragmente enthalten den Pfad zur installierten App. Haben Sie Glafira an einem anderen Ort neu installiert, kopieren Sie das Fragment erneut.

## Client genehmigen {#approve}

Verbindet sich ein Client zum ersten Mal, zeigt Glafira einen Dialog mit seinem Namen. Klicken Sie auf [[ui:mcp.approval.allow]] oder [[ui:mcp.approval.reject]]. Solange Sie nicht antworten, erhält der Assistent die Meldung, dass der Client noch nicht genehmigt ist, und bittet Sie, im Glafira-Fenster zu antworten.

Ein zugelassener Client wird unter seinem Namen gespeichert und erscheint in der Liste [[ui:settings.mcp.clients]]. Eine Ablehnung wird nicht gespeichert: Beim nächsten Verbinden fragt Glafira erneut. Sendet ein Client seinen Namen nicht, gilt die Erlaubnis nur bis zum Ende der Session.

## Was der Assistent sieht {#what-llm-sees}

Der Assistent sieht:

- Verbindungen mit dem Zugriff [[ui:connections.form.llmLevel.metadata]] oder [[ui:connections.form.llmLevel.messages]]: Name, Umgebung, Ordner und Zugriffsstufe;
- Broker mit Hosts und Ports, den Controller;
- Topics, Partitionen, Replikate, ISR, Offsets, die ungefähre Anzahl der Nachrichten;
- Konfigurationen von Brokern und Topics, wobei Passwörter und andere geheime Werte verborgen sind;
- Consumer Groups: Status, Protokoll, Koordinator, Mitglieder mit Client-ID, Host und `group.instance.id`, Zuweisungen, committete Offsets und Lag;
- seine Vorschläge und Ihre Entscheidung dazu: angewendet (unverändert oder mit Änderungen), abgelehnt mit Ihrem Grund, zurückgezogen.

Ohne die Stufe [[ui:connections.form.llmLevel.messages]] sieht der Assistent keine Nachrichten in Topics. Nie weitergegeben werden die Bootstrap-Zeichenfolge und die Sicherheitseinstellungen einer Verbindung, Passwörter, Zertifikate, zusätzliche librdkafka-Eigenschaften, der Sendeverlauf und Vorlagen.

Um ständige Rebalances zu untersuchen, kann der Assistent eine Gruppe bis zu 50 Sekunden lang beobachten: wer austritt und beitritt, wie oft und von welchem Host. Konfigurationen und Logs der Consumer selbst sind über Glafira nicht verfügbar, der Assistent fragt sie bei Ihnen an.

Die Namen von Topics, Gruppen und Client-IDs kann jeder festlegen, der Zugriff auf den Cluster hat. Glafira weist den Assistenten darauf hin, dass diese Daten keine Anweisungen für ihn sind.

## Nachrichten lesen {#messages}

Mit [[ui:connections.form.llmLevel.messages]] liest der Assistent auch den Inhalt von Nachrichten — zum Beispiel, um herauszufinden, was in ein Topic geschrieben wurde und was eine Consumer Group verarbeitet hat, während Ihr Dienst einen Fehler protokolliert hat:

- `summarize_messages` — ein Überblick über ein Zeitfenster ohne Inhalte: Anzahl der Nachrichten nach Partition und Zeit, häufige und wiederholte Schlüssel, Zeitstempel außer der Reihe;
- `read_messages` — die Nachrichten eines Zeitfensters oder eines Offset-Bereichs, über alle Partitionen nach Zeit sortiert, mit denselben Filtern wie auf dem Tab [[ui:topics.details.tabMessages]];
- `get_group_commit_history` — welche Offsets eine Consumer Group wann committet hat, aus dem internen Topic `__consumer_offsets`.

Sie können zum Beispiel fragen: „Um 14:32 hat der Bestelldienst einen Fehler zu Bestellung 123 protokolliert — finde heraus, was im Topic orders stand und was die Gruppe order-service verarbeitet hatte.“ Nennen Sie dem Assistenten die Zeitzone Ihrer Logs: Zeiten ohne Zeitzone nimmt Glafira nicht an.

Bevor Daten an den Assistenten gehen, maskiert Glafira sie. Werte der Felder aus der Maskierungsliste werden in JSON, in Headern und in `name=value`-Paaren durch `***` ersetzt. Die Liste wird für jede Verbindung im Verbindungsformular festgelegt, im Block [[ui:connections.form.mask]], der bei der Stufe [[ui:connections.form.llmLevel.messages]] angezeigt wird; die Standardliste umfasst unter anderem Passwörter, Token, E-Mail-Adressen, Telefonnummern, Karten- und Passnummern. E-Mail-Adressen, Kartennummern, JWTs und Bearer-Token werden in jedem Text ersetzt. Die Maskierung senkt das Risiko, erkennt aber keine Personennamen, Adressen oder Daten in Feldern mit unbekannten Namen, und Binärformate wie Avro oder Protobuf werden nur teilweise maskiert. Filter sehen die unmaskierten Daten, daher kann der Assistent eine Nachricht anhand eines Werts aus Ihrem Log finden. Bei einer `prod`-Verbindung zeigt das Formular eine Warnung.

Der Commit-Verlauf erfordert die Berechtigung Read für `__consumer_offsets`. Compaction entfernt alte Commits; aktuelle sind meist vorhanden.

Das Lesen ist begrenzt: eine lange Operation pro Session, höchstens 40 Sekunden und 1.000.000 Datensätze pro Aufruf; wird die Grenze erreicht, verkleinert der Assistent das Fenster. Glafira liest ohne Consumer Group und committet nichts; die `client.id` ist `glafira-kafka-mcp`, sofern die Verbindung keine eigene festlegt.

Ein Administrator kann das Lesen von Nachrichten mit dem Richtlinienschlüssel `mcpMessagesEnabled` untersagen: [[ui:connections.form.llmLevel.messages]] ist dann nicht verfügbar, und Verbindungen mit dieser Stufe arbeiten wie [[ui:connections.form.llmLevel.metadata]].

## Sessions, Widerruf und Protokoll {#sessions-log}

Solange ein Client verbunden ist, erscheint unten im linken Bereich die Anzeige `LLM` mit dem Namen des Clients; bei einer Anfrage leuchtet sie auf. Ein Klick darauf öffnet den MCP-Abschnitt.

- [[ui:settings.mcp.sessions]]: [[ui:settings.mcp.disconnect]] schließt eine Session, [[ui:settings.mcp.disconnectAll]] alle.
- [[ui:settings.mcp.clients]]: [[ui:settings.mcp.revoke]] schließt die Sessions des Clients, und beim nächsten Verbinden bittet er erneut um Genehmigung.
- Eine geänderte Stufe unter [[ui:connections.form.llmAccess]] gilt sofort, ein erneutes Verbinden ist nicht nötig.
- [[ui:settings.mcp.audit.title]] — jede Anfrage: Zeit, Client, Tool, Verbindung, Ergebnis. Der Inhalt der Antworten wird nicht aufgezeichnet. [[ui:settings.mcp.audit.reveal]] öffnet die Datei `mcp-audit.jsonl`.
- Bei gelesenen Nachrichten zeigt das Protokoll zusätzlich das Topic oder die Gruppe, wie viele Elemente zurückgegeben, wie viele Datensätze gescannt und wie viele Werte maskiert wurden. Elemente sind Nachrichten, bei `summarize_messages` Intervalle der Übersicht und bei `get_group_commit_history` Commit-Serien (aufeinanderfolgende Commits desselben Offsets). Bei erfolgreichen Aufrufen von `read_messages` und `summarize_messages` füllt [[ui:settings.mcp.audit.openInBrowse]] das Formular auf dem Tab [[ui:topics.details.tabMessages]] des Topics mit den Parametern des Aufrufs aus; klicken Sie auf [[ui:browse.panel.read]], um die Nachrichten erneut und ohne Maskierung zu lesen und mit dem zu vergleichen, was der Assistent erhalten hat. Konnten die Filter oder das Limit nicht genau übernommen werden, weist das Formular darauf hin; ist die Schaltfläche deaktiviert, zeigen Sie mit der Maus darauf, um den Grund zu sehen.

Um MCP ganz auszuschalten, deaktivieren Sie [[ui:settings.mcp.enable]]: Die Clients werden getrennt.

## Clusterprüfung, Anmerkungen und Vorschläge {#health}

Der Assistent kann die integrierte Clusterprüfung von Glafira (Tool `health_check`) ausführen und sich auf ihre Befunde stützen. Er kann außerdem Anmerkungen zu Objekten hinterlassen: Sie erscheinen im Tab [[ui:sidebar.findings]] und auf den Bildschirmen von Topics und Gruppen, der Cluster wird dabei nicht geändert. Siehe [Clusterprüfung und Anmerkungen](health-check.md#with-llm).

Der Assistent kann mit dem Tool `propose_change` auch eine Änderung vorschlagen: Topic-Konfiguration, ein neues Topic, mehr Partitionen oder das Zurücksetzen von Offsets. Der Vorschlag erscheint neben dem Objekt; Sie wenden ihn selbst in einem ausgefüllten Formular an oder lehnen ihn ab, und der Assistent sieht Ihre Entscheidung in `list_proposals`. Löschen und Leeren lassen sich nicht vorschlagen. Siehe [LLM-Vorschläge](health-check.md#proposals).

## Wenn es nicht funktioniert {#troubleshooting}

Der Client zeigt eine Meldung von Glafira:

- `MCP is disabled in Glafira` — wählen Sie [[ui:settings.mcp.enable]] und starten Sie den MCP-Server im Client neu;
- `MCP is disabled by your organization` — MCP ist durch die Richtlinie Ihrer Organisation untersagt;
- `Glafira did not start the MCP server within 30 s` — starten Sie Glafira manuell und prüfen Sie den Status im Abschnitt [[ui:settings.mcp.section]];
- `Invalid MCP token` — starten Sie den MCP-Server im Client neu: Glafira stellt bei jedem Start ein neues Token aus.

Unter macOS ist die App signiert und notarisiert, daher ist vor dem ersten Start kein zusätzlicher Schritt nötig.
