# Änderungsprotokoll

Deutsche Fassung von [CHANGELOG.md](../../CHANGELOG.md), dort stehen auch frühere Versionen. Der Abschnitt einer Version wird unter „Über Glafira“ als Versionshinweise zum Update angezeigt.

## 0.5.1
- Versionshinweise unter „Über Glafira“ → „Updates“ erscheinen in der Sprache der Oberfläche (ohne Übersetzung auf Englisch) und als formatierter Text statt als rohes Markdown. Das gilt für Updates, die ab Version 0.5.1 angeboten werden; frühere Versionen zeigen die Hinweise als einfachen englischen Text.
- Das Änderungsprotokoll wird jetzt auf Englisch geführt (`CHANGELOG.md`); die Fassungen in den anderen Oberflächensprachen liegen in `docs/changelog/`.

## 0.5.0
- Nachrichten lesen durch einen LLM-Assistenten. Eine Verbindung hat drei Stufen des LLM-Zugriffs: „Aus“, „Nur Metadaten“, „Metadaten und Nachrichten“. Auf der dritten Stufe kann der Assistent über MCP:
  - Nachrichten in einem Zeitfenster oder nach Offsets lesen, mit denselben Filtern wie auf dem Tab „Nachrichten“ (`read_messages`);
  - einen Überblick über ein Zeitfenster ohne Inhalte abrufen: Volumen nach Partition und Zeit, häufige Schlüssel, Zeitstempel außer der Reihe (`summarize_messages`);
  - anhand von `__consumer_offsets` sehen, welche Offsets eine Consumer Group wann committet hat (`get_group_commit_history`).
- Zeitangaben werden nur mit Zeitzone übergeben. Ein Aufruf dauert höchstens 40 Sekunden, durchsucht höchstens eine Million Datensätze und liefert höchstens 100 KB.
- Maskierung, bevor Daten an den Assistenten gehen:
  - Werte von Feldern aus einer Namensliste — der Standardliste oder der eigenen Liste der Verbindung — werden durch `***` ersetzt;
  - E-Mail-Adressen, Kartennummern, JWTs und Autorisierungstoken werden in jedem Text maskiert;
  - Filter arbeiten auf den unmaskierten Daten, sodass sich eine Nachricht über einen Wert aus einem Log finden lässt.
- Zugriffsstufe und Maskierungsliste werden weder exportiert noch importiert.
- Der Richtlinienschlüssel `mcpMessagesEnabled` untersagt das Lesen von Nachrichten: Die Stufe „Metadaten und Nachrichten“ wirkt dann wie „Nur Metadaten“.
- MCP-Protokoll: Bei Nachrichtenzugriffen sind Topic oder Gruppe zu sehen, wie viel vom Durchsuchten ausgegeben und wie viele Werte maskiert wurden. Die Schaltfläche „In ‚Nachrichten‘ öffnen“ füllt das Formular auf dem Bildschirm „Nachrichten“ mit demselben Zeitfenster und denselben Filtern; lassen sie sich nicht genau übertragen, weist das Formular darauf hin.
- MCP: Clients der MCP-Spezifikation 2026-07-28 werden unterstützt (zum Beispiel Claude Code 2.1.310): Client-Name und Genehmigung gleich beim Verbinden. Eine wartende Session hat die Schaltfläche „Genehmigen“. Alle Tools sind ausdrücklich als nur lesend gekennzeichnet (`idempotentHint`).
- Der Katalogserver `glafira-mcp-catalog` für MCP-Verzeichnisse (Glama): ein Docker-Image, die echte Liste der Tools, und Aufrufe antworten, dass die Desktop-App nötig ist. Die README ist auf Englisch (Russisch — `README.ru.md`).
- Offsets zurücksetzen auf LLM-Vorschlag: Vor dem Schreiben wird die Zusammensetzung der Gruppe erneut geprüft. Hat der Broker abgelehnt, weil inzwischen Mitglieder der Gruppe beigetreten sind, erscheint der verständliche Fehler „Gruppe ist nicht leer“, und das Formular zum Zurücksetzen zeigt sofort den aktuellen Zustand der Gruppe.
- Oberfläche:
  - Platzhaltertexte in leeren Feldern sind kursiv, damit sie nicht mit eingegebenen Werten verwechselt werden;
  - die Felder für Startposition und Partitionen auf dem Bildschirm „Nachrichten“ zeigen beim Daraufzeigen Hinweise;
  - über den Kennzeichen für Umgebung und LLM im Verbindungsbaum erscheint der normale Mauszeiger.
- Handbuch: Abschnitt „Nachrichten lesen“ im Kapitel „LLM-Assistenten (MCP)“ in 12 Sprachen.
