# Lokale Modelle

Der Assistent muss nicht in der Cloud laufen: Glafira beantwortet jeden MCP-Client gleich, und der Client kann mit einem Modell in Ihrem Netzwerk oder auf Ihrem Computer sprechen. Dieses Kapitel beschreibt, wo das Modell laufen kann, welche Hardware ein lokales Modell braucht, wie Sie es mit Glafira verbinden und wie Sie das Ergebnis prüfen. Wie der Zugriff des Assistenten auf Cluster funktioniert, steht in [LLM-Assistenten (MCP)](llm-mcp.md#how-it-works).

## Wo das Modell läuft {#where}

1. **Ein Cloud-Modell** (Claude, GPT und andere): die stärkste Variante, aber Anfragen mit Cluster-Metadaten — und bei der Stufe [[ui:connections.form.llmLevel.messages]] auch mit dem Inhalt von Nachrichten — verlassen Ihr Netzwerk. Viele Unternehmen verbieten das.
2. **Ein firmeneigener Modellserver** in Ihrem Netzwerk: Die Daten bleiben im Perimeter, und die Administratoren wählen Modell und Hardware. Gibt es noch keinen solchen Server, ist dies der nächste Schritt.
3. **Ein Modell auf dem Computer des Entwicklers**: für den Fall, dass die Cloud verboten ist und ein firmeneigener Server noch nicht existiert oder gerade eingerichtet wird. Die Daten verlassen den Computer nie, aber das Modell ist schwächer als ein Cloud-Modell und antwortet langsamer.

Der Rest des Kapitels handelt von der dritten Variante; einen Client für die zweite richten Sie genauso ein, nur die Serveradresse ändert sich.

## Was ein lokales Modell kann {#capabilities}

Getestet auf einem Mac mit M4 Pro und 48 GB Speicher, einem Kontext von 64k Token und einem Testcluster mit einem Broker und drei Topics (7 Partitionen, etwa 3.000 Nachrichten). Drei typische Aufgaben: eine Cluster-Prüfung mit Erklärung der Befunde, der Lag einer Gruppe ohne Mitglieder und das Finden einer Nachricht anhand der Uhrzeit aus einem Service-Log. Jedes Modell lief dreimal.

| Modell (4 Bit) | Server | Richtige Antworten | Generierung | Cluster-Prüfung | Group-Lag | Nachrichtensuche |
|---|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | Ollama | 9 von 9 | 70–80 Token/s | 23–29 s | 8–31 s | 34–43 s |
| Qwen3.6-35B-A3B | vllm-metal | 9 von 9 | 25–30 Token/s | 33–52 s | 43–60 s | etwa 2 min |
| Qwen3.8-27B | vllm-metal | 9 von 9 | 8–10 Token/s | 8–16 min | 10–12 min | 28–46 min |

Qwen3.8-27B liefert ausführlichere Antworten, ist aber ein dichtes Modell: Es nutzt für jedes Token alle 27 Mrd. Parameter, während Qwen3.6-35B-A3B etwa 3 Mrd. nutzt. Für die tägliche Arbeit auf einem Mac wählen Sie Qwen3.6-35B-A3B in Ollama: Dort antwortet dasselbe Modell 1,5- bis 3-mal schneller als auf vllm-metal.

Lange Untersuchungen mit vielen Schritten bewältigt ein lokales Modell schlechter als ein Cloud-Modell: Es macht mehr Fehler bei den Argumenten von Tools und kann aus unvollständigen Daten einen falschen Schluss ziehen. Prüfen Sie seine Schlussfolgerungen anhand des MCP-Protokolls.

## Hardware und Modelle {#hardware}

Auf gewöhnlicher Hardware funktionieren MoE-Modelle mit wenigen aktiven Parametern (etwa 3 Mrd., wie Qwen3.6-35B-A3B) am besten: Sie sind schneller als dichte Modelle derselben Größe. Der Kontext muss mindestens 32k Token betragen, besser 64k.

| Plattform | Modellserver | Was passt |
|---|---|---|
| macOS, Apple Silicon, 48 GB oder mehr | vllm-metal oder Ollama | Qwen3.6-35B-A3B (getestet) |
| macOS, Apple Silicon, 32 GB | Ollama | gpt-oss-20b; 35B-A3B ist an der Grenze |
| Linux, NVIDIA mit 24 GB Grafikspeicher | vLLM | 35B-A3B mit 4 Bit und kleinerem Kontext |
| Windows, NVIDIA | Ollama; vLLM nur in WSL2 | wie unter Linux |
| x86 ohne GPU, 32–64 GB DDR5 | Ollama | nur MoE-Modelle, langsam |

Ein Mac mit Intel-Prozessor eignet sich nicht für lokale Modelle.

## Modellserver {#server}

Der Server muss eine OpenAI-kompatible API mit Tool Calling bereitstellen. Auf einem Mac ist [Ollama](https://ollama.com) die einfachste Möglichkeit: App installieren, das Modell herunterladen und eine Variante mit einem Kontext von 64k Token erstellen – Ollama verwendet standardmäßig einen kleineren Kontext, und die Tool-Beschreibungen von Glafira passen nicht hinein:

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

Der Client benötigt die Adresse `http://127.0.0.1:11434/v1`, das Modell `qwen3.6-64k:35b-a3b` und einen beliebigen nicht leeren API-Schlüssel. Ollama lauscht nur auf `127.0.0.1`, damit sich andere Computer nicht mit dem Modell verbinden können.

Nach 5 Minuten Leerlauf entlädt Ollama das Modell aus dem Speicher, und die nächste Frage wartet, bis es wieder geladen ist. Damit das Modell länger im Speicher bleibt, setzen Sie die Umgebungsvariable `OLLAMA_KEEP_ALIVE`, zum Beispiel auf `1h`.

Unter Linux mit NVIDIA ist vLLM die übliche Wahl. Es parst Tool-Aufrufe mit dem Flag `--tool-call-parser`, das für die Modellfamilie gesetzt wird (`qwen3_coder` für Qwen3.6); in Ollama übernimmt das die Vorlage des Modells.

## Einstellungen in Glafira {#glafira-settings}

Im Abschnitt [[ui:settings.mcp.section]]:

- [[ui:settings.mcp.responseLimit]] — die größte Antwort eines Tool-Aufrufs. Eine Antwort von 100 KB belegt etwa 30k Token; wählen Sie daher für einen Kontext von 64k 48 oder 24 KB: Glafira kürzt lange Listen und weist das Modell an, die Anfrage einzugrenzen.
- [[ui:settings.mcp.colCompact]] in der Liste [[ui:settings.mcp.clients]] — ein Tool-Satz ohne Anmerkungen und Änderungsvorschläge: Ihre Beschreibungen belegen etwa 1.000 Token Kontext. Das Kontrollkästchen erscheint, sobald der Client sich verbindet und Sie ihn genehmigen; die neue Liste sieht der Client nach dem erneuten Verbinden.

## Prüfung {#check}

1. Verbinden Sie den Client und genehmigen Sie ihn im Dialog von Glafira.
2. Öffnen Sie eine Verbindung für den Assistenten, am besten eine Testverbindung, mit der Stufe [[ui:connections.form.llmLevel.metadata]].
3. Stellen Sie drei oder vier typische Fragen: „Prüfe den Cluster und erkläre die Befunde“, „Warum hat die Gruppe … einen großen Lag?“, „Welche Topics haben keine Replikation?“.
4. Prüfen Sie unter [[ui:settings.mcp.audit.title]], welche Tools das Modell aufgerufen hat und mit welchem Ergebnis, und vergleichen Sie seine Antwort mit den Daten in Glafira.
5. Wenn Sie Nachrichten öffnen, prüfen Sie die Maskierung: Die Schaltfläche [[ui:settings.mcp.audit.openInBrowse]] zeigt dieselben Nachrichten ohne Maskierung.
6. Schalten Sie das Netzwerk ab und wiederholen Sie eine Frage: Die Antwort muss auch ohne Internetzugang kommen.

## Einschränkungen {#limits}

- Ein lokales Modell antwortet um ein Mehrfaches langsamer als ein Cloud-Modell, und dichte Modelle wie Qwen3.8-27B sind auf einem Mac noch langsamer (siehe die [Tabelle](local-llm.md#capabilities)).
- Solange das Modell läuft, bleibt dem Computer weniger Speicher: Auf einem Mac mit 48 GB belegt ein Modell mit 35 Mrd. Parametern etwa 20 GB.
- Das Modell kann einen falschen Schluss ziehen. Glafira lässt es nichts im Cluster ändern, aber die Entscheidungen treffen Sie.
