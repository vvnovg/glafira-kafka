# Modele lokalne

Asystent nie musi działać w chmurze: Glafira odpowiada każdemu klientowi MCP tak samo, a klient może komunikować się z modelem w Twojej sieci lub na Twoim komputerze. Ten rozdział opisuje, gdzie model może działać, jakiego sprzętu wymaga model lokalny, jak połączyć go z Glafira i jak sprawdzić wynik. O tym, jak działa dostęp asystenta do klastrów, mówi rozdział [Asystenci LLM (MCP)](llm-mcp.md#how-it-works).

## Gdzie działa model {#where}

1. **Model w chmurze** (Claude, GPT i inne): najsilniejsza opcja, ale żądania z metadanymi klastra — a przy poziomie [[ui:connections.form.llmLevel.messages]] także z treścią wiadomości — opuszczają Twoją sieć. Wiele firm tego zabrania.
2. **Firmowy serwer modeli** wewnątrz sieci: dane zostają wewnątrz obwodu, a administratorzy wybierają model i sprzęt. Jeśli takiego serwera jeszcze nie ma, to kolejny krok.
3. **Model na komputerze inżyniera**: gdy chmura jest zabroniona, a firmowy serwer jeszcze nie istnieje lub dopiero jest wdrażany. Dane nigdy nie opuszczają komputera, ale model jest słabszy niż chmurowy i odpowiada wolniej.

Dalsza część rozdziału dotyczy trzeciej opcji; klienta dla drugiej konfiguruje się tak samo, zmienia się tylko adres serwera.

## Co potrafi model lokalny {#capabilities}

Test przeprowadzono na komputerze Mac z M4 Pro i 48 GB pamięci, z kontekstem 64 tys. tokenów i testowym klastrem z jednym brokerem i trzema topikami (7 partycji, około 3 000 wiadomości). Trzy typowe zadania: sprawdzenie klastra z wyjaśnieniem ustaleń, lag grupy bez członków oraz znalezienie wiadomości według czasu z logu usługi. Każdy model uruchomiono trzykrotnie.

| Model (4 bity) | Serwer | Poprawne odpowiedzi | Generowanie | Sprawdzenie klastra | Lag grupy | Wyszukiwanie wiadomości |
|---|---|---|---|---|---|---|
| Qwen3.6-35B-A3B | Ollama | 9 z 9 | 70–80 tokenów/s | 23–29 s | 8–31 s | 34–43 s |
| Qwen3.6-35B-A3B | vllm-metal | 9 z 9 | 25–30 tokenów/s | 33–52 s | 43–60 s | około 2 min |
| Qwen3.8-27B | vllm-metal | 9 z 9 | 8–10 tokenów/s | 8–16 min | 10–12 min | 28–46 min |

Qwen3.8-27B daje bardziej szczegółowe odpowiedzi, ale to model gęsty (dense): do każdego tokena używa wszystkich 27 miliardów parametrów, podczas gdy Qwen3.6-35B-A3B używa około 3 miliardów. Do codziennej pracy na komputerze Mac należy wybrać Qwen3.6-35B-A3B w Ollama: tam ten sam model odpowiada od 1,5 do 3 razy szybciej niż na vllm-metal.

Model lokalny gorzej radzi sobie z długimi, wieloetapowymi dochodzeniami niż model chmurowy: częściej myli się w argumentach narzędzi i może wyciągnąć błędny wniosek z niepełnych danych. Wnioski należy porównywać z logiem MCP.

## Sprzęt i modele {#hardware}

Na zwykłym sprzęcie najlepiej działają modele MoE z małą liczbą aktywnych parametrów (około 3 miliardów, takie jak Qwen3.6-35B-A3B): są szybsze niż modele gęste o tym samym rozmiarze. Kontekst musi wynosić co najmniej 32 tys. tokenów, najlepiej 64 tys.

| Platforma | Serwer modeli | Co się mieści |
|---|---|---|
| macOS, Apple Silicon, 48 GB lub więcej | vllm-metal lub Ollama | Qwen3.6-35B-A3B (przetestowano) |
| macOS, Apple Silicon, 32 GB | Ollama | gpt-oss-20b; 35B-A3B jest na granicy |
| Linux, NVIDIA z 24 GB pamięci wideo | vLLM | 35B-A3B w 4 bitach z mniejszym kontekstem |
| Windows, NVIDIA | Ollama; vLLM tylko w WSL2 | jak w systemie Linux |
| x86 bez GPU, 32–64 GB DDR5 | Ollama | tylko modele MoE, powoli |

Komputer Mac z procesorem Intel nie nadaje się do modeli lokalnych.

## Serwer modeli {#server}

Serwer musi udostępniać API zgodne z OpenAI z wywoływaniem narzędzi. Na komputerze Mac najprościej użyć [Ollama](https://ollama.com): zainstaluj aplikację, pobierz model i utwórz jego wariant z kontekstem 64 tys. tokenów — domyślnie Ollama używa mniejszego kontekstu, a opisy narzędzi Glafira się w nim nie mieszczą:

```bash
ollama pull qwen3.6:35b-a3b
printf 'FROM qwen3.6:35b-a3b\nPARAMETER num_ctx 65536\n' > Modelfile
ollama create qwen3.6-64k:35b-a3b -f Modelfile
```

Klient potrzebuje adresu `http://127.0.0.1:11434/v1`, modelu `qwen3.6-64k:35b-a3b` i dowolnego niepustego klucza API. Ollama nasłuchuje tylko na `127.0.0.1`, więc inne komputery nie mogą połączyć się z modelem.

Po 5 minutach bezczynności Ollama usuwa model z pamięci, a kolejne pytanie czeka, aż zostanie ponownie załadowany. Aby model dłużej pozostawał w pamięci, należy ustawić zmienną środowiskową `OLLAMA_KEEP_ALIVE`, np. na `1h`.

Na Linuksie z NVIDIA zwykle wybiera się vLLM. Analizuje on wywołania narzędzi za pomocą flagi `--tool-call-parser`, ustawianej dla danej rodziny modeli (`qwen3_coder` dla Qwen3.6); w Ollama robi to szablon modelu.

## Ustawienia Glafira {#glafira-settings}

W sekcji [[ui:settings.mcp.section]]:

- [[ui:settings.mcp.responseLimit]] — największa odpowiedź jednego wywołania narzędzia. Odpowiedź 100 KB to około 30 tys. tokenów, więc dla kontekstu 64 tys. należy wybrać 48 lub 24 KB: Glafira skraca długie listy i każe modelowi zawęzić żądanie.
- [[ui:settings.mcp.colCompact]] na liście [[ui:settings.mcp.clients]] — zestaw narzędzi bez notatek i propozycji zmian: ich opisy zajmują w kontekście około 1 000 tokenów. Pole wyboru pojawia się, gdy klient się połączy i zostanie zatwierdzony; nową listę klient widzi po ponownym połączeniu.

## Sprawdzanie {#check}

1. Połączyć klienta i zatwierdzić go w oknie dialogowym Glafira.
2. Otworzyć dla asystenta jedno połączenie, najlepiej testowe, z poziomem [[ui:connections.form.llmLevel.metadata]].
3. Zadać trzy lub cztery typowe pytania: „Sprawdź klaster i wyjaśnij ustalenia”, „Dlaczego grupa … ma duży lag?”, „Które topiki nie mają replikacji?”.
4. W sekcji [[ui:settings.mcp.audit.title]] sprawdzić, jakie narzędzia model wywołał i z jakim wynikiem, oraz porównać jego odpowiedź z danymi w Glafira.
5. Przy otwartym dostępie do wiadomości sprawdzić maskowanie: przycisk [[ui:settings.mcp.audit.openInBrowse]] pokazuje te same wiadomości bez maskowania.
6. Wyłączyć sieć i powtórzyć jedno pytanie: odpowiedź musi nadejść bez dostępu do internetu.

## Ograniczenia {#limits}

- Model lokalny odpowiada kilka razy wolniej niż chmurowy, a modele gęste, takie jak Qwen3.8-27B, są na komputerze Mac jeszcze wolniejsze (zob. [tabelę](local-llm.md#capabilities)).
- Gdy model działa, w komputerze zostaje mniej pamięci: na komputerze Mac z 48 GB model o 35 miliardach parametrów zajmuje około 20 GB.
- Model może wyciągnąć błędny wniosek. Glafira nie pozwala mu niczego zmienić w klastrze, ale decyzje należą do Ciebie.
