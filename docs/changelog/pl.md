# Lista zmian

Polska wersja [CHANGELOG.md](../../CHANGELOG.md), gdzie są też wcześniejsze wersje. Sekcja wersji jest wyświetlana w „O programie” jako informacje o aktualizacji.

## 0.5.2
- Dokładniejsze maskowanie wiadomości odczytywanych przez asystenta LLM:
  - w tekście wartości ciasteczek i nagłówków `Authorization` (także `Proxy-Authorization`) są maskowane do końca wiersza, więc ukryte są wszystkie ciasteczka, parametry Digest i podpisy AWS;
  - w parach `name=value` rozpoznawane są nazwy w dowolnym alfabecie, na przykład nazwa pola z własnej listy połączenia zapisana cyrylicą;
  - wykrywane są numery kart sklejone z literami (`id4111…`) lub zapisane w nietypowych grupach, a także ujemne i ułamkowe numery kart w JSON.
- Topik bez uprawnienia Describe: karta „Wiadomości” oraz narzędzia `read_messages`, `summarize_messages` i `get_group_commit_history` zgłaszają „Brak uprawnień do wykonania operacji” zamiast „nie znaleziono”.
- `get_group_commit_history`: jeśli historia zatwierdzeń nie mieści się w 100 KB nawet bez serii zatwierdzeń, asystent zamiast pustej historii dostaje błąd „odpowiedź za duża” z radą, by zawęzić zapytanie. `summarize_messages` zużywa mniej pamięci przy dużych kluczach.
- Formularz połączenia:
  - gdy otwarte były dwa formularze połączeń, poziom dostępu dla LLM wybrany w jednym z nich nie był widoczny, choć zapisywał się poprawnie; teraz każdy formularz pokazuje swój;
  - czytniki ekranu odczytują ostrzeżenie, że treść wiadomości z klastra produkcyjnego będzie przekazywana dostawcy LLM;
  - jeśli nie udało się wczytać stanu MCP, komunikat wyjaśnia, dlaczego nie można wybrać poziomu „Metadane i wiadomości”;
  - lista maskowania ukryta na poziomach „Wyłączony” i „Tylko metadane” nie blokuje już zapisu.
- Znaczki `LLM` w drzewie połączeń: czytniki ekranu odczytują je w całości, na przykład „Dostęp dla LLM: metadane i wiadomości”, a nie „LLM”; tekst ma wyższy kontrast.
- Log MCP: przycisk „Otwórz w »Wiadomościach«” wyjaśnia, że jest niedostępny, gdy nie udało się wczytać listy połączeń.

## 0.5.1
- Informacje o aktualizacji w „O programie” → „Aktualizacje” są wyświetlane w języku interfejsu (bez tłumaczenia — po angielsku) i jako sformatowany tekst zamiast surowego Markdownu. Dotyczy to aktualizacji proponowanych dla wersji 0.5.1 i nowszych; wcześniejsze wersje pokazują informacje jako zwykły tekst po angielsku.
- Lista zmian jest teraz prowadzona po angielsku (`CHANGELOG.md`); jej wersje w pozostałych językach interfejsu znajdują się w `docs/changelog/`.

## 0.5.0
- Odczyt wiadomości przez asystenta LLM. Połączenie ma trzy poziomy dostępu dla LLM: „Wyłączony”, „Tylko metadane”, „Metadane i wiadomości”. Na trzecim poziomie asystent może przez MCP:
  - czytać wiadomości z okna czasu lub według offsetów, z tymi samymi filtrami co na karcie „Wiadomości” (`read_messages`);
  - uzyskać przegląd okna bez treści: wolumen według partycji i w czasie, częste klucze, znaczniki czasu poza kolejnością (`summarize_messages`);
  - sprawdzić na podstawie `__consumer_offsets`, które offsety i kiedy zatwierdziła grupa konsumentów (`get_group_commit_history`).
- Czas jest przekazywany tylko ze strefą czasową. Jedno wywołanie trwa najwyżej 40 sekund, przegląda najwyżej milion rekordów i zwraca najwyżej 100 KB.
- Maskowanie przed przekazaniem danych asystentowi:
  - wartości pól z listy nazw — standardowej lub własnej listy połączenia — są zastępowane przez `***`;
  - adresy e-mail, numery kart, tokeny JWT i tokeny autoryzacji są maskowane w każdym tekście;
  - filtry działają na danych bez maskowania, więc wiadomość można znaleźć po wartości z logu.
- Poziom dostępu i lista maskowania nie są eksportowane ani importowane.
- Klucz zasad `mcpMessagesEnabled` blokuje odczyt wiadomości: poziom „Metadane i wiadomości” działa wtedy jak „Tylko metadane”.
- Log MCP: przy odczycie wiadomości widać topik lub grupę, ile zwrócono z przejrzanych rekordów i ile wartości zamaskowano. Przycisk „Otwórz w »Wiadomościach«” wypełnia formularz ekranu „Wiadomości” tym samym oknem i filtrami; jeśli nie da się ich przenieść dokładnie, formularz o tym informuje.
- MCP: obsługiwani są klienci protokołu MCP 2026-07-28 (na przykład Claude Code 2.1.310): nazwa klienta i zatwierdzenie od razu przy połączeniu. Oczekująca sesja ma przycisk „Zatwierdź”. Wszystkie narzędzia są wyraźnie oznaczone jako tylko do odczytu (`idempotentHint`).
- Serwer katalogowy `glafira-mcp-catalog` dla rejestrów MCP (Glama): obraz Dockera, prawdziwa lista narzędzi, a wywołania odpowiadają, że potrzebna jest aplikacja desktopowa. README jest po angielsku (po rosyjsku — `README.ru.md`).
- Resetowanie offsetów z propozycji LLM: przed zapisem skład grupy jest sprawdzany ponownie. Jeśli broker odmówił, bo w międzyczasie do grupy dołączyli członkowie, wyświetlany jest zrozumiały błąd „grupa nie jest pusta”, a formularz resetowania od razu pokazuje aktualny stan grupy.
- Interfejs:
  - teksty zastępcze w pustych polach są pisane kursywą, aby nie mylić ich z wpisanymi wartościami;
  - pola pozycji początkowej i partycji na ekranie „Wiadomości” mają podpowiedzi po najechaniu kursorem;
  - nad znaczkami środowiska i LLM w drzewie połączeń widać zwykłą strzałkę.
- Podręcznik: sekcja „Odczyt wiadomości” w rozdziale „Asystenci LLM (MCP)” w 12 językach.
