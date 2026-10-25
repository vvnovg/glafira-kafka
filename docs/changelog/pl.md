# Lista zmian

Polska wersja [CHANGELOG.md](../../CHANGELOG.md), gdzie są też wcześniejsze wersje. Sekcja wersji jest wyświetlana w „O programie” jako informacje o aktualizacji.

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
