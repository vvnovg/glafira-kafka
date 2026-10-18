# Grupy konsumentów

Grupa konsumentów to zbiór konsumentów, którzy wspólnie czytają topiki. Kafka przydziela każdą partycję jednemu członkowi grupy i przechowuje dla każdej partycji offset zatwierdzony przez grupę. Po ponownym uruchomieniu aplikacje kontynuują odczyt od tego offsetu.

Ekran [[ui:sidebar.groups]] otwiera się pod połączeniem w panelu [[ui:sidebar.connections]]. Grupy jednego topiku są też dostępne na karcie [[ui:topics.details.tabGroups]] tego topiku. Zob. [Ekran topiku](topics.md#topic-details).

## Lista grup {#group-list}

Lista pokazuje wszystkie grupy konsumentów klastra. Kolumny:

- [[ui:groups.list.colGroup]] — ID grupy (`group.id`).
- [[ui:groups.list.colState]] — stan zgłaszany przez Kafkę, na przykład `Stable` (członkowie czytają), `Empty` (brak członków), `PreparingRebalance` lub `CompletingRebalance` (partycje są rozdzielane na nowo) i `Dead` (grupa już nie istnieje).
- [[ui:groups.list.colMembers]] — liczba aktywnych członków.
- `Assignor` — strategia rozdzielania partycji, na przykład `range`.
- [[ui:groups.list.colLag]] — łączny lag grupy. Zob. [Lag](groups.md#lag).
- [[ui:groups.list.colTopics]] — topiki, dla których grupa ma zatwierdzone offsety.

Tekst wpisany w polu [[ui:groups.list.search]] pokazuje grupy, których ID go zawiera; wielkość liter nie ma znaczenia. Lista stanów obok pokazuje tylko grupy w jednym stanie; wartość [[ui:groups.list.allStates]] usuwa ten filtr. Licznik pokazuje, ile grup jest wyświetlanych, a ile jest łącznie.

Członkowie, assignor i lag są wczytywane tylko dla widocznych wierszy, w miarę przewijania. Do czasu ich wczytania komórka pokazuje `…`. Jeśli nie można ich wczytać, komórka pokazuje `—`; po najechaniu na nią wskaźnikiem widać przyczynę.

Przycisk [[ui:groups.list.refresh]] wczytuje ponownie listę i jej dane. Jeśli niektóre brokery nie odpowiedziały, ostrzeżenie informuje, że lista może być niekompletna.

## Lag {#lag}

Lag to liczba wiadomości, których grupa jeszcze nie przeczytała. Dla każdej partycji jest to koniec logu minus zatwierdzony offset. Lag nigdy nie jest pokazywany poniżej zera.

Lag zależy wyłącznie od zatwierdzonych offsetów. Aktywny konsument mógł przeczytać więcej, niż zatwierdził, więc jego rzeczywisty lag może być mniejszy. Gdy grupa ponownie zatwierdzi offsety, lag spada.

Partycja bez zatwierdzonego offsetu pokazuje [[ui:groups.lag.noCommit]] i nie jest wliczana do sumy. Łączny lag grupy pokazuje `—`, gdy grupa nie ma żadnych zatwierdzonych offsetów ([[ui:groups.lag.noCommits]]) lub gdy nie udało się odczytać offsetów którejś partycji ([[ui:groups.lag.unknown]]).

Lag liczy offsety, a nie wiadomości. W topikach z kompaktowaniem lub transakcjami niektóre offsety nie zawierają widocznych wiadomości, więc grupie może zostać do przeczytania mniej wiadomości, niż pokazuje lag.

Wartości są migawką z chwili wczytania. Aby śledzić lag, należy otworzyć grupę i włączyć autoodświeżanie.

## Szczegóły grupy {#group-details}

Kliknięcie grupy na liście otwiera jej kartę. U góry podsumowanie pokazuje stan, liczbę członków i assignor.

- [[ui:groups.details.autoRefresh]] — [[ui:groups.details.refreshOff]], [[ui:groups.details.refresh5]] lub [[ui:groups.details.refresh10]].
- [[ui:groups.details.refresh]] — natychmiastowe ponowne wczytanie.
- [[ui:groups.details.join]] — uruchamia [sesję konsumenta](consumer-sessions.md) w tej grupie. Formularz sesji otrzymuje ID grupy i topiki, dla których grupa ma zatwierdzone offsety. Wymaga Pro, a w połączeniach z włączoną opcją [[ui:common.readOnly]] przycisk jest nieaktywny; po najechaniu na przycisk widać przyczynę.

Tabela [[ui:groups.details.partitions]] zawiera partycje, dla których grupa ma zatwierdzone offsety, oraz partycje przypisane jej członkom. Łączny lag grupy jest wyświetlany nad tabelą. Kolumny to [[ui:groups.details.colTopic]], [[ui:groups.details.colPartition]], `Committed` (zatwierdzony offset), `Log end` (końcowy offset partycji), [[ui:groups.details.colLag]] i [[ui:groups.details.colMember]] (client ID członka, który czyta partycję).

Tabela [[ui:groups.details.members]] zawiera aktywnych członków: `Client ID`, [[ui:groups.details.colHost]], `Member ID` i [[ui:groups.details.colAssignment]] (partycje członka w postaci `topic:partition`). Grupa bez członków pokazuje komunikat [[ui:groups.details.noMembers]].

Drugi wiersz zawiera przyciski [[ui:groups.details.resetOffsets]] i [[ui:groups.details.deleteGroup]]. Jest ukryty w połączeniach tylko do odczytu.

## Reset offsetów {#reset-offsets}

Reset offsetów przesuwa zatwierdzone offsety grupy. Grupa ponownie czyta wtedy wiadomości lub je pomija. Offsety można zresetować tylko dla grupy bez aktywnych członków (stan `Empty` lub `Dead`). W przeciwnym razie członkowie nadpisaliby nowe offsety własnymi zatwierdzeniami. Najpierw trzeba zatrzymać aplikacje grupy.

1. Na karcie grupy kliknąć przycisk [[ui:groups.details.resetOffsets]]. Dopóki grupa ma członków, przycisk jest nieaktywny; po najechaniu na przycisk widać przyczynę.
2. W polu [[ui:groups.reset.strategy]] wybrać strategię. Zob. [Strategie resetu](groups.md#reset-strategies).
3. W polu [[ui:groups.reset.scope]] wybrać zakres. Zob. [Zakres resetu](groups.md#reset-scope).
4. Kliknąć przycisk [[ui:groups.reset.preview]].
5. Sprawdzić tabelę i kliknąć przycisk zastosowania. Pokazuje on liczbę partycji, które się zmienią.

Podgląd niczego nie zmienia. Dla każdej partycji pokazuje topik, partycję, [[ui:groups.reset.colCurrent]] (zatwierdzony offset), [[ui:groups.reset.colNew]] (docelowy offset) oraz `Low / High` (pierwszy i końcowy offset partycji). Partycja, dla której nie da się obliczyć celu, jest oznaczona komunikatem [[ui:groups.reset.noTarget]] z przyczyną. Takie partycje są pomijane.

Stosowany jest tylko plan pokazany w podglądzie. Każda zmiana w formularzu ukrywa podgląd i trzeba ponownie kliknąć przycisk [[ui:groups.reset.preview]]. W połączeniu prod zastosowanie wymaga wpisania nazwy połączenia; w innych połączeniach następuje od razu.

Przed zastosowaniem aplikacja ponownie sprawdza, czy grupa nie ma członków. Po zastosowaniu sekcja [[ui:groups.reset.result]] zawiera każdą partycję z nowym offsetem lub błędem.

Reset można też przygotować z poziomu wiadomości: zob. [Reset grupy do wiadomości](browse.md#reset-group-here). Przycisk [[ui:common.close]] zamyka panel resetu.

## Strategie resetu {#reset-strategies}

Pole [[ui:groups.reset.strategy]] określa docelowy offset dla każdej partycji w zakresie:

- [[ui:groups.reset.strategyEarliest]] — pierwszy offset, który jest jeszcze przechowywany. Grupa ponownie czyta wszystko, co topik przechowuje.
- [[ui:groups.reset.strategyLatest]] — koniec partycji. Grupa pomija wszystko, co się nagromadziło, i czyta tylko nowe wiadomości.
- [[ui:groups.reset.strategyTimestamp]] — pierwsza wiadomość zapisana w chwili podanej w polu [[ui:groups.reset.timestamp]] lub później. W partycjach bez nowszych wiadomości celem jest koniec.
- [[ui:groups.reset.strategyOffset]] — wartość z pola [[ui:groups.reset.offset]], liczba całkowita równa 0 lub większa, w każdej partycji. Jeśli partycja nie ma tego offsetu, celem jest jej pierwszy lub końcowy offset.
- [[ui:groups.reset.strategyShift]] — przesuwa zatwierdzony offset o wartość z pola [[ui:groups.reset.shift]]. Liczba ujemna oznacza ponowny odczyt N wiadomości wstecz, dodatnia — pominięcie N wiadomości. Wynik nie wykracza poza pierwszy i końcowy offset. Partycji bez zatwierdzonego offsetu nie można przesunąć i są pomijane.

Czas jest czasem lokalnym, z sekundami. Kafka wyszukuje offset według znaczników czasu wiadomości.

## Zakres resetu {#reset-scope}

Pole [[ui:groups.reset.scope]] określa, które partycje są resetowane. Pozostałe partycje zachowują swoje offsety.

- [[ui:groups.reset.scopeAll]] — wszystkie partycje, dla których grupa ma zatwierdzone offsety. Jeśli grupa nie ma jeszcze zatwierdzonych offsetów, należy zamiast tego wybrać topiki.
- [[ui:groups.reset.scopeTopics]] — wszystkie partycje podanych topików, także partycje bez zatwierdzonych offsetów. Nazwy oddziela się przecinkami, spacjami lub podziałami wierszy. W ten sposób można sprawić, by grupa zaczęła czytać nowy topik od wybranego miejsca.
- [[ui:groups.reset.scopePartitions]] — tylko podane partycje. Należy wpisać je w polu [[ui:groups.reset.partitionsLabel]] w postaci `topic:partition`, oddzielone przecinkami lub spacjami, na przykład `orders:0, orders:3`.

## Usuwanie grupy {#delete-group}

Usunięcie grupy usuwa wszystkie jej zatwierdzone offsety. Usunąć można tylko grupę bez członków, w stanie `Empty`.

1. Na karcie grupy kliknąć przycisk [[ui:groups.details.deleteGroup]]. Dopóki grupa ma członków, przycisk jest nieaktywny; po najechaniu na przycisk widać przyczynę.
2. Wpisać ID grupy w celu potwierdzenia. W połączeniu prod wpisać też nazwę połączenia.
3. Kliknąć przycisk [[ui:common.delete]].

Po usunięciu karta grupy się zamyka. Jeśli aplikacja z tym `group.id` zostanie ponownie uruchomiona, Kafka utworzy grupę od nowa. Bez zatwierdzonych offsetów aplikacja zaczyna od miejsca określonego ustawieniem `auto.offset.reset`: od początku lub od końca.

W połączeniach z włączoną opcją [[ui:common.readOnly]] przycisk jest ukryty.
