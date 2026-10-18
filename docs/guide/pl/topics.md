# Klaster i topiki

W tym rozdziale opisano przegląd klastra, listę topików, tworzenie i usuwanie topików, ekran topiku i konfigurację topiku. Pod każdym połączeniem w panelu [[ui:sidebar.connections]] znajdują się linki [[ui:sidebar.cluster]] i [[ui:sidebar.topics]]. Każdy z nich otwiera się we własnej karcie.

## Przegląd klastra {#cluster-overview}

Ekran [[ui:cluster.overview.title]] pokazuje stan klastra w chwili wczytania. Przycisk [[ui:cluster.overview.refresh]] wczytuje go ponownie.

- `Cluster ID` — identyfikator klastra.
- [[ui:cluster.overview.controller]] — ID brokera, którego klaster zgłasza jako kontroler. W klastrach KRaft klienci nie komunikują się bezpośrednio z kontrolerami: klaster zgłasza tu losowy broker, a nie aktywny kontroler, i zgłaszany broker może się zmieniać między odświeżeniami.
- [[ui:cluster.overview.brokers]] — liczba brokerów.
- [[ui:cluster.overview.topicsInternal]] — wszystkie topiki, w nawiasie liczba topików wewnętrznych.
- [[ui:cluster.overview.partitions]] — liczba partycji we wszystkich topikach.
- `Under-replicated` — partycje, w których nie wszystkie repliki są zsynchronizowane (w ISR). Zwykle oznacza to, że broker nie działa lub nie nadąża.
- `Offline` — partycje bez leadera. Nie można do nich zapisywać ani z nich czytać.

Niezerowa wartość w `Under-replicated` lub `Offline` jest wyróżniona.

Poniżej znajduje się tabela brokerów z kolumnami `ID` i [[ui:cluster.overview.address]]. Broker z pola [[ui:cluster.overview.controller]] jest oznaczony etykietą [[ui:cluster.overview.controllerBadge]]. W klastrach KRaft jest to ten sam losowy broker, a nie aktywny kontroler. Po kliknięciu brokera pod tabelą wyświetla się jego konfiguracja. Konfigurację brokera można tylko przeglądać. Pole [[ui:common.config.searchParam]] pozwala znaleźć parametr według nazwy, a opcja [[ui:common.config.onlyChanged]] ukrywa parametry z wartościami domyślnymi. Ikona kłódki oznacza parametry tylko do odczytu.

## Lista topików {#topic-list}

Ekran [[ui:sidebar.topics]] zawiera listę topików klastra. Kliknięcie wiersza otwiera topik we własnej karcie.

Kolumny to [[ui:topics.list.colName]], [[ui:topics.list.colPartitions]], `RF` (współczynnik replikacji), [[ui:topics.list.colMessages]], `cleanup.policy` i [[ui:topics.list.colUnderRepl]] (liczba niedoreplikowanych partycji). Liczba wiadomości jest przybliżona: to różnica między końcowym a początkowym offsetem partycji. Kompaktowanie i znaczniki transakcji sprawiają, że rzeczywista liczba jest mniejsza. Liczby wiadomości i `cleanup.policy` są wczytywane tylko dla wierszy, do których przewinięto listę.

Elementy paska narzędzi:

- [[ui:topics.list.search]] — wpisanie części nazwy wyszukuje topiki; wielkość liter nie ma znaczenia. Aby wyszukać wyrażeniem regularnym, należy umieścić je między ukośnikami, na przykład `/^orders\..*/`.
- [[ui:topics.list.internal]] — pokazuje topiki wewnętrzne, takie jak `__consumer_offsets`. Są oznaczone etykietą [[ui:topics.list.internalBadge]].
- Lista autoodświeżania — [[ui:topics.list.autoRefreshOff]], [[ui:topics.list.every10]] lub [[ui:topics.list.every30]].
- [[ui:topics.list.refresh]] — natychmiast wczytuje ponownie listę i liczby wiadomości.

Obok paska narzędzi aplikacja pokazuje, ile topików pasuje do wyszukiwania w stosunku do łącznej liczby. W połączeniach z włączoną opcją [[ui:common.readOnly]] przyciski [[ui:topics.list.create]] i [[ui:topics.list.select]] są ukryte.

## Tworzenie topiku {#create-topic}

1. Na liście topików kliknąć przycisk [[ui:topics.list.create]]. Nad listą otwiera się formularz [[ui:topics.create.title]].
2. W polu [[ui:topics.create.name]] wpisać nazwę. Może zawierać litery łacińskie, cyfry, `.`, `_` i `-`, maksymalnie 249 znaków. Formularz ostrzega, jeśli nazwa zawiera jednocześnie `.` i `_`: nazwy metryk Kafki dla takich topików mogą kolidować z innymi topikami.
3. W polu [[ui:topics.create.partitions]] ustawić liczbę partycji. Partycje są czytane równolegle, a kolejność jest gwarantowana tylko w obrębie jednej partycji. Liczbę można później zwiększyć, ale nigdy zmniejszyć.
4. W polu [[ui:topics.create.rf]] ustawić, ile kopii każdej partycji przechowują brokery. Wartość nie może być większa niż liczba brokerów, którą formularz pokazuje pod polem. Wartość domyślna to 3 lub liczba brokerów, jeśli jest ich mniej. `-1` oznacza wartość domyślną brokera.
5. W polu [[ui:topics.create.preset]] wybrać profil:
   - [[ui:topics.create.presetDelete]] — stare wiadomości są usuwane według czasu.
   - `compact` — przechowywana jest tylko najnowsza wartość dla każdego klucza.
   - `compact + delete` — oba sposoby.
   - [[ui:topics.create.presetTest]] — wiadomości są przechowywane przez godzinę.
6. Opcjonalnie dodać [[ui:topics.create.extraConfigs]], po jednym w wierszu, na przykład `retention.ms=86400000`. Zastępują one ustawienia profilu. Wiersze zaczynające się od `#` są ignorowane.
7. Kliknąć przycisk [[ui:topics.create.submit]].

Gdy formularz jest poprawnie wypełniony, pokazuje równoważne polecenie `kafka-topics.sh`. Można je skopiować, na przykład aby utworzyć taki sam topik w innym klastrze.

W połączeniu prod utworzenie trzeba potwierdzić, wpisując nazwę połączenia. Po utworzeniu topiku otwiera się jego karta.

## Usuwanie topików {#delete-topics}

Aby usunąć jeden topik, należy go otworzyć i kliknąć przycisk [[ui:topics.actions.deleteTopic]]. Następnie trzeba wpisać nazwę topiku w celu potwierdzenia i kliknąć przycisk [[ui:topics.delete.confirm]]. Karta topiku się zamyka.

Aby usunąć kilka topików naraz:

1. Na liście topików kliknąć przycisk [[ui:topics.list.select]]. Obok nazw pojawiają się pola wyboru.
2. Zaznaczyć topiki. Topików wewnętrznych nie można zaznaczyć.
3. Kliknąć przycisk usuwania. Pokazuje on liczbę zaznaczonych topików.
4. Sprawdzić listę nazw, wpisać ich liczbę w celu potwierdzenia i kliknąć przycisk [[ui:topics.delete.confirm]].

Przycisk [[ui:topics.list.cancelSelect]] ukrywa pola wyboru.

W połączeniu prod oba sposoby usuwania wymagają także wpisania nazwy połączenia.

Usunięcia nie można cofnąć: dane zostają utracone. Jeśli niektóre topiki nie zostaną usunięte, na przykład z powodu braku uprawnień, aplikacja wyświetla ich listę z przyczyną. Pozostałe topiki są usuwane.

## Ekran topiku {#topic-details}

Karta topiku pokazuje liczbę partycji, współczynnik replikacji i przybliżoną liczbę wiadomości. Przyciski u góry:

- [[ui:topics.details.refresh]] — ponownie wczytuje dane topiku.
- [[ui:topics.details.produce]] — otwiera ekran [[ui:sidebar.producer]] z tym topikiem. Zob. [Producer](producer.md).
- [[ui:topics.details.consume]] — uruchamia sesję konsumenta dla tego topiku. Wymaga Pro. Zob. [Sesje konsumenta](consumer-sessions.md).

W połączeniach z włączoną opcją [[ui:common.readOnly]] oba przyciski są nieaktywne.

Drugi wiersz zawiera działania zmieniające topik. Jest ukryty w połączeniach tylko do odczytu i dla topików wewnętrznych.

- [[ui:topics.actions.addPartitions]] — należy wpisać nową łączną liczbę i kliknąć przycisk [[ui:topics.actions.addSubmit]]. Liczby partycji nie można zmniejszyć. Po zwiększeniu wiadomości z tym samym kluczem mogą trafiać do innej partycji, więc kolejność według klucza zostaje zaburzona.
- [[ui:topics.actions.purge]] — usuwa wszystkie wiadomości do bieżącego końca wybranych partycji. Sam topik pozostaje. Należy zaznaczyć partycje, wpisać nazwę topiku i kliknąć przycisk [[ui:topics.actions.purgeConfirm]]. Czyszczenie działa tylko wtedy, gdy `cleanup.policy` zawiera `delete`; w przeciwnym razie przycisk jest nieaktywny, a obok wyświetlana jest przyczyna. W topiku `compact,delete` czyszczenie usuwa też najnowsze wartości kluczy.
- [[ui:topics.actions.deleteTopic]] — zob. [Usuwanie topików](topics.md#delete-topics).

W połączeniu prod każde z tych działań wymaga także wpisania nazwy połączenia.

Karty poniżej:

- [[ui:topics.details.tabMessages]] — przeglądanie wiadomości. Zob. [Przeglądanie wiadomości](browse.md).
- [[ui:topics.details.tabPartitions]] — tabela z każdą partycją: [[ui:topics.details.colLeader]], [[ui:topics.details.colReplicas]], `ISR` (repliki zsynchronizowane), początkowy (`Low`) i końcowy (`High`) offset oraz liczba wiadomości. Partycja bez leadera pokazuje wartość [[ui:topics.details.noLeader]].
- [[ui:topics.details.tabConfigs]] — parametry topiku. Zob. [Konfiguracja topiku](topics.md#topic-config).
- [[ui:topics.details.tabGroups]] — grupy konsumentów, które mają zatwierdzone offsety dla tego topiku, z kolumnami [[ui:topics.groups.colState]], [[ui:topics.groups.colPartitions]] i [[ui:topics.groups.colLag]]. Kliknięcie grupy ją otwiera. Zob. [Grupy konsumentów](groups.md).

## Konfiguracja topiku {#topic-config}

Karta [[ui:topics.details.tabConfigs]] zawiera wszystkie parametry topiku w trzech kolumnach:

- [[ui:common.config.colParam]] — nazwa parametru. Nazwy pogrubione są ustawione jawnie: w topiku lub w konfiguracji brokera.
- [[ui:common.config.colValue]] — bieżąca wartość. Wartości poufne są ukryte jako `••••••`.
- [[ui:common.config.colSource]] — skąd pochodzi wartość. Na przykład `DynamicTopic` oznacza wartość ustawioną w tym topiku, `StaticBroker` i `DynamicBroker` pochodzą z konfiguracji brokera, a `Default` to wartość domyślna Kafki.

Pole [[ui:common.config.searchParam]] pozwala znaleźć parametr według nazwy, a opcja [[ui:common.config.onlyChanged]] pokazuje tylko parametry pogrubione. Ikona kłódki oznacza parametry tylko do odczytu. Ikona `?` obok nazwy objaśnia parametr.

## Zmiana konfiguracji {#edit-config}

1. Na karcie [[ui:topics.details.tabConfigs]] kliknąć przycisk [[ui:topics.details.edit]]. W połączeniach z włączoną opcją [[ui:common.readOnly]] ten przycisk jest ukryty.
2. Zmienić wartości w kolumnie [[ui:common.config.colValue]]. Parametrów tylko do odczytu i poufnych nie można edytować. Aby usunąć własną wartość topiku, należy kliknąć [[ui:common.config.reset]]: obowiązuje wtedy wartość brokera. Pusta wartość jest niedozwolona.
3. Kliknąć przycisk [[ui:common.config.review]]. Aplikacja pokazuje każdą zmianę jako stara wartość → nowa wartość; [[ui:common.config.defaultValue]] oznacza przywrócenie wartości domyślnej.
4. Kliknąć przycisk [[ui:common.config.apply]] albo przycisk [[ui:common.config.back]], aby kontynuować edycję.

Do momentu zastosowania nic nie zmienia się w klastrze. Przycisk [[ui:common.config.discard]] anuluje wszystkie zmiany. Dopóki są niezapisane zmiany, przycisk [[ui:topics.details.closeEditor]] jest nieaktywny. W połączeniu prod zmiany trzeba potwierdzić, wpisując nazwę połączenia.

Wysyłane są tylko zmienione parametry. Pozostałe parametry topiku pozostają bez zmian. Broker sprawdza wartości i odrzuca nieprawidłowe z błędem.

## Parametry topiku {#config-parameters}

Topiki Kafki mają dziesiątki parametrów. Ikony `?` w tabeli konfiguracji objaśniają każdy z nich. Najczęściej potrzebne są te:

- `cleanup.policy` — co dzieje się ze starymi danymi. `delete` usuwa stare segmenty według czasu lub rozmiaru. `compact` przechowuje tylko najnowszą wartość dla każdego klucza. `compact,delete` robi jedno i drugie.
- `retention.ms` — jak długo wiadomości są przechowywane, w milisekundach. Wartość domyślna to `604800000` (7 dni); `-1` przechowuje je bezterminowo. Działa, gdy `cleanup.policy` zawiera `delete`.
- `retention.bytes` — maksymalny rozmiar każdej partycji, po przekroczeniu którego usuwane są stare segmenty. Domyślne `-1` oznacza brak limitu rozmiaru.
- `segment.ms` i `segment.bytes` — kiedy broker rozpoczyna nowy segment logu. Retencja i kompaktowanie działają na całych segmentach, więc wiadomość może pozostać dłużej niż `retention.ms`, dopóki jej segment nie zostanie zamknięty.
- `min.insync.replicas` — ile replik musi potwierdzić zapis przy `acks=all`. Zwykle 2 przy współczynniku replikacji 3.
- `max.message.bytes` — największy batch, jaki broker przyjmuje dla tego topiku. Wartość domyślna to około 1 MB. Producenci muszą dopuszczać taki sam rozmiar (`max.request.size`).
- `compression.type` — jak broker przechowuje wiadomości. `producer` zachowuje kompresję producenta.
- `message.timestamp.type` — `CreateTime` (czas ustawiony przez producenta) lub `LogAppendTime` (czas zapisania wiadomości przez broker). Ten czas jest wyświetlany podczas przeglądania wiadomości.
- `delete.retention.ms` — jak długo znaczniki usunięcia (wiadomości z wartością null) pozostają w topiku kompaktowanym. Wartość domyślna to 1 dzień.

Niektóre parametry istnieją tylko w nowszych wersjach Kafki, a niektóre są przestarzałe. Ikona `?` informuje o tym tam, gdzie ma to znaczenie. Parametry tiered storage, takie jak `remote.storage.enable`, działają tylko wtedy, gdy klaster je obsługuje.
