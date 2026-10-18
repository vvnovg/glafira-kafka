# Producer

Ekran [[ui:sidebar.producer]] wysyła wiadomości do Kafki: pojedynczo z formularza lub wiele naraz. Otwiera się go z poziomu połączenia w panelu [[ui:sidebar.connections]] albo przyciskiem [[ui:topics.details.produce]] na karcie topiku; przycisk wypełnia pole topiku. Każde połączenie ma własną kartę Producer.

W połączeniach z włączoną opcją [[ui:common.readOnly]] ekran Producer otwiera się z ostrzeżeniem [[ui:producer.readOnly]]. Przyciski wysyłania są ukryte, ale szablony i historię nadal można przeglądać.

## Wysyłanie wiadomości {#send-message}

1. W polu [[ui:producer.topic]] wpisać topik. Pole podpowiada topiki klastra, bez topików wewnętrznych.
2. Opcjonalnie w polu [[ui:producer.partition]] wpisać numer partycji. Gdy pole pokazuje wartość [[ui:producer.partitionAuto]], partycja jest wybierana według klucza: wiadomości z tym samym kluczem trafiają do tej samej partycji, tak jak w kliencie Java. Wiadomości bez klucza trafiają do losowej partycji.
3. Wypełnić pola [[ui:producer.key]] i [[ui:producer.value]] oraz wybrać ich formaty. Zob. [Formaty klucza i wartości](producer.md#key-value-formats).
4. Opcjonalnie dodać nagłówki.
5. Kliknąć przycisk [[ui:producer.send]].

Po udanym wysłaniu wiersz pod formularzem pokazuje partycję, offset i czas wiadomości. Przy `acks` ustawionym na `0` broker nie zwraca offsetu i zamiast niego wyświetlany jest znak `—`.

Jeśli numer partycji jest zbyt duży lub broker nie potwierdzi wiadomości w limicie czasu połączenia, aplikacja pokazuje błąd. Nieistniejący topik również powoduje błąd, chyba że broker tworzy topiki automatycznie (`auto.create.topics.enable`). Przy wyłączonym automatycznym tworzeniu ten błąd może pojawić się dopiero po około 30 s (limit czasu połączenia). Błędy w formularzu, takie jak niepoprawny JSON lub nieznany symbol zastępczy, są wyświetlane, zanim cokolwiek zostanie wysłane.

W połączeniu prod wysłanie wymaga wpisania nazwy połączenia i potwierdzenia przyciskiem [[ui:producer.confirmLabel]]. Zob. [Środowiska i prod](connections.md#environments).

Przycisk [[ui:producer.clearForm]] opróżnia formularz i przywraca domyślne ustawienia producenta. Topik pozostaje.

## Formaty klucza i wartości {#key-value-formats}

Pola [[ui:producer.keyFormat]] i [[ui:producer.valueFormat]] określają, jak tekst z pola jest zamieniany na bajty:

- `string` — tekst bez zmian, w UTF-8.
- `json` — tekst jest sprawdzany pod kątem poprawności JSON i wysyłany tak, jak został wpisany, bez przeformatowania.
- `hex` — dwie cyfry szesnastkowe na bajt, na przykład `48656c6c6f`. Spacje i podziały wierszy są ignorowane.
- `base64` — bajty w standardowym Base64.
- `null` — całkowity brak klucza lub wartości. Pole tekstowe jest nieaktywne.

Domyślnie klucz ma format `string`, a wartość — `json`. Puste pole klucza z formatem `string` wysyła pusty klucz, a nie brak klucza: wszystkie takie wiadomości mają ten sam hash i trafiają do jednej partycji. Aby wysłać wiadomość bez klucza, należy wybrać `null`.

Wiadomość z wartością `null` to tombstone. W topiku z `cleanup.policy=compact` sygnalizuje Kafce, że należy usunąć starsze wiadomości z tym samym kluczem.

## Nagłówki {#headers}

Nagłówki to pary nazwa–wartość wysyłane razem z wiadomością. Przycisk [[ui:producer.headers.add]] dodaje wiersz, w którym należy wypełnić kolumny [[ui:producer.headers.colName]], [[ui:producer.headers.colValue]] i [[ui:producer.headers.colFormat]]. Przycisk `×` usuwa wiersz.

Format wartości nagłówka to `string` (tekst w UTF-8) lub `hex` (bajty jako cyfry szesnastkowe). Wiersze z pustą nazwą nie są wysyłane. Kafka dopuszcza kilka nagłówków o tej samej nazwie i wszystkie są wysyłane.

Symbole zastępcze działają w wartościach nagłówków, ale nie w ich nazwach.

## Symbole zastępcze {#placeholders}

Symbole zastępcze są zastępowane wartościami przy każdym wysłaniu. Działają w kluczu, wartości i wartościach nagłówków. Ich lista jest wyświetlana pod nagłówkami:

- `{{uuid}}` — nowy losowy UUID, na przykład `0b6f6f2c-3c1e-4a8e-9a54-2f0d6f1d7c21`.
- `{{now}}` — bieżący czas UTC w formacie ISO 8601 z milisekundami, na przykład `2026-10-15T12:30:00.000Z`.
- `{{nowMillis}}` — bieżący czas w milisekundach od 1970-01-01 UTC.
- `{{seq}}` — numer kopii w partii, licząc od 0. Przy pojedynczym wysłaniu wynosi `0`.
- `{{randomInt 1 100}}` — losowa liczba całkowita z przedziału między dwiema liczbami, włącznie z nimi.
- `{{randomString 8}}` — losowy ciąg liter łacińskich i cyfr o podanej długości, maksymalnie 100000.
- `{{env NAME}}` — wartość zmiennej środowiskowej `NAME`. Jeśli nie jest ustawiona, nic nie jest wysyłane i wyświetlany jest błąd.

Spacje wewnątrz nawiasów klamrowych są dozwolone. Nieznany symbol zastępczy lub brak `}}` zatrzymuje wysyłanie z błędem. Aby wysłać `{{` jako tekst, należy napisać `\{{`.

W formacie `json` poprawność JSON jest sprawdzana po podstawieniu symboli zastępczych. Symbole zastępcze, które dają tekst, wymagają cudzysłowów: `{"id": "{{uuid}}"}`. Liczby ich nie wymagają: `{"n": {{seq}}}`.

`{{env NAME}}` odczytuje środowisko procesu Glafiry. Jeśli Glafira jest uruchamiana z Docka lub menu Start, może brakować zmiennych ustawionych tylko w profilu terminala.

Gdy wiadomość pochodzi z [otwierania na ekranie Producer](browse.md#open-in-producer) lub z historii, każde `{{` jest w niej zapisane jako `\{{`. Wiadomość jest wtedy wysyłana dokładnie w takiej postaci, w jakiej była, bez żadnych podstawień.

## Szablony {#templates}

Szablon zapisuje cały formularz: topik, partycję, klucz, wartość, nagłówki, formaty i ustawienia producenta. Symbole zastępcze są zapisywane tak, jak zostały wpisane, i podstawiane przy każdym wysłaniu.

Aby zapisać szablon, należy kliknąć przycisk [[ui:producer.templates.saveAs]], w polu [[ui:producer.templates.nameLabel]] wpisać nazwę i kliknąć przycisk [[ui:common.save]]. Topik może być pusty. Domyślnie szablon należy do tego połączenia. Zaznaczenie opcji [[ui:producer.templates.global]] pokazuje go we wszystkich połączeniach; takie szablony są oznaczone na liście dopiskiem [[ui:producer.templates.allSuffix]].

Aby użyć szablonu, należy wybrać go z listy [[ui:producer.templates.label]]. Formularz zostaje zastąpiony szablonem. Przed wysłaniem formularz można edytować; szablon się nie zmienia.

Zapisanie zawsze tworzy nowy szablon. Aby zmienić istniejący, należy go wczytać, zmienić formularz, zapisać pod nową nazwą i usunąć stary. Aby usunąć wybrany szablon, należy kliknąć przycisk [[ui:producer.templates.delete]] i potwierdzić przyciskiem [[ui:common.delete]].

Szablony połączenia są usuwane razem z połączeniem.

## Historia {#history}

Sekcja [[ui:producer.history.title]] zawiera 100 ostatnich pojedynczych wysłań tego połączenia, od najnowszych. Wiadomości z wysyłek wsadowych nie są dodawane. Każdy wiersz pokazuje czas, topik, partycję i offset oraz początek klucza.

Historia przechowuje wiadomość w takiej postaci, w jakiej została wysłana, z już podstawionymi symbolami zastępczymi. Przycisk [[ui:producer.history.repeat]] wczytuje ją do formularza razem z ustawieniami producenta. Nic nie jest wysyłane, dopóki nie zostanie kliknięty przycisk [[ui:producer.send]]. Kopia jest wysyłana dokładnie tak, jak została zapisana: na przykład ponownie wysyłany jest ten sam UUID.

Klucz, wartość lub wartość nagłówka dłuższe niż 64 KB są zapisywane tylko częściowo. Dla takich wierszy przycisk [[ui:producer.history.repeat]] jest nieaktywny.

Historia jest przechowywana na tym komputerze otwartym tekstem, łącznie z wartościami `{{env NAME}}`. Jeśli ma to znaczenie, nie należy wysyłać sekretów przez ekran Producer. Historia połączenia jest usuwana razem z połączeniem.

## N kopii formularza {#batch-copies}

Sekcja [[ui:producer.batch.legend]] pod formularzem wysyła wiele wiadomości w jednym przebiegu. W trybie [[ui:producer.batch.modeCopies]] wysyła formularz podaną liczbę razy.

1. Wypełnić formularz jak do pojedynczego wysłania.
2. Wybrać tryb [[ui:producer.batch.modeCopies]] i w polu [[ui:producer.batch.count]] wpisać liczbę od 1 do 1000000.
3. Opcjonalnie ustawić pole [[ui:producer.batch.rate]]. Zob. [Szybkość wysyłania](producer.md#batch-rate).
4. Kliknąć przycisk [[ui:producer.batch.start]].

Symbole zastępcze są podstawiane osobno dla każdej kopii. `{{seq}}` to numer kopii, od 0 do N−1, więc każda wiadomość może otrzymać własny klucz, na przykład `order-{{seq}}`. Błędy symboli zastępczych są sprawdzane na pierwszej kopii przed rozpoczęciem przebiegu.

W trakcie przebiegu panel pokazuje, ile wiadomości wysłano, a ilu nie udało się wysłać. Na koniec pokazuje stan [[ui:producer.batch.done]]. Błędy są wyświetlane z numerem kopii, liczonym od 1. Lista pokazuje pierwsze 20 błędów; jeśli jest ich więcej, uwaga pod listą podaje łączną liczbę.

Przycisk [[ui:producer.batch.cancel]] zatrzymuje przebieg. Wiadomości, które jeszcze nie opuściły aplikacji, nie są wysyłane, a panel pokazuje ich liczbę. Aplikacja czeka do 2 sekund, aż broker potwierdzi już wysłane wiadomości. Wiadomości, które nadal nie są potwierdzone, liczą się jako błędy, ponieważ nie wiadomo, czy zostały zapisane. Zamknięcie karty Producer również zatrzymuje przebieg.

W połączeniu prod przebieg wymaga wpisania nazwy połączenia i potwierdzenia przyciskiem [[ui:producer.batch.confirmLabel]].

## Wysyłanie z pliku {#batch-file}

W trybie [[ui:producer.batch.modeFile]] każdy wiersz lub rekord pliku staje się wiadomością. Obsługiwane pliki to JSON Lines (`.jsonl`, `.ndjson`) i CSV (`.csv`).

1. W polu [[ui:producer.topic]] w formularzu powyżej wpisać topik. Wszystkie wiadomości trafiają do tego topiku.
2. Wybrać tryb [[ui:producer.batch.modeFile]] i kliknąć przycisk [[ui:producer.batch.chooseFile]].
3. Kliknąć przycisk [[ui:producer.batch.start]].

Klucz, wartość, nagłówki i partycja z formularza nie są używane, a symbole zastępcze nie są podstawiane. Obowiązują ustawienia producenta z formularza.

W JSON Lines każdy wiersz to jeden obiekt JSON, na przykład `{"key": "k1", "value": {"id": 1}, "headers": {"source": "test"}, "partition": 0}`. Wszystkie pola są opcjonalne:

- `key` i `value` — ciąg JSON jest wysyłany jako jego tekst, bez cudzysłowów. Każda inna wartość JSON, na przykład obiekt lub liczba, jest wysyłana jako tekst JSON. `null` lub brak pola oznacza brak klucza lub wartości.
- `headers` — obiekt z wartościami tekstowymi.
- `partition` — numer partycji. Bez niego partycja jest wybierana według klucza.

Puste wiersze są pomijane. Plik musi być w UTF-8.

W CSV pierwszy wiersz zawiera nazwy kolumn. Kolumna `value` jest wymagana; kolumny `key`, `partition` i `header.NAME` są opcjonalne, na przykład `header.source`. Pusta komórka `key` oznacza brak klucza, a pusta komórka nagłówka — brak tego nagłówka. Pusta komórka `value` wysyła pustą wartość, a nie `null`, więc z CSV nie można wysłać tombstone.

Plik jest odczytywany strumieniowo, więc łączna liczba nie jest znana z góry i jest wyświetlana jako `—`. Wiersz lub rekord większy niż 16 MB, niepoprawny JSON lub ujemny numer partycji liczy się jako błąd dla danego wiersza, a przebieg jest kontynuowany od następnego wiersza. Błędy są wyświetlane z numerem wiersza.

## Szybkość wysyłania {#batch-rate}

Pole [[ui:producer.batch.rate]] ogranicza partię do podanej liczby wiadomości na sekundę. Należy wpisać liczbę całkowitą równą 1 lub większą. Puste pole oznacza wartość [[ui:producer.batch.rateUnlimited]].

Wiadomości są rozkładane równomiernie w czasie: przy 10 wiadomościach na sekundę jedna wiadomość wychodzi mniej więcej co 100 ms. Limit pozwala uniknąć przeciążenia współdzielonego klastra lub symulować stałe obciążenie.

Bez limitu aplikacja wysyła tak szybko, jak broker przyjmuje wiadomości. Szybkość dotyczy obu trybów wysyłki wsadowej.

## Ustawienia producenta {#producer-settings}

[[ui:producer.settings.legend]] obowiązują przy pojedynczych wysłaniach i w partiach. Są zapisywane w szablonach i historii.

`acks` określa, ile replik musi potwierdzić zapis:

- `all` — domyślnie. Leader czeka na wszystkie zsynchronizowane repliki. To najbezpieczniejszy wybór. Jeśli zsynchronizowanych replik jest mniej, niż wymaga ustawienie topiku `min.insync.replicas`, zapis kończy się błędem.
- `1` — potwierdza tylko leader. Szybciej, ale wiadomość może zostać utracona, jeśli leader ulegnie awarii, zanim followery ją skopiują.
- `0` — producent nie czeka na potwierdzenie. Wiadomości mogą zostać utracone, a offset nie jest zwracany.

Pole [[ui:producer.settings.compression]] kompresuje partie wiadomości: `none` (domyślnie), `gzip`, `snappy`, `lz4` lub `zstd`. Kompresja oszczędza ruch sieciowy i miejsce na dysku kosztem pewnego obciążenia procesora. Często wybierane są `zstd` i `lz4`. Przy pojedynczej małej wiadomości zysk jest niewielki.

Opcja [[ui:producer.settings.idempotence]] ustawia `enable.idempotence`. Zapobiega duplikatom, gdy klient ponawia wysłanie po błędzie sieci. Wymaga `acks` ustawionego na `all`; przy innych wartościach aplikacja pokazuje błąd i niczego nie wysyła.
