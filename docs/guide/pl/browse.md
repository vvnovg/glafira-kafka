# Przeglądanie wiadomości

Wiadomości przegląda się na karcie [[ui:topics.details.tabMessages]] topiku. Przeglądanie nie dołącza do grupy konsumentów i niczego nie zatwierdza, więc klaster się nie zmienia. Działa też w połączeniach z włączoną opcją [[ui:common.readOnly]]. Aby czytać jako członek grupy, należy użyć [sesji konsumenta](consumer-sessions.md).

## Rozpoczęcie odczytu {#start-reading}

1. Otworzyć topik z listy [[ui:sidebar.topics]]. Jako pierwsza otwiera się karta [[ui:topics.details.tabMessages]].
2. W polu [[ui:browse.form.from]] wybrać, od którego miejsca zacząć. Zob. [Pozycja początkowa](browse.md#start-position).
3. Opcjonalnie ustawić formaty i filtry.
4. Kliknąć przycisk [[ui:browse.panel.read]].

Wiadomości pojawiają się w tabeli w miarę napływania. Wiersz obok przycisków pokazuje, ile wiadomości przeskanowano, ile pasuje do filtrów i ile jest w tabeli. Pokazuje też stan: [[ui:browse.panel.reading]], [[ui:browse.panel.finishLimit]], [[ui:browse.panel.finishEnd]] lub [[ui:browse.panel.finishCancelled]].

Bez `Live tail` odczyt kończy się na końcu topiku w stanie z chwili kliknięcia przycisku [[ui:browse.panel.read]] albo wcześniej, po osiągnięciu wartości z pola [[ui:browse.form.limit]]. Limit liczy wiadomości pasujące do filtrów. Może wynosić od 1 do 100000; duże wartości spowalniają odczyt.

Z włączonym `Live tail` odczyt nie kończy się na końcu topiku: nowe wiadomości są dodawane w miarę ich zapisywania. Gdy wszystkie stare wiadomości zostaną odczytane, stan pokazuje [[ui:browse.panel.waiting]]. Tabela przechowuje ostatnie wiadomości do wysokości limitu i odrzuca najstarsze.

Przycisk [[ui:browse.panel.stop]] zatrzymuje odczyt i zachowuje tabelę. Podczas odczytu przycisk [[ui:browse.panel.read]] zmienia się w przycisk [[ui:browse.panel.restart]]: uruchamia odczyt od nowa z bieżącymi ustawieniami i czyści tabelę. Jeśli połączenie z brokerem zostanie przerwane, aplikacja sama łączy się ponownie i informuje o tym w wierszu stanu.

Wiadomości z różnych partycji są w tabeli przemieszane. Kafka zachowuje kolejność tylko w obrębie jednej partycji. Kolumna `P` pokazuje partycję, a `Offset` — pozycję w niej. Zaznaczenie `UTC` pokazuje czas w UTC zamiast czasu lokalnego.

## Pozycja początkowa {#start-position}

Pole [[ui:browse.form.startPosition]] określa, gdzie w każdej partycji zaczyna się odczyt:

- [[ui:browse.form.startLatest]] — ostatnie N wiadomości. N jest dzielone równo między partycje, więc przy 100 wiadomościach i 4 partycjach aplikacja pobiera do 25 z każdej.
- [[ui:browse.form.startEarliest]] — od najstarszej wiadomości, która jest jeszcze przechowywana.
- [[ui:browse.form.startOffset]] — od tego samego offsetu w każdej partycji. Jeśli partycja nie ma takiego offsetu, odczyt zaczyna się od jej początku lub końca. Ten tryb jest najbardziej przydatny przy jednej partycji.
- [[ui:browse.form.startTimestamp]] — od pierwszej wiadomości ze znacznikiem czasu równym podanemu czasowi lokalnemu lub późniejszym. W partycjach bez nowszych wiadomości odczyt zaczyna się od końca.
- [[ui:browse.form.startLastMinutes]] — to samo, licząc wstecz od teraz.

Pole z tekstem [[ui:browse.form.partitionsPlaceholder]] ogranicza odczyt do wybranych partycji. Należy wpisać numery partycji oddzielone przecinkami, na przykład `0,2`. Puste pole oznacza odczyt wszystkich partycji.

## Filtry {#filters}

Filtry pokazują tylko pasujące wiadomości. Aplikacja i tak czyta wszystkie wiadomości od pozycji początkowej i sprawdza każdą z nich, więc filtry nie przyspieszają odczytu. Muszą być spełnione wszystkie wypełnione pola.

- [[ui:browse.filters.keyContains]] i [[ui:browse.filters.valueContains]] — tekst w kluczu lub wartości.
- `regex` — traktuje te pola oraz wartość nagłówka jako wyrażenia regularne.
- `Aa` — uwzględnia wielkość liter. Domyślnie wielkość liter nie ma znaczenia.
- [[ui:browse.filters.header]] — wiadomość ma nagłówek o dokładnie takiej nazwie. Pole [[ui:browse.filters.headerValue]] sprawdza też jego wartość.
- `JSONPath` — warunek dotyczący wartości JSON, na przykład `$.order.status == "FAILED"`. Bez `==` lub `!=` ścieżka musi po prostu istnieć, na przykład `$.error`. Wiadomości, których wartość nie jest poprawnym dokumentem JSON, nie pasują.
- [[ui:browse.filters.timeFrom]] i [[ui:browse.filters.timeTo]] — znacznik czasu wiadomości mieści się w tym zakresie, w czasie lokalnym. Oba końce są uwzględniane.

Filtry sprawdzają tekst wyświetlany w tabeli, po zdekodowaniu. Jeśli format wartości to `hex`, filtr wartości przeszukuje tekst szesnastkowy. Filtry są stosowane po kliknięciu przycisku [[ui:browse.panel.read]]; po ich zmianie trzeba odczytać ponownie.

Filtr nie przesuwa pozycji początkowej. Na przykład [[ui:browse.form.startLatest]] z filtrem znajduje dopasowania tylko wśród ostatnich N wiadomości. Aby przeszukać cały topik, należy zacząć od pozycji [[ui:browse.form.startEarliest]].

## Formaty {#formats}

Pole [[ui:browse.form.format]] określa, jak dekodowane są klucze i wartości, osobno dla pozycji [[ui:browse.form.key]] i [[ui:browse.form.value]]. Zmienia się tylko sposób wyświetlania, a nie dane.

- `auto` — domyślnie. Poprawne obiekty i tablice JSON są wyświetlane jako JSON, inny tekst UTF-8 jako ciąg znaków, dane binarne jako hex.
- `string` — tekst w UTF-8. Bajty, które nie są poprawnym UTF-8, są zastępowane znakiem `�`.
- `json` — JSON z wcięciami w szczegółach wiadomości. Jeśli wartość nie jest poprawnym dokumentem JSON, szczegóły pokazują błąd.
- `hex` — każdy bajt jako dwie cyfry szesnastkowe.
- `base64` — bajty w Base64.

Szczegóły pokazują, jaki format został faktycznie użyty, na przykład `json`, gdy `auto` wykrył JSON. Wartości nagłówków są wyświetlane jako tekst; binarne wartości nagłówków — jako `0x` i ciąg szesnastkowy. Brakujący klucz lub brakująca wartość są wyświetlane jako `null`.

Bardzo duże klucze i wartości są wyświetlane tylko częściowo, mniej więcej pierwszy 1 MB tekstu. Dotyczy to także `hex` i `base64`. Szczegóły wiadomości pokazują wtedy ostrzeżenie.

## Szczegóły wiadomości {#message-details}

Kliknięcie wiersza otwiera szczegóły wiadomości obok tabeli. U góry są partycja, offset i czas. Poniżej:

- [[ui:browse.details.value]] i [[ui:browse.details.key]] — pełny tekst z formatem i rozmiarem w bajtach. JSON jest wyświetlany z wcięciami. Przycisk [[ui:browse.details.copy]] kopiuje tekst.
- [[ui:browse.table.colHeaders]] — tabela nazw i wartości nagłówków.

Kolumny tabeli to `P`, `Offset`, [[ui:browse.table.colTime]], [[ui:browse.table.colKey]], [[ui:browse.table.colValue]], [[ui:browse.table.colHeaders]] (ich liczba) i [[ui:browse.table.colSize]] (rozmiar klucza i wartości w bajtach). Czas to znacznik czasu wiadomości. W zależności od ustawienia topiku `message.timestamp.type` ustawia go producent lub broker.

## Kopiowanie wiadomości {#export-messages}

Glafira nie zapisuje przeglądanych wiadomości do pliku. Aby wynieść wiadomość poza aplikację, należy ją skopiować:

- [[ui:browse.details.copyJson]] kopiuje całą wiadomość jako JSON: topik, partycję, offset, znacznik czasu, klucz, wartość, nagłówki i rozmiar. Klucz i wartość są dołączane w postaci zdekodowanej, z formatem.
- Przycisk [[ui:browse.details.copy]] obok pozycji [[ui:browse.details.value]] lub [[ui:browse.details.key]] kopiuje tylko ten tekst.

Skopiowany tekst ma format wybrany do odczytu. Aby dokładnie skopiować dane binarne, należy odczytać je jako `hex` lub `base64`. Bardzo duże klucze i wartości są kopiowane tylko częściowo, tak jak są wyświetlane, nawet w `hex` i `base64`. Zob. [Formaty](browse.md#formats).

## Otwieranie na ekranie Producer {#open-in-producer}

Przycisk [[ui:browse.details.resend]] otwiera wiadomość na ekranie [[ui:sidebar.producer]] tego samego połączenia. Formularz otrzymuje topik, klucz, wartość, nagłówki i formaty wiadomości. Wybór partycji pozostaje po stronie Kafki. Symbole zastępcze w tekście wiadomości nie są podstawiane: kopia jest wysyłana w takiej postaci, w jakiej została odczytana.

Nic nie jest wysyłane od razu. Należy sprawdzić formularz, w razie potrzeby go zmienić i wysłać z ekranu Producer. Zob. [Producer](producer.md).

Przycisk jest nieaktywny w dwóch przypadkach, a obok wyświetlana jest przyczyna:

- klucz lub wartość zostały wyświetlone tylko częściowo, ponieważ są zbyt duże;
- tekst został zdekodowany ze stratami, ponieważ nie jest poprawnym UTF-8. Aby wysłać dokładną kopię, należy odczytać go jako `hex` lub `base64`.

W połączeniach z włączoną opcją [[ui:common.readOnly]] przycisk jest ukryty.

## Reset grupy do wiadomości {#reset-group-here}

Polecenie [[ui:browse.details.resetHere]] przygotowuje reset offsetów tak, aby grupa konsumentów zaczęła czytać od tej wiadomości.

1. W szczegółach wiadomości kliknąć [[ui:browse.details.resetHere]].
2. Wybrać grupę z listy. Wyświetlane są tylko istniejące grupy.
3. Kliknąć przycisk [[ui:browse.details.goToReset]].

Otwiera się karta grupy z wypełnionym formularzem resetu. Pole [[ui:groups.reset.strategy]] ma wartość [[ui:groups.reset.strategyOffset]] z offsetem wiadomości. Pole [[ui:groups.reset.scope]] ma wartość [[ui:groups.reset.scopePartitions]] z topikiem i partycją wiadomości. Na razie nic się nie zmienia: należy kliknąć przycisk [[ui:groups.reset.preview]], aby zobaczyć nowe offsety, a następnie zastosować reset w tym miejscu. Grupa nie może mieć aktywnych członków. Zob. [Grupy konsumentów](groups.md).

W połączeniach z włączoną opcją [[ui:common.readOnly]] przycisk jest ukryty.
