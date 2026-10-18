# Sesje konsumenta

Sesja konsumenta czyta topiki jako rzeczywisty członek grupy konsumentów. Sesje konsumenta są funkcją Pro.

## Czym jest sesja {#what-is}

[Przeglądanie](browse.md) czyta wiadomości bez dołączania do grupy i niczego nie zmienia w klastrze. Sesja działa inaczej: dołącza do grupy o podanym `group.id`, otrzymuje partycje od Kafki i może zatwierdzać offsety. Służy do czytania tego, co czytałaby grupa, do przesuwania offsetów grupy przez zatwierdzanie lub do obserwowania rebalansu grupy.

Sesja wpływa na klaster. Gdy dołącza lub odchodzi, Kafka rozdziela partycje grupy na nowo, a pozostali członkowie przechodzą rebalans. Zatwierdzenia zmieniają offsety grupy, z których będą korzystać jej aplikacje.

W Free elementy [[ui:sidebar.consumer]], [[ui:topics.details.consume]] i [[ui:groups.details.join]] są nieaktywne i oznaczone kłódką oraz napisem `Pro`. Jeśli karta sesji otworzy się bez aktywnej licencji Pro, pokazuje komunikat [[ui:consumer.proOnly]] i link [[ui:common.openLicense]]. Zob. [Aktywacja klucza](license-updates.md#activate).

Sesje nie są dostępne w połączeniach z włączoną opcją [[ui:common.readOnly]]: karta pokazuje komunikat [[ui:consumer.readOnlyBlocked]]. Jeśli połączenie stanie się tylko do odczytu w trakcie działania sesji, sesja opuszcza grupę.

## Uruchamianie sesji {#start-session}

Sesję można otworzyć na trzy sposoby:

- link [[ui:sidebar.consumer]] pod połączeniem w panelu [[ui:sidebar.connections]] — pusty formularz;
- przycisk [[ui:topics.details.consume]] na karcie topiku — topik jest już wpisany;
- przycisk [[ui:groups.details.join]] na karcie grupy — grupa i topiki, dla których ma zatwierdzone offsety, są już wpisane.

Za każdym razem otwiera się nowa karta, więc kilka sesji może działać jednocześnie.

W sekcji [[ui:consumer.form.groupTopics]]:

- `group.id` — grupa, do której należy dołączyć. Jeśli pole pozostanie puste, aplikacja zaproponuje nową nazwę, na przykład `glafira-kafka-anna-1a2b3c`. Nowa grupa nie ma zatwierdzonych offsetów.
- [[ui:consumer.form.topicsLabel]] — nazwy topików oddzielone przecinkami lub spacjami. Można też zaznaczyć opcję [[ui:consumer.form.useRegex]] i wpisać wyrażenie w polu [[ui:consumer.form.patternLabel]], na przykład `orders\..*`. Wyrażenie jest dopasowywane od początku nazwy: jeśli brakuje `^`, jest dodawany. Pasujące topiki utworzone później są dodawane w ciągu kilku minut (przy odświeżeniu metadanych); powoduje to rebalans.
- `auto.offset.reset` — gdzie zacząć w partycjach, dla których grupa nie ma zatwierdzonego offsetu: [[ui:consumer.form.resetLatest]] (domyślnie) lub [[ui:consumer.form.resetEarliest]]. Tam, gdzie grupa ma zatwierdzony offset, odczyt jest kontynuowany od niego.
- [[ui:consumer.form.autoCommit]] — automatyczne zatwierdzanie odczytanych offsetów. Domyślnie wyłączone. Zob. [Zatwierdzanie offsetów](consumer-sessions.md#commits).
- `isolation.level` — `read_committed` (domyślnie) pokazuje tylko wiadomości z zatwierdzonych transakcji. `read_uncommitted` pokazuje też wiadomości z otwartych i przerwanych transakcji.

W sekcji [[ui:consumer.form.formatLegend]] należy wybrać wartości w polach [[ui:consumer.form.keyFormat]] i [[ui:consumer.form.valueFormat]], tak jak przy przeglądaniu. Zob. [Formaty](browse.md#formats). Pole [[ui:consumer.form.buffer]] określa, ile wiadomości przechowuje tabela, od 1 do 100000; domyślnie 5000.

Filtry działają tak jak przy przeglądaniu. Zob. [Filtry](browse.md#filters).

Następnie należy kliknąć przycisk [[ui:consumer.actions.connect]]. W połączeniu prod aplikacja prosi o wpisanie nazwy połączenia i potwierdzenie przyciskiem [[ui:consumer.dialog.confirm]]. Podczas działania sesji formularza nie można zmieniać.

## Odczyt {#reading}

Wiersz stanu obok przycisków pokazuje [[ui:consumer.status.connecting]], a potem [[ui:consumer.status.inGroup]]. Pokazuje też, ile wiadomości odczytano, ile pasuje do filtrów i ile jest w tabeli. Jeśli połączenie z brokerem zostanie przerwane, sesja sama łączy się ponownie i informuje o tym w wierszu stanu.

Wiadomości pojawiają się w tabeli w miarę napływania, a sesja czyta nowe wiadomości, dopóki nie zostanie zatrzymana. Gdy tabela się zapełni, najstarsze wiadomości są usuwane. Kliknięcie wiersza pokazuje szczegóły wiadomości, tak jak przy przeglądaniu. Zob. [Szczegóły wiadomości](browse.md#message-details). Zaznaczenie `UTC` pokazuje czas w UTC.

Sesja czyta tylko partycje, które przydzieliła jej Kafka. Jeśli grupa ma innych członków, czytają oni pozostałe partycje. Jeśli grupa ma więcej członków niż partycji, sesja może nie otrzymać żadnej partycji.

Przycisk [[ui:consumer.disconnect]] opuszcza grupę, a jej partycje otrzymują pozostali członkowie. Zamknięcie karty sesji również opuszcza grupę. Po zakończeniu sesji wiersz stanu pokazuje [[ui:consumer.status.disconnected]].

Jeśli topik nie istnieje, sesja zatrzymuje się z błędem i pokazuje nazwy do sprawdzenia.

## Zatwierdzanie offsetów {#commits}

Zatwierdzenie (commit) zapisuje offset dla grupy. Następny członek, który czyta partycję, lub ta sama aplikacja po ponownym uruchomieniu zaczyna od tego offsetu.

Z włączoną opcją [[ui:consumer.form.autoCommit]] sesja zatwierdza odczytane offsety w tle co kilka sekund, a także jeszcze raz przy opuszczaniu grupy. Wiadomości ukryte przez filtry liczą się jako odczytane.

Bez autozatwierdzania sesja sama niczego nie zatwierdza. Służą do tego przyciski:

- [[ui:consumer.actions.commitSelected]] — zatwierdza offset następujący po zaznaczonej wiadomości, tylko w jej partycji. Grupa będzie kontynuować od następnej wiadomości. Jeśli zaznaczona wiadomość jest starsza niż bieżący zatwierdzony offset, offset przesuwa się wstecz.
- [[ui:consumer.actions.commitAll]] — zatwierdza bieżącą pozycję w każdej przydzielonej partycji. Obejmuje to wiadomości ukryte przez filtry oraz wiadomości już usunięte z tabeli.

Po zatwierdzeniu wiersz pokazuje zatwierdzone offsety w postaci `topic:partition → offset`. W połączeniu prod każde zatwierdzenie wymaga wpisania nazwy połączenia.

Jeśli sesja nie odpowie na czas, aplikacja ostrzega, że nie wiadomo, czy zatwierdzenie zostało zastosowane. Należy wtedy sprawdzić offsety na karcie grupy. Zob. [Szczegóły grupy](groups.md#group-details).

## Wstrzymywanie partycji {#pause-partitions}

Panel na prawo od tabeli zawiera partycje przydzielone sesji w postaci `topic:partition`. Przycisk [[ui:consumer.assignment.pause]] obok partycji wstrzymuje jej odczyt, a przycisk [[ui:consumer.assignment.resume]] go wznawia. Wstrzymana partycja jest oznaczona jako [[ui:consumer.assignment.paused]].

Wstrzymanie nie opuszcza grupy, więc nie ma rebalansu. Partycja pozostaje przydzielona sesji, a inni członkowie nie czytają jej, dopóki jest wstrzymana. Wstrzymanie niczego nie zmienia w klastrze i nie wymaga potwierdzenia.

Po rebalansie nowo przydzielone partycje nie są wstrzymane. Gdy sesja opuszcza grupę, wszystkie wstrzymania się kończą.

## Dziennik rebalansów {#rebalance-log}

[[ui:consumer.assignment.logTitle]] pod listą partycji rejestruje każdą zmianę przydziału, od najnowszych. Każdy wpis ma czas i jeden z trzech rodzajów: partycje przydzielone sesji, partycje jej odebrane lub błąd rebalansu z jego treścią.

Rebalans następuje, gdy członek dołącza do grupy lub ją opuszcza, gdy zmieniają się subskrybowane topiki lub gdy członek przestaje odpowiadać na czas. Aby go zaobserwować, należy uruchomić drugą sesję z tym samym `group.id` w innej karcie: partycje zostaną podzielone między dwie sesje i oba dzienniki to pokażą.

Dziennik przechowuje ostatnie 200 wpisów. Przed pierwszym rebalansem pokazuje komunikat [[ui:consumer.assignment.logEmpty]].

## Ograniczenia {#limits}

- Sesje wymagają Pro. Jeśli Pro przestanie być aktywne w trakcie działania sesji, sesja nadal czyta, ale zatwierdzanie, wstrzymywanie i wznawianie są odrzucane, a nowej sesji nie można uruchomić.
- Sesje nie są dostępne w połączeniach z włączoną opcją [[ui:common.readOnly]].
- Sesji nie można uruchomić od wybranego offsetu. Aby ustawić początek, należy najpierw zresetować offsety grupy: zob. [Reset offsetów](groups.md#reset-offsets). Reset wymaga grupy bez członków, więc przed resetem trzeba rozłączyć sesje tej grupy.
- Tabela przechowuje maksymalnie 100000 wiadomości. Starsze wiadomości są usuwane, ale nadal liczą się jako odczytane.
- Bardzo duże klucze i wartości są wyświetlane tylko częściowo, tak jak przy przeglądaniu.
- Nie należy dołączać do grupy działającej aplikacji bez wyraźnej potrzeby. Sesja zabiera partycje aplikacji, a aplikacja ich nie przetwarza, dopóki trzyma je sesja. Zatwierdzenia z sesji, w tym automatyczne, przesuwają offsety aplikacji.
