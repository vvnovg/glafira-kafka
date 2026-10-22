# Sprawdzanie klastra i notatki

Glafira sprawdza klaster zestawem wbudowanych reguł i pokazuje znalezione problemy obok obiektów: w drzewie połączeń, na listach topików i grup oraz na ekranach topiku i grupy. Asystent LLM może uzupełnić te ustalenia własnymi notatkami. Sprawdzenie tylko odczytuje klaster i niczego w nim nie zmienia.

## Uruchamianie sprawdzenia {#run}

Należy otworzyć [[ui:sidebar.cluster]] lub [[ui:sidebar.findings]] połączenia i kliknąć przycisk [[ui:health.check]]. Sprawdzenie trwa do 45 sekund: Glafira odczytuje brokery, partycje, konfiguracje topików (do 2000 topików) i grupy konsumentów (do 500). Jeśli topików lub grup jest więcej, czegoś nie udało się odczytać albo upłynęło 45 sekund, wynik jest oznaczony jako [[ui:health.partial]], a ustalenie [[ui:health.ruleNames.check.partial]] pokazuje, czego nie odczytano.

Glafira sama nie odpytuje klastra: sprawdzenie uruchamia się tylko po kliknięciu przycisku albo na prośbę asystenta LLM. Wynik jest przechowywany do następnego sprawdzenia.

## Co jest sprawdzane {#rules}

- Brokery i partycje: brak kontrolera, niedostępne i niedoreplikowane partycje, klaster z jednym brokerem, nierówny rozkład leaderów, leaderzy poza preferowaną repliką.
- Trwałość topików: współczynnik replikacji 1, `min.insync.replicas` większe niż współczynnik replikacji, `min.insync.replicas` równe współczynnikowi replikacji, `min.insync.replicas` równe 1, nieczysty wybór leadera.
- Konfiguracja brokerów: automatyczne tworzenie topików, domyślny współczynnik replikacji 1, domyślne `min.insync.replicas` 1, ważne ustawienia, które różnią się między brokerami.
- Grupy konsumentów: puste grupy z lagiem, grupy w trakcie rebalansu, członkowie bez partycji, lag, który wzrósł od poprzedniego sprawdzenia.

## Waga i środowisko {#severity}

Każde ustalenie ma wagę: [[ui:health.severity.critical]], [[ui:health.severity.warning]] lub [[ui:health.severity.info]]. Zależy ona od środowiska połączenia. Na przykład współczynnik replikacji 1 na prod jest krytyczny, na stage jest ostrzeżeniem, a na dev, test i local nie jest pokazywany. Środowisko ustawia się w formularzu połączenia.

## Karta „Ustalenia” {#tab}

Na karcie [[ui:sidebar.findings]] ustalenia są pogrupowane według reguł; każde wyjaśnia problem i co zrobić. Można je filtrować według kryteriów [[ui:health.filterSeverity]], [[ui:health.filterSource]] i [[ui:health.filterKind]] albo wyszukiwać po nazwie. Nazwa obiektu otwiera jego ekran.

Znaczek z liczbą w drzewie połączeń i na listach topików i grup pokazuje, ile ustaleń i otwartych notatek LLM o wadze [[ui:health.severity.critical]] lub [[ui:health.severity.warning]] dotyczy obiektu. Po najechaniu na znaczek widać ich tytuły.

## Ukrywanie ustaleń {#hide}

Jeśli ustalenie jest oczekiwane dla konkretnego obiektu, na przykład testowego topiku ze współczynnikiem replikacji 1 na prod, należy kliknąć przycisk [[ui:health.hide]]. Ukryte ustalenie nie jest liczone w znaczkach i pozostaje ukryte przy kolejnych sprawdzeniach. Aby zobaczyć ukryte, należy zaznaczyć opcję „Pokaż ukryte”; tam jest też przycisk [[ui:health.restore]].

## Notatki LLM {#notes}

Asystent LLM połączony przez MCP (zob. [Asystenci LLM (MCP)](llm-mcp.md#how-it-works)) może zostawić notatkę o klastrze, brokerze, topiku lub grupie. Notatki pojawiają się w sekcji [[ui:health.notes]] i na ekranach obiektów, podpisane nazwą klienta. Tekst jest pokazywany bez zmian, bez formatowania i linków.

- [[ui:health.resolve]] i [[ui:health.dismiss]] zamykają notatkę; zamknięte są przechowywane w zwiniętej sekcji.
- [[ui:common.delete]] usuwa notatkę, a [[ui:health.deleteAll]] usuwa wszystkie notatki połączenia.
- Notatka jest usuwana automatycznie 7 dni po utworzeniu. Połączenie może mieć najwyżej 200 otwartych notatek.

## Propozycje LLM {#proposals}

Oprócz notatek asystent LLM może zaproponować zmianę: konfigurację topiku, nowy topik, więcej partycji lub reset offsetów pustej grupy konsumentów. Propozycja pojawia się w sekcji [[ui:health.proposals.title]] na karcie [[ui:sidebar.findings]] oraz na ekranie topiku lub grupy — z uzasadnieniem asystenta, opisem zmiany i nazwą klienta. Nic nie jest stosowane samo z siebie, a usunięcia i czyszczenia nie da się zaproponować wcale.

- [[ui:health.proposals.openForm]] otwiera kartę obiektu ze zwykłym, już wypełnionym formularzem: edytorem konfiguracji z porównaniem „było → będzie”, formularzem tworzenia topiku, oknem partycji lub resetem offsetów z podglądem. W razie potrzeby popraw wartości i potwierdź jak zwykle; przy połączeniu prod Glafira poprosi o jego nazwę. Po udanej zmianie propozycja zostaje oznaczona jako zastosowana (ze zmianami, jeśli coś poprawiono), a asystent widzi to w `list_proposals`.
- [[ui:health.proposals.reject]] zamyka propozycję; można podać powód, który zobaczy asystent. [[ui:health.reopen]] przywraca odrzuconą propozycję.
- Przy połączeniu tylko do odczytu propozycja zostaje zachowana, ale [[ui:health.proposals.openForm]] jest niedostępne, dopóki tryb tylko do odczytu nie zostanie wyłączony w formularzu połączenia; asystent też się o tym dowiaduje.
- [[ui:common.delete]] usuwa propozycję, [[ui:health.proposals.deleteAll]] — wszystkie propozycje połączenia. Propozycja jest usuwana automatycznie 7 dni po utworzeniu; połączenie może mieć najwyżej 50 otwartych propozycji.

## Razem z asystentem LLM {#with-llm}

Asystent uruchamia to samo sprawdzenie narzędziem `health_check` i widzi te same ustalenia, w tym ukryte, oznaczone jako ukryte. Jeśli ostatnie sprawdzenie ma mniej niż 5 minut, asystent dostaje jego wynik bez ponownego odpytywania klastra. Można na przykład poprosić: „Sprawdź stan klastra i zostaw notatki o najważniejszych problemach”.
