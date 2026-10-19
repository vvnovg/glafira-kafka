# Sprawdzanie klastra i notatki

Glafira sprawdza klaster zestawem wbudowanych reguł i pokazuje znalezione problemy obok obiektów: w drzewie połączeń, na listach topików i grup oraz na ekranach topiku i grupy. Z Glafira Pro asystent LLM może uzupełnić te ustalenia własnymi notatkami. Sprawdzenie tylko odczytuje klaster i niczego w nim nie zmienia.

## Uruchamianie sprawdzenia {#run}

Otwórz [[ui:sidebar.cluster]] lub [[ui:sidebar.findings]] połączenia i kliknij [[ui:health.check]]. Sprawdzenie trwa do 45 sekund: Glafira odczytuje brokery, partycje, konfiguracje topików (do 2000 topików) i grupy konsumentów (do 500). W dużych klastrach wynik jest oznaczony jako [[ui:health.partial]], a ustalenie [[ui:health.ruleNames.check.partial]] pokazuje, czego nie odczytano.

Glafira sama nie odpytuje klastra: sprawdzenie uruchamia się tylko po kliknięciu przycisku albo na prośbę asystenta LLM. Wynik jest przechowywany do następnego sprawdzenia.

## Co jest sprawdzane {#rules}

- Brokery i partycje: brak kontrolera, niedostępne i niedoreplikowane partycje, klaster z jednym brokerem, nierówny rozkład leaderów, leaderzy poza preferowaną repliką.
- Trwałość topików: współczynnik replikacji 1, `min.insync.replicas` większe lub równe współczynnikowi replikacji albo równe 1, nieczysty wybór leadera.
- Konfiguracja brokerów: automatyczne tworzenie topików, domyślny współczynnik replikacji i `min.insync.replicas`, ważne ustawienia, które różnią się między brokerami.
- Grupy konsumentów: puste grupy z lagiem, grupy w trakcie rebalansu, członkowie bez partycji, lag, który wzrósł od poprzedniego sprawdzenia.

## Waga i środowisko {#severity}

Każde ustalenie ma wagę: [[ui:health.severity.critical]], [[ui:health.severity.warning]] lub [[ui:health.severity.info]]. Zależy ona od środowiska połączenia. Na przykład współczynnik replikacji 1 na prod jest krytyczny, na stage jest ostrzeżeniem, a na dev, test i local nie jest pokazywany. Środowisko ustawia się w formularzu połączenia.

## Karta „Ustalenia” {#tab}

Na karcie [[ui:sidebar.findings]] ustalenia są pogrupowane według reguł; każde wyjaśnia problem i co zrobić. Filtruj je według kryteriów [[ui:health.filterSeverity]], [[ui:health.filterSource]] i [[ui:health.filterKind]] albo szukaj po nazwie. Nazwa obiektu otwiera jego ekran.

Znaczek z liczbą w drzewie połączeń i na listach topików i grup pokazuje, ile krytycznych ustaleń i ostrzeżeń dotyczy obiektu.

## Ukrywanie ustaleń {#hide}

Jeśli ustalenie jest oczekiwane dla konkretnego obiektu, na przykład testowego topiku ze współczynnikiem replikacji 1 na prod, kliknij [[ui:health.hide]]. Ukryte ustalenie nie jest liczone w znaczkach i pozostaje ukryte przy kolejnych sprawdzeniach. Aby zobaczyć ukryte, zaznacz „Pokaż ukryte”; tam znajdziesz [[ui:health.restore]].

## Notatki LLM {#notes}

Z Glafira Pro asystent LLM połączony przez MCP (zobacz [Asystenci LLM (MCP)](llm-mcp.md#how-it-works)) może zostawić notatkę o klastrze, brokerze, topiku lub grupie. Notatki pojawiają się w sekcji [[ui:health.notes]] i na ekranach obiektów, podpisane nazwą klienta. Tekst jest pokazywany bez zmian, bez formatowania i linków.

- [[ui:health.resolve]] i [[ui:health.dismiss]] zamykają notatkę; zamknięte są przechowywane w zwiniętej sekcji.
- [[ui:common.delete]] usuwa notatkę, a [[ui:health.deleteAll]] usuwa wszystkie notatki połączenia.
- Notatka jest usuwana automatycznie 7 dni po utworzeniu. Połączenie może mieć najwyżej 200 otwartych notatek.

Bez Pro asystent może uruchamiać sprawdzenie i czytać ustalenia, ale nie może zostawiać notatek.

## Razem z asystentem LLM {#with-llm}

Asystent uruchamia to samo sprawdzenie narzędziem `health_check` i widzi te same ustalenia, w tym ukryte, oznaczone jako ukryte. Jeśli ostatnie sprawdzenie ma mniej niż 5 minut, asystent dostaje jego wynik bez ponownego odpytywania klastra. Poproś na przykład: „Sprawdź stan klastra i zostaw notatki o najważniejszych problemach”.
