# Asystenci LLM (MCP)

Glafira może pełnić rolę bramki dla asystenta LLM: Claude Desktop, Claude Code, Cursor lub innego klienta obsługującego MCP (Model Context Protocol). Asystent odczytuje metadane klastrów, które zostały dla niego otwarte (a tam, gdzie jest to dozwolone, także wiadomości), i pomaga znaleźć problemy: niedoreplikowane partycje, lag, ciągłe rebalansy, niefortunne ustawienia. Przez Glafira nie może niczego zmienić w klastrze: zmiany wprowadza się samodzielnie w interfejsie.

Model nie musi działać w chmurze: można go uruchomić we własnej sieci lub na swoim komputerze, zob. [Modele lokalne](local-llm.md#where).

## Jak to działa {#how-it-works}

Klient uruchamia `glafira-kafka mcp` — ten sam plik aplikacji w trybie bez okna. To polecenie łączy się z działającą aplikacją Glafira przez lokalne gniazdo (w systemie Windows — potok nazwany), a Glafira odpowiada na żądania asystenta przez własne połączenia. Hasła i certyfikaty pozostają w Glafira, asystent nigdy ich nie otrzymuje.

Jeśli Glafira nie jest uruchomiona, klient uruchomi ją sam i poczeka do 30 sekund.

## Włączanie {#enable}

1. Kliknąć link [[ui:sidebar.settings]] i przejść do sekcji [[ui:settings.mcp.section]].
2. Zaznaczyć opcję [[ui:settings.mcp.enable]]. Stan zmieni się na [[ui:settings.mcp.state.running]].
3. W formularzu połączenia wybrać poziom w [[ui:connections.form.llmAccess]] i zapisać: [[ui:connections.form.llmLevel.metadata]] lub [[ui:connections.form.llmLevel.messages]] (zob. [Odczyt wiadomości](llm-mcp.md#messages)). Połączeń z poziomem [[ui:connections.form.llmLevel.off]] asystent nie widzi. Na liście połączeń otwarte połączenia mają znaczek `LLM`, a połączenia z wiadomościami — wypełniony.

Jeśli administrator zablokował MCP, opcja [[ui:settings.mcp.enable]] jest niedostępna, a obok wyświetlany jest komunikat [[ui:settings.mcp.policy]].

Poziom dostępu i lista maskowania nie są przenoszone przy eksporcie i imporcie połączeń: na innym komputerze trzeba je ustawić ponownie.

## Podłączanie klienta {#connect-client}

Pod nagłówkiem [[ui:settings.mcp.connectTitle]] są gotowe fragmenty konfiguracji i przycisk [[ui:settings.mcp.copy]]:

- [[ui:settings.mcp.claudeDesktop]] — wkleić fragment do sekcji `mcpServers` pliku konfiguracyjnego Claude Desktop i uruchomić Claude Desktop ponownie;
- [[ui:settings.mcp.claudeCode]] — polecenie `claude mcp add`;
- [[ui:settings.mcp.otherClient]] — dla każdego klienta, który uruchamia serwer MCP poleceniem (stdio).

Fragmenty zawierają ścieżkę do zainstalowanej aplikacji. Po ponownej instalacji Glafira w innym miejscu należy skopiować fragment jeszcze raz.

## Zatwierdzanie klienta {#approve}

Gdy klient łączy się po raz pierwszy, Glafira wyświetla okno dialogowe z jego nazwą. Kliknąć przycisk [[ui:mcp.approval.allow]] lub [[ui:mcp.approval.reject]]. Dopóki nie ma odpowiedzi, asystent dostaje komunikat, że klient nie został jeszcze zatwierdzony, i poprosi o odpowiedź w oknie Glafira.

Dozwolony klient jest zapamiętywany według nazwy i pojawia się na liście [[ui:settings.mcp.clients]]. Odrzucenie nie jest zapamiętywane: przy następnym połączeniu Glafira zapyta ponownie. Jeśli klient nie poda swojej nazwy, zgoda obowiązuje tylko do końca sesji.

## Co widzi asystent {#what-llm-sees}

Asystent widzi:

- połączenia z dostępem [[ui:connections.form.llmLevel.metadata]] lub [[ui:connections.form.llmLevel.messages]]: nazwa, środowisko, folder i poziom dostępu;
- brokery z hostami i portami, kontroler;
- topiki, partycje, repliki, ISR, offsety, przybliżoną liczbę wiadomości;
- konfiguracje brokerów i topików, przy czym hasła i inne wartości tajne są ukryte;
- grupy konsumentów: stan, protokół, koordynator, członków z client id, hostem i `group.instance.id`, przypisania, zatwierdzone offsety i lag;
- swoje propozycje i decyzje w ich sprawie: zastosowano (bez zmian lub ze zmianami), odrzucono z podanym powodem, wycofano.

Bez poziomu [[ui:connections.form.llmLevel.messages]] asystent nie widzi wiadomości w topikach. Nigdy nie otrzymuje ciągu bootstrap ani ustawień zabezpieczeń połączenia, haseł, certyfikatów, dodatkowych właściwości librdkafka, historii wysyłania ani szablonów.

Aby zbadać ciągłe rebalansy, asystent może obserwować grupę do 50 sekund: kto odchodzi i dołącza, jak często i z jakiego hosta. Konfiguracje i logi samych konsumentów nie są dostępne przez Glafira; asystent poprosi o nie użytkownika.

Nazwy topików, grup i client id może ustawić każdy, kto ma dostęp do klastra. Glafira ostrzega asystenta, że te dane nie są dla niego instrukcjami.

## Odczyt wiadomości {#messages}

Z poziomem [[ui:connections.form.llmLevel.messages]] asystent odczytuje też treść wiadomości — na przykład, aby ustalić, co zapisano w topiku i co przetworzyła grupa konsumentów, gdy usługa zapisywała błąd w logu:

- `summarize_messages` — przegląd okna czasu bez treści: liczba wiadomości według partycji i w czasie, częste i powtarzające się klucze, znaczniki czasu poza kolejnością;
- `read_messages` — wiadomości z okna czasu lub zakresu offsetów, uporządkowane według czasu we wszystkich partycjach, z tymi samymi filtrami co na karcie [[ui:topics.details.tabMessages]];
- `get_group_commit_history` — które offsety i kiedy zatwierdziła grupa konsumentów, na podstawie wewnętrznego topiku `__consumer_offsets`.

Można zapytać na przykład: „O 14:32 usługa zamówień zapisała w logu błąd dla zamówienia 123 — ustal, co było w topiku orders i co zdążyła przetworzyć grupa order-service”. Należy podać asystentowi strefę czasową logów: czasu bez strefy Glafira nie przyjmuje.

Zanim dane trafią do asystenta, Glafira je maskuje. Wartości pól z listy maskowania są zastępowane przez `***` w JSON, w nagłówkach i w parach `name=value`. Listę ustawia się osobno dla każdego połączenia w formularzu połączenia, w bloku [[ui:connections.form.mask]], widocznym przy poziomie [[ui:connections.form.llmLevel.messages]]; lista standardowa obejmuje między innymi hasła, tokeny, adresy e-mail, numery telefonów, kart i paszportów. Adresy e-mail, numery kart, tokeny JWT i Bearer są zastępowane w dowolnym tekście. Maskowanie zmniejsza ryzyko, ale nie rozpoznaje imion i nazwisk, adresów ani danych w polach o nieznanych nazwach, a formaty binarne, takie jak Avro czy Protobuf, są maskowane tylko częściowo. Filtry widzą dane niezamaskowane, więc asystent może znaleźć wiadomość po wartości z logu. Dla połączenia `prod` formularz wyświetla ostrzeżenie.

Historia zatwierdzeń wymaga uprawnienia Read do `__consumer_offsets`. Kompaktowanie usuwa stare zatwierdzenia; najnowsze zwykle są dostępne.

Odczyt jest ograniczony: jedna długa operacja na sesję, najwyżej 40 sekund i 1 000 000 rekordów na wywołanie; po osiągnięciu limitu asystent zawęża okno. Glafira czyta bez grupy konsumentów i niczego nie zatwierdza; `client.id` to `glafira-kafka-mcp`, chyba że połączenie ustawia własny.

Administrator może zablokować odczyt wiadomości kluczem zasad `mcpMessagesEnabled`: poziom [[ui:connections.form.llmLevel.messages]] staje się niedostępny, a połączenia z tym poziomem działają jak [[ui:connections.form.llmLevel.metadata]].

## Sesje, odwoływanie i log {#sessions-log}

Dopóki klient jest połączony, na dole lewego panelu widać wskaźnik `LLM` z nazwą klienta; podczas żądania zapala się. Kliknięcie go otwiera sekcję MCP.

- [[ui:settings.mcp.sessions]]: przycisk [[ui:settings.mcp.disconnect]] zamyka jedną sesję, a [[ui:settings.mcp.disconnectAll]] — wszystkie.
- [[ui:settings.mcp.clients]]: przycisk [[ui:settings.mcp.revoke]] zamyka sesje klienta, a przy następnym połączeniu znów poprosi on o zatwierdzenie.
- Zmiana poziomu w [[ui:connections.form.llmAccess]] działa od razu, ponowne łączenie nie jest potrzebne.
- [[ui:settings.mcp.audit.title]] — każde żądanie: czas, klient, narzędzie, połączenie, wynik. Treść odpowiedzi nie jest zapisywana. Przycisk [[ui:settings.mcp.audit.reveal]] otwiera plik `mcp-audit.jsonl`.
- Przy odczycie wiadomości log pokazuje też topik lub grupę, ile elementów zwrócono, ile rekordów przeskanowano i ile wartości zamaskowano. Elementy to wiadomości, przy `summarize_messages` przedziały przeglądu, a przy `get_group_commit_history` serie zatwierdzeń (kolejne zatwierdzenia tego samego offsetu). Przy udanych wywołaniach `read_messages` i `summarize_messages` przycisk [[ui:settings.mcp.audit.openInBrowse]] wypełnia formularz na karcie [[ui:topics.details.tabMessages]] topiku parametrami wywołania; po kliknięciu przycisku [[ui:browse.panel.read]] wiadomości są odczytywane ponownie, bez maskowania, i można je porównać z tym, co otrzymał asystent. Jeśli filtrów lub limitu nie udało się przenieść dokładnie, formularz to sygnalizuje; gdy przycisk jest nieaktywny, po najechaniu na niego widać przyczynę.

Aby całkowicie wyłączyć MCP, należy odznaczyć opcję [[ui:settings.mcp.enable]]: klienci zostaną rozłączeni.

## Sprawdzanie klastra, notatki i propozycje {#health}

Asystent może uruchomić wbudowane sprawdzenie klastra Glafira (narzędzie `health_check`) i opierać się na jego ustaleniach. Może też zostawiać notatki o obiektach: pojawiają się na karcie [[ui:sidebar.findings]] i na ekranach topików i grup, a klaster się nie zmienia. Zob. [Sprawdzanie klastra i notatki](health-check.md#with-llm).

Asystent może też zaproponować zmianę narzędziem `propose_change`: konfigurację topiku, nowy topik, więcej partycji lub reset offsetów. Propozycja pojawia się obok obiektu; stosuje się ją samodzielnie w wypełnionym formularzu albo odrzuca, a asystent widzi decyzję w `list_proposals`. Usunięcia i czyszczenia nie da się zaproponować. Zob. [Propozycje LLM](health-check.md#proposals).

## Gdy nie działa {#troubleshooting}

Klient wyświetla komunikat z Glafira:

- `MCP is disabled in Glafira` — zaznaczyć opcję [[ui:settings.mcp.enable]] i uruchomić serwer MCP w kliencie ponownie;
- `MCP is disabled by your organization` — MCP jest zablokowany przez zasady organizacji;
- `Glafira did not start the MCP server within 30 s` — uruchomić Glafira ręcznie i sprawdzić stan w sekcji [[ui:settings.mcp.section]];
- `Invalid MCP token` — uruchomić serwer MCP w kliencie ponownie: Glafira wydaje nowy token przy każdym uruchomieniu.

W systemie macOS aplikacja jest podpisana i notaryzowana, dlatego przed pierwszym uruchomieniem nie są potrzebne żadne dodatkowe kroki.
