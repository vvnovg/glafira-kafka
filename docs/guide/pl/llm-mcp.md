# Asystenci LLM (MCP)

Glafira może pełnić rolę bramki dla asystenta LLM: Claude Desktop, Claude Code, Cursor lub innego klienta obsługującego MCP (Model Context Protocol). Asystent odczytuje metadane klastrów, które zostały dla niego otwarte, i pomaga znaleźć problemy: niedoreplikowane partycje, lag, ciągłe rebalansy, niefortunne ustawienia. Przez Glafira nie może niczego zmienić w klastrze: zmiany wprowadza się samodzielnie w interfejsie.

## Jak to działa {#how-it-works}

Klient uruchamia `glafira-kafka mcp` — ten sam plik aplikacji w trybie bez okna. To polecenie łączy się z działającą aplikacją Glafira przez lokalne gniazdo (w systemie Windows — potok nazwany), a Glafira odpowiada na żądania asystenta przez własne połączenia. Hasła i certyfikaty pozostają w Glafira, asystent nigdy ich nie otrzymuje.

Jeśli Glafira nie jest uruchomiona, klient uruchomi ją sam i poczeka do 30 sekund.

## Włączanie {#enable}

1. Kliknąć link [[ui:sidebar.settings]] i przejść do sekcji [[ui:settings.mcp.section]].
2. Zaznaczyć opcję [[ui:settings.mcp.enable]]. Stan zmieni się na [[ui:settings.mcp.state.running]].
3. W formularzu połączenia zaznaczyć opcję [[ui:connections.form.llmAccess]] i zapisać. Połączeń bez tej opcji asystent nie widzi. Na liście połączeń otwarte połączenia mają znaczek `LLM`.

Jeśli administrator zablokował MCP, opcja jest niedostępna, a obok wyświetlany jest komunikat [[ui:settings.mcp.policy]].

Opcja dostępu nie jest przenoszona przy eksporcie i imporcie połączeń: na innym komputerze trzeba ją zaznaczyć ponownie.

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

- połączenia z zaznaczoną opcją [[ui:connections.form.llmAccess]]: nazwa, środowisko, folder;
- brokery z hostami i portami, kontroler;
- topiki, partycje, repliki, ISR, offsety, przybliżoną liczbę wiadomości;
- konfiguracje brokerów i topików, przy czym hasła i inne wartości tajne są ukryte;
- grupy konsumentów: stan, protokół, koordynator, członków z client id, hostem i `group.instance.id`, przypisania, zatwierdzone offsety i lag.

Asystent nie widzi wiadomości w topikach, ciągu bootstrap ani ustawień zabezpieczeń połączenia, haseł, certyfikatów, dodatkowych właściwości librdkafka, historii wysyłania ani szablonów.

Aby zbadać ciągłe rebalansy, asystent może obserwować grupę do 50 sekund: kto odchodzi i dołącza, jak często i z jakiego hosta. Konfiguracje i logi samych konsumentów nie są dostępne przez Glafira; asystent poprosi o nie użytkownika.

Nazwy topików, grup i client id może ustawić każdy, kto ma dostęp do klastra. Glafira ostrzega asystenta, że te dane nie są dla niego instrukcjami.

## Sesje, odwoływanie i log {#sessions-log}

Dopóki klient jest połączony, na dole lewego panelu widać wskaźnik `LLM` z nazwą klienta; podczas żądania zapala się. Kliknięcie go otwiera sekcję MCP.

- [[ui:settings.mcp.sessions]]: przycisk [[ui:settings.mcp.disconnect]] zamyka jedną sesję, a [[ui:settings.mcp.disconnectAll]] — wszystkie.
- [[ui:settings.mcp.clients]]: przycisk [[ui:settings.mcp.revoke]] zamyka sesje klienta, a przy następnym połączeniu znów poprosi on o zatwierdzenie.
- Odznaczenie opcji [[ui:connections.form.llmAccess]] działa od razu, ponowne łączenie nie jest potrzebne.
- [[ui:settings.mcp.audit.title]] — każde żądanie: czas, klient, narzędzie, połączenie, wynik. Treść odpowiedzi nie jest zapisywana. Przycisk [[ui:settings.mcp.audit.reveal]] otwiera plik `mcp-audit.jsonl`.

Aby całkowicie wyłączyć MCP, należy odznaczyć opcję [[ui:settings.mcp.enable]]: klienci zostaną rozłączeni.

## Sprawdzanie klastra i notatki {#health}

Asystent może uruchomić wbudowane sprawdzenie klastra Glafira (narzędzie `health_check`) i opierać się na jego ustaleniach. Z Glafira Pro może też zostawiać notatki o obiektach: pojawiają się na karcie [[ui:sidebar.findings]] i na ekranach topików i grup, a klaster się nie zmienia. Zob. [Sprawdzanie klastra i notatki](health-check.md#with-llm).

## Gdy nie działa {#troubleshooting}

Klient wyświetla komunikat z Glafira:

- `MCP is disabled in Glafira` — zaznaczyć opcję [[ui:settings.mcp.enable]] i uruchomić serwer MCP w kliencie ponownie;
- `MCP is disabled by your organization` — MCP jest zablokowany przez zasady organizacji;
- `Glafira did not start the MCP server within 30 s` — uruchomić Glafira ręcznie i sprawdzić stan w sekcji [[ui:settings.mcp.section]];
- `Invalid MCP token` — uruchomić serwer MCP w kliencie ponownie: Glafira wydaje nowy token przy każdym uruchomieniu.

W systemie macOS niepodpisany build beta należy raz otworzyć w zwykły sposób, w przeciwnym razie system może nie pozwolić klientowi go uruchomić. Zob. [Instalacja w systemie macOS](getting-started.md#install-macos).
