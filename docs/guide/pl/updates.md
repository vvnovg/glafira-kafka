# Aktualizacje i diagnostyka

W tym rozdziale opisano, jak aktualizować aplikację, ekran [[ui:sidebar.about]], archiwum diagnostyczne i jak wesprzeć projekt.

## Aktualizacje {#updates}

Przy uruchomieniu aplikacja jednorazowo sprawdza, czy jest dostępna nowa wersja. Jeśli tak, powiadomienie nad kartami podaje jej numer. Przycisk [[ui:about.notice.more]] otwiera ekran [[ui:sidebar.about]]. Jeśli sprawdzenie się nie powiedzie, na przykład bez dostępu do internetu, żaden komunikat nie jest wyświetlany.

Aby sprawdzić ręcznie:

1. Otworzyć ekran [[ui:sidebar.about]] na dole lewego panelu.
2. W sekcji [[ui:about.updates.title]] kliknąć przycisk [[ui:about.updates.check]].
3. Jeśli jest dostępna nowa wersja, ekran pokazuje jej numer i informacje o wydaniu. Kliknąć przycisk [[ui:about.updates.install]].

Aplikacja pobiera aktualizację, sprawdza jej podpis, instaluje ją i uruchamia się ponownie. Pobieranie przerwane przez problem z siecią jest automatycznie ponawiane, łącznie do trzech prób. Jeśli wszystkie trzy próby się nie powiodą, aplikacja zgłasza błąd [[ui:errors.updates.downloadInterrupted]]. Jeśli weryfikacja podpisu się nie powiedzie, aplikacja zgłasza błąd [[ui:errors.updates.badSignature]].

Instalacja może wymagać hasła administratora, ponieważ aplikacja jest zainstalowana dla wszystkich użytkowników komputera: instalatorem `.pkg` w systemie macOS, instalatorem `.msi` w systemie Windows lub pakietem `.deb` albo `.rpm` w systemie Linux. Użytkownicy bez uprawnień administratora nie mogą aktualizować takiej instalacji.

Jeśli trwa odczyt wiadomości, działa sesja konsumenta lub trwa wysyłka wsadowa, aplikacja najpierw pyta o zgodę i podaje, ile sesji jest aktywnych. Przycisk [[ui:about.updates.stopAndUpdate]] zatrzymuje je i kontynuuje, a przycisk [[ui:common.cancel]] rezygnuje z aktualizacji. Po aktualizacji połączenia, ustawienia i inne dane są zachowywane.

Aktualizacje są pobierane ze strony wydań w GitHub. Jeśli sprawdzenie kończy się błędem [[ui:errors.updates.checkFailed]], należy sprawdzić sieć lub serwer proxy. Komunikat [[ui:errors.updates.notConfigured]] oznacza, że ten build został przygotowany bez klucza aktualizacji, na przykład jako build deweloperski lub testowy. W takim przypadku nowe wersje należy pobierać ze strony wydań: https://github.com/vvnovg/glafira-kafka-releases/releases

Organizacja może za pomocą zasad wyłączyć sprawdzanie aktualizacji lub skierować je do wewnętrznego serwera lustrzanego. Gdy sprawdzanie jest wyłączone, po kliknięciu przycisku [[ui:about.updates.check]] sekcja [[ui:about.updates.title]] pokazuje komunikat [[ui:errors.updates.managed]]. Podobnie jak [[ui:errors.updates.notConfigured]], ten komunikat pojawia się dopiero po kliknięciu; sprawdzenie przy uruchomieniu w tych przypadkach niczego nie pokazuje.

## O programie {#about}

Ekran [[ui:sidebar.about]] otwiera się linkiem na dole lewego panelu. W systemie macOS jest też dostępny w menu Pomoc jako [[ui:menu.about]].

Ekran pokazuje krótki opis aplikacji oraz:

- [[ui:about.version]] — numer wersji, oraz [[ui:about.build]] — identyfikator kodu źródłowego, z którego zbudowano aplikację. Oba należy podać w zgłoszeniu do pomocy technicznej.
- [[ui:about.system]] — system operacyjny i architektura procesora.
- [[ui:about.license]] — Apache 2.0; link otwiera tekst licencji w przeglądarce.
- [[ui:about.policy]] — [[ui:about.policySet]] lub [[ui:about.policyNone]]. Błędy w zasadach są wymienione poniżej.
- [[ui:about.dataDir]] — folder z profilami połączeń, szablonami, historią, ustawieniami.
- [[ui:about.logDir]] — folder z plikami logów. Zob. [Logi](troubleshooting.md#logs).

Na tym ekranie znajdują się też sekcje [[ui:about.updates.title]] i [[ui:about.diagnostics.title]].

## Archiwum diagnostyczne {#diagnostics}

Archiwum diagnostyczne zbiera to, czego pomoc techniczna potrzebuje, aby zrozumieć problem. Aby je utworzyć:

1. Otworzyć ekran [[ui:sidebar.about]].
2. W sekcji [[ui:about.diagnostics.title]] kliknąć przycisk [[ui:about.diagnostics.collect]]. W systemie macOS można też wybrać polecenie [[ui:menu.diagnostics]] w menu Pomoc.
3. Wybrać miejsce zapisu pliku. Proponowana nazwa to `diagnostics-<date>.zip`.

Po zapisaniu archiwum ekran pokazuje jego ścieżkę. Plik ZIP zawiera:

- `info.json` — wersję i build aplikacji, system operacyjny, ścieżki folderów danych i logów, stan zasad;
- `connections.json` — profile połączeń: nazwy, środowiska, serwery bootstrap, protokoły, nazwy użytkowników SASL, ścieżki plików certyfikatów, dodatkowe właściwości i limity czasu;
- `logs/` — pliki logów aplikacji.

Archiwum nie zawiera haseł: są one przechowywane w systemowym magazynie sekretów, a dodatkowe właściwości z hasłami i sekretami są odrzucane przy zapisie połączenia. Nie są do niego dołączane również pliki certyfikatów. Nazwy hostów, nazwy użytkowników i ścieżki plików są zapisane otwartym tekstem, a logi zawierają adresy brokerów z komunikatów błędów Kafki. Przed wysłaniem archiwum poza firmę warto je przejrzeć.

Aplikacja sama niczego nie wysyła. Archiwum należy samodzielnie dołączyć do zgłoszenia do pomocy technicznej, zob. [Kontakt z pomocą techniczną](troubleshooting.md#support).

## Wesprzyj projekt {#support}

Glafira jest bezpłatna i otwarta na licencji Apache License 2.0. Darowizny pozwalają ją rozwijać. Przycisk [[ui:support.title]] u dołu lewego panelu, na ekranie [[ui:sidebar.about]] oraz, w systemie macOS, w menu Pomoc otwiera stronę wsparcia w przeglądarce: https://github.com/vvnovg/glafira-kafka-releases/blob/main/SUPPORT.md
