# Licencja i aktualizacje

W tym rozdziale opisano, co daje Pro, jak aktywować klucz licencyjny, co się dzieje po wygaśnięciu licencji, jak aktualizować aplikację, ekran [[ui:sidebar.about]] i archiwum diagnostyczne.

## Free i Pro {#free-and-pro}

Free obejmuje codzienną pracę z Kafką: wszystkie protokoły zabezpieczeń, w tym mTLS, przegląd klastra i topiki, przeglądanie wiadomości, ekran Producer z wysyłką wsadową, grupy konsumentów i reset offsetów. Free obejmuje też wszystkie języki interfejsu. Zob. [Język interfejsu](getting-started.md#language).

Pro dodaje:

- sesje konsumenta: odczyt jako członek grupy, zatwierdzanie offsetów, wstrzymywanie partycji. Zob. [Sesje konsumenta](consumer-sessions.md#what-is);
- tryb prod: tworzenie połączeń prod, przełączanie połączeń na prod i wyłączanie opcji [[ui:common.readOnly]] w połączeniu prod. Zob. [Środowiska i prod](connections.md#environments).

W Free te funkcje są widoczne, ale niedostępne. Polecenia sesji konsumenta są nieaktywne i oznaczone kłódką oraz napisem `Pro`. Opcja prod jest wyświetlana jako [[ui:connections.form.prodPro]]. Jeśli działanie wymaga Pro, błąd brzmi [[ui:errors.license.proRequired]] i proponuje przycisk [[ui:common.openLicense]].

Ekran [[ui:tabs.license]] wymienia sesje konsumenta i tryb prod w sekcji [[ui:license.whatProGives]].

## Aktywacja klucza {#activate}

Klucz licencyjny to wiersz tekstu wydany osobie lub organizacji. Aplikacja sprawdza klucz na komputerze użytkownika i nie łączy się z żadnym serwerem.

1. Kliknąć link [[ui:tabs.license]] na dole lewego panelu.
2. Wkleić cały klucz w pole [[ui:license.keyLabel]].
3. Kliknąć przycisk [[ui:license.activate]].

Funkcje Pro stają się dostępne od razu. Ekran pokazuje pozycje [[ui:license.tier]] (Pro), [[ui:license.licensee]], [[ui:license.kind]] ( [[ui:license.kindPersonal]] lub [[ui:license.kindOrganization]] ) i [[ui:license.expires]]: datę lub wartość [[ui:license.perpetual]]. Klucz jest zapisywany w folderze danych aplikacji, więc pozostaje po ponownym uruchomieniu lub aktualizacji. Nowy klucz zastępuje poprzedni.

Jeśli klucz nie zostanie przyjęty, pod polem wyświetlana jest przyczyna:

- [[ui:errors.license.malformed]] — część klucza jest niekompletna lub zmieniona. Należy ponownie skopiować klucz jako jeden wiersz, bez dodatkowych znaków.
- [[ui:errors.license.badSignature]] — klucz nie został wydany dla Glafiry lub był edytowany.
- Klucz wygasł — komunikat podaje datę jego wygaśnięcia. Należy poprosić o nowy klucz.
- Klucz wymaga nowszej wersji aplikacji — komunikat podaje wersję klucza. Najpierw trzeba zaktualizować aplikację, zob. [Aktualizacje](license-updates.md#updates).

Aby usunąć klucz, należy kliknąć przycisk [[ui:license.removeKey]]. Aplikacja pyta: [[ui:license.removeQuestion]] Usunięcie potwierdza się przyciskiem [[ui:license.removeYes]]. Dane użytkownika zostają zachowane.

Organizacja może aktywować Pro dla wszystkich użytkowników za pomocą zasad. Pole klucza i przyciski są wtedy ukryte, a ekran informuje: [[ui:license.managed]] Klucz wpisany samodzielnie nie jest używany, dopóki ustawiony jest klucz z zasad. Sekcja [[ui:license.policyTitle]] pokazuje, skąd pochodzą zasady, i wymienia ich błędy. Jeśli klucz organizacji jest nieprawidłowy, przyczyna jest wyświetlana na ekranie [[ui:tabs.license]]. W takim przypadku należy skontaktować się z administratorami. Administratorzy znajdą ustawienia zasad w pliku `docs/admin.md` w kodzie źródłowym Glafiry.

## Wygaśnięcie {#expiry}

Klucz może mieć datę wygaśnięcia, widoczną w pozycji [[ui:license.expires]]. Licencja jest ważna do końca tego dnia.

Gdy zostaje mniej niż 14 dni, powiadomienie nad kartami informuje, ile dni pozostało, a ekran [[ui:tabs.license]] pokazuje datę wygaśnięcia. Przycisk [[ui:license.notice.more]] w powiadomieniu otwiera ekran [[ui:tabs.license]]. Przycisk `×` ukrywa powiadomienie do ponownego uruchomienia aplikacji.

Po dacie wygaśnięcia plan przełącza się na Free. Powiadomienie i ekran [[ui:tabs.license]] informują, że licencja wygasła, a funkcje Pro są wyłączone. Co się zmienia:

- Nie można uruchamiać nowych sesji konsumenta. W już działającej sesji zatwierdzanie i wstrzymywanie partycji są odrzucane z błędem, który podaje datę wygaśnięcia.
- Połączenia prod nadal działają z tymi samymi potwierdzeniami. Nie można tworzyć nowych połączeń prod ani wyłączyć opcji [[ui:common.readOnly]] w połączeniu prod.

Połączenia, hasła, szablony i historia wysyłania są zachowywane. Aby dalej korzystać z Pro, należy aktywować nowy klucz zgodnie z opisem w sekcji [Aktywacja klucza](license-updates.md#activate).

## Aktualizacje {#updates}

Przy uruchomieniu aplikacja jednorazowo sprawdza, czy jest dostępna nowa wersja. Jeśli tak, powiadomienie nad kartami podaje jej numer. Przycisk [[ui:license.notice.more]] otwiera ekran [[ui:sidebar.about]]. Jeśli sprawdzenie się nie powiedzie, na przykład bez dostępu do internetu, żaden komunikat nie jest wyświetlany.

Aby sprawdzić ręcznie:

1. Otworzyć ekran [[ui:sidebar.about]] na dole lewego panelu.
2. W sekcji [[ui:about.updates.title]] kliknąć przycisk [[ui:about.updates.check]].
3. Jeśli jest dostępna nowa wersja, ekran pokazuje jej numer i informacje o wydaniu. Kliknąć przycisk [[ui:about.updates.install]].

Aplikacja pobiera aktualizację, sprawdza jej podpis, instaluje ją i uruchamia się ponownie. Pobieranie przerwane przez problem z siecią jest automatycznie ponawiane, łącznie do trzech prób. Jeśli wszystkie trzy próby się nie powiodą, aplikacja zgłasza błąd [[ui:errors.updates.downloadInterrupted]]. Jeśli weryfikacja podpisu się nie powiedzie, aplikacja zgłasza błąd [[ui:errors.updates.badSignature]].

Instalacja może wymagać hasła administratora, ponieważ aplikacja jest zainstalowana dla wszystkich użytkowników komputera: instalatorem `.pkg` w systemie macOS, instalatorem `.msi` w systemie Windows lub pakietem `.deb` albo `.rpm` w systemie Linux. Użytkownicy bez uprawnień administratora nie mogą aktualizować takiej instalacji.

Jeśli trwa odczyt wiadomości, działa sesja konsumenta lub trwa wysyłka wsadowa, aplikacja najpierw pyta o zgodę i podaje, ile sesji jest aktywnych. Przycisk [[ui:about.updates.stopAndUpdate]] zatrzymuje je i kontynuuje, a przycisk [[ui:common.cancel]] rezygnuje z aktualizacji. Po aktualizacji połączenia, ustawienia, klucz licencyjny i inne dane są zachowywane.

Aktualizacje są pobierane ze strony wydań w GitHub. Jeśli sprawdzenie kończy się błędem [[ui:errors.updates.checkFailed]], należy sprawdzić sieć lub serwer proxy. Komunikat [[ui:errors.updates.notConfigured]] oznacza, że ten build został przygotowany bez klucza aktualizacji, na przykład jako build deweloperski lub testowy. W takim przypadku nowe wersje należy pobierać ze strony wydań: https://github.com/vvnovg/glafira-kafka-releases/releases

Organizacja może za pomocą zasad wyłączyć sprawdzanie aktualizacji lub skierować je do wewnętrznego serwera lustrzanego. Gdy sprawdzanie jest wyłączone, po kliknięciu przycisku [[ui:about.updates.check]] sekcja [[ui:about.updates.title]] pokazuje komunikat [[ui:errors.updates.managed]]. Podobnie jak [[ui:errors.updates.notConfigured]], ten komunikat pojawia się dopiero po kliknięciu; sprawdzenie przy uruchomieniu w tych przypadkach niczego nie pokazuje.

## O programie {#about}

Ekran [[ui:sidebar.about]] otwiera się linkiem na dole lewego panelu. W systemie macOS jest też dostępny w menu Pomoc jako [[ui:menu.about]].

Ekran pokazuje krótki opis aplikacji oraz:

- [[ui:about.version]] — numer wersji, oraz [[ui:about.build]] — identyfikator kodu źródłowego, z którego zbudowano aplikację. Oba należy podać w zgłoszeniu do pomocy technicznej.
- [[ui:about.system]] — system operacyjny i architektura procesora.
- [[ui:about.tier]] — Free lub Pro.
- [[ui:about.policy]] — [[ui:about.policySet]] lub [[ui:about.policyNone]]. Błędy w zasadach są wymienione poniżej.
- [[ui:about.dataDir]] — folder z profilami połączeń, szablonami, historią, ustawieniami i kluczem licencyjnym.
- [[ui:about.logDir]] — folder z plikami logów. Zob. [Logi](troubleshooting.md#logs).

Na tym ekranie znajdują się też sekcje [[ui:about.updates.title]] i [[ui:about.diagnostics.title]].

## Archiwum diagnostyczne {#diagnostics}

Archiwum diagnostyczne zbiera to, czego pomoc techniczna potrzebuje, aby zrozumieć problem. Aby je utworzyć:

1. Otworzyć ekran [[ui:sidebar.about]].
2. W sekcji [[ui:about.diagnostics.title]] kliknąć przycisk [[ui:about.diagnostics.collect]]. W systemie macOS można też wybrać polecenie [[ui:menu.diagnostics]] w menu Pomoc.
3. Wybrać miejsce zapisu pliku. Proponowana nazwa to `diagnostics-<date>.zip`.

Po zapisaniu archiwum ekran pokazuje jego ścieżkę. Plik ZIP zawiera:

- `info.json` — wersję i build aplikacji, system operacyjny, ścieżki folderów danych i logów, stan zasad i stan licencji (plan, licencjobiorca, typ, data wygaśnięcia);
- `connections.json` — profile połączeń: nazwy, środowiska, serwery bootstrap, protokoły, nazwy użytkowników SASL, ścieżki plików certyfikatów, dodatkowe właściwości i limity czasu;
- `logs/` — pliki logów aplikacji.

Archiwum nie zawiera haseł: są one przechowywane w systemowym magazynie sekretów, a dodatkowe właściwości z hasłami i sekretami są odrzucane przy zapisie połączenia. Nie są do niego dołączane również klucz licencyjny i pliki certyfikatów. Nazwy hostów, nazwy użytkowników i ścieżki plików są zapisane otwartym tekstem, a logi zawierają adresy brokerów z komunikatów błędów Kafki. Przed wysłaniem archiwum poza firmę warto je przejrzeć.

Aplikacja sama niczego nie wysyła. Archiwum należy samodzielnie dołączyć do zgłoszenia do pomocy technicznej, zob. [Kontakt z pomocą techniczną](troubleshooting.md#support).
