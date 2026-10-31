# Rozwiązywanie problemów

W tym rozdziale wyjaśniono typowe błędy połączenia i to, co sprawdzić w przypadku każdego z nich. Pokazano też, gdzie znajdują się logi i co dołączyć do zgłoszenia do pomocy technicznej. W systemie macOS ten rozdział otwiera polecenie [[ui:menu.troubleshooting]] w menu Pomoc.

Komunikat o błędzie zawiera krótki tekst, a na większości ekranów także kod błędu w nawiasie, na przykład `TIMEOUT`. Po rozwinięciu sekcji [[ui:common.details]] widać oryginalny komunikat klienta Kafki (librdkafka). Często podaje on dokładny adres brokera i przyczynę.

## Nie można się połączyć {#cannot-connect}

Na początek należy użyć przycisku [[ui:connections.form.test]] w formularzu połączenia, zob. [Testowanie połączenia](connections.md#test). Test zatrzymuje się na pierwszym nieudanym kroku, a krok wskazuje, gdzie szukać:

- [[ui:connections.form.step.config]] — problem leży w samym formularzu: adres bez portu, brakujący lub nieczytelny plik certyfikatu, błędne hasło certyfikatu. Błąd wskazuje pole. Pliki certyfikatów opisano w sekcji [Błędy TLS](troubleshooting.md#tls-errors).
- [[ui:connections.form.step.tcp]] — komputer nie może dotrzeć do serwerów bootstrap. Zob. [DNS i TCP](troubleshooting.md#dns-tcp).
- [[ui:connections.form.step.kafka]] — sieć działa, ale handshake TLS, logowanie SASL lub żądanie metadanych nie powiodło się. Zob. [Błędy TLS](troubleshooting.md#tls-errors), [Błędy SASL](troubleshooting.md#sasl-errors) i [Limity czasu](troubleshooting.md#timeouts).

Najczęstszą przyczyną jest protokół, który nie pasuje do portu. Każdy listener brokera ma własny port i protokół. Który port należy do którego protokołu, wiedzą administratorzy Kafki, zob. [Protokoły zabezpieczeń](connections.md#security).

Jeśli test przechodzi, ale odczyt lub wysyłanie się nie udaje, przyczyną są często adresy brokerów. Klient pobiera listę brokerów z serwera bootstrap, a następnie łączy się z każdym brokerem pod adresem, który ten broker ogłasza (`advertised.listeners`). Jeśli są to adresy wewnętrzne, na przykład nazwy hostów Dockera lub Kubernetes, nie są one osiągalne z komputera użytkownika. Adresy są wymienione w tabeli brokerów w przeglądzie klastra, zob. [Przegląd klastra](topics.md#cluster-overview). Należy poprosić administratorów o listener osiągalny z zewnątrz.

## DNS i TCP {#dns-tcp}

Komunikat [[ui:errors.kafka.tcpUnreachable]] oznacza, że aplikacja nie mogła nawiązać połączenia TCP z żadnym adresem z pola [[ui:connections.form.bootstrap]]. Każdy adres ma do 5 sekund. Szczegóły pokazują ostatni sprawdzany adres i przyczynę. `DNS` w szczegółach oznacza, że nie udało się rozwiązać nazwy hosta.

Należy sprawdzić:

- nazwę hosta i port, bez prefiksu protokołu, takiego jak `https://`;
- czy VPN jest włączony, jeśli klaster znajduje się w sieci prywatnej;
- czy zapora nie blokuje portu;
- w przypadku brokera w Dockerze — czy port jest opublikowany na hoście, na przykład `-p 9092:9092`.

Aby sprawdzić port poza aplikacją, można uruchomić `nc -vz <host> <port>` w systemie macOS lub Linux albo `Test-NetConnection <host> -Port <port>` w PowerShell w systemie Windows.

Komunikat [[ui:errors.kafka.connectionFailed]] po pomyślnym kroku TCP zwykle oznacza, że broker zamknął połączenie lub później okazał się nieosiągalny. Warto też sprawdzić adresy brokerów, zob. [Nie można się połączyć](troubleshooting.md#cannot-connect).

## Błędy TLS {#tls-errors}

Błędy w kroku [[ui:connections.form.step.kafka]] podczas handshake'u TLS:

- [[ui:errors.kafka.handshakeClosed]]. Częste przyczyny: port nie obsługuje TLS — należy sprawdzić port i protokół; broker odrzucił certyfikat klienta bez podania przyczyny — należy sprawdzić datę ważności i wystawcę certyfikatu; serwer proxy przed brokerami, np. Istio lub Envoy, nie ma trasy dla tego adresu — należy łączyć się przez nazwę, której używają aplikacje klastra. Dokładną przyczynę można znaleźć w logu brokera.
- [[ui:errors.kafka.caUntrusted]]. W polu [[ui:connections.tls.ca]] należy wskazać plik CA, który podpisał certyfikaty brokerów. Ten plik mogą przekazać administratorzy. Przy pustym polu zaufane są tylko systemowe certyfikaty główne.
- [[ui:errors.kafka.hostnameMismatch]]. Należy łączyć się przez nazwę, dla której wystawiono certyfikat, a nie przez adres IP lub alias. Wyłącznie w środowiskach testowych można wyłączyć opcję [[ui:connections.tls.verifyHostname]].
- [[ui:errors.kafka.clientCertRejected]]. Broker wymaga certyfikatu klienta lub nie ufa używanemu certyfikatowi. Należy sprawdzić, czy opcja [[ui:connections.tls.mtls]] jest włączona i czy certyfikat został wystawiony dla tego klastra. Zob. [Certyfikat klienta (mTLS)](connections.md#mtls).
- [[ui:errors.kafka.tlsError]] — inny problem z TLS. Przyczynę widać po rozwinięciu sekcji [[ui:common.details]].

Jeśli wybrano protokół bez SSL, a mimo to pojawia się błąd TLS, port najprawdopodobniej należy do listenera TLS. Szczegóły zawierają wtedy tekst `connecting to a SSL listener?`. Należy wybrać `SSL` lub `SASL_SSL` albo użyć portu listenera bez TLS.

Problemy z plikami certyfikatów są zgłaszane w kroku [[ui:connections.form.step.config]], zanim aplikacja się połączy. Komunikat zaczyna się od nazwy pola: zaufany CA, certyfikat klienta lub klucz klienta. Typowe przypadki:

- Ścieżka nie jest bezwzględna lub pliku nie można odczytać. Należy wpisać pełną ścieżkę lub wybrać plik przyciskiem [[ui:connections.tls.pick]].
- [[ui:errors.tls.keyEncrypted]], [[ui:errors.tls.keyBadPassword]], [[ui:errors.tls.clientPkcs12BadPassword]] lub [[ui:errors.tls.jksBadStorePassword]] — należy wpisać w formularzu właściwe hasło.
- [[ui:errors.tls.keyMismatch]] — plik klucza należy do innego certyfikatu.
- Certyfikat klienta wygasł lub nie jest jeszcze ważny — komunikat podaje datę. Należy poprosić o nowy certyfikat.
- Magazyn PKCS#12 używa przestarzałego algorytmu lub magazyn jest w formacie JCEKS. Plik należy przekonwertować zgodnie z opisem w sekcji [Formaty certyfikatów](connections.md#certificate-formats).

## Błędy SASL {#sasl-errors}

Komunikat [[ui:errors.kafka.authFailed]] oznacza, że broker nie przyjął logowania SASL. Należy sprawdzić:

- nazwę użytkownika i hasło. Podczas edycji zapisanego połączenia puste pole hasła zachowuje zapisane hasło. W razie wątpliwości co do poprawności hasła należy wpisać je ponownie;
- pole [[ui:connections.form.saslMechanism]]. Musi to być mechanizm, dla którego utworzono użytkownika. Poświadczenia SCRAM są tworzone osobno dla `SCRAM-SHA-256` i `SCRAM-SHA-512`, a użytkownicy `PLAIN` są konfigurowani niezależnie od nich;
- protokół: `SASL_SSL` i `SASL_PLAINTEXT` zwykle używają różnych portów.

[[ui:errors.connections.saslPasswordMissing]] — dla tego połączenia nie zapisano hasła, na przykład po imporcie. Należy otworzyć połączenie, wpisać hasło i zapisać. Zob. [Hasła](connections.md#passwords).

[[ui:errors.secrets.keychain]] — aplikacja nie może odczytać hasła z systemowego magazynu sekretów. W systemie macOS należy zezwolić na dostęp, gdy pyta o to pęk kluczy. W systemie Linux należy sprawdzić, czy w sesji działa GNOME Keyring, KWallet lub inny dostawca Secret Service.

Komunikat [[ui:errors.kafka.notAuthorized]] oznacza coś innego: logowanie się powiodło, ale użytkownik Kafki nie ma uprawnień (ACL) do tej operacji, na przykład do odczytu topiku lub grupy. Należy poprosić administratorów o uprawnienia. Warto pamiętać, że Kafka nie zgłasza błędu dla topików, do których użytkownik nie ma uprawnienia Describe: po prostu nie ma ich na liście topików.

## Limity czasu {#timeouts}

Komunikat [[ui:errors.kafka.timeout]] oznacza, że broker nie odpowiedział na czas. Typowe przyczyny to wolna sieć lub VPN, przeciążony klaster albo nieosiągalne adresy brokerów, zob. [Nie można się połączyć](troubleshooting.md#cannot-connect).

Problem z TLS lub SASL może wyglądać jak przekroczenie limitu czasu. Aplikacja próbuje znaleźć rzeczywistą przyczynę i pokazać ją zamiast tego komunikatu, ale nie zawsze jest to możliwe. Warto rozwinąć sekcję [[ui:common.details]]: komunikaty takie jak `SSL handshake failed` lub `SASL authentication error` wskazują przyczynę.

Jeśli sieć jest po prostu wolna, należy zwiększyć wartość w polu [[ui:connections.form.timeout]] w formularzu połączenia. Wartość domyślna to `30000` ms. Ta wartość dotyczy żądań do brokerów, wysyłania wiadomości i odczytu. Test połączenia używa własnych limitów (5 sekund na każdy adres TCP i 10 sekund na krok Kafka), więc większa wartość nie zmienia testu.

## Działanie jest niedostępne lub odrzucone {#read-only-blocked}

Jeśli przycisk jest ukryty lub nieaktywny albo działanie zostało odrzucone, należy sprawdzić następujące przyczyny:

- Połączenie jest tylko do odczytu. Oznacza je ikona kłódki obok nazwy. Przyciski zmieniające klaster są ukryte lub nieaktywne. Jeśli takie żądanie mimo to dotrze do rdzenia aplikacji, na przykład po włączeniu trybu przy otwartym ekranie, błąd informuje, że połączenie jest tylko do odczytu. Aby wprowadzić zmiany, należy otworzyć połączenie, wyłączyć opcję [[ui:common.readOnly]] i zapisać. Zob. [Tylko do odczytu](connections.md#read-only).
- Połączenie jest typu prod. Zmiany wymagają wpisania nazwy połączenia. Dopóki nazwa się nie zgadza, działanie nie jest wykonywane. Zob. [Środowiska i prod](connections.md#environments).
- Grupa konsumentów ma aktywnych członków. Offsety można zresetować, a grupę usunąć tylko wtedy, gdy nie ma członków. Zob. [Reset offsetów](groups.md#reset-offsets).
- Ustawieniem zarządza organizacja. Zasady mogą ustawić język interfejsu i sprawdzanie aktualizacji, a tych ustawień nie można zmienić w aplikacji. Zob. [Ustawienia określone przez organizację](getting-started.md#managed-settings).
- Kafka odrzuciła operację z komunikatem [[ui:errors.kafka.notAuthorized]]. To kwestia uprawnień w klastrze, a nie ustawień Glafiry. Zob. [Błędy SASL](troubleshooting.md#sasl-errors).

## Logi {#logs}

Aplikacja zapisuje log swojej pracy i błędów klienta Kafki. Dokładny folder jest pokazany na ekranie [[ui:sidebar.about]] w pozycji [[ui:about.logDir]]. Domyślnie:

- macOS: `~/Library/Logs/io.github.vvnovg.glafira.kafka`
- Windows: `%LOCALAPPDATA%\io.github.vvnovg.glafira.kafka\logs`
- Linux: `~/.local/share/io.github.vvnovg.glafira.kafka/logs`

Bieżący plik to `app.log`. Gdy osiągnie 5 MB, zostaje przemianowany na `app.1.log`, a starsze pliki przesuwają się do `app.2.log` i dalej. Aplikacja przechowuje do pięciu plików i usuwa najstarszy.

Hasła nie są zapisywane w logach. Zapisywane są adresy brokerów i treści błędów Kafki. Nie trzeba zbierać plików ręcznie: archiwum diagnostyczne je zawiera, zob. [Archiwum diagnostyczne](updates.md#diagnostics).

## Kontakt z pomocą techniczną {#support}

Przed skontaktowaniem się z pomocą techniczną należy użyć przycisku [[ui:connections.form.test]] i zanotować, który krok się nie powiódł. Następnie trzeba wysłać zgłoszenie do osób, które zapewniają wsparcie dla Glafiry, na przykład do administratorów. Należy dołączyć:

- archiwum diagnostyczne, zob. [Archiwum diagnostyczne](updates.md#diagnostics). Zawiera wersję, build, logi i profile połączeń bez haseł;
- opis krok po kroku: co zostało zrobione, czego oczekiwano i co się stało;
- pełny tekst błędu wraz z zawartością sekcji [[ui:common.details]]. Ten tekst można zaznaczyć i skopiować;
- zrzut ekranu, jeśli problem dotyczy interfejsu;
- czas wystąpienia błędu, aby można go było znaleźć w logach.

Jeśli nie można utworzyć archiwum, należy podać wartości [[ui:about.version]] i [[ui:about.build]] z ekranu [[ui:sidebar.about]] i dołączyć pliki logów z folderu logów.
