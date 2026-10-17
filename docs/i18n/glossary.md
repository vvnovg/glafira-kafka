# Глоссарий переводов Glafira

Единые решения по терминам и стилю для всех словарей `src/locales/*.json`.
Эталон ключей и параметров — `en.json`, эталон смысла — `ru.json`.
Меняя решение здесь, исправьте его во всех словарях одним коммитом.

## Общие правила
- Коротко и нейтрально, как в системных приложениях. Без восклицаний и шуток.
- Параметры `{{…}}` и теги `<code>…</code>`, `<offset/>`, `<total/>`, `<file>`, `<name>` переносятся без изменений,
  их нельзя переводить, склонять или удалять. Порядок внутри фразы можно менять.
- Значение параметра (имя топика, подключения, группы) не склоняется: ставьте перед ним
  существительное в нужном падеже и берите имя в кавычки языка: pl `topik „{{name}}”`, de `Topic „{{name}}“`.
- Многоточие — один символ `…`, не `...`.
- Тире в паузе — `—` с пробелами (во французском тоже), в диапазонах — `–`.
- Единицы: `ms`, `KB`, `MB` как в en; число и единица разделяются неразрывным пробелом там, где он уже есть в en.
- Плюралы: заполняются все формы языка (см. таблицу в README), даже если формы совпадают по тексту.

## Не переводится
Kafka, Glafira, Pro, Free, SASL, SSL, TLS, mTLS, PEM, PKCS#12, JKS, JCEKS, CSV, JSON, JSON Lines, librdkafka,
DNS, TCP, ID, Build (сборка: подпись в «О программе» и слово в тексте об обновлениях — «build», не «версия»/«компиляция»), Leader (лидер партиции); имена параметров Kafka (`cleanup.policy`, `retention.ms`, `acks`, `enable.idempotence`);
команды, пути, адреса (`docker compose -f dev/docker-compose.yml up -d`, `keytool …`, `localhost:9092`);
название экрана «Producer» (как в ru).

## Стиль по языкам

| Язык | Обращение | Кнопки и пункты меню | Вопрос-подтверждение | Кавычки | Особенности |
|---|---|---|---|---|---|
| de | Sie, по возможности без обращения | инфинитив: Speichern, Löschen | «Topic „{{name}}“ löschen?» | „…“ | составные слова через дефис с англ. терминами: Topic-Name, Consumer-Group-ID |
| fr | vous, по возможности без обращения | инфинитив: Enregistrer, Supprimer | «Supprimer le topic « {{name}} » ?» | « … » с U+00A0 внутри | U+00A0 перед `:` `;` `!` `?` и после `«` / перед `»` |
| es | tú, по возможности без обращения | инфинитив: Guardar, Eliminar | «¿Eliminar el topic “{{name}}”?» | “…” | `¿…?`, `¡…!` |
| it | tu, по возможности без обращения | повелительное как в macOS/Windows: Salva, Elimina | «Eliminare il topic “{{name}}”?» | “…” | артикль перед англ. терминами: il topic, l’offset |
| pl | безличные формы, без «Ty/Pan» | повелительное: Zapisz, Usuń | «Usunąć topik „{{name}}”?» | „…” | значения параметров только в именительном после существительного |
| pt-BR | você, по возможности без обращения | инфинитив: Salvar, Excluir | «Excluir o tópico “{{name}}”?» | “…” | бразильская норма: arquivo, tela, excluir (не ficheiro, ecrã, eliminar) |

## Термины

| en | ru | de | fr | es | it | pl | pt-BR |
|---|---|---|---|---|---|---|---|
| topic | топик | Topic (das, Pl. Topics) | topic (m.) | topic (m.) | topic (m., pl. invar.) | topik (m.: topiku, topiki, topików) | tópico (m.) |
| partition | партиция | Partition (die) | partition (f.) | partición (f.) | partizione (f.) | partycja | partição (f.) |
| offset | оффсет | Offset (der, Pl. Offsets) | offset (m.) | offset (m.) | offset (m., pl. invar.) | offset (m.: offsetu, offsety) | offset (m.) |
| consumer group | consumer group | Consumer Group (die) | consumer group (m.) | consumer group (m.) | consumer group (m.) | grupa konsumentów | consumer group (m.) |
| broker | брокер | Broker (der) | broker (m.) | broker (m.) | broker (m.) | broker (m.: brokera) | broker (m.) |
| producer / consumer (роль) | producer / consumer | Producer / Consumer | producteur / consommateur | productor / consumidor | producer / consumer | producent / konsument | produtor / consumidor |
| lag | лаг | Lag (der) | lag (m.) | lag (m.) | lag (m.) | lag (m.: lagu) | lag (m.) |
| message | сообщение | Nachricht | message (m.) | mensaje | messaggio | wiadomość | mensagem |
| key / value | ключ / значение | Schlüssel / Wert | clé / valeur | clave / valor | chiave / valore | klucz / wartość | chave / valor |
| headers | заголовки | Header (Pl.) | en-têtes | encabezados | intestazioni | nagłówki | cabeçalhos |
| connection | подключение | Verbindung | connexion | conexión | connessione | połączenie | conexão |
| prod connection | prod-подключение | Prod-Verbindung | connexion prod | conexión prod | connessione prod | połączenie prod | conexão prod |
| cluster | кластер | Cluster | cluster | clúster | cluster | klaster | cluster |
| bootstrap server | bootstrap-сервер | Bootstrap-Server | serveur bootstrap | servidor bootstrap | server bootstrap | serwer bootstrap | servidor bootstrap |
| consumer session | consumer-сессия | Consumer-Session (die, Pl. Sessions) | session consumer | sesión de consumer | sessione consumer | sesja konsumenta | sessão de consumer |
| read-only | только чтение | Schreibgeschützt | Lecture seule | Solo lectura | Sola lettura | Tylko do odczytu | Somente leitura |
| preset (набор настроек при создании топика) | пресет | Voreinstellung | préréglage | preajuste | profilo | profil (не «szablon» — так названы шаблоны продюсера) | predefinição |
| committed offset | закоммиченный оффсет | committeter Offset | offset commité | offset con commit | offset sottoposto a commit | zatwierdzony offset | offset com commit |
| reset offsets | сбросить оффсеты | Offsets zurücksetzen | Réinitialiser les offsets | Restablecer offsets | Reimposta gli offset | Zresetuj offsety | Redefinir offsets |
| retention | хранение (retention) | Aufbewahrung | rétention | retención | conservazione | retencja | retenção |
| replication factor | фактор репликации | Replikationsfaktor | facteur de réplication | factor de replicación | fattore di replica | współczynnik replikacji | fator de replicação |
| under-replicated | недореплицированные | unterrepliziert | sous-répliquées | subreplicadas | sotto-replicate | niedoreplikowane | sub-replicadas |
| trusted CA | доверенный CA | vertrauenswürdige CA | CA de confiance | CA de confianza | CA attendibile | zaufany CA | CA confiável |
| client certificate / key | сертификат / ключ клиента | Client-Zertifikat / -Schlüssel | certificat / clé client | certificado / clave de cliente | certificato / chiave client | certyfikat / klucz klienta | certificado / chave do cliente |
| system secret store | системное хранилище секретов | Schlüsselbund des Systems | trousseau du système | almacén de secretos del sistema | archivio dei segreti di sistema | systemowy magazyn sekretów | cofre de segredos do sistema |
| license | лицензия | Lizenz | licence | licencia | licenza | licencja | licença |
| settings | настройки | Einstellungen | Paramètres | Ajustes | Impostazioni | Ustawienia | Configurações |
| about | о программе | Über Glafira | À propos | Acerca de | Informazioni | O programie | Sobre |
| update | обновление | Update | mise à jour | actualización | aggiornamento | aktualizacja | atualização |
| diagnostics archive | архив диагностики | Diagnosearchiv | archive de diagnostic | archivo de diagnóstico | archivio diagnostico | archiwum diagnostyczne | arquivo de diagnóstico |
| set by your organization | задан вашей организацией | von Ihrer Organisation festgelegt | défini par votre organisation | establecido por tu organización | impostato dalla tua organizzazione | ustawione przez organizację | definido pela sua organização |
| System default (…) | Как в системе (…) | Systemsprache ({{name}}) | Langue du système ({{name}}) | Idioma del sistema ({{name}}) | Lingua di sistema ({{name}}) | Język systemu ({{name}}) | Idioma do sistema ({{name}}) |

## Частые слова интерфейса

| en | de | fr | es | it | pl | pt-BR |
|---|---|---|---|---|---|---|
| Save | Speichern | Enregistrer | Guardar | Salva | Zapisz | Salvar |
| Cancel | Abbrechen | Annuler | Cancelar | Annulla | Anuluj | Cancelar |
| Delete | Löschen | Supprimer | Eliminar | Elimina | Usuń | Excluir |
| Close | Schließen | Fermer | Cerrar | Chiudi | Zamknij | Fechar |
| Retry | Erneut versuchen | Réessayer | Reintentar | Riprova | Spróbuj ponownie | Tentar novamente |
| Loading… | Wird geladen… | Chargement… | Cargando… | Caricamento… | Wczytywanie… | Carregando… |
| Details | Details | Détails | Detalles | Dettagli | Szczegóły | Detalhes |
| Apply | Anwenden | Appliquer | Aplicar | Applica | Zastosuj | Aplicar |
| Back | Zurück | Retour | Atrás | Indietro | Wstecz | Voltar |
| Search | Suchen | Rechercher | Buscar | Cerca | Szukaj | Pesquisar |
| Refresh | Aktualisieren | Actualiser | Actualizar | Aggiorna | Odśwież | Atualizar |
| Create | Erstellen | Créer | Crear | Crea | Utwórz | Criar |
| Edit | Bearbeiten | Modifier | Editar | Modifica | Edytuj | Editar |
| Send | Senden | Envoyer | Enviar | Invia | Wyślij | Enviar |
| Copy | Kopieren | Copier | Copiar | Copia | Kopiuj | Copiar |
| Export | Exportieren | Exporter | Exportar | Esporta | Eksportuj | Exportar |
| Import | Importieren | Importer | Importar | Importa | Importuj | Importar |
