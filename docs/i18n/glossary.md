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
DNS, TCP, ID, Build (сборка: подпись в «О программе» и слово в тексте об обновлениях — «build», не «версия»/«компиляция»), Leader (лидер партиции; в ja и ko — リーダー и 리더); имена параметров Kafka (`cleanup.policy`, `retention.ms`, `acks`, `enable.idempotence`); значения acks (0, 1, all), стратегии earliest/latest, пресеты delete/compact/test;
команды, пути, адреса (`docker compose -f dev/docker-compose.yml up -d`, `keytool …`, `localhost:9092`);
название экрана «Producer» (как в ru); значения CreateTime / LogAppendTime, producer / uncompressed в compression.type.

## Стиль по языкам

| Язык | Обращение | Кнопки и пункты меню | Вопрос-подтверждение | Кавычки | Особенности |
|---|---|---|---|---|---|
| de | Sie, по возможности без обращения | инфинитив: Speichern, Löschen | «Topic „{{name}}“ löschen?» | „…“ | составные слова через дефис с англ. терминами: Topic-Name, Consumer-Group-ID |
| fr | vous, по возможности без обращения | инфинитив: Enregistrer, Supprimer | «Supprimer le topic « {{name}} » ?» | « … » с U+00A0 внутри | U+00A0 перед `:` `;` `!` `?` и после `«` / перед `»` |
| es | tú, по возможности без обращения | инфинитив: Guardar, Eliminar | «¿Eliminar el topic “{{name}}”?» | “…” | `¿…?`, `¡…!` |
| it | tu, по возможности без обращения | повелительное как в macOS/Windows: Salva, Elimina | «Eliminare il topic “{{name}}”?» | “…” | артикль перед англ. терминами: il topic, l’offset |
| pl | безличные формы, без «Ty/Pan» | повелительное: Zapisz, Usuń | «Usunąć topik „{{name}}”?» | „…” | значения параметров только в именительном после существительного |
| pt-BR | você, по возможности без обращения | инфинитив: Salvar, Excluir | «Excluir o tópico “{{name}}”?» | “…” | бразильская норма: arquivo, tela, excluir (не ficheiro, ecrã, eliminar) |
| zh-CN | 你, по возможности без обращения | глагол: 保存、删除 | «删除主题“{{name}}”？» | “…” | полноширинная пунктуация; пробел между иероглифами и латиницей/цифрами |
| zh-TW | 你, по возможности без обращения | глагол: 儲存、刪除 | «刪除主題「{{name}}」？» | 「…」 | тайваньская лексика (檔案, 程式, 設定), не перекодированный zh-CN |
| ja | без местоимений; в тексте です/ます | краткие формы: 保存, 削除, 閉じる | «トピック「{{name}}」を削除しますか？» | 「…」 | без пробелов; заимствования катаканой по глоссарию |
| ko | без местоимений; в тексте -습니다/-ㅂ니다 | существительные: 저장, 삭제 | «'{{name}}' 토픽을 삭제하시겠습니까?» | '…' | пробелы между словами; ASCII-пунктуация; частица — после постоянного слова, не после параметра |

## Термины

| en | ru | de | fr | es | it | pl | pt-BR | zh-CN | zh-TW | ja | ko |
|---|---|---|---|---|---|---|---|---|---|---|---|
| topic | топик | Topic (das, Pl. Topics) | topic (m.) | topic (m.) | topic (m., pl. invar.) | topik (m.: topiku, topiki, topików) | tópico (m.) | 主题 | 主題 | トピック | 토픽 |
| partition | партиция | Partition (die) | partition (f.) | partición (f.) | partizione (f.) | partycja | partição (f.) | 分区 | 分區 | パーティション | 파티션 |
| offset | оффсет | Offset (der, Pl. Offsets) | offset (m.) | offset (m.) | offset (m., pl. invar.) | offset (m.: offsetu, offsety) | offset (m.) | 偏移量 | 偏移量 | オフセット | 오프셋 |
| consumer group | consumer group | Consumer Group (die) | consumer group (m.) | consumer group (m.) | consumer group (m.) | grupa konsumentów | consumer group (m.) | 消费者组 | 消費者群組 | コンシューマーグループ | 컨슈머 그룹 |
| broker | брокер | Broker (der) | broker (m.) | broker (m.) | broker (m.) | broker (m.: brokera) | broker (m.) | Broker | Broker | ブローカー | 브로커 |
| producer / consumer (роль) | producer / consumer | Producer / Consumer | producteur / consommateur | productor / consumidor | producer / consumer | producent / konsument | produtor / consumidor | 生产者 / 消费者 | 生產者 / 消費者 | プロデューサー / コンシューマー | 프로듀서 / 컨슈머 |
| lag | лаг | Lag (der) | lag (m.) | lag (m.) | lag (m.) | lag (m.: lagu) | lag (m.) | 延迟 (в тексте можно 消费延迟) | 延遲 (в тексте можно 消費延遲) | ラグ | 랙 |
| message | сообщение | Nachricht | message (m.) | mensaje | messaggio | wiadomość | mensagem | 消息 | 訊息 | メッセージ | 메시지 |
| key / value | ключ / значение | Schlüssel / Wert | clé / valeur | clave / valor | chiave / valore | klucz / wartość | chave / valor | 键 / 值 | 鍵 / 值 | キー / 値 | 키 / 값 |
| headers | заголовки | Header (Pl.) | en-têtes | encabezados | intestazioni | nagłówki | cabeçalhos | 标头 | 標頭 | ヘッダー | 헤더 |
| connection | подключение | Verbindung | connexion | conexión | connessione | połączenie | conexão | 连接 | 連線 | 接続 | 연결 |
| folder | папка | Ordner | dossier | carpeta | cartella | folder | pasta | 文件夹 | 資料夾 | フォルダー | 폴더 |
| prod connection | prod-подключение | Prod-Verbindung | connexion prod | conexión prod | connessione prod | połączenie prod | conexão prod | 生产环境连接 | 正式環境連線 | 本番接続 | 운영 환경 연결 |
| cluster | кластер | Cluster | cluster | clúster | cluster | klaster | cluster | 集群 | 叢集 | クラスター | 클러스터 |
| bootstrap server | bootstrap-сервер | Bootstrap-Server | serveur bootstrap | servidor bootstrap | server bootstrap | serwer bootstrap | servidor bootstrap | Bootstrap 服务器 | Bootstrap 伺服器 | ブートストラップサーバー | 부트스트랩 서버 |
| consumer session | consumer-сессия | Consumer-Session (die, Pl. Sessions) | session consumer | sesión de consumer | sessione consumer | sesja konsumenta | sessão de consumer | 消费者会话 | 消費者工作階段 | コンシューマーセッション | 컨슈머 세션 |
| read-only | только чтение | Schreibgeschützt | Lecture seule | Solo lectura | Sola lettura | Tylko do odczytu | Somente leitura | 只读 | 唯讀 | 読み取り専用 | 읽기 전용 |
| preset (набор настроек при создании топика) | пресет | Voreinstellung | préréglage | preajuste | profilo | profil (не «szablon» — так названы шаблоны продюсера) | predefinição | 预设 | 預設集 (не «預設» — это «по умолчанию») | プリセット | 프리셋 |
| committed offset | закоммиченный оффсет | committeter Offset | offset commité | offset con commit | offset sottoposto a commit | zatwierdzony offset | offset com commit | 已提交的偏移量 | 已提交的偏移量 | コミット済みオフセット | 커밋된 오프셋 |
| reset offsets | сбросить оффсеты | Offsets zurücksetzen | Réinitialiser les offsets | Restablecer offsets | Reimposta gli offset | Zresetuj offsety | Redefinir offsets | 重置偏移量 | 重設偏移量 | オフセットをリセット | 오프셋 재설정 |
| retention | хранение (retention) | Aufbewahrung | rétention | retención | conservazione | retencja | retenção | 保留 | 保留 | 保持 | 보존 |
| replication factor | фактор репликации | Replikationsfaktor | facteur de réplication | factor de replicación | fattore di replica | współczynnik replikacji | fator de replicação | 副本因子 | 複寫係數 | レプリケーションファクター | 복제 계수 |
| under-replicated | недореплицированные | unterrepliziert | sous-répliquées | subreplicadas | sotto-replicate | niedoreplikowane | sub-replicadas | 副本不足 | 複本不足 | レプリカ不足 | 복제 부족 |
| trusted CA | доверенный CA | vertrauenswürdige CA | CA de confiance | CA de confianza | CA attendibile | zaufany CA | CA confiável | 受信任的 CA | 受信任的 CA | 信頼された CA | 신뢰할 수 있는 CA |
| client certificate / key | сертификат / ключ клиента | Client-Zertifikat / -Schlüssel | certificat / clé client | certificado / clave de cliente | certificato / chiave client | certyfikat / klucz klienta | certificado / chave do cliente | 客户端证书 / 私钥 | 用戶端憑證 / 私密金鑰 | クライアント証明書 / 秘密鍵 | 클라이언트 인증서 / 개인 키 |
| system secret store | системное хранилище секретов | Schlüsselbund des Systems | trousseau du système | almacén de secretos del sistema | archivio dei segreti di sistema | systemowy magazyn sekretów | cofre de segredos do sistema | 系统密钥存储 | 系統金鑰儲存區 | システムの資格情報ストア | 시스템 보안 저장소 |
| idempotence | идемпотентность | Idempotenz | idempotence | idempotencia | idempotenza | idempotencja | idempotência | 幂等性 | 冪等性 | 冪等性 | 멱등성 |
| compression | сжатие | Komprimierung | compression | compresión | compressione | kompresja | compressão | 压缩 | 壓縮 | 圧縮 | 압축 |
| rebalance | ребалансировка | Rebalance (das) | rééquilibrage | rebalanceo | ribilanciamento | rebalans | rebalanceamento | 重平衡 | 重新平衡 | リバランス | 리밸런싱 |
| placeholder (Producer) | плейсхолдер | Platzhalter | variable | marcador | segnaposto | symbol zastępczy | marcador | 占位符 | 預留位置 | プレースホルダー | 플레이스홀더 |
| hint (значок «?») | подсказка | Hinweis | aide | ayuda | suggerimento | podpowiedź | dica | 提示 | 提示 | ヒント | 도움말 |
| segment (лога) | сегмент | Segment | segment | segmento | segmento | segment | segmento | 日志段 | 日誌區段 | セグメント | 세그먼트 |
| compaction | compaction | Compaction (die) | compaction (f.) | compactación | compattazione | kompaktowanie | compactação | Compaction (не «压缩» — это compression) | Compaction (не «壓縮» — это compression) | コンパクション | 컴팩션 |
| tiered storage | tiered storage | Tiered Storage | tiered storage | tiered storage | tiered storage | tiered storage | tiered storage | 分层存储 | 分層儲存 | 階層型ストレージ | 계층형 스토리지 |
| follower (реплика) | фолловер | Follower | follower | follower | follower | follower | follower | Follower | Follower | フォロワー | 팔로워 |
| license | лицензия | Lizenz | licence | licencia | licenza | licencja | licença | 许可证 | 授權 | ライセンス | 라이선스 |
| settings | настройки | Einstellungen | Paramètres | Ajustes | Impostazioni | Ustawienia | Configurações | 设置 | 設定 | 設定 | 설정 |
| about | о программе | Über Glafira | À propos | Acerca de | Informazioni | O programie | Sobre | 关于 Glafira | 關於 Glafira | Glafira について | Glafira 정보 |
| update | обновление | Update | mise à jour | actualización | aggiornamento | aktualizacja | atualização | 更新 | 更新 | アップデート | 업데이트 |
| diagnostics archive | архив диагностики | Diagnosearchiv | archive de diagnostic | archivo de diagnóstico | archivio diagnostico | archiwum diagnostyczne | arquivo de diagnóstico | 诊断归档 | 診斷壓縮檔 | 診断アーカイブ | 진단 아카이브 |
| set by your organization | задан вашей организацией | von Ihrer Organisation festgelegt | défini par votre organisation | establecido por tu organización | impostato dalla tua organizzazione | ustawione przez organizację | definido pela sua organização | 由你的组织设置 | 由你的組織設定 | 組織によって設定済み | 조직에서 설정함 |
| System default (…) | Как в системе (…) | Systemsprache ({{name}}) | Langue du système ({{name}}) | Idioma del sistema ({{name}}) | Lingua di sistema ({{name}}) | Język systemu ({{name}}) | Idioma do sistema ({{name}}) | 跟随系统（{{name}}） | 系統語言（{{name}}） | システムの言語（{{name}}） | 시스템 언어({{name}}) |

## Частые слова интерфейса

| en | de | fr | es | it | pl | pt-BR | zh-CN | zh-TW | ja | ko |
|---|---|---|---|---|---|---|---|---|---|---|
| Save | Speichern | Enregistrer | Guardar | Salva | Zapisz | Salvar | 保存 | 儲存 | 保存 | 저장 |
| Cancel | Abbrechen | Annuler | Cancelar | Annulla | Anuluj | Cancelar | 取消 | 取消 | キャンセル | 취소 |
| Delete | Löschen | Supprimer | Eliminar | Elimina | Usuń | Excluir | 删除 | 刪除 | 削除 | 삭제 |
| Close | Schließen | Fermer | Cerrar | Chiudi | Zamknij | Fechar | 关闭 | 關閉 | 閉じる | 닫기 |
| Retry | Erneut versuchen | Réessayer | Reintentar | Riprova | Spróbuj ponownie | Tentar novamente | 重试 | 重試 | 再試行 | 다시 시도 |
| Loading… | Wird geladen… | Chargement… | Cargando… | Caricamento… | Wczytywanie… | Carregando… | 加载中… | 載入中… | 読み込み中… | 불러오는 중… |
| Details | Details | Détails | Detalles | Dettagli | Szczegóły | Detalhes | 详细信息 | 詳細資訊 | 詳細 | 세부 정보 |
| Apply | Anwenden | Appliquer | Aplicar | Applica | Zastosuj | Aplicar | 应用 | 套用 | 適用 | 적용 |
| Back | Zurück | Retour | Atrás | Indietro | Wstecz | Voltar | 返回 | 返回 | 戻る | 뒤로 |
| Search | Suchen | Rechercher | Buscar | Cerca | Szukaj | Pesquisar | 搜索 | 搜尋 | 検索 | 검색 |
| Refresh | Aktualisieren | Actualiser | Actualizar | Aggiorna | Odśwież | Atualizar | 刷新 | 重新整理 | 更新 | 새로 고침 |
| Create | Erstellen | Créer | Crear | Crea | Utwórz | Criar | 创建 | 建立 | 作成 | 만들기 |
| Edit | Bearbeiten | Modifier | Editar | Modifica | Edytuj | Editar | 编辑 | 編輯 | 編集 | 편집 |
| Send | Senden | Envoyer | Enviar | Invia | Wyślij | Enviar | 发送 | 傳送 | 送信 | 보내기 |
| Copy | Kopieren | Copier | Copiar | Copia | Kopiuj | Copiar | 复制 | 複製 | コピー | 복사 |
| Export | Exportieren | Exporter | Exportar | Esporta | Eksportuj | Exportar | 导出 | 匯出 | エクスポート | 내보내기 |
| Import | Importieren | Importer | Importar | Importa | Importuj | Importar | 导入 | 匯入 | インポート | 가져오기 |

## Правила письма: китайский, японский, корейский

Проверяет `pnpm i18n:check` (словари и главы руководства, вне `` `кода` ``, `{{…}}` и тегов): в zh-CN, zh-TW, ja нет
полуширинных `, . : ; ? !` сразу после иероглифа или каны; в zh-CN, zh-TW нет пробела между двумя иероглифами.
Остальное — ручная проверка по этому разделу и `review-checklist.md`.

### Общее для zh-CN, zh-TW, ja, ko
- Множественное число — одна форма `_other` (`Intl.PluralRules` этих языков); счётные слова — внутри фразы:
  zh-CN «{{count}} 条消息», «{{count}} 个主题»; zh-TW «{{count}} 則訊息», «{{count}} 個主題»; ja «{{count}} 件のメッセージ»;
  ko «메시지 {{count}}개».
- Названия Kafka, протоколов, форматов и параметров — латиницей, как в «Не переводится»; экран «Producer» — латиницей.
  Broker в zh — латиницей (Broker), в ja и ko — устоявшиеся ブローカー и 브로커; Leader и Follower — латиницей в zh, リーダー/フォロワー в ja, 리더/팔로워 в ko.
  Lag — в интерфейсе и подсказках коротко 延迟 / 延遲 (в прозе руководства можно 消费延迟 / 消費延遲), ラグ, 랙.
- Многоточие — один символ `…`, как во всех языках (не «……» и не `...`).
- Числа, единицы (`ms`, `MB`) и латиница — полуширинные; полноширинные цифры и латиницу (`１２`, `ＭＢ`) не использовать.
- Имя (параметр `{{name}}`) не склоняется и не меняет соседние слова: в кавычках языка и рядом с существительным
  («主题“{{name}}”», «トピック「{{name}}」», «'{{name}}' 토픽»).

### zh-CN и zh-TW
- Пунктуация полноширинная: «，。：；？！（）»; перечисление однородных слов — «、».
  Внутри латинского фрагмента (адрес, параметр, команда) — как в оригинале: `localhost:9092`, `acks=all`.
- Кавычки: zh-CN “…” (вложенные ‘…’), zh-TW 「…」 (вложенные 『…』).
- Пробел между иероглифом и латиницей, цифрой, `{{count}}`, `` `кодом` ``: «连接 Kafka 集群», «已加载 {{count}} 条消息»,
  «设置 `retention.ms`». Не ставится между иероглифом и полноширинной пунктуацией и между двумя иероглифами.
- Обращение «你», если без него не обойтись (как в системах Apple и Microsoft); «您» не использовать.
- zh-TW — тайваньская норма, а не механическая замена знаков zh-CN:

| смысл | zh-CN | zh-TW |
|---|---|---|
| файл | 文件 | 檔案 |
| программа / приложение | 程序 / 应用 | 程式 / 應用程式 |
| настройки | 设置 | 設定 |
| по умолчанию | 默认 | 預設 |
| сервер | 服务器 | 伺服器 |
| сеть | 网络 | 網路 |
| данные | 数据 | 資料 |
| клиент | 客户端 | 用戶端 |
| сертификат / ключ шифрования | 证书 / 密钥 | 憑證 / 金鑰 |
| загрузить (данные) | 加载 | 載入 |
| запустить | 运行 | 執行 |
| окно | 窗口 | 視窗 |
| поддержка | 支持 | 支援 |
| информация | 信息 | 資訊 |
| сессия | 会话 | 工作階段 |
| журнал (логи) | 日志 | 日誌 |

### ja
- В предложениях — です/ます; на кнопках и в пунктах меню — краткие формы (保存, 削除, 閉じる, 再試行).
- Заимствования — катаканой по этому глоссарию; долгий гласный в конце слова пишется: サーバー, フォルダー, ユーザー,
  パラメーター, コンシューマー, プロデューサー.
- Пунктуация полноширинная: «、。：？！（）»; кавычки 「…」 (вложенные 『…』).
- Пробелов нет — ни между японскими словами, ни между японским текстом и латиницей или числом («Kafkaクラスター»,
  「{{count}}件のメッセージ」); пробел остаётся только внутри латинской фразы («Schema Registry»).

### ko
- В предложениях — официально-вежливый стиль (-습니다/-ㅂ니다); на кнопках — существительные или краткие формы (저장, 삭제, 닫기).
- Пробелы между словами по нормам 띄어쓰기; пунктуация ASCII: `, . : ? !` и `( )`; кавычки для имён — `'…'`.
- Частица не ставится сразу после `{{…}}`: её форма зависит от последнего слога значения. Ставьте после параметра
  постоянное слово («'{{name}}' 토픽을…», «메시지 {{count}}개를…»), а не «{{name}}을(를)».

### Руководство (docs/guide)
- В zh-CN, zh-TW, ja метка ставится вплотную к тексту: «打开[[ui:sidebar.settings]]。», «[[ui:sidebar.settings]]を開きます。»;
  в ko — с пробелами по 띄어쓰기 («[[ui:sidebar.settings]] 화면»).
- Если значение метки начинается или кончается пунктуацией («导出…», «（高级）», «「…」»), `**…**` вплотную к иероглифу
  по CommonMark не станет жирным (ни в приложении, ни на GitHub). `pnpm i18n:check` такое находит; перестройте фразу так,
  чтобы рядом с меткой была пунктуация, или поставьте метку в конец предложения.
- Абзац — одной строкой: перенос строки внутри абзаца между иероглифами в приложении отображается как пробел.

## Слова руководства (de)

Термины, которые часто встречаются в `docs/guide/de` и отсутствуют в словаре интерфейса:
tab — Tab (der, Pl. Tabs); screen — Bildschirm; panel (левая панель) — Bereich; button — Schaltfläche;
checkbox — Kontrollkästchen; logs — Protokolle (Protokolldateien); batch send — Stapelversand, Stapel;
release page — Release-Seite; installer — Installationsprogramm; support request — Supportanfrage;
macOS menu Help — Menü „Hilfe“.

## Слова руководства (fr)

Термины, которые часто встречаются в `docs/guide/fr` и отсутствуют в словаре интерфейса:
tab — onglet; screen — écran; panel (левая панель) — panneau; button — bouton; checkbox — case à cocher;
logs — journaux (fichiers journaux); batch send — envoi par lot, lot; batch (Kafka) — batch;
release page — page des versions; installer — programme d’installation; support request — demande d’assistance;
support — assistance; timestamp — horodatage; tombstone — tombstone; listener — listener;
macOS menu Help — menu « Aide »; to commit — commiter; read-only connection — connexion en lecture seule
(с меткой: «connexions en [[ui:common.readOnly]]»).

## Слова руководства (es)

Термины, которые часто встречаются в `docs/guide/es` и отсутствуют в словаре интерфейса:
tab — pestaña; screen — pantalla; panel (левая панель) — panel; button — botón; checkbox — casilla;
logs — registros (archivos de registro, как «Registros:» в интерфейсе); batch send — envío por lotes, lote; batch (Kafka) — batch;
release page — página de versiones; installer — instalador; build — build (m.); support request — solicitud de soporte;
support — soporte; timestamp — marca de tiempo; tombstone — tombstone; listener — listener; handshake — handshake;
purge — purgar, purga (как в интерфейсе); to commit — hacer commit; committed offsets — offsets con commit;
macOS menu Help — menú Ayuda; Keychain — Llavero; Credential Manager — Administrador de credenciales;
read-only connection — conexión de solo lectura (с меткой: «en las conexiones con la opción [[ui:common.readOnly]] activada»).

## Слова руководства (it)

Термины, которые часто встречаются в `docs/guide/it` и отсутствуют в словаре интерфейса:
tab — scheda; screen — schermata; panel (левая панель) — pannello; button — pulsante; checkbox — casella di controllo;
form — modulo (как в UI: Svuota modulo); template — modello; history — cronologia; logs — log (m., file di log);
batch send — invio in batch (как в UI); batch (Kafka) — batch; release page — pagina delle release;
installer — programma di installazione; build — build (f.); support request — richiesta di assistenza; support — assistenza;
timestamp — timestamp; tombstone — tombstone; listener — listener; handshake — handshake; policy — policy (f., как в UI);
browse (просмотр сообщений) — consultazione, lettura; purge — svuotare, svuotamento (как в UI: Svuota);
to commit — eseguire il commit; committed offset — offset sottoposto a commit; lock icon — icona a forma di lucchetto;
macOS menu Help — menu Aiuto; Keychain — Portachiavi; Credential Manager — Gestione credenziali;
read-only connection — connessione in sola lettura (с меткой: «sulle connessioni con l’opzione [[ui:common.readOnly]] attiva»).

## Слова руководства (pl)

Термины, которые часто встречаются в `docs/guide/pl` и отсутствуют в словаре интерфейса:
регистр — безличный: шаги инфинитивом («Kliknąć przycisk…»), в тексте «należy», «można», «trzeba», возвратные и страдательные формы;
tab — karta; screen — ekran; panel (левая панель) — panel; button — przycisk; checkbox — pole wyboru; option — opcja;
logs — logi (как «Logi:» в интерфейсе), log (m.); batch send — wysyłka wsadowa (как в UI), partia; batch (Kafka) — batch;
release page — strona wydań; installer — instalator; build — build (m.: buildu); support request — zgłoszenie do pomocy technicznej;
support — pomoc techniczna; timestamp — znacznik czasu; tombstone — tombstone; listener — listener; handshake — handshake;
policy — zasady (pl., как «Zasady organizacji» в UI); purge — wyczyścić, czyszczenie (как в UI: Wyczyść);
to commit — zatwierdzić, zatwierdzanie (как в UI: Zatwierdź); commit (сущ.) — zatwierdzenie; committed offset — zatwierdzony offset;
экран Producer — «ekran Producer», название не склоняется; macOS menu Help — menu Pomoc; Keychain — pęk kluczy;
Credential Manager — Menedżer poświadczeń; lock icon — ikona kłódki;
метки — только в именительном: «w polu [[ui:…]] wpisać…», «przycisk [[ui:…]]», «sekcja [[ui:…]]»;
read-only connection — połączenie tylko do odczytu (с меткой: «w połączeniach z włączoną opcją [[ui:common.readOnly]]»).

## Слова руководства (pt-BR)

Термины, которые часто встречаются в `docs/guide/pt-BR` и отсутствуют в словаре интерфейса:
обращение — você, по возможности без местоимения или безличными оборотами («É possível…»); бразильская норма: arquivo, tela, excluir, usuário;
app — aplicativo (как в UI; приложения-потребители группы — тоже aplicativos); tab — aba; screen — tela; panel (левая панель) — painel;
button — botão; checkbox — caixa de seleção; option — opção; field — campo; section — seção; form — formulário; template — modelo;
history — histórico; logs — logs (m., arquivos de log); batch send — envio em lote (как в UI), lote; batch (Kafka) — batch;
release page — página de releases; installer — instalador; build — build (m.); support request — solicitação de suporte; support — suporte;
timestamp — timestamp; tombstone — tombstone; listener — listener; handshake — handshake; policy — política (как в UI);
browse (просмотр сообщений) — navegação, navegar pelas mensagens; purge — limpar, limpeza (как в UI: Limpar);
to commit — fazer commit; committed offset — offset com commit; timeout — tempo limite; lock icon — ícone de cadeado;
macOS menu Help — menu Ajuda; Keychain — Chaves; Credential Manager — Gerenciador de Credenciais;
метки полей — «em [[ui:…]], digite/escolha…», «o campo [[ui:…]]», «a seção [[ui:…]]», «a tela [[ui:…]]»;
read-only connection — conexão somente leitura (с меткой: «nas conexões com a opção [[ui:common.readOnly]] ativada»).
