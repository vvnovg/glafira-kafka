# Glafira for Apache Kafka

Desktop-приложение (Tauri 2 + Rust + React) для интерактивной работы с кластерами Apache Kafka.

Apache Kafka is a trademark of the Apache Software Foundation; this project is not affiliated with the ASF.
Спецификация: `docs/superpowers/specs/2026-10-14-kafka-explorer-design.md`.

## Установка
Установщики (macOS `.dmg`/`.pkg`, Windows `.msi`, Linux `.deb`/`.rpm`/`.AppImage`) — в разделе релизов:
https://github.com/vvnovg/glafira-kafka-releases/releases

Бета-сборки могут быть неподписанными. Чтобы открыть:
- **macOS:** правый клик по приложению → «Открыть» → «Открыть», либо «Системные настройки» → «Конфиденциальность и безопасность» → «Все равно открыть».
- **Windows:** SmartScreen → «Подробнее» → «Выполнить в любом случае».

Для администраторов (тихая установка, политика, зеркало обновлений): `docs/admin.md`. Лицензии: `docs/licensing.md`.

## Требования
- Rust stable (`rustup`), cmake, Xcode Command Line Tools (macOS)
- Node 22+, pnpm
- Docker — для локальной Kafka (`confluentinc/cp-kafka:7.6.1`) и интеграционных тестов

## Запуск
```bash
pnpm install
docker compose -f dev/docker-compose.yml up -d   # локальная Kafka, см. «Локальный стенд»
docker wait ke-kafka-init   # дождаться создания SCRAM-пользователя (печатает 0); нужно SCRAM-тестам
pnpm tauri dev
```

## Локальный стенд (`dev/docker-compose.yml`)
- PLAINTEXT: `localhost:9092`.
- SASL_PLAINTEXT: `localhost:9094`, механизмы PLAIN, SCRAM-SHA-256 и SCRAM-SHA-512.
  - PLAIN: `tester` / `tester-secret`, `admin` / `admin-secret`.
  - SCRAM-SHA-256 и SCRAM-SHA-512: `scram-user` / `scram-secret` (создаётся одноразовым сервисом `kafka-init` после старта брокера).

## Тесты
```bash
pnpm test                                              # фронтенд
cargo test --manifest-path src-tauri/Cargo.toml --lib  # Rust unit
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 KE_IT_MTLS_BOOTSTRAP=localhost:9095 \
  cargo test --manifest-path src-tauri/Cargo.toml --tests   # Rust интеграционные (нужен docker compose)
```
Без переменной `KE_IT_*` соответствующий интеграционный тест пропускается.
Windows: если падает `librdkafka_is_built_with_every_compression_codec` (snappy), librdkafka собрана до подключения `src-tauri/cmake/rdkafka-windows.cmake` — выполните `cargo clean -p rdkafka-sys --manifest-path src-tauri/Cargo.toml` и пересоберите.

## Поведение
- Просмотр сообщений переживает перезапуск брокера: временные ошибки терпятся, сессия завершается ошибкой, если связь не восстановилась за 60 с.

## Где хранятся данные
- Профили подключений: `connections.json` в каталоге данных приложения (macOS: `~/Library/Application Support/io.github.vvnovg.glafira.kafka/`).
- Пароли — в системном хранилище секретов (Keychain / Credential Manager / Secret Service), сервис `glafira-kafka`.
- Переход со старого имени (Kafka Explorer, `dev.kafkaexplorer.app`): при первом запуске, если в новом каталоге данных нет
  `connections.json`, из старого каталога рядом копируются `connections.json`, `templates.json`, `license.json`, `history/`,
  а пароли подключений — из сервиса keychain `kafka-explorer` в `glafira-kafka`. Уже существующее не перезаписывается,
  старые данные и записи keychain не удаляются (их можно удалить вручную). В логе — `миграция данных`.
  macOS может один раз спросить доступ к старым записям Keychain.
- Шаблоны сообщений: `templates.json`; история отправки — `history/<id подключения>.json` (последние 100, значения до 64 КБ). История хранится открытым текстом и содержит уже отрисованные сообщения: значения, подставленные из `{{env NAME}}`, и значения заголовков (например, токены) записываются как есть. Файлы данных создаются с правами только для владельца (0600, каталог `history/` — 0700 на macOS/Linux). Если `templates.json` не читается (например, создан более новой версией), недоступны только шаблоны — ошибка показывается в панели шаблонов.
- Лицензия: `license.json` (только если ключ введён вручную).
- Логи: `RUST_LOG=debug pnpm tauri dev`.

## Защита от изменений
- **Только чтение** (флаг профиля): все изменяющие операции (создание/изменение/очистка/удаление топиков, сброс и удаление групп) отклоняются в Rust с кодом `READ_ONLY`, кнопки изменений в интерфейсе скрыты.
- **prod**: каждая изменяющая операция требует ввести имя подключения; удаление и очистка дополнительно — имя объекта. Подтверждения — внутри страницы (диалог), не системные.
- Сброс оффсетов и удаление группы — только для группы без участников (`Empty`) или несуществующей.
- «Сбросить группу сюда» из просмотра сообщений выбирает группу только из списка существующих.
- Размер топика на диске вне MVP (librdkafka не поддерживает DescribeLogDirs).

Проверить сброс вручную:
```bash
docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group>
```

## Отправка и чтение как группа
- **Producer**: форматы string / json / hex / base64 / null, заголовки, acks / сжатие / идемпотентность. Плейсхолдеры: `{{uuid}}`, `{{now}}`, `{{nowMillis}}`, `{{seq}}`, `{{randomInt 1 100}}`, `{{randomString 8}}`, `{{env NAME}}`; `\{{` — буквальные `{{`.
- **Пакетная отправка**: N копий формы или файл JSON Lines (`{"key":…, "value":…, "headers":{…}, "partition":…}` в строке) / CSV (колонки `key,value,partition,header.<имя>`). Пакеты не попадают в историю.
- **«Отправить повторно»** из просмотра сообщений и **«Повторить»** из истории открывают Producer с копией ключа, значения и заголовков байт в байт (усечённые и декодированные с потерями значения не отправляются). Чтобы шаблонизатор не тронул чужой текст, каждое `{{` в форме экранируется как `\{{`: плейсхолдеры из прочитанного сообщения (в том числе `{{env NAME}}`) не подставляются, а повтор из истории отправляет уже отрисованные значения. Шаблоны, загруженные из списка «Шаблон», отрисовываются как обычно. Партицию выбирает партиционер (не исходная партиция), timestamp — новый.
- **Consumer-сессия** вступает в группу (ребаланс у других участников!). Автокоммит по умолчанию выключен; коммит — вручную кнопками. На подключениях «только чтение» отправка и consumer-сессии запрещены.

Проверить коммиты сессии:
```bash
docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group>
```

## Сертификаты (SSL и mTLS)
- **Доверенный CA** — файл PEM, PKCS#12 или JKS (truststore). Пусто — системные корневые сертификаты.
- **Клиентский сертификат (mTLS)** — PEM (сертификат + ключ), PKCS#12 (`.p12`, `.pfx`) или JKS. Пароли необязательны и хранятся в системном хранилище секретов.
- Файлы читаются при каждом подключении: после ротации сертификата достаточно переподключиться.

Пример для кластера в Kubernetes с Istio / cert-manager (секрет с `tls.crt`, `tls.key`, `ca.crt`):
```bash
kubectl get secret kafka-client -o jsonpath='{.data.tls\.crt}' | base64 -d > ~/certs/tls.crt
kubectl get secret kafka-client -o jsonpath='{.data.tls\.key}' | base64 -d > ~/certs/tls.key
kubectl get secret kafka-client -o jsonpath='{.data.ca\.crt}' | base64 -d > ~/certs/ca.crt
```
Протокол `SSL`, доверенный CA — `ca.crt`, клиентский сертификат PEM — `tls.crt` и `tls.key`, пароли пустые.

Локальный стенд слушает mTLS на `localhost:9095` (сертификаты — `dev/certs/out`, см. `dev/certs/README.md`).

## Free и Pro
- **Free:** просмотр, топики и группы, Producer, все виды подключений (включая mTLS), «только чтение».
- **Pro:** consumer-сессии и режим prod: создание и перевод подключений в prod (с подтверждением именем подключения на любое изменение). Free сохраняет подтверждения на уже существующих prod-подключениях и маркировку остальных окружений.
- Лицензия вводится на экране «Лицензия» (ссылка внизу дерева подключений) или раздаётся администратором через политику (`licenseKey`). Проверка офлайн.
- Выдача ключей — `docs/licensing.md`.
