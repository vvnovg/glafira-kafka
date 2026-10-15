# Kafka Explorer

Desktop-приложение (Tauri 2 + Rust + React) для интерактивной работы с Kafka-кластерами.
Спецификация: `docs/superpowers/specs/2026-10-14-kafka-explorer-design.md`.

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
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 \
  cargo test --manifest-path src-tauri/Cargo.toml --tests   # Rust интеграционные (нужен docker compose)
```

## Поведение
- Просмотр сообщений переживает перезапуск брокера: временные ошибки терпятся, сессия завершается ошибкой, если связь не восстановилась за 60 с.

## Где хранятся данные
- Профили подключений: `connections.json` в каталоге данных приложения (macOS: `~/Library/Application Support/dev.kafkaexplorer.app/`).
- Пароли — в системном хранилище секретов (Keychain / Credential Manager / Secret Service), сервис `kafka-explorer`.
- Шаблоны сообщений: `templates.json`; история отправки — `history/<id подключения>.json` (последние 100, значения до 64 КБ). История хранится открытым текстом.
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
- **«Отправить повторно»** из просмотра сообщений открывает Producer с точной копией сообщения (усечённые и декодированные с потерями значения не отправляются).
- **Consumer-сессия** вступает в группу (ребаланс у других участников!). Автокоммит по умолчанию выключен; коммит — вручную кнопками. На подключениях «только чтение» отправка и consumer-сессии запрещены.

Проверить коммиты сессии:
```bash
docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group>
```
