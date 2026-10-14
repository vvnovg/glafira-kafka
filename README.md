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
docker compose -f dev/docker-compose.yml up -d   # Kafka: PLAINTEXT localhost:9092, SASL PLAIN localhost:9094 (tester / tester-secret)
pnpm tauri dev
```

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
- Логи: `RUST_LOG=debug pnpm tauri dev`.
