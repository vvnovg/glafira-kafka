# Kafka Explorer — M1 «Каркас и чтение»: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Собрать desktop-приложение, которое подключается к Kafka (PLAINTEXT, SSL, SASL PLAIN/SCRAM), показывает обзор кластера, список и детали топиков и позволяет просматривать сообщения (Browse) с фильтрами и live tail.

**Architecture:** Tauri 2. Вся работа с Kafka идёт в Rust через rust-rdkafka: модули `kafka/` (клиенты, метаданные, топики), `browse/` (сессии чтения) и `storage/` с `secrets.rs` (профили и keychain). Тонкий слой `commands/` отдаёт всё это во фронтенд через `invoke` и `Channel`. Фронтенд на React + TypeScript: дерево подключений, вкладки, виртуализированные таблицы.

**Tech Stack:** Rust stable, Tauri 2, rdkafka 0.37 (librdkafka, статическая сборка), keyring 3, serde_json_path 0.7, regex; React 18, TypeScript, Vite, TanStack Query 5, TanStack Virtual 3, Zustand 5, Vitest + Testing Library; Docker (apache/kafka 3.9.0) для интеграционных тестов.

**Спецификация:** `docs/superpowers/specs/2026-10-14-kafka-explorer-design.md`

## Global Constraints

- Платформы: macOS (arm64, x64), Windows x64, Linux x64; поддерживаемые версии Kafka — 2.8+ (ZooKeeper и KRaft), Redpanda, AWS MSK, Confluent Cloud.
- Вся работа с Kafka — в Rust; фронтенд не знает о librdkafka.
- Все команды возвращают `Result<T, AppError>`, где `AppError { code, message, details?, retryable }` сериализуется в camelCase, а `code` — в SCREAMING_SNAKE_CASE.
- Секреты (пароли SASL) — только в OS keychain (`keyring`), ключ `kafka-explorer` / `<connection-id>/<field>`; в `connections.json` их нет.
- Данные приложения лежат в app data dir: `connections.json` с полем `schemaVersion`.
- Browse — consumer **без коммитов**: партиции назначаются через `assign`, `enable.auto.commit=false`.
- Лимит Browse — от 1 до 100 000 сообщений; значения больше 1 МБ обрезаются в превью (`truncated: true`).
- Данные из Rust во фронтенд идут батчами: до 200 сообщений или раз в 100 мс.
- Ни одна операция не блокирует UI: блокирующие вызовы librdkafka выполняются в `spawn_blocking` или отдельном потоке, у каждой сетевой операции есть таймаут.
- Нет телеметрии и удалённого контента; в CSP разрешены только `self` и IPC.
- Тексты UI в M1 пишутся по-русски прямо в коде (локализация ru/en — этап M4).
- Мутирующих операций в M1 нет; флаги `environment` и `readOnly` только сохраняются (защита от изменений появится в M2).

---

## Структура файлов M1

```
kafka-explorer/
├── dev/docker-compose.yml               # Kafka KRaft: PLAINTEXT :9092, SASL_PLAINTEXT(PLAIN) :9094
├── README.md
├── package.json, vite.config.ts, vitest.config.ts, tsconfig.json, index.html
├── src/
│   ├── main.tsx, App.tsx, styles.css
│   ├── api/types.ts                     # TS-зеркало Rust DTO
│   ├── api/client.ts                    # обёртки invoke/Channel, toAppError
│   ├── lib/format.ts, lib/env.ts        # форматирование, цвета окружений
│   ├── state/workspace.ts               # Zustand: вкладки
│   ├── test/setup.ts, test/render.tsx
│   └── components/
│       ├── Layout.tsx, Sidebar.tsx, TabContent.tsx
│       ├── common/ErrorBanner.tsx, common/ConfigTable.tsx
│       ├── connections/profile.ts, connections/ConnectionForm.tsx, connections/ConnectionFormTab.tsx
│       ├── cluster/ClusterOverview.tsx
│       ├── topics/TopicList.tsx, topics/topicFilter.ts, topics/TopicDetails.tsx
│       └── browse/state.ts, browse/params.ts, browse/useBrowseSession.ts,
│           browse/BrowseForm.tsx, browse/MessageTable.tsx, browse/MessageDetails.tsx, browse/BrowsePanel.tsx
└── src-tauri/
    ├── Cargo.toml, tauri.conf.json, capabilities/default.json
    ├── src/main.rs, src/lib.rs, src/state.rs, src/error.rs, src/secrets.rs
    ├── src/storage/{mod.rs, connections.rs}
    ├── src/kafka/{mod.rs, config.rs, context.rs, pool.rs, metadata.rs, cluster.rs, configs.rs, connectivity.rs, topics.rs}
    ├── src/browse/{mod.rs, format.rs, types.rs, filter.rs, plan.rs, session.rs, registry.rs}
    ├── src/commands/{mod.rs, connections.rs, cluster.rs, topics.rs, browse.rs}
    └── tests/{common/mod.rs, it_cluster.rs, it_connectivity.rs, it_topics.rs, it_browse.rs}
```

**Как запускать тесты** (из корня репозитория):
- Unit-тесты Rust: `cargo test --manifest-path src-tauri/Cargo.toml --lib <фильтр>`
- Интеграционные тесты Rust (нужен Docker): `docker compose -f dev/docker-compose.yml up -d`, затем `KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --test <имя>`. Без переменных окружения интеграционные тесты пропускаются (печатают `skip` и проходят).
- Фронтенд: `pnpm vitest run <путь>`

---

### Task 1: Тулчейн, каркас Tauri, локальная Kafka

**Files:**
- Create: весь каркас из шаблона `create-tauri-app` (react-ts)
- Create: `dev/docker-compose.yml`
- Modify: `src-tauri/tauri.conf.json`, `src-tauri/Cargo.toml`, `package.json`
- Create: `vitest.config.ts`, `src/test/setup.ts`, `src/test/render.tsx`

**Interfaces:**
- Produces: Rust-крейт `kafka-explorer` с библиотекой `kafka_explorer_lib` (интеграционные тесты импортируют её), функция `kafka_explorer_lib::run()`; `renderWithClient(ui)` для тестов фронтенда.

- [ ] **Step 1: Установить Rust и cmake**

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
brew install cmake
xcode-select -p || xcode-select --install
rustc --version && cmake --version
```
Expected: печатаются версии `rustc 1.x` и `cmake version 3.x`/`4.x`.

- [ ] **Step 2: Сгенерировать каркас и перенести его в репозиторий**

```bash
TMP=$(mktemp -d)
cd "$TMP" && pnpm create tauri-app@latest kafka-explorer --template react-ts --manager pnpm --identifier dev.kafkaexplorer.app --yes
rsync -a "$TMP/kafka-explorer/" /Users/vvnovg/kafka-explorer/
cd /Users/vvnovg/kafka-explorer && git init && pnpm install
```
Expected: в корне появились `package.json`, `src/`, `src-tauri/`; каталог `docs/` не затронут.

- [ ] **Step 3: Прописать зависимости Rust**

В `src-tauri/Cargo.toml` секции `[dependencies]` и `[dev-dependencies]` должны быть такими (блоки `[package]`, `[lib]` и `[build-dependencies]` из шаблона не трогаем; в `[lib]` должно стоять `name = "kafka_explorer_lib"`):

```toml
[dependencies]
tauri = { version = "2", features = [] }
tauri-plugin-opener = "2"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
rdkafka = { version = "0.37", features = ["cmake-build", "ssl-vendored", "libz-static", "zstd"] }
keyring = { version = "3", features = ["apple-native", "windows-native", "sync-secret-service"] }
thiserror = "2"
uuid = { version = "1", features = ["v4"] }
regex = "1"
serde_json_path = "0.7"
hex = "0.4"
base64 = "0.22"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

[dev-dependencies]
tempfile = "3"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

- [ ] **Step 4: Настроить окно и CSP**

В `src-tauri/tauri.conf.json` задать:
```json
{
  "productName": "Kafka Explorer",
  "identifier": "dev.kafkaexplorer.app",
  "app": {
    "windows": [{ "title": "Kafka Explorer", "width": 1400, "height": 900, "minWidth": 900, "minHeight": 600 }],
    "security": {
      "csp": "default-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src ipc: http://ipc.localhost"
    }
  }
}
```
Остальные ключи шаблона (`$schema`, `version`, `build`, `bundle`) оставить как есть.

- [ ] **Step 5: Зависимости фронтенда и Vitest**

```bash
pnpm add @tanstack/react-query@5 @tanstack/react-virtual@3 zustand@5
pnpm add -D vitest@3 jsdom @testing-library/react @testing-library/jest-dom @testing-library/user-event
```

Создать `vitest.config.ts`:
```ts
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
  },
});
```

Создать `src/test/setup.ts`:
```ts
import "@testing-library/jest-dom/vitest";
```

Создать `src/test/render.tsx`:
```tsx
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import type { ReactElement } from "react";

export function renderWithClient(ui: ReactElement) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={client}>{ui}</QueryClientProvider>);
}
```

В `package.json` → `"scripts"` добавить `"test": "vitest run"`.

В `tsconfig.json` → `compilerOptions.types` добавить `"vitest/globals"` (если ключа `types` нет — создать `"types": ["vitest/globals"]`).

- [ ] **Step 6: Локальная Kafka для разработки и тестов**

Создать `dev/docker-compose.yml`:
```yaml
services:
  kafka:
    image: apache/kafka:3.9.0
    container_name: ke-kafka
    ports:
      - "9092:9092"
      - "9094:9094"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: INTERNAL://:19092,CONTROLLER://:9093,EXTERNAL://:9092,SASL_EXT://:9094
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092,SASL_EXT://localhost:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,SASL_EXT:SASL_PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_SASL_ENABLED_MECHANISMS: PLAIN
      KAFKA_LISTENER_NAME_SASL__EXT_PLAIN_SASL_JAAS_CONFIG: >-
        org.apache.kafka.common.security.plain.PlainLoginModule required
        username="admin" password="admin-secret"
        user_admin="admin-secret" user_tester="tester-secret";
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
```
Образ `apache/kafka` переводит переменные `KAFKA_*` в `server.properties`: `_` → `.`, `__` → `_`. Поэтому `SASL__EXT` превращается в `sasl_ext`.

Run: `docker compose -f dev/docker-compose.yml up -d && sleep 10 && docker exec ke-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`
Expected: команда завершается без ошибки (список пуст).
Если SASL-листенер не поднялся (`docker logs ke-kafka` содержит `Could not find a 'KafkaServer' or 'sasl_ext.KafkaServer' entry`), проверьте имя переменной JAAS: оно должно в точности совпадать с `KAFKA_LISTENER_NAME_SASL__EXT_PLAIN_SASL_JAAS_CONFIG`.

- [ ] **Step 7: Проверить сборку**

Run: `cargo check --manifest-path src-tauri/Cargo.toml && pnpm build && pnpm test --passWithNoTests`
Expected: всё зелёное. Первая сборка librdkafka и OpenSSL занимает 3–8 минут.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "chore: каркас Tauri 2 + React, dev Kafka в docker compose"
```

---

### Task 2: AppError и классификация ошибок Kafka

**Files:**
- Create: `src-tauri/src/error.rs`
- Modify: `src-tauri/src/lib.rs` (добавить `pub mod error;` первой строкой)

**Interfaces:**
- Produces:
  - `enum ErrorCode { ConnectionFailed, TlsError, AuthFailed, NotAuthorized, Timeout, InvalidInput, NotFound, Internal }`
  - `struct AppError { code: ErrorCode, message: String, details: Option<String>, retryable: bool }`, `type AppResult<T> = Result<T, AppError>`
  - `AppError::new(code, msg)`, `.with_details(d)`, `AppError::invalid(msg)`, `AppError::not_found(msg)`, `AppError::internal(msg)`
  - `fn classify_message(text: &str) -> Option<ErrorCode>`
  - `fn from_kafka(err: &KafkaError, client_error: Option<&str>) -> AppError`

- [ ] **Step 1: Написать тесты**

Создать `src-tauri/src/error.rs`, пока только с этим тестовым модулем (реализация добавляется над ним в Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::types::RDKafkaErrorCode;

    #[test]
    fn authorization_wins_over_authentication() {
        assert_eq!(classify_message("Broker: Topic authorization failed"), Some(ErrorCode::NotAuthorized));
    }

    #[test]
    fn ssl_errors_are_tls() {
        assert_eq!(
            classify_message("sasl_ssl://localhost:9094/bootstrap: SSL handshake failed: certificate verify failed"),
            Some(ErrorCode::TlsError)
        );
    }

    #[test]
    fn sasl_errors_are_auth_even_with_sasl_ssl_url() {
        assert_eq!(
            classify_message("sasl_ssl://localhost:9094/bootstrap: SASL authentication error: Authentication failed"),
            Some(ErrorCode::AuthFailed)
        );
    }

    #[test]
    fn timeouts_and_transport() {
        assert_eq!(classify_message("Local: Timed out"), Some(ErrorCode::Timeout));
        assert_eq!(classify_message("Local: Broker transport failure"), Some(ErrorCode::ConnectionFailed));
        assert_eq!(classify_message("something else"), None);
    }

    #[test]
    fn client_error_is_more_precise_than_operation_error() {
        let err = KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut);
        let app = from_kafka(&err, Some("ssl://h:9093/bootstrap: SSL handshake failed"));
        assert_eq!(app.code, ErrorCode::TlsError);
        assert!(app.details.unwrap().contains("SSL handshake failed"));
    }

    #[test]
    fn serializes_for_frontend() {
        let json = serde_json::to_value(AppError::invalid("плохо")).unwrap();
        assert_eq!(json, serde_json::json!({"code": "INVALID_INPUT", "message": "плохо", "retryable": false}));
    }
}
```

- [ ] **Step 2: Убедиться, что тесты не компилируются**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib error`
Expected: FAIL — `cannot find function classify_message`.

- [ ] **Step 3: Реализация** (вставить над тестовым модулем)

```rust
use rdkafka::error::KafkaError;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    ConnectionFailed,
    TlsError,
    AuthFailed,
    NotAuthorized,
    Timeout,
    InvalidInput,
    NotFound,
    Internal,
}

#[derive(Debug, Clone, Serialize, thiserror::Error)]
#[serde(rename_all = "camelCase")]
#[error("{message}")]
pub struct AppError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    pub retryable: bool,
}

pub type AppResult<T> = Result<T, AppError>;

impl AppError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        let retryable = matches!(code, ErrorCode::ConnectionFailed | ErrorCode::Timeout);
        Self { code, message: message.into(), details: None, retryable }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidInput, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }
}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        AppError::internal(format!("Ошибка ввода-вывода: {e}"))
    }
}

impl From<serde_json::Error> for AppError {
    fn from(e: serde_json::Error) -> Self {
        AppError::internal(format!("Ошибка JSON: {e}"))
    }
}

/// Классифицирует текст ошибки librdkafka по ключевым словам.
/// URL брокеров (`sasl_ssl://...`) отбрасываются, иначе протокол в адресе
/// путает SSL и SASL. "authoriz" проверяется раньше "authenticat".
pub fn classify_message(text: &str) -> Option<ErrorCode> {
    let t = text
        .split_whitespace()
        .filter(|w| !w.contains("://"))
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    if t.contains("authoriz") {
        Some(ErrorCode::NotAuthorized)
    } else if t.contains("ssl") || t.contains("certificate") || t.contains("handshake") {
        Some(ErrorCode::TlsError)
    } else if t.contains("sasl") || t.contains("authenticat") {
        Some(ErrorCode::AuthFailed)
    } else if t.contains("timed out") || t.contains("timeout") {
        Some(ErrorCode::Timeout)
    } else if t.contains("transport") || t.contains("all brokers down") || t.contains("resolve") || t.contains("connect") {
        Some(ErrorCode::ConnectionFailed)
    } else {
        None
    }
}

/// `client_error` — последняя ошибка из `ClientContext::error`. Она точнее
/// ошибки операции: при проблеме с TLS сама операция падает просто по таймауту.
pub fn from_kafka(err: &KafkaError, client_error: Option<&str>) -> AppError {
    let op = err.to_string();
    let code = client_error
        .and_then(classify_message)
        .or_else(|| classify_message(&op))
        .unwrap_or(ErrorCode::Internal);
    let message = match code {
        ErrorCode::ConnectionFailed => "Не удалось подключиться к брокерам",
        ErrorCode::TlsError => "Ошибка TLS-соединения",
        ErrorCode::AuthFailed => "Ошибка аутентификации",
        ErrorCode::NotAuthorized => "Недостаточно прав для операции",
        ErrorCode::Timeout => "Операция не уложилась в таймаут",
        _ => "Ошибка Kafka",
    };
    let details = match client_error {
        Some(c) => format!("{op}\n{c}"),
        None => op,
    };
    AppError::new(code, message).with_details(details)
}
```

- [ ] **Step 4: Запустить тесты**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib error`
Expected: PASS, 6 тестов.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/error.rs src-tauri/src/lib.rs
git commit -m "feat(core): AppError и классификация ошибок librdkafka"
```

---

### Task 3: Хранилище секретов

**Files:**
- Create: `src-tauri/src/secrets.rs`
- Modify: `src-tauri/src/lib.rs` (добавить `pub mod secrets;`)

**Interfaces:**
- Consumes: `AppError`, `AppResult` (Task 2)
- Produces:
  - `trait SecretStore: Send + Sync { fn get(&self, key: &str) -> AppResult<Option<String>>; fn set(&self, key: &str, value: &str) -> AppResult<()>; fn delete(&self, key: &str) -> AppResult<()>; }`
  - `struct KeyringStore;` (реальный keychain), `#[derive(Default)] struct MemoryStore` (тесты и временные секреты)
  - `fn secret_key(connection_id: &str, field: &str) -> String`

- [ ] **Step 1: Тесты** (в конец `secrets.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_store_roundtrip() {
        let s = MemoryStore::default();
        assert_eq!(s.get("a").unwrap(), None);
        s.set("a", "1").unwrap();
        assert_eq!(s.get("a").unwrap(), Some("1".to_string()));
        s.delete("a").unwrap();
        s.delete("a").unwrap(); // повторное удаление — не ошибка
        assert_eq!(s.get("a").unwrap(), None);
    }

    #[test]
    fn key_format() {
        assert_eq!(secret_key("c1", "sasl.password"), "c1/sasl.password");
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib secrets`
Expected: FAIL — `cannot find type MemoryStore`.

- [ ] **Step 3: Реализация**

```rust
use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::{AppError, AppResult};

const SERVICE: &str = "kafka-explorer";

pub trait SecretStore: Send + Sync {
    fn get(&self, key: &str) -> AppResult<Option<String>>;
    fn set(&self, key: &str, value: &str) -> AppResult<()>;
    fn delete(&self, key: &str) -> AppResult<()>;
}

pub fn secret_key(connection_id: &str, field: &str) -> String {
    format!("{connection_id}/{field}")
}

/// OS keychain: macOS Keychain, Windows Credential Manager, Secret Service.
pub struct KeyringStore;

fn keyring_err(e: keyring::Error) -> AppError {
    AppError::internal("Ошибка доступа к системному хранилищу секретов").with_details(e.to_string())
}

impl SecretStore for KeyringStore {
    fn get(&self, key: &str) -> AppResult<Option<String>> {
        let entry = keyring::Entry::new(SERVICE, key).map_err(keyring_err)?;
        match entry.get_password() {
            Ok(v) => Ok(Some(v)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(keyring_err(e)),
        }
    }

    fn set(&self, key: &str, value: &str) -> AppResult<()> {
        keyring::Entry::new(SERVICE, key)
            .and_then(|e| e.set_password(value))
            .map_err(keyring_err)
    }

    fn delete(&self, key: &str) -> AppResult<()> {
        let entry = keyring::Entry::new(SERVICE, key).map_err(keyring_err)?;
        match entry.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(keyring_err(e)),
        }
    }
}

#[derive(Default)]
pub struct MemoryStore {
    values: Mutex<HashMap<String, String>>,
}

impl SecretStore for MemoryStore {
    fn get(&self, key: &str) -> AppResult<Option<String>> {
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    fn set(&self, key: &str, value: &str) -> AppResult<()> {
        self.values.lock().unwrap().insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn delete(&self, key: &str) -> AppResult<()> {
        self.values.lock().unwrap().remove(key);
        Ok(())
    }
}
```

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib secrets`
Expected: PASS, 2 теста.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/secrets.rs src-tauri/src/lib.rs
git commit -m "feat(core): хранилище секретов (keychain + in-memory)"
```

---

### Task 4: Профили подключений и их хранилище

**Files:**
- Create: `src-tauri/src/storage/mod.rs`, `src-tauri/src/storage/connections.rs`
- Modify: `src-tauri/src/lib.rs` (добавить `pub mod storage;`)

**Interfaces:**
- Consumes: `AppError`, `AppResult`
- Produces:
  - `enum Environment { Local, Dev, Test, Stage, Prod }` (serde: lowercase)
  - `enum SaslMechanism { Plain, ScramSha256, ScramSha512 }` (serde: `"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`), `fn as_str(self) -> &'static str`
  - `struct TlsConfig { ca_location: Option<String>, verify_hostname: bool }`
  - `struct SaslConfig { mechanism: SaslMechanism, username: String }`
  - `enum SecurityConfig { Plaintext, Ssl { tls }, SaslPlaintext { sasl }, SaslSsl { tls, sasl } }` (serde: `tag = "protocol"`, значения `PLAINTEXT`/`SSL`/`SASL_PLAINTEXT`/`SASL_SSL`)
  - `struct ConnectionProfile { id, name, color, environment, read_only, bootstrap_servers, security, extra_properties: BTreeMap<String,String>, request_timeout_ms: u64 }` (camelCase)
  - `fn parse_bootstrap(s: &str) -> AppResult<Vec<(String, u16)>>`, `fn validate_profile(p: &ConnectionProfile) -> AppResult<()>`
  - `struct ConnectionStore` с методами `open(dir: &Path) -> AppResult<Self>`, `list() -> Vec<ConnectionProfile>`, `get(id) -> AppResult<ConnectionProfile>`, `upsert(p) -> AppResult<ConnectionProfile>`, `delete(id) -> AppResult<()>`
  - `#[cfg(test)] pub fn sample_profile(id: &str) -> ConnectionProfile` — фикстура для unit-тестов других модулей

- [ ] **Step 1: Тесты** (в конец `connections.rs`)

```rust
#[cfg(test)]
pub fn sample_profile(id: &str) -> ConnectionProfile {
    ConnectionProfile {
        id: id.to_string(),
        name: format!("conn-{id}"),
        color: "#4f8cff".into(),
        environment: Environment::Dev,
        read_only: false,
        bootstrap_servers: "localhost:9092".into(),
        security: SecurityConfig::Plaintext,
        extra_properties: BTreeMap::new(),
        request_timeout_ms: 30_000,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn parse_bootstrap_variants() {
        assert_eq!(
            parse_bootstrap(" a:9092, b:9093 ,").unwrap(),
            vec![("a".to_string(), 9092), ("b".to_string(), 9093)]
        );
        assert_eq!(parse_bootstrap("[::1]:9092").unwrap(), vec![("::1".to_string(), 9092)]);
        assert_eq!(parse_bootstrap("host").unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(parse_bootstrap("host:abc").unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(parse_bootstrap(" , ").unwrap_err().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn persists_and_reloads() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        let mut b = sample_profile("b");
        b.security = SecurityConfig::SaslSsl {
            tls: TlsConfig { ca_location: Some("/ca.pem".into()), verify_hostname: false },
            sasl: SaslConfig { mechanism: SaslMechanism::ScramSha512, username: "alice".into() },
        };
        store.upsert(b.clone()).unwrap();

        let reopened = ConnectionStore::open(dir.path()).unwrap();
        assert_eq!(reopened.list().len(), 2);
        assert_eq!(reopened.get("b").unwrap(), b);
    }

    #[test]
    fn upsert_replaces_and_delete_removes() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        let mut a2 = sample_profile("a");
        a2.name = "renamed".into();
        store.upsert(a2).unwrap();
        assert_eq!(store.list().len(), 1);
        assert_eq!(store.get("a").unwrap().name, "renamed");
        store.delete("a").unwrap();
        assert_eq!(store.get("a").unwrap_err().code, ErrorCode::NotFound);
        assert_eq!(store.delete("a").unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn validation_rejects_bad_profiles() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        let mut p = sample_profile("a");
        p.name = "  ".into();
        assert_eq!(store.upsert(p).unwrap_err().code, ErrorCode::InvalidInput);
        let mut p = sample_profile("a");
        p.bootstrap_servers = "nope".into();
        assert_eq!(store.upsert(p).unwrap_err().code, ErrorCode::InvalidInput);
        assert!(store.list().is_empty());
    }

    #[test]
    fn json_shape_matches_frontend_contract() {
        let mut p = sample_profile("x");
        p.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig { mechanism: SaslMechanism::ScramSha256, username: "u".into() },
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["bootstrapServers"], "localhost:9092");
        assert_eq!(v["environment"], "dev");
        assert_eq!(v["security"]["protocol"], "SASL_PLAINTEXT");
        assert_eq!(v["security"]["sasl"]["mechanism"], "SCRAM-SHA-256");
        assert_eq!(v["requestTimeoutMs"], 30000);
    }

    #[test]
    fn file_contains_schema_version_and_no_password_field() {
        let dir = tempfile::tempdir().unwrap();
        let store = ConnectionStore::open(dir.path()).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        let raw = std::fs::read_to_string(dir.path().join("connections.json")).unwrap();
        assert!(raw.contains("\"schemaVersion\": 1"));
        assert!(!raw.to_lowercase().contains("password"));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

`src-tauri/src/storage/mod.rs`:
```rust
pub mod connections;
```
Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib storage`
Expected: FAIL — не найдены `ConnectionProfile`, `ConnectionStore`.

- [ ] **Step 3: Реализация** (над тестами в `connections.rs`)

```rust
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Environment {
    Local,
    Dev,
    Test,
    Stage,
    Prod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaslMechanism {
    #[serde(rename = "PLAIN")]
    Plain,
    #[serde(rename = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(rename = "SCRAM-SHA-512")]
    ScramSha512,
}

impl SaslMechanism {
    pub fn as_str(self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_request_timeout() -> u64 {
    30_000
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsConfig {
    #[serde(default)]
    pub ca_location: Option<String>,
    #[serde(default = "default_true")]
    pub verify_hostname: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SaslConfig {
    pub mechanism: SaslMechanism,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "protocol", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityConfig {
    Plaintext,
    Ssl { tls: TlsConfig },
    SaslPlaintext { sasl: SaslConfig },
    SaslSsl { tls: TlsConfig, sasl: SaslConfig },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionProfile {
    pub id: String,
    pub name: String,
    pub color: String,
    pub environment: Environment,
    pub read_only: bool,
    pub bootstrap_servers: String,
    pub security: SecurityConfig,
    #[serde(default)]
    pub extra_properties: BTreeMap<String, String>,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_ms: u64,
}

pub fn parse_bootstrap(s: &str) -> AppResult<Vec<(String, u16)>> {
    let mut out = Vec::new();
    for raw in s.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        let (host, port) = raw
            .rsplit_once(':')
            .ok_or_else(|| AppError::invalid(format!("Адрес «{raw}» должен быть в формате host:port")))?;
        let port: u16 = port
            .parse()
            .map_err(|_| AppError::invalid(format!("Некорректный порт в адресе «{raw}»")))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if host.is_empty() {
            return Err(AppError::invalid(format!("Пустой хост в адресе «{raw}»")));
        }
        out.push((host.to_string(), port));
    }
    if out.is_empty() {
        return Err(AppError::invalid("Укажите хотя бы один bootstrap-сервер"));
    }
    Ok(out)
}

pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
    if p.id.trim().is_empty() {
        return Err(AppError::invalid("У подключения нет идентификатора"));
    }
    if p.name.trim().is_empty() {
        return Err(AppError::invalid("Имя подключения не может быть пустым"));
    }
    parse_bootstrap(&p.bootstrap_servers)?;
    if let SecurityConfig::SaslPlaintext { sasl } | SecurityConfig::SaslSsl { sasl, .. } = &p.security {
        if sasl.username.trim().is_empty() {
            return Err(AppError::invalid("Укажите имя пользователя SASL"));
        }
    }
    if p.request_timeout_ms < 1_000 {
        return Err(AppError::invalid("Таймаут запроса должен быть не меньше 1000 мс"));
    }
    Ok(())
}

const CURRENT_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConnectionsFile {
    schema_version: u32,
    connections: Vec<ConnectionProfile>,
}

pub struct ConnectionStore {
    path: PathBuf,
    cache: RwLock<Vec<ConnectionProfile>>,
}

impl ConnectionStore {
    pub fn open(dir: &Path) -> AppResult<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join("connections.json");
        let connections = if path.exists() {
            let file: ConnectionsFile = serde_json::from_slice(&fs::read(&path)?)?;
            if file.schema_version > CURRENT_SCHEMA_VERSION {
                return Err(AppError::internal(format!(
                    "connections.json создан более новой версией приложения (schemaVersion {})",
                    file.schema_version
                )));
            }
            file.connections
        } else {
            Vec::new()
        };
        Ok(Self { path, cache: RwLock::new(connections) })
    }

    pub fn list(&self) -> Vec<ConnectionProfile> {
        self.cache.read().unwrap().clone()
    }

    pub fn get(&self, id: &str) -> AppResult<ConnectionProfile> {
        self.cache
            .read()
            .unwrap()
            .iter()
            .find(|c| c.id == id)
            .cloned()
            .ok_or_else(|| AppError::not_found(format!("Подключение {id} не найдено")))
    }

    pub fn upsert(&self, profile: ConnectionProfile) -> AppResult<ConnectionProfile> {
        validate_profile(&profile)?;
        let mut cache = self.cache.write().unwrap();
        let mut next = cache.clone();
        match next.iter_mut().find(|c| c.id == profile.id) {
            Some(slot) => *slot = profile.clone(),
            None => next.push(profile.clone()),
        }
        self.persist(&next)?;
        *cache = next;
        Ok(profile)
    }

    pub fn delete(&self, id: &str) -> AppResult<()> {
        let mut cache = self.cache.write().unwrap();
        if !cache.iter().any(|c| c.id == id) {
            return Err(AppError::not_found(format!("Подключение {id} не найдено")));
        }
        let next: Vec<_> = cache.iter().filter(|c| c.id != id).cloned().collect();
        self.persist(&next)?;
        *cache = next;
        Ok(())
    }

    /// Запись через временный файл + rename, чтобы не оставить битый JSON.
    fn persist(&self, list: &[ConnectionProfile]) -> AppResult<()> {
        let file = ConnectionsFile { schema_version: CURRENT_SCHEMA_VERSION, connections: list.to_vec() };
        let tmp = self.path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(&file)?)?;
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}
```

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib storage`
Expected: PASS, 6 тестов.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/storage src-tauri/src/lib.rs
git commit -m "feat(core): профили подключений и JSON-хранилище"
```

---

### Task 5: Конфигурация клиента, контекст ошибок, пул клиентов

**Files:**
- Create: `src-tauri/src/kafka/mod.rs`, `src-tauri/src/kafka/config.rs`, `src-tauri/src/kafka/context.rs`, `src-tauri/src/kafka/pool.rs`
- Modify: `src-tauri/src/lib.rs` (добавить `pub mod kafka;`)

**Interfaces:**
- Consumes: `ConnectionProfile`, `SecurityConfig`, `SecretStore`, `secret_key`, `from_kafka`
- Produces:
  - `const SASL_PASSWORD: &str = "sasl.password"`
  - `fn build_client_config(p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<ClientConfig>`
  - `#[derive(Clone, Default)] struct CapturingContext` с методами `last_error() -> Option<String>`, `clear()`; реализует `ClientContext + ConsumerContext`
  - `struct KafkaHandles { admin: AdminClient<CapturingContext>, consumer: BaseConsumer<CapturingContext>, ctx: CapturingContext, timeout: Duration }`, `KafkaHandles::create(p, secrets) -> AppResult<Self>`, `.kafka_err(e: KafkaError) -> AppError`
  - `#[derive(Default)] struct ClientPool` с методами `get(&self, p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<Arc<KafkaHandles>>`, `evict(&self, id: &str)`

- [ ] **Step 1: Тесты** (в конец `config.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::{secret_key, MemoryStore, SecretStore};
    use crate::storage::connections::{sample_profile, SaslConfig, SaslMechanism, SecurityConfig, TlsConfig};

    #[test]
    fn plaintext_with_extra_properties_overriding() {
        let mut p = sample_profile("c1");
        p.extra_properties.insert("client.id".into(), "custom".into());
        let c = build_client_config(&p, &MemoryStore::default()).unwrap();
        assert_eq!(c.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(c.get("security.protocol"), Some("plaintext"));
        assert_eq!(c.get("client.id"), Some("custom"));
    }

    #[test]
    fn ssl_uses_probe_when_no_ca_and_can_disable_hostname_check() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::Ssl { tls: TlsConfig { ca_location: None, verify_hostname: false } };
        let c = build_client_config(&p, &MemoryStore::default()).unwrap();
        assert_eq!(c.get("security.protocol"), Some("ssl"));
        assert_eq!(c.get("ssl.ca.location"), Some("probe"));
        assert_eq!(c.get("ssl.endpoint.identification.algorithm"), Some("none"));
    }

    #[test]
    fn sasl_ssl_reads_password_from_secrets() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::SaslSsl {
            tls: TlsConfig { ca_location: Some("/ca.pem".into()), verify_hostname: true },
            sasl: SaslConfig { mechanism: SaslMechanism::ScramSha512, username: "alice".into() },
        };
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("c1", SASL_PASSWORD), "pw").unwrap();
        let c = build_client_config(&p, &secrets).unwrap();
        assert_eq!(c.get("security.protocol"), Some("sasl_ssl"));
        assert_eq!(c.get("ssl.ca.location"), Some("/ca.pem"));
        assert_eq!(c.get("ssl.endpoint.identification.algorithm"), Some("https"));
        assert_eq!(c.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
        assert_eq!(c.get("sasl.username"), Some("alice"));
        assert_eq!(c.get("sasl.password"), Some("pw"));
    }

    #[test]
    fn sasl_without_password_is_invalid_input() {
        let mut p = sample_profile("c1");
        p.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig { mechanism: SaslMechanism::Plain, username: "alice".into() },
        };
        let err = build_client_config(&p, &MemoryStore::default()).unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidInput);
    }
}
```

И в конец `pool.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    #[test]
    fn pool_reuses_handles_until_evicted() {
        let pool = ClientPool::default();
        let secrets = MemoryStore::default();
        let p = sample_profile("c1");
        let a = pool.get(&p, &secrets).unwrap();
        let b = pool.get(&p, &secrets).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        pool.evict("c1");
        let c = pool.get(&p, &secrets).unwrap();
        assert!(!Arc::ptr_eq(&a, &c));
        assert_eq!(c.timeout, Duration::from_millis(30_000));
    }
}
```
(Создание клиента librdkafka не подключается к брокеру сразу, поэтому тест работает без Kafka.)

- [ ] **Step 2: Убедиться в падении**

`src-tauri/src/kafka/mod.rs`:
```rust
pub mod config;
pub mod context;
pub mod pool;
```
Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka`
Expected: FAIL — не найдены `build_client_config`, `ClientPool`.

- [ ] **Step 3: Реализация `config.rs`** (над тестами)

```rust
use rdkafka::ClientConfig;

use crate::error::{AppError, AppResult};
use crate::secrets::{secret_key, SecretStore};
use crate::storage::connections::{ConnectionProfile, SaslConfig, SecurityConfig, TlsConfig};

pub const SASL_PASSWORD: &str = "sasl.password";

pub fn build_client_config(p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<ClientConfig> {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", p.bootstrap_servers.trim())
        .set("client.id", "kafka-explorer")
        .set("socket.connection.setup.timeout.ms", "10000")
        .set("security.protocol", protocol_name(&p.security));
    match &p.security {
        SecurityConfig::Plaintext => {}
        SecurityConfig::Ssl { tls } => apply_tls(&mut c, tls),
        SecurityConfig::SaslPlaintext { sasl } => apply_sasl(&mut c, &p.id, sasl, secrets)?,
        SecurityConfig::SaslSsl { tls, sasl } => {
            apply_tls(&mut c, tls);
            apply_sasl(&mut c, &p.id, sasl, secrets)?;
        }
    }
    // Дополнительные свойства применяются последними и могут переопределить всё выше.
    for (k, v) in &p.extra_properties {
        c.set(k, v);
    }
    Ok(c)
}

fn protocol_name(s: &SecurityConfig) -> &'static str {
    match s {
        SecurityConfig::Plaintext => "plaintext",
        SecurityConfig::Ssl { .. } => "ssl",
        SecurityConfig::SaslPlaintext { .. } => "sasl_plaintext",
        SecurityConfig::SaslSsl { .. } => "sasl_ssl",
    }
}

fn apply_tls(c: &mut ClientConfig, tls: &TlsConfig) {
    // "probe" — librdkafka сам ищет системные CA (vendored OpenSSL не знает путей ОС).
    let ca = tls.ca_location.as_deref().filter(|s| !s.trim().is_empty()).unwrap_or("probe");
    c.set("ssl.ca.location", ca);
    c.set(
        "ssl.endpoint.identification.algorithm",
        if tls.verify_hostname { "https" } else { "none" },
    );
}

fn apply_sasl(c: &mut ClientConfig, id: &str, sasl: &SaslConfig, secrets: &dyn SecretStore) -> AppResult<()> {
    let password = secrets
        .get(&secret_key(id, SASL_PASSWORD))?
        .ok_or_else(|| AppError::invalid("Не задан пароль SASL для подключения"))?;
    c.set("sasl.mechanism", sasl.mechanism.as_str())
        .set("sasl.username", &sasl.username)
        .set("sasl.password", password);
    Ok(())
}
```

- [ ] **Step 4: Реализация `context.rs`**

```rust
use std::sync::{Arc, Mutex};

use rdkafka::client::ClientContext;
use rdkafka::consumer::ConsumerContext;
use rdkafka::error::KafkaError;

/// Запоминает последнюю ошибку клиента (TLS, SASL, транспорт),
/// чтобы показать пользователю реальную причину, а не просто «таймаут».
#[derive(Clone, Default)]
pub struct CapturingContext {
    last_error: Arc<Mutex<Option<String>>>,
}

impl CapturingContext {
    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        *self.last_error.lock().unwrap() = None;
    }
}

impl ClientContext for CapturingContext {
    fn error(&self, error: KafkaError, reason: &str) {
        tracing::warn!(%error, reason, "kafka client error");
        *self.last_error.lock().unwrap() = Some(format!("{error}: {reason}"));
    }
}

impl ConsumerContext for CapturingContext {}
```

- [ ] **Step 5: Реализация `pool.rs`** (над тестами)

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::consumer::BaseConsumer;
use rdkafka::error::KafkaError;

use crate::error::{from_kafka, AppError, AppResult};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;

/// Клиенты одного подключения: admin — для DescribeConfigs и т. п.,
/// consumer — для метаданных и ListOffsets (никогда не подписывается и не коммитит).
pub struct KafkaHandles {
    pub admin: AdminClient<CapturingContext>,
    pub consumer: BaseConsumer<CapturingContext>,
    pub ctx: CapturingContext,
    pub timeout: Duration,
}

impl KafkaHandles {
    pub fn create(p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<Self> {
        let base = build_client_config(p, secrets)?;
        let ctx = CapturingContext::default();
        let admin: AdminClient<CapturingContext> =
            base.create_with_context(ctx.clone()).map_err(|e| from_kafka(&e, None))?;
        let mut consumer_cfg = base.clone();
        consumer_cfg
            .set("group.id", "kafka-explorer-metadata")
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false");
        let consumer: BaseConsumer<CapturingContext> =
            consumer_cfg.create_with_context(ctx.clone()).map_err(|e| from_kafka(&e, None))?;
        Ok(Self { admin, consumer, ctx, timeout: Duration::from_millis(p.request_timeout_ms) })
    }

    pub fn kafka_err(&self, e: KafkaError) -> AppError {
        from_kafka(&e, self.ctx.last_error().as_deref())
    }
}

#[derive(Default)]
pub struct ClientPool {
    inner: Mutex<HashMap<String, Arc<KafkaHandles>>>,
}

impl ClientPool {
    pub fn get(&self, p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<Arc<KafkaHandles>> {
        let mut map = self.inner.lock().unwrap();
        if let Some(h) = map.get(&p.id) {
            return Ok(h.clone());
        }
        let handles = Arc::new(KafkaHandles::create(p, secrets)?);
        map.insert(p.id.clone(), handles.clone());
        Ok(handles)
    }

    pub fn evict(&self, id: &str) {
        self.inner.lock().unwrap().remove(id);
    }
}
```

- [ ] **Step 6: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka`
Expected: PASS, 5 тестов.

- [ ] **Step 7: Commit**

```bash
git add src-tauri/src/kafka src-tauri/src/lib.rs
git commit -m "feat(kafka): сборка ClientConfig из профиля, контекст ошибок, пул клиентов"
```

---

### Task 6: Метаданные, обзор кластера, конфиги ресурсов + каркас интеграционных тестов

**Files:**
- Create: `src-tauri/src/kafka/metadata.rs`, `src-tauri/src/kafka/cluster.rs`, `src-tauri/src/kafka/configs.rs`
- Create: `src-tauri/tests/common/mod.rs`, `src-tauri/tests/it_cluster.rs`
- Modify: `src-tauri/src/kafka/mod.rs`

**Interfaces:**
- Consumes: `KafkaHandles` (Task 5)
- Produces:
  - `struct BrokerInfo { id: i32, host: String, port: i32 }`
  - `struct PartitionMeta { id, leader: i32, replicas: Vec<i32>, isr: Vec<i32> }` с методами `under_replicated()`, `offline()`
  - `struct TopicMeta { name: String, partitions: Vec<PartitionMeta> }` с методами `internal()`, `replication_factor()`
  - `fn is_internal(name: &str) -> bool`, `fn convert(md: &Metadata) -> (Vec<BrokerInfo>, Vec<TopicMeta>)`
  - `struct ClusterOverview { cluster_id: Option<String>, controller_id: Option<i32>, brokers, topic_count, internal_topic_count, partition_count, under_replicated_partitions, offline_partitions }`
  - `fn summarize_cluster(brokers, topics: &[TopicMeta], cluster_id, controller_id) -> ClusterOverview`, `fn cluster_overview(h: &KafkaHandles) -> AppResult<ClusterOverview>`
  - `struct ConfigEntryDto { name, value: Option<String>, source: String, is_default, is_read_only, is_sensitive }`
  - `async fn describe_configs(h: &KafkaHandles, spec: ResourceSpecifier<'_>) -> AppResult<Vec<ConfigEntryDto>>`
  - Тестовые хелперы `tests/common`: `bootstrap()`, `sasl_bootstrap()`, `plaintext_profile(bs)`, `unique_topic(prefix)`, `create_topic(bs, name, partitions)`, `produce(bs, topic, &[TestMsg])`

- [ ] **Step 1: Unit-тесты** (в конец `cluster.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::metadata::{PartitionMeta, TopicMeta};

    fn part(id: i32, leader: i32, replicas: &[i32], isr: &[i32]) -> PartitionMeta {
        PartitionMeta { id, leader, replicas: replicas.to_vec(), isr: isr.to_vec() }
    }

    #[test]
    fn summarizes_partitions_and_internal_topics() {
        let topics = vec![
            TopicMeta { name: "orders".into(), partitions: vec![part(0, 1, &[1, 2], &[1, 2]), part(1, 2, &[1, 2], &[2])] },
            TopicMeta { name: "__consumer_offsets".into(), partitions: vec![part(0, -1, &[1], &[])] },
        ];
        let brokers = vec![BrokerInfo { id: 1, host: "a".into(), port: 9092 }];
        let o = summarize_cluster(brokers, &topics, Some("cid".into()), Some(1));
        assert_eq!(o.topic_count, 2);
        assert_eq!(o.internal_topic_count, 1);
        assert_eq!(o.partition_count, 3);
        assert_eq!(o.under_replicated_partitions, 2); // orders/1 и __consumer_offsets/0
        assert_eq!(o.offline_partitions, 1);
        assert_eq!(o.cluster_id.as_deref(), Some("cid"));
    }

    #[test]
    fn replication_factor_and_internal_detection() {
        let t = TopicMeta { name: "_schemas".into(), partitions: vec![part(0, 1, &[1, 2, 3], &[1, 2, 3])] };
        assert!(t.internal());
        assert_eq!(t.replication_factor(), 3);
        assert!(!crate::kafka::metadata::is_internal("orders"));
        assert!(crate::kafka::metadata::is_internal("_confluent-metrics"));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

В `kafka/mod.rs` добавить:
```rust
pub mod cluster;
pub mod configs;
pub mod metadata;
```
Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib cluster`
Expected: FAIL — не найдены `summarize_cluster`, `TopicMeta`.

- [ ] **Step 3: Реализация `metadata.rs`**

```rust
use rdkafka::metadata::Metadata;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionMeta {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

impl PartitionMeta {
    pub fn under_replicated(&self) -> bool {
        self.isr.len() < self.replicas.len()
    }

    pub fn offline(&self) -> bool {
        self.leader < 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TopicMeta {
    pub name: String,
    pub partitions: Vec<PartitionMeta>,
}

impl TopicMeta {
    pub fn internal(&self) -> bool {
        is_internal(&self.name)
    }

    pub fn replication_factor(&self) -> usize {
        self.partitions.iter().map(|p| p.replicas.len()).max().unwrap_or(0)
    }
}

pub fn is_internal(name: &str) -> bool {
    name.starts_with("__") || name.starts_with("_confluent") || name == "_schemas"
}

pub fn convert(md: &Metadata) -> (Vec<BrokerInfo>, Vec<TopicMeta>) {
    let brokers = md
        .brokers()
        .iter()
        .map(|b| BrokerInfo { id: b.id(), host: b.host().to_string(), port: b.port() })
        .collect();
    let topics = md
        .topics()
        .iter()
        .map(|t| TopicMeta {
            name: t.name().to_string(),
            partitions: t
                .partitions()
                .iter()
                .map(|p| PartitionMeta {
                    id: p.id(),
                    leader: p.leader(),
                    replicas: p.replicas().to_vec(),
                    isr: p.isr().to_vec(),
                })
                .collect(),
        })
        .collect();
    (brokers, topics)
}
```

- [ ] **Step 4: Реализация `cluster.rs`** (над тестами)

```rust
use std::ffi::{c_void, CStr};

use rdkafka::consumer::Consumer;
use serde::Serialize;

use crate::error::AppResult;
use crate::kafka::metadata::{convert, BrokerInfo, TopicMeta};
use crate::kafka::pool::KafkaHandles;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterOverview {
    pub cluster_id: Option<String>,
    pub controller_id: Option<i32>,
    pub brokers: Vec<BrokerInfo>,
    pub topic_count: usize,
    pub internal_topic_count: usize,
    pub partition_count: usize,
    pub under_replicated_partitions: usize,
    pub offline_partitions: usize,
}

pub fn summarize_cluster(
    mut brokers: Vec<BrokerInfo>,
    topics: &[TopicMeta],
    cluster_id: Option<String>,
    controller_id: Option<i32>,
) -> ClusterOverview {
    brokers.sort_by_key(|b| b.id);
    let partitions = topics.iter().flat_map(|t| t.partitions.iter());
    ClusterOverview {
        cluster_id,
        controller_id,
        brokers,
        topic_count: topics.len(),
        internal_topic_count: topics.iter().filter(|t| t.internal()).count(),
        partition_count: partitions.clone().count(),
        under_replicated_partitions: partitions.clone().filter(|p| p.under_replicated()).count(),
        offline_partitions: partitions.filter(|p| p.offline()).count(),
    }
}

pub fn cluster_overview(h: &KafkaHandles) -> AppResult<ClusterOverview> {
    let md = h.consumer.fetch_metadata(None, h.timeout).map_err(|e| h.kafka_err(e))?;
    let (brokers, topics) = convert(&md);
    let (cluster_id, controller_id) = native_cluster_info(h);
    Ok(summarize_cluster(brokers, &topics, cluster_id, controller_id))
}

/// rust-rdkafka не оборачивает rd_kafka_clusterid/rd_kafka_controllerid — вызываем C API.
/// Если в вашей версии rdkafka нет реэкспорта `rdkafka::bindings`, добавьте в Cargo.toml
/// `rdkafka-sys` той же версии, что в Cargo.lock, и замените путь на `rdkafka_sys::bindings`.
fn native_cluster_info(h: &KafkaHandles) -> (Option<String>, Option<i32>) {
    use rdkafka::bindings as rd;
    let rk = h.consumer.client().native_ptr();
    let ms = h.timeout.as_millis().min(i32::MAX as u128) as i32;
    unsafe {
        let ptr = rd::rd_kafka_clusterid(rk, ms);
        let cluster_id = if ptr.is_null() {
            None
        } else {
            let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
            rd::rd_kafka_mem_free(rk, ptr as *mut c_void);
            Some(s)
        };
        let controller = rd::rd_kafka_controllerid(rk, ms);
        (cluster_id, (controller >= 0).then_some(controller))
    }
}
```

- [ ] **Step 5: Реализация `configs.rs`**

```rust
use rdkafka::admin::{AdminOptions, ResourceSpecifier};
use rdkafka::error::KafkaError;
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::kafka::pool::KafkaHandles;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigEntryDto {
    pub name: String,
    pub value: Option<String>,
    /// Debug-имя ConfigSource: Default, DynamicTopic, DynamicBroker, DynamicDefaultBroker, StaticBroker, Unknown.
    pub source: String,
    pub is_default: bool,
    pub is_read_only: bool,
    pub is_sensitive: bool,
}

pub async fn describe_configs(h: &KafkaHandles, spec: ResourceSpecifier<'_>) -> AppResult<Vec<ConfigEntryDto>> {
    let opts = AdminOptions::new().request_timeout(Some(h.timeout));
    let mut results = h.admin.describe_configs(&[spec], &opts).await.map_err(|e| h.kafka_err(e))?;
    let resource = results
        .pop()
        .ok_or_else(|| AppError::internal("Пустой ответ DescribeConfigs"))?
        .map_err(|code| h.kafka_err(KafkaError::AdminOp(code)))?;
    let mut entries: Vec<ConfigEntryDto> = resource
        .entries
        .into_iter()
        .map(|e| ConfigEntryDto {
            name: e.name,
            value: e.value,
            source: format!("{:?}", e.source),
            is_default: e.is_default,
            is_read_only: e.is_read_only,
            is_sensitive: e.is_sensitive,
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(entries)
}
```

- [ ] **Step 6: Unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib cluster`
Expected: PASS, 2 теста.

- [ ] **Step 7: Хелперы интеграционных тестов**

`src-tauri/tests/common/mod.rs`:
```rust
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use kafka_explorer_lib::storage::connections::{ConnectionProfile, Environment, SecurityConfig};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

/// Интеграционные тесты выполняются, только если задан KE_IT_BOOTSTRAP (см. dev/docker-compose.yml).
pub fn bootstrap() -> Option<String> {
    let v = std::env::var("KE_IT_BOOTSTRAP").ok();
    if v.is_none() {
        eprintln!("skip: KE_IT_BOOTSTRAP не задан");
    }
    v
}

pub fn sasl_bootstrap() -> Option<String> {
    let v = std::env::var("KE_IT_SASL_BOOTSTRAP").ok();
    if v.is_none() {
        eprintln!("skip: KE_IT_SASL_BOOTSTRAP не задан");
    }
    v
}

pub fn plaintext_profile(bs: &str) -> ConnectionProfile {
    ConnectionProfile {
        id: "it".into(),
        name: "it".into(),
        color: "#888888".into(),
        environment: Environment::Local,
        read_only: false,
        bootstrap_servers: bs.into(),
        security: SecurityConfig::Plaintext,
        extra_properties: BTreeMap::new(),
        request_timeout_ms: 10_000,
    }
}

pub fn unique_topic(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::new_v4().simple())
}

pub async fn create_topic(bs: &str, name: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> =
        ClientConfig::new().set("bootstrap.servers", bs).create().unwrap();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin
        .create_topics(&[NewTopic::new(name, partitions, TopicReplication::Fixed(1))], &opts)
        .await
        .unwrap();
    for r in results {
        r.unwrap();
    }
    // даём брокеру распространить метаданные
    tokio::time::sleep(Duration::from_millis(500)).await;
}

pub struct TestMsg<'a> {
    pub partition: i32,
    pub key: Option<&'a str>,
    pub value: &'a str,
    pub headers: &'a [(&'a str, &'a str)],
}

pub async fn produce(bs: &str, topic: &str, msgs: &[TestMsg<'_>]) {
    let producer: FutureProducer = ClientConfig::new().set("bootstrap.servers", bs).create().unwrap();
    for m in msgs {
        let mut headers = OwnedHeaders::new();
        for (k, v) in m.headers {
            headers = headers.insert(Header { key: *k, value: Some(*v) });
        }
        let mut record: FutureRecord<str, str> =
            FutureRecord::to(topic).partition(m.partition).payload(m.value).headers(headers);
        if let Some(k) = m.key {
            record = record.key(k);
        }
        producer.send(record, Duration::from_secs(5)).await.unwrap();
    }
}
```

- [ ] **Step 8: Интеграционный тест кластера**

`src-tauri/tests/it_cluster.rs`:
```rust
mod common;

use kafka_explorer_lib::kafka::cluster::cluster_overview;
use kafka_explorer_lib::kafka::configs::describe_configs;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::admin::ResourceSpecifier;

#[tokio::test(flavor = "multi_thread")]
async fn overview_and_broker_config() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::unique_topic("it-cluster");
    common::create_topic(&bs, &topic, 2).await;

    let h = KafkaHandles::create(&common::plaintext_profile(&bs), &MemoryStore::default()).unwrap();
    let h = std::sync::Arc::new(h);
    let h2 = h.clone();
    let o = tokio::task::spawn_blocking(move || cluster_overview(&h2)).await.unwrap().unwrap();
    assert_eq!(o.brokers.len(), 1);
    assert!(o.cluster_id.is_some());
    assert_eq!(o.controller_id, Some(1));
    assert!(o.partition_count >= 2);

    let entries = describe_configs(&h, ResourceSpecifier::Broker(1)).await.unwrap();
    assert!(entries.iter().any(|e| e.name == "log.retention.hours"));
}
```

- [ ] **Step 9: Запустить интеграционный тест**

Run: `docker compose -f dev/docker-compose.yml up -d && KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_cluster`
Expected: PASS. Без переменной тест тоже проходит, но выводит `skip`.
Если `controller_id` в KRaft приходит `None`: librdkafka для KRaft-кластеров возвращает id брокера, через который пришли метаданные, и в однонодовом стенде это 1. Если нет — заменить проверку на `assert!(o.controller_id.is_some())` и записать этот факт в README.

- [ ] **Step 10: Commit**

```bash
git add src-tauri/src/kafka src-tauri/tests src-tauri/Cargo.toml
git commit -m "feat(kafka): метаданные, обзор кластера, DescribeConfigs + каркас интеграционных тестов"
```

---

### Task 7: Проверка подключения (test connection)

**Files:**
- Create: `src-tauri/src/kafka/connectivity.rs`
- Create: `src-tauri/tests/it_connectivity.rs`
- Modify: `src-tauri/src/kafka/mod.rs` (добавить `pub mod connectivity;`)

**Interfaces:**
- Consumes: `parse_bootstrap`, `KafkaHandles::create`, `AppError`, `ErrorCode`
- Produces:
  - `struct TestStep { name: String, ok: bool, message: String, error: Option<AppError> }`
  - `struct TestReport { ok: bool, steps: Vec<TestStep> }`
  - `fn test_connection(p: &ConnectionProfile, secrets: &dyn SecretStore) -> TestReport` — шаги `config` → `tcp` → `kafka`

- [ ] **Step 1: Unit-тесты** (в конец `connectivity.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    #[test]
    fn invalid_bootstrap_fails_on_config_step() {
        let mut p = sample_profile("c");
        p.bootstrap_servers = "no-port".into();
        let r = test_connection(&p, &MemoryStore::default());
        assert!(!r.ok);
        assert_eq!(r.steps.len(), 1);
        assert_eq!(r.steps[0].name, "config");
        assert_eq!(r.steps[0].error.as_ref().unwrap().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn closed_port_fails_on_tcp_step() {
        let mut p = sample_profile("c");
        p.bootstrap_servers = "127.0.0.1:1".into();
        let r = test_connection(&p, &MemoryStore::default());
        assert!(!r.ok);
        let last = r.steps.last().unwrap();
        assert_eq!(last.name, "tcp");
        assert_eq!(last.error.as_ref().unwrap().code, ErrorCode::ConnectionFailed);
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib connectivity`
Expected: FAIL — не найдена `test_connection`.

- [ ] **Step 3: Реализация** (над тестами)

```rust
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use rdkafka::consumer::Consumer;
use serde::Serialize;

use crate::error::{AppError, AppResult, ErrorCode};
use crate::kafka::pool::KafkaHandles;
use crate::secrets::SecretStore;
use crate::storage::connections::{parse_bootstrap, ConnectionProfile};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestStep {
    pub name: String,
    pub ok: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TestReport {
    pub ok: bool,
    pub steps: Vec<TestStep>,
}

const TCP_TIMEOUT: Duration = Duration::from_secs(5);
const KAFKA_TIMEOUT: Duration = Duration::from_secs(10);

/// Пошаговая проверка: конфигурация → DNS+TCP → Kafka (TLS + SASL + metadata).
/// Останавливается на первом неуспешном шаге.
pub fn test_connection(p: &ConnectionProfile, secrets: &dyn SecretStore) -> TestReport {
    let mut steps = Vec::new();

    let servers = match parse_bootstrap(&p.bootstrap_servers) {
        Ok(s) => s,
        Err(e) => return failed(steps, "config", e),
    };
    steps.push(passed("config", "Конфигурация корректна"));

    match tcp_probe(&servers) {
        Ok(addr) => steps.push(passed("tcp", format!("TCP-соединение с {addr} установлено"))),
        Err(e) => return failed(steps, "tcp", e),
    }

    let handles = match KafkaHandles::create(p, secrets) {
        Ok(h) => h,
        Err(e) => return failed(steps, "kafka", e),
    };
    match handles.consumer.fetch_metadata(None, KAFKA_TIMEOUT) {
        Ok(md) => steps.push(passed(
            "kafka",
            format!("Брокеров: {}, топиков: {}", md.brokers().len(), md.topics().len()),
        )),
        Err(e) => return failed(steps, "kafka", handles.kafka_err(e)),
    }

    TestReport { ok: true, steps }
}

fn passed(name: &str, message: impl Into<String>) -> TestStep {
    TestStep { name: name.into(), ok: true, message: message.into(), error: None }
}

fn failed(mut steps: Vec<TestStep>, name: &str, error: AppError) -> TestReport {
    steps.push(TestStep { name: name.into(), ok: false, message: error.message.clone(), error: Some(error) });
    TestReport { ok: false, steps }
}

fn tcp_probe(servers: &[(String, u16)]) -> AppResult<String> {
    let mut last = String::new();
    for (host, port) in servers {
        let addrs = match (host.as_str(), *port).to_socket_addrs() {
            Ok(a) => a,
            Err(e) => {
                last = format!("{host}: DNS: {e}");
                continue;
            }
        };
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, TCP_TIMEOUT) {
                Ok(_) => return Ok(format!("{host}:{port}")),
                Err(e) => last = format!("{addr}: {e}"),
            }
        }
    }
    Err(AppError::new(ErrorCode::ConnectionFailed, "Ни один bootstrap-сервер не доступен по TCP").with_details(last))
}
```

- [ ] **Step 4: Unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib connectivity`
Expected: PASS, 2 теста.

- [ ] **Step 5: Интеграционные тесты (PLAINTEXT и SASL PLAIN)**

`src-tauri/tests/it_connectivity.rs`:
```rust
mod common;

use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::config::SASL_PASSWORD;
use kafka_explorer_lib::kafka::connectivity::test_connection;
use kafka_explorer_lib::secrets::{secret_key, MemoryStore, SecretStore};
use kafka_explorer_lib::storage::connections::{SaslConfig, SaslMechanism, SecurityConfig};

#[test]
fn plaintext_succeeds() {
    let Some(bs) = common::bootstrap() else { return };
    let r = test_connection(&common::plaintext_profile(&bs), &MemoryStore::default());
    assert!(r.ok, "{r:?}");
    assert_eq!(r.steps.iter().map(|s| s.name.as_str()).collect::<Vec<_>>(), ["config", "tcp", "kafka"]);
}

fn sasl_profile(bs: &str) -> kafka_explorer_lib::storage::connections::ConnectionProfile {
    let mut p = common::plaintext_profile(bs);
    p.security = SecurityConfig::SaslPlaintext {
        sasl: SaslConfig { mechanism: SaslMechanism::Plain, username: "tester".into() },
    };
    p
}

#[test]
fn sasl_plain_succeeds_with_right_password() {
    let Some(bs) = common::sasl_bootstrap() else { return };
    let p = sasl_profile(&bs);
    let secrets = MemoryStore::default();
    secrets.set(&secret_key(&p.id, SASL_PASSWORD), "tester-secret").unwrap();
    let r = test_connection(&p, &secrets);
    assert!(r.ok, "{r:?}");
}

#[test]
fn sasl_plain_wrong_password_is_auth_failed() {
    let Some(bs) = common::sasl_bootstrap() else { return };
    let p = sasl_profile(&bs);
    let secrets = MemoryStore::default();
    secrets.set(&secret_key(&p.id, SASL_PASSWORD), "wrong").unwrap();
    let r = test_connection(&p, &secrets);
    assert!(!r.ok);
    let step = r.steps.last().unwrap();
    assert_eq!(step.name, "kafka");
    assert_eq!(step.error.as_ref().unwrap().code, ErrorCode::AuthFailed, "{step:?}");
}
```

- [ ] **Step 6: Запустить**

Run: `KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --test it_connectivity`
Expected: PASS, 3 теста. Если в `sasl_plain_wrong_password_is_auth_failed` приходит `Timeout`, посмотрите `details`: там текст из `ClientContext::error`. Добавьте его ключевые слова в `classify_message` и покройте этот текст unit-тестом в Task 2.

- [ ] **Step 7: Commit**

```bash
git add src-tauri/src/kafka src-tauri/tests
git commit -m "feat(kafka): пошаговая проверка подключения"
```

---

### Task 8: Топики — список, счётчики сообщений, детали, конфиги

**Files:**
- Create: `src-tauri/src/kafka/topics.rs`
- Create: `src-tauri/tests/it_topics.rs`
- Modify: `src-tauri/src/kafka/mod.rs` (добавить `pub mod topics;`)

**Interfaces:**
- Consumes: `KafkaHandles`, `convert`, `TopicMeta`, `CapturingContext`
- Produces:
  - `struct TopicSummary { name, internal, partition_count, replication_factor, under_replicated }`
  - `fn summarize_topics(topics: Vec<TopicMeta>) -> Vec<TopicSummary>` (сортировка по имени)
  - `fn list_topics(h: &KafkaHandles) -> AppResult<Vec<TopicSummary>>`
  - `fn message_count(low: i64, high: i64) -> i64`
  - `type PartitionBoundsMap = HashMap<(String, i32), Result<(i64, i64), String>>`
  - `fn fetch_bounds(c: &BaseConsumer<CapturingContext>, parts: &[(String, i32)], timeout: Duration) -> KafkaResult<PartitionBoundsMap>` — (low, high) по каждой партиции двумя пакетными ListOffsets; ошибка партиции (лидер недоступен, нет прав, нет ответа) возвращается как `Err(текст)` и **никогда не превращается в 0**
  - `struct TopicStats { name, message_count: Option<i64> }` (`None` — для части партиций оффсеты недоступны), `fn topic_stats(h, names: &[String]) -> AppResult<Vec<TopicStats>>`
  - `struct PartitionDetails { id, leader, replicas, isr, low: Option<i64>, high: Option<i64>, message_count: Option<i64>, offsets_error: Option<String>, under_replicated }`
  - `struct TopicDetails { name, internal, replication_factor, message_count: Option<i64> /* None, если у какой-либо партиции оффсеты недоступны */, partitions: Vec<PartitionDetails> }`, `fn topic_details(h, name: &str) -> AppResult<TopicDetails>`

- [ ] **Step 1: Unit-тесты** (в конец `topics.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::metadata::{PartitionMeta, TopicMeta};

    fn topic(name: &str, rf: usize, parts: i32, isr_short: bool) -> TopicMeta {
        let replicas: Vec<i32> = (1..=rf as i32).collect();
        TopicMeta {
            name: name.into(),
            partitions: (0..parts)
                .map(|id| PartitionMeta {
                    id,
                    leader: 1,
                    replicas: replicas.clone(),
                    isr: if isr_short && id == 0 { vec![1] } else { replicas.clone() },
                })
                .collect(),
        }
    }

    #[test]
    fn summaries_sorted_with_counts() {
        let s = summarize_topics(vec![topic("b", 3, 2, true), topic("a", 1, 4, false), topic("__x", 1, 1, false)]);
        assert_eq!(s.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(), ["__x", "a", "b"]);
        assert!(s[0].internal);
        assert_eq!(s[1].partition_count, 4);
        assert_eq!(s[2].replication_factor, 3);
        assert_eq!(s[2].under_replicated, 1);
    }

    #[test]
    fn message_count_never_negative() {
        assert_eq!(message_count(5, 12), 7);
        assert_eq!(message_count(5, 5), 0);
        assert_eq!(message_count(9, 3), 0);
    }

    #[test]
    fn combine_never_turns_errors_into_zero() {
        let ok = |v: i64| Ok::<i64, String>(v);
        let err = |m: &str| Err::<i64, String>(m.to_string());
        assert_eq!(combine(Some(&ok(3)), Some(&ok(9))), Ok((3, 9)));
        assert_eq!(combine(Some(&ok(3)), Some(&err("leader not available"))), Err("leader not available".to_string()));
        assert_eq!(combine(Some(&err("denied")), Some(&ok(9))), Err("denied".to_string()));
        assert_eq!(combine(Some(&ok(3)), None), Err(NO_OFFSET.to_string()));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib topics`
Expected: FAIL — не найдена `summarize_topics`.

- [ ] **Step 3: Реализация** (над тестами)

```rust
use std::collections::{HashMap, HashSet};
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::{Offset, TopicPartitionList};
use serde::Serialize;

use crate::error::{AppError, AppResult};
use crate::kafka::context::CapturingContext;
use crate::kafka::metadata::{convert, TopicMeta};
use crate::kafka::pool::KafkaHandles;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicSummary {
    pub name: String,
    pub internal: bool,
    pub partition_count: usize,
    pub replication_factor: usize,
    pub under_replicated: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicStats {
    pub name: String,
    /// None — для части партиций оффсеты получить не удалось.
    pub message_count: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionDetails {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low: Option<i64>,
    pub high: Option<i64>,
    pub message_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offsets_error: Option<String>,
    pub under_replicated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDetails {
    pub name: String,
    pub internal: bool,
    pub replication_factor: usize,
    /// None, если у какой-либо партиции оффсеты недоступны.
    pub message_count: Option<i64>,
    pub partitions: Vec<PartitionDetails>,
}

pub fn summarize_topics(topics: Vec<TopicMeta>) -> Vec<TopicSummary> {
    let mut out: Vec<TopicSummary> = topics
        .into_iter()
        .map(|t| TopicSummary {
            internal: t.internal(),
            partition_count: t.partitions.len(),
            replication_factor: t.replication_factor(),
            under_replicated: t.partitions.iter().filter(|p| p.under_replicated()).count(),
            name: t.name,
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

pub fn list_topics(h: &KafkaHandles) -> AppResult<Vec<TopicSummary>> {
    let md = h.consumer.fetch_metadata(None, h.timeout).map_err(|e| h.kafka_err(e))?;
    Ok(summarize_topics(convert(&md).1))
}

/// Приблизительное число сообщений: high − low (компакция и транзакционные
/// маркеры делают реальное число меньше).
pub fn message_count(low: i64, high: i64) -> i64 {
    (high - low).max(0)
}

/// Результат ListOffsets по партиции: (low, high) или текст ошибки брокера.
pub type PartitionBoundsMap = HashMap<(String, i32), Result<(i64, i64), String>>;

const NO_OFFSET: &str = "Брокер не вернул оффсет партиции";

/// (low, high) watermark по каждой партиции двумя пакетными ListOffsets-запросами
/// (offsets_for_times со спецзначениями -2 = earliest, -1 = latest).
/// Ошибка отдельной партиции возвращается как Err и никогда не превращается в «0 сообщений».
pub fn fetch_bounds(
    c: &BaseConsumer<CapturingContext>,
    parts: &[(String, i32)],
    timeout: Duration,
) -> KafkaResult<PartitionBoundsMap> {
    if parts.is_empty() {
        return Ok(HashMap::new());
    }
    let low = list_offsets(c, parts, Offset::Beginning, timeout)?;
    let high = list_offsets(c, parts, Offset::End, timeout)?;
    Ok(parts.iter().map(|k| (k.clone(), combine(low.get(k), high.get(k)))).collect())
}

fn combine(low: Option<&Result<i64, String>>, high: Option<&Result<i64, String>>) -> Result<(i64, i64), String> {
    match (low, high) {
        (Some(Ok(l)), Some(Ok(h))) => Ok((*l, *h)),
        (Some(Err(e)), _) | (_, Some(Err(e))) => Err(e.clone()),
        _ => Err(NO_OFFSET.to_string()),
    }
}

fn list_offsets(
    c: &BaseConsumer<CapturingContext>,
    parts: &[(String, i32)],
    which: Offset,
    timeout: Duration,
) -> KafkaResult<HashMap<(String, i32), Result<i64, String>>> {
    let mut tpl = TopicPartitionList::with_capacity(parts.len());
    for (t, p) in parts {
        tpl.add_partition_offset(t, *p, which)?;
    }
    let res = c.offsets_for_times(tpl, timeout)?;
    Ok(res
        .elements()
        .iter()
        .map(|e| {
            let value = match (e.error(), e.offset()) {
                (Err(err), _) => Err(err.to_string()),
                (Ok(()), Offset::Offset(o)) => Ok(o),
                (Ok(()), other) => Err(format!("Неожиданный оффсет {other:?}")),
            };
            ((e.topic().to_string(), e.partition()), value)
        })
        .collect())
}

pub fn topic_stats(h: &KafkaHandles, names: &[String]) -> AppResult<Vec<TopicStats>> {
    let md = h.consumer.fetch_metadata(None, h.timeout).map_err(|e| h.kafka_err(e))?;
    let (_, topics) = convert(&md);
    let wanted: HashSet<&str> = names.iter().map(String::as_str).collect();
    let parts: Vec<(String, i32)> = topics
        .iter()
        .filter(|t| wanted.contains(t.name.as_str()))
        .flat_map(|t| t.partitions.iter().map(move |p| (t.name.clone(), p.id)))
        .collect();
    let bounds = fetch_bounds(&h.consumer, &parts, h.timeout).map_err(|e| h.kafka_err(e))?;
    // None, если хотя бы у одной партиции топика оффсеты недоступны.
    let mut counts: HashMap<&str, Option<i64>> = HashMap::new();
    for ((t, _), b) in &bounds {
        let entry = counts.entry(t.as_str()).or_insert(Some(0));
        *entry = match (*entry, b) {
            (Some(sum), Ok((l, hi))) => Some(sum + message_count(*l, *hi)),
            _ => None,
        };
    }
    Ok(names
        .iter()
        .filter_map(|n| counts.get(n.as_str()).map(|c| TopicStats { name: n.clone(), message_count: *c }))
        .collect())
}

pub fn topic_details(h: &KafkaHandles, name: &str) -> AppResult<TopicDetails> {
    let md = h.consumer.fetch_metadata(Some(name), h.timeout).map_err(|e| h.kafka_err(e))?;
    let (_, topics) = convert(&md);
    let t = topics
        .into_iter()
        .find(|t| t.name == name && !t.partitions.is_empty())
        .ok_or_else(|| AppError::not_found(format!("Топик «{name}» не найден")))?;
    let keys: Vec<(String, i32)> = t.partitions.iter().map(|p| (t.name.clone(), p.id)).collect();
    let bounds = fetch_bounds(&h.consumer, &keys, h.timeout).map_err(|e| h.kafka_err(e))?;
    let mut partitions: Vec<PartitionDetails> = t
        .partitions
        .iter()
        .map(|p| {
            let b = bounds.get(&(t.name.clone(), p.id)).cloned().unwrap_or_else(|| Err(NO_OFFSET.to_string()));
            let (low, high, offsets_error) = match b {
                Ok((l, hi)) => (Some(l), Some(hi), None),
                Err(e) => (None, None, Some(e)),
            };
            PartitionDetails {
                id: p.id,
                leader: p.leader,
                replicas: p.replicas.clone(),
                isr: p.isr.clone(),
                low,
                high,
                message_count: low.zip(high).map(|(l, hi)| message_count(l, hi)),
                offsets_error,
                under_replicated: p.under_replicated(),
            }
        })
        .collect();
    partitions.sort_by_key(|p| p.id);
    Ok(TopicDetails {
        internal: t.internal(),
        replication_factor: t.replication_factor(),
        message_count: partitions.iter().map(|p| p.message_count).sum::<Option<i64>>(),
        name: t.name,
        partitions,
    })
}
```

- [ ] **Step 4: Unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib topics`
Expected: PASS, 2 теста.

- [ ] **Step 5: Интеграционный тест**

`src-tauri/tests/it_topics.rs`:
```rust
mod common;

use std::sync::Arc;

use common::TestMsg;
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::configs::describe_configs;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::kafka::topics::{list_topics, topic_details, topic_stats};
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::admin::ResourceSpecifier;

#[tokio::test(flavor = "multi_thread")]
async fn list_stats_details_configs() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::unique_topic("it-topics");
    common::create_topic(&bs, &topic, 3).await;
    let msgs: Vec<TestMsg> = (0..5).map(|i| TestMsg { partition: i % 3, key: None, value: "v", headers: &[] }).collect();
    common::produce(&bs, &topic, &msgs).await;

    let h = Arc::new(KafkaHandles::create(&common::plaintext_profile(&bs), &MemoryStore::default()).unwrap());
    let (h1, t1) = (h.clone(), topic.clone());
    let (list, stats, details, missing) = tokio::task::spawn_blocking(move || {
        (
            list_topics(&h1).unwrap(),
            topic_stats(&h1, &[t1.clone(), "does-not-exist".into()]).unwrap(),
            topic_details(&h1, &t1).unwrap(),
            topic_details(&h1, "does-not-exist").unwrap_err(),
        )
    })
    .await
    .unwrap();

    let summary = list.iter().find(|t| t.name == topic).expect("топик в списке");
    assert_eq!(summary.partition_count, 3);
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].message_count, Some(5));
    assert_eq!(details.partitions.len(), 3);
    assert_eq!(details.partitions[0].high, Some(2)); // сообщения 0 и 3
    assert!(details.partitions.iter().all(|p| p.offsets_error.is_none()));
    assert_eq!(details.message_count, Some(5));
    assert_eq!(missing.code, ErrorCode::NotFound);

    let configs = describe_configs(&h, ResourceSpecifier::Topic(&topic)).await.unwrap();
    let cleanup = configs.iter().find(|c| c.name == "cleanup.policy").unwrap();
    assert_eq!(cleanup.value.as_deref(), Some("delete"));
}
```

- [ ] **Step 6: Запустить**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_topics`
Expected: PASS.
Если `stats[0].message_count` равен `Some(0)` или `None`, значит, пакетный `offsets_for_times` со спецзначениями -1/-2 в вашей версии librdkafka не работает. Тогда перепишите `list_offsets` на поштучный `c.fetch_watermarks(topic, partition, timeout)`: он возвращает `(low, high)` для одной партиции. `fetch_bounds` при этом должен заполнять map из этих пар (ошибки — как `Err(текст)`).

- [ ] **Step 7: Commit**

```bash
git add src-tauri/src/kafka src-tauri/tests
git commit -m "feat(kafka): список топиков, счётчики сообщений, детали партиций"
```

---

### Task 9: Browse — декодирование данных и типы событий

**Files:**
- Create: `src-tauri/src/browse/mod.rs`, `src-tauri/src/browse/format.rs`, `src-tauri/src/browse/types.rs`
- Modify: `src-tauri/src/lib.rs` (добавить `pub mod browse;`)

**Interfaces:**
- Consumes: `AppError`
- Produces:
  - `enum DataFormat { Auto, String, Json, Hex, Base64 }` (serde lowercase)
  - `struct DecodedData { format: DataFormat, text: String, size: usize, truncated: bool, error: Option<String> }`
  - `const MAX_TEXT_BYTES: usize = 1_048_576`, `fn decode(bytes: &[u8], format: DataFormat) -> DecodedData`, `fn header_text(bytes: &[u8]) -> String`
  - `struct HeaderView { name: String, value: Option<String> }`
  - `struct BrowsedMessage { partition: i32, offset: i64, timestamp: Option<i64>, key: Option<DecodedData>, value: Option<DecodedData>, headers: Vec<HeaderView>, size: usize }`
  - `enum FinishReason { Limit, EndOfTopic, Cancelled }` (serde camelCase)
  - `enum BrowseEvent { Messages { messages }, Progress { scanned: u64, matched: u64 }, ReachedEnd, Finished { scanned, matched, reason }, Error { error: AppError } }` (serde: `tag = "type"`, camelCase)

- [ ] **Step 1: Тесты** (в конец `format.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_detects_json_string_and_binary() {
        assert_eq!(decode(br#" {"a":1}"#, DataFormat::Auto).format, DataFormat::Json);
        assert_eq!(decode(b"[1,2]", DataFormat::Auto).format, DataFormat::Json);
        assert_eq!(decode(b"{not json", DataFormat::Auto).format, DataFormat::String);
        assert_eq!(decode("привет\n".as_bytes(), DataFormat::Auto).format, DataFormat::String);
        let bin = decode(&[0x00, 0x01, 0xff], DataFormat::Auto);
        assert_eq!(bin.format, DataFormat::Hex);
        assert_eq!(bin.text, "0001ff");
        assert_eq!(decode(&[0x01, b'a'], DataFormat::Auto).format, DataFormat::Hex); // управляющий символ
    }

    #[test]
    fn explicit_json_reports_error_but_keeps_text() {
        let d = decode(b"oops", DataFormat::Json);
        assert_eq!(d.format, DataFormat::Json);
        assert_eq!(d.text, "oops");
        assert!(d.error.unwrap().starts_with("Невалидный JSON"));
    }

    #[test]
    fn hex_and_base64() {
        assert_eq!(decode(b"AB", DataFormat::Hex).text, "4142");
        assert_eq!(decode(b"AB", DataFormat::Base64).text, "QUI=");
        assert_eq!(decode(b"AB", DataFormat::String).size, 2);
    }

    #[test]
    fn truncates_on_char_boundary() {
        let s = "я".repeat(MAX_TEXT_BYTES); // 2 байта на символ
        let d = decode(s.as_bytes(), DataFormat::String);
        assert!(d.truncated);
        assert!(d.text.len() <= MAX_TEXT_BYTES);
        assert_eq!(d.size, MAX_TEXT_BYTES * 2);
    }

    #[test]
    fn header_text_utf8_or_hex() {
        assert_eq!(header_text(b"abc"), "abc");
        assert_eq!(header_text(&[0xff, 0x00]), "0xff00");
    }
}
```

В конец `types.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_serialize_with_type_tag() {
        let v = serde_json::to_value(BrowseEvent::Finished { scanned: 3, matched: 1, reason: FinishReason::EndOfTopic }).unwrap();
        assert_eq!(v, serde_json::json!({"type": "finished", "scanned": 3, "matched": 1, "reason": "endOfTopic"}));
        assert_eq!(serde_json::to_value(BrowseEvent::ReachedEnd).unwrap(), serde_json::json!({"type": "reachedEnd"}));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

`src-tauri/src/browse/mod.rs`:
```rust
pub mod format;
pub mod types;
```
Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib browse`
Expected: FAIL — не найдены `decode`, `BrowseEvent`.

- [ ] **Step 3: Реализация `format.rs`** (над тестами)

```rust
use base64::Engine;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    Auto,
    String,
    Json,
    Hex,
    Base64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DecodedData {
    /// Фактический формат (для Auto — определённый автоматически).
    pub format: DataFormat,
    pub text: String,
    /// Размер исходных байтов.
    pub size: usize,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub const MAX_TEXT_BYTES: usize = 1024 * 1024;

pub fn decode(bytes: &[u8], format: DataFormat) -> DecodedData {
    let resolved = if format == DataFormat::Auto { detect(bytes) } else { format };
    let (text, error) = match resolved {
        DataFormat::String => (String::from_utf8_lossy(bytes).into_owned(), None),
        DataFormat::Json => {
            let text = String::from_utf8_lossy(bytes).into_owned();
            match serde_json::from_slice::<serde_json::Value>(bytes) {
                Ok(_) => (text, None),
                Err(e) => (text, Some(format!("Невалидный JSON: {e}"))),
            }
        }
        DataFormat::Hex => (hex::encode(bytes), None),
        DataFormat::Base64 => (base64::engine::general_purpose::STANDARD.encode(bytes), None),
        DataFormat::Auto => unreachable!("detect никогда не возвращает Auto"),
    };
    let (text, truncated) = truncate(text, MAX_TEXT_BYTES);
    DecodedData { format: resolved, text, size: bytes.len(), truncated, error }
}

fn detect(bytes: &[u8]) -> DataFormat {
    let Ok(s) = std::str::from_utf8(bytes) else { return DataFormat::Hex };
    let trimmed = s.trim_start();
    if (trimmed.starts_with('{') || trimmed.starts_with('[')) && serde_json::from_str::<serde_json::Value>(s).is_ok() {
        return DataFormat::Json;
    }
    if s.chars().any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t')) {
        return DataFormat::Hex;
    }
    DataFormat::String
}

fn truncate(mut s: String, max: usize) -> (String, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut cut = max;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    (s, true)
}

pub fn header_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => format!("0x{}", hex::encode(bytes)),
    }
}
```

- [ ] **Step 4: Реализация `types.rs`** (над тестами)

```rust
use serde::Serialize;

use crate::browse::format::DecodedData;
use crate::error::AppError;

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderView {
    pub name: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowsedMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<DecodedData>,
    pub value: Option<DecodedData>,
    pub headers: Vec<HeaderView>,
    pub size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FinishReason {
    Limit,
    EndOfTopic,
    Cancelled,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum BrowseEvent {
    Messages { messages: Vec<BrowsedMessage> },
    Progress { scanned: u64, matched: u64 },
    /// Только в live-режиме: все партиции дочитаны до конца, дальше идут новые сообщения.
    ReachedEnd,
    Finished { scanned: u64, matched: u64, reason: FinishReason },
    Error { error: AppError },
}
```

- [ ] **Step 5: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib browse`
Expected: PASS, 6 тестов.

- [ ] **Step 6: Commit**

```bash
git add src-tauri/src/browse src-tauri/src/lib.rs
git commit -m "feat(browse): декодирование ключей/значений и типы событий"
```

---

### Task 10: Browse — фильтры

**Files:**
- Create: `src-tauri/src/browse/filter.rs`
- Modify: `src-tauri/src/browse/mod.rs` (добавить `pub mod filter;`)

**Interfaces:**
- Consumes: `HeaderView`, `BrowsedMessage`, `AppError`
- Produces:
  - `struct TextMatch { pattern: String, regex: bool, case_sensitive: bool }`
  - `struct HeaderMatch { name: String, value: Option<TextMatch> }`
  - `struct FilterSpec { key: Option<TextMatch>, value: Option<TextMatch>, header: Option<HeaderMatch>, json_path: Option<String>, from_timestamp: Option<i64>, to_timestamp: Option<i64> }` (camelCase, `Default`)
  - `struct MessageView<'a> { key: Option<&'a str>, value: Option<&'a str>, headers: &'a [HeaderView], timestamp: Option<i64> }`
  - `impl BrowsedMessage { pub fn view(&self) -> MessageView<'_> }`
  - `struct CompiledFilter`: `compile(spec: &FilterSpec) -> AppResult<Self>`, `matches(&self, m: &MessageView) -> bool`
  - Синтаксис JSON-условия: `<JSONPath> [== | != <литерал>]`. Литерал — JSON, `'строка'` или голое слово; без оператора условие истинно, если путь находит хотя бы один узел.

- [ ] **Step 1: Тесты** (в конец `filter.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn tm(p: &str) -> Option<TextMatch> {
        Some(TextMatch { pattern: p.into(), regex: false, case_sensitive: false })
    }

    fn view<'a>(key: Option<&'a str>, value: Option<&'a str>, headers: &'a [HeaderView], ts: Option<i64>) -> MessageView<'a> {
        MessageView { key, value, headers, timestamp: ts }
    }

    #[test]
    fn empty_spec_matches_everything() {
        let f = CompiledFilter::compile(&FilterSpec::default()).unwrap();
        assert!(f.matches(&view(None, None, &[], None)));
    }

    #[test]
    fn substring_is_case_insensitive_by_default() {
        let f = CompiledFilter::compile(&FilterSpec { value: tm("order-7"), ..Default::default() }).unwrap();
        assert!(f.matches(&view(None, Some("id=ORDER-7"), &[], None)));
        assert!(!f.matches(&view(None, Some("order-8"), &[], None)));
        assert!(!f.matches(&view(None, None, &[], None)));
    }

    #[test]
    fn case_sensitive_and_regex() {
        let cs = FilterSpec { key: Some(TextMatch { pattern: "Abc".into(), regex: false, case_sensitive: true }), ..Default::default() };
        let f = CompiledFilter::compile(&cs).unwrap();
        assert!(!f.matches(&view(Some("abc"), None, &[], None)));
        let re = FilterSpec { key: Some(TextMatch { pattern: r"^user-\d+$".into(), regex: true, case_sensitive: false }), ..Default::default() };
        let f = CompiledFilter::compile(&re).unwrap();
        assert!(f.matches(&view(Some("USER-42"), None, &[], None)));
        assert!(!f.matches(&view(Some("user-x"), None, &[], None)));
    }

    #[test]
    fn invalid_regex_is_invalid_input() {
        let spec = FilterSpec { value: Some(TextMatch { pattern: "(".into(), regex: true, case_sensitive: false }), ..Default::default() };
        assert_eq!(CompiledFilter::compile(&spec).err().unwrap().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn header_by_name_and_value() {
        let headers = vec![HeaderView { name: "trace".into(), value: Some("t-3".into()) }, HeaderView { name: "empty".into(), value: None }];
        let by_name = FilterSpec { header: Some(HeaderMatch { name: "empty".into(), value: None }), ..Default::default() };
        assert!(CompiledFilter::compile(&by_name).unwrap().matches(&view(None, None, &headers, None)));
        let by_value = FilterSpec { header: Some(HeaderMatch { name: "trace".into(), value: tm("t-3") }), ..Default::default() };
        assert!(CompiledFilter::compile(&by_value).unwrap().matches(&view(None, None, &headers, None)));
        let wrong = FilterSpec { header: Some(HeaderMatch { name: "trace".into(), value: tm("t-4") }), ..Default::default() };
        assert!(!CompiledFilter::compile(&wrong).unwrap().matches(&view(None, None, &headers, None)));
    }

    #[test]
    fn json_conditions() {
        let doc = r#"{"order":{"id":7,"status":"FAILED"},"items":[{"qty":1},{"qty":3}]}"#;
        let check = |cond: &str| {
            let f = CompiledFilter::compile(&FilterSpec { json_path: Some(cond.into()), ..Default::default() }).unwrap();
            f.matches(&view(None, Some(doc), &[], None))
        };
        assert!(check(r#"$.order.status == "FAILED""#));
        assert!(check("$.order.status == 'FAILED'"));
        assert!(check("$.order.status == FAILED"));
        assert!(!check("$.order.status == 'NEW'"));
        assert!(check("$.order.status != 'NEW'"));
        assert!(check("$.order.id == 7"));
        assert!(check("$.order.id"));
        assert!(!check("$.missing"));
        assert!(check("$.items[?@.qty > 2]"));
        let f = CompiledFilter::compile(&FilterSpec { json_path: Some("$.a".into()), ..Default::default() }).unwrap();
        assert!(!f.matches(&view(None, Some("not json"), &[], None)));
        let bad = CompiledFilter::compile(&FilterSpec { json_path: Some("$$$".into()), ..Default::default() });
        assert_eq!(bad.err().unwrap().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn time_range() {
        let f = CompiledFilter::compile(&FilterSpec { from_timestamp: Some(100), to_timestamp: Some(200), ..Default::default() }).unwrap();
        assert!(f.matches(&view(None, None, &[], Some(150))));
        assert!(!f.matches(&view(None, None, &[], Some(99))));
        assert!(!f.matches(&view(None, None, &[], Some(201))));
        assert!(!f.matches(&view(None, None, &[], None)));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib filter`
Expected: FAIL — не найден `CompiledFilter`.

- [ ] **Step 3: Реализация** (над тестами)

```rust
use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use serde_json::Value;
use serde_json_path::JsonPath;

use crate::browse::types::{BrowsedMessage, HeaderView};
use crate::error::{AppError, AppResult};

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TextMatch {
    pub pattern: String,
    #[serde(default)]
    pub regex: bool,
    #[serde(default)]
    pub case_sensitive: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HeaderMatch {
    pub name: String,
    #[serde(default)]
    pub value: Option<TextMatch>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterSpec {
    #[serde(default)]
    pub key: Option<TextMatch>,
    #[serde(default)]
    pub value: Option<TextMatch>,
    #[serde(default)]
    pub header: Option<HeaderMatch>,
    #[serde(default)]
    pub json_path: Option<String>,
    #[serde(default)]
    pub from_timestamp: Option<i64>,
    #[serde(default)]
    pub to_timestamp: Option<i64>,
}

pub struct MessageView<'a> {
    pub key: Option<&'a str>,
    pub value: Option<&'a str>,
    pub headers: &'a [HeaderView],
    pub timestamp: Option<i64>,
}

impl BrowsedMessage {
    pub fn view(&self) -> MessageView<'_> {
        MessageView {
            key: self.key.as_ref().map(|d| d.text.as_str()),
            value: self.value.as_ref().map(|d| d.text.as_str()),
            headers: &self.headers,
            timestamp: self.timestamp,
        }
    }
}

enum Matcher {
    Substring { needle: String, case_sensitive: bool },
    Regex(Regex),
}

impl Matcher {
    fn compile(m: &TextMatch) -> AppResult<Option<Self>> {
        if m.pattern.is_empty() {
            return Ok(None);
        }
        if m.regex {
            let re = RegexBuilder::new(&m.pattern)
                .case_insensitive(!m.case_sensitive)
                .build()
                .map_err(|e| AppError::invalid(format!("Некорректное регулярное выражение «{}»: {e}", m.pattern)))?;
            return Ok(Some(Matcher::Regex(re)));
        }
        let needle = if m.case_sensitive { m.pattern.clone() } else { m.pattern.to_lowercase() };
        Ok(Some(Matcher::Substring { needle, case_sensitive: m.case_sensitive }))
    }

    fn is_match(&self, text: &str) -> bool {
        match self {
            Matcher::Substring { needle, case_sensitive: true } => text.contains(needle.as_str()),
            Matcher::Substring { needle, case_sensitive: false } => text.to_lowercase().contains(needle.as_str()),
            Matcher::Regex(re) => re.is_match(text),
        }
    }
}

enum CmpOp {
    Eq,
    Ne,
}

struct JsonCondition {
    path: JsonPath,
    cmp: Option<(CmpOp, Value)>,
}

impl JsonCondition {
    /// `<JSONPath> [== | != <литерал>]`. Делим по последнему оператору; если левая часть
    /// не парсится как JSONPath (оператор был внутри фильтра `[?...]`), всё выражение — путь.
    fn parse(s: &str) -> AppResult<Self> {
        let s = s.trim();
        for (op_str, op) in [("!=", CmpOp::Ne), ("==", CmpOp::Eq)] {
            if let Some((left, right)) = s.rsplit_once(op_str) {
                if let Ok(path) = JsonPath::parse(left.trim()) {
                    return Ok(Self { path, cmp: Some((op, parse_literal(right))) });
                }
            }
        }
        let path = JsonPath::parse(s).map_err(|e| AppError::invalid(format!("Некорректный JSONPath «{s}»: {e}")))?;
        Ok(Self { path, cmp: None })
    }

    fn matches(&self, doc: &Value) -> bool {
        let nodes = self.path.query(doc).all();
        match &self.cmp {
            None => !nodes.is_empty(),
            Some((CmpOp::Eq, lit)) => nodes.iter().any(|n| *n == lit),
            Some((CmpOp::Ne, lit)) => !nodes.is_empty() && nodes.iter().all(|n| *n != lit),
        }
    }
}

fn parse_literal(raw: &str) -> Value {
    let t = raw.trim();
    if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
        return Value::String(t[1..t.len() - 1].to_string());
    }
    serde_json::from_str(t).unwrap_or_else(|_| Value::String(t.to_string()))
}

pub struct CompiledFilter {
    key: Option<Matcher>,
    value: Option<Matcher>,
    header: Option<(String, Option<Matcher>)>,
    json: Option<JsonCondition>,
    from: Option<i64>,
    to: Option<i64>,
}

impl CompiledFilter {
    pub fn compile(spec: &FilterSpec) -> AppResult<Self> {
        let header = match &spec.header {
            Some(h) if !h.name.is_empty() => {
                Some((h.name.clone(), h.value.as_ref().map(Matcher::compile).transpose()?.flatten()))
            }
            _ => None,
        };
        let json = spec
            .json_path
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .map(JsonCondition::parse)
            .transpose()?;
        Ok(Self {
            key: spec.key.as_ref().map(Matcher::compile).transpose()?.flatten(),
            value: spec.value.as_ref().map(Matcher::compile).transpose()?.flatten(),
            header,
            json,
            from: spec.from_timestamp,
            to: spec.to_timestamp,
        })
    }

    pub fn matches(&self, m: &MessageView) -> bool {
        if self.from.is_some() || self.to.is_some() {
            let Some(ts) = m.timestamp else { return false };
            if self.from.is_some_and(|f| ts < f) || self.to.is_some_and(|t| ts > t) {
                return false;
            }
        }
        if let Some(k) = &self.key {
            if !m.key.is_some_and(|t| k.is_match(t)) {
                return false;
            }
        }
        if let Some(v) = &self.value {
            if !m.value.is_some_and(|t| v.is_match(t)) {
                return false;
            }
        }
        if let Some((name, vm)) = &self.header {
            let found = m.headers.iter().any(|h| {
                h.name == *name
                    && match vm {
                        None => true,
                        Some(vm) => h.value.as_deref().is_some_and(|v| vm.is_match(v)),
                    }
            });
            if !found {
                return false;
            }
        }
        if let Some(cond) = &self.json {
            let Some(text) = m.value else { return false };
            let Ok(doc) = serde_json::from_str::<Value>(text) else { return false };
            if !cond.matches(&doc) {
                return false;
            }
        }
        true
    }
}
```

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib filter`
Expected: PASS, 7 тестов.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/browse
git commit -m "feat(browse): фильтры по ключу, значению, заголовкам, JSONPath и времени"
```

---

### Task 11: Browse — расчёт стартовых позиций и отслеживание конца

**Files:**
- Create: `src-tauri/src/browse/plan.rs`
- Modify: `src-tauri/src/browse/mod.rs` (добавить `pub mod plan;`)

**Interfaces:**
- Produces:
  - `enum StartPosition { Earliest, Latest { count: i64 }, Offset { offset: i64 }, Timestamp { timestamp_ms: i64 }, LastMinutes { minutes: i64 } }` (serde: `tag = "type"`, camelCase; поля тоже camelCase)
  - `struct PartitionBounds { partition: i32, low: i64, high: i64 }`
  - `struct PartitionPlan { partition: i32, start: i64, end: i64 }` (`end` — high watermark на момент старта, не включительно)
  - `fn plan_earliest(b: &[PartitionBounds]) -> Vec<PartitionPlan>`, `fn plan_latest(b, count: i64)`, `fn plan_offset(b, offset: i64)`, `fn plan_from_offsets(b, resolved: &HashMap<i32, i64>)`
  - `struct EndTracker`: `new(plans: &[PartitionPlan])`, `observe(&mut self, partition: i32, offset: i64) -> bool`, `mark_done(&mut self, partition: i32)`, `all_done(&self) -> bool`

- [ ] **Step 1: Тесты** (в конец `plan.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn b(partition: i32, low: i64, high: i64) -> PartitionBounds {
        PartitionBounds { partition, low, high }
    }

    fn starts(plans: &[PartitionPlan]) -> Vec<i64> {
        plans.iter().map(|p| p.start).collect()
    }

    #[test]
    fn latest_splits_count_across_partitions() {
        let bounds = [b(0, 0, 10), b(1, 5, 7), b(2, 0, 0)];
        let plans = plan_latest(&bounds, 6); // по 2 на партицию
        assert_eq!(starts(&plans), vec![8, 5, 0]);
        assert_eq!(plans.iter().map(|p| p.end).collect::<Vec<_>>(), vec![10, 7, 0]);
        assert_eq!(starts(&plan_latest(&bounds, 7)), vec![7, 5, 0]); // ceil(7/3) = 3
    }

    #[test]
    fn earliest_offset_and_resolved() {
        let bounds = [b(0, 5, 10), b(1, 0, 3)];
        assert_eq!(starts(&plan_earliest(&bounds)), vec![5, 0]);
        assert_eq!(starts(&plan_offset(&bounds, 2)), vec![5, 2]);
        assert_eq!(starts(&plan_offset(&bounds, 100)), vec![10, 3]);
        let resolved = HashMap::from([(0, 7)]);
        assert_eq!(starts(&plan_from_offsets(&bounds, &resolved)), vec![7, 3]); // нет оффсета → high
    }

    #[test]
    fn tracker_detects_end() {
        let plans = [PartitionPlan { partition: 0, start: 0, end: 2 }, PartitionPlan { partition: 1, start: 4, end: 4 }];
        let mut t = EndTracker::new(&plans);
        assert!(!t.all_done()); // партиция 1 пустая и уже done, партиция 0 — нет
        assert!(t.observe(0, 0));
        assert!(!t.all_done());
        assert!(t.observe(0, 1));
        assert!(t.all_done());
        assert!(!t.observe(0, 2)); // за пределами снимка
        assert!(!t.observe(9, 0)); // неизвестная партиция
    }

    #[test]
    fn tracker_mark_done_handles_gaps() {
        let plans = [PartitionPlan { partition: 0, start: 0, end: 10 }];
        let mut t = EndTracker::new(&plans);
        t.observe(0, 7); // offsets 8..9 — транзакционные маркеры, не придут
        t.mark_done(0); // PartitionEOF
        assert!(t.all_done());
    }

    #[test]
    fn start_position_json() {
        let p: StartPosition = serde_json::from_str(r#"{"type":"timestamp","timestampMs":5}"#).unwrap();
        assert!(matches!(p, StartPosition::Timestamp { timestamp_ms: 5 }));
        let p: StartPosition = serde_json::from_str(r#"{"type":"lastMinutes","minutes":15}"#).unwrap();
        assert!(matches!(p, StartPosition::LastMinutes { minutes: 15 }));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib plan`
Expected: FAIL — не найдена `plan_latest`.

- [ ] **Step 3: Реализация** (над тестами)

```rust
use std::collections::{HashMap, HashSet};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum StartPosition {
    Earliest,
    /// Последние `count` сообщений на всю выборку, поровну по партициям.
    Latest { count: i64 },
    Offset { offset: i64 },
    Timestamp { timestamp_ms: i64 },
    LastMinutes { minutes: i64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionBounds {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionPlan {
    pub partition: i32,
    pub start: i64,
    /// High watermark на момент старта (не включительно).
    pub end: i64,
}

fn plan_with(bounds: &[PartitionBounds], start: impl Fn(&PartitionBounds) -> i64) -> Vec<PartitionPlan> {
    bounds
        .iter()
        .map(|b| PartitionPlan { partition: b.partition, start: start(b).clamp(b.low, b.high.max(b.low)), end: b.high })
        .collect()
}

pub fn plan_earliest(bounds: &[PartitionBounds]) -> Vec<PartitionPlan> {
    plan_with(bounds, |b| b.low)
}

pub fn plan_latest(bounds: &[PartitionBounds], count: i64) -> Vec<PartitionPlan> {
    let n = bounds.len().max(1) as i64;
    let per = (count + n - 1) / n;
    plan_with(bounds, |b| b.high - per)
}

pub fn plan_offset(bounds: &[PartitionBounds], offset: i64) -> Vec<PartitionPlan> {
    plan_with(bounds, |_| offset)
}

/// `resolved` — результат offsets_for_times; если для партиции оффсета нет
/// (нет сообщений новее метки), читаем с конца.
pub fn plan_from_offsets(bounds: &[PartitionBounds], resolved: &HashMap<i32, i64>) -> Vec<PartitionPlan> {
    plan_with(bounds, |b| resolved.get(&b.partition).copied().unwrap_or(b.high))
}

pub struct EndTracker {
    ends: HashMap<i32, i64>,
    done: HashSet<i32>,
}

impl EndTracker {
    pub fn new(plans: &[PartitionPlan]) -> Self {
        Self {
            ends: plans.iter().map(|p| (p.partition, p.end)).collect(),
            done: plans.iter().filter(|p| p.start >= p.end).map(|p| p.partition).collect(),
        }
    }

    /// Возвращает true, если сообщение входит в снимок (offset < end).
    pub fn observe(&mut self, partition: i32, offset: i64) -> bool {
        let Some(&end) = self.ends.get(&partition) else { return false };
        if offset >= end {
            self.done.insert(partition);
            return false;
        }
        if offset + 1 >= end {
            self.done.insert(partition);
        }
        true
    }

    pub fn mark_done(&mut self, partition: i32) {
        if self.ends.contains_key(&partition) {
            self.done.insert(partition);
        }
    }

    pub fn all_done(&self) -> bool {
        self.done.len() == self.ends.len()
    }
}
```

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib plan`
Expected: PASS, 5 тестов.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/browse
git commit -m "feat(browse): расчёт стартовых оффсетов и отслеживание конца топика"
```

---

### Task 12: Browse — сессия чтения и реестр сессий

**Files:**
- Create: `src-tauri/src/browse/session.rs`, `src-tauri/src/browse/registry.rs`
- Create: `src-tauri/tests/it_browse.rs`
- Modify: `src-tauri/src/browse/mod.rs` (добавить `pub mod registry;` и `pub mod session;`)

**Interfaces:**
- Consumes: `build_client_config`, `CapturingContext`, `from_kafka`, `convert`, `fetch_bounds`, `decode`, `header_text`, `CompiledFilter`, `FilterSpec`, `plan_*`, `EndTracker`, `StartPosition`, `BrowseEvent`, `BrowsedMessage`, `FinishReason`, `HeaderView`
- Produces:
  - `trait EventSink: Send + 'static { fn send(&self, event: BrowseEvent) -> bool; }` (false — получатель закрыт)
  - `struct BrowseParams { connection_id: String, topic: String, partitions: Option<Vec<i32>>, start: StartPosition, limit: usize, live: bool, key_format: DataFormat, value_format: DataFormat, filter: FilterSpec }` (camelCase)
  - `const MAX_LIMIT: usize = 100_000`, `fn validate(params: &BrowseParams) -> AppResult<()>`
  - `fn run_browse(profile: &ConnectionProfile, secrets: &dyn SecretStore, params: BrowseParams, cancel: Arc<AtomicBool>, sink: &dyn EventSink)` — блокирующая, все ошибки отдаёт событием `Error`
  - `#[derive(Default)] struct SessionRegistry`: `register() -> (String, Arc<AtomicBool>)`, `cancel(&self, id: &str) -> bool`, `remove(&self, id: &str)`, `cancel_all(&self)`, `len(&self) -> usize`
- Семантика: при `live=false` чтение заканчивается на снимке high watermark или по достижении `limit` найденных сообщений; при `live=true` чтение идёт до отмены, `limit` применяет UI (кольцевой буфер), а после дочитывания до конца приходит `ReachedEnd`.

- [ ] **Step 1: Unit-тесты** (в конец `registry.rs` и `session.rs`)

`registry.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_cancel_remove() {
        let r = SessionRegistry::default();
        let (id, flag) = r.register();
        assert_eq!(r.len(), 1);
        assert!(r.cancel(&id));
        assert!(flag.load(Ordering::Relaxed));
        r.remove(&id);
        assert!(!r.cancel(&id));
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn cancel_all_sets_every_flag() {
        let r = SessionRegistry::default();
        let (_, a) = r.register();
        let (_, b) = r.register();
        r.cancel_all();
        assert!(a.load(Ordering::Relaxed) && b.load(Ordering::Relaxed));
    }
}
```

`session.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn params(start: StartPosition, limit: usize) -> BrowseParams {
        BrowseParams {
            connection_id: "c".into(),
            topic: "t".into(),
            partitions: None,
            start,
            limit,
            live: false,
            key_format: DataFormat::Auto,
            value_format: DataFormat::Auto,
            filter: FilterSpec::default(),
        }
    }

    #[test]
    fn validation() {
        assert!(validate(&params(StartPosition::Earliest, 500)).is_ok());
        assert_eq!(validate(&params(StartPosition::Earliest, 0)).unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(validate(&params(StartPosition::Earliest, MAX_LIMIT + 1)).unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(validate(&params(StartPosition::Latest { count: 0 }, 10)).unwrap_err().code, ErrorCode::InvalidInput);
        assert_eq!(validate(&params(StartPosition::LastMinutes { minutes: -1 }, 10)).unwrap_err().code, ErrorCode::InvalidInput);
        let mut p = params(StartPosition::Earliest, 10);
        p.topic = " ".into();
        assert_eq!(validate(&p).unwrap_err().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn params_json_contract() {
        let p: BrowseParams = serde_json::from_str(
            r#"{"connectionId":"c","topic":"t","partitions":[0,2],"start":{"type":"latest","count":10},
                "limit":500,"live":true,"keyFormat":"string","valueFormat":"auto",
                "filter":{"value":{"pattern":"x"},"jsonPath":"$.a"}}"#,
        )
        .unwrap();
        assert_eq!(p.partitions, Some(vec![0, 2]));
        assert!(p.live);
        assert_eq!(p.key_format, DataFormat::String);
        assert_eq!(p.filter.json_path.as_deref(), Some("$.a"));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib browse::`
Expected: FAIL — не найдены `SessionRegistry`, `BrowseParams`.

- [ ] **Step 3: Реализация `registry.rs`** (над тестами)

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Длительные сессии (browse, позже — consumer-сессии) по id; отмена через флаг.
#[derive(Default)]
pub struct SessionRegistry {
    sessions: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

impl SessionRegistry {
    pub fn register(&self) -> (String, Arc<AtomicBool>) {
        let id = uuid::Uuid::new_v4().to_string();
        let flag = Arc::new(AtomicBool::new(false));
        self.sessions.lock().unwrap().insert(id.clone(), flag.clone());
        (id, flag)
    }

    pub fn cancel(&self, id: &str) -> bool {
        match self.sessions.lock().unwrap().get(id) {
            Some(flag) => {
                flag.store(true, Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    pub fn remove(&self, id: &str) {
        self.sessions.lock().unwrap().remove(id);
    }

    pub fn cancel_all(&self) {
        for flag in self.sessions.lock().unwrap().values() {
            flag.store(true, Ordering::Relaxed);
        }
    }

    pub fn len(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
```

- [ ] **Step 4: Реализация `session.rs`** (над тестами)

```rust
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::{Offset, TopicPartitionList};
use serde::Deserialize;

use crate::browse::filter::{CompiledFilter, FilterSpec};
use crate::browse::format::{decode, header_text, DataFormat};
use crate::browse::plan::{
    plan_earliest, plan_from_offsets, plan_latest, plan_offset, EndTracker, PartitionBounds, PartitionPlan, StartPosition,
};
use crate::browse::types::{BrowseEvent, BrowsedMessage, FinishReason, HeaderView};
use crate::error::{from_kafka, AppError, AppResult};
use crate::kafka::config::build_client_config;
use crate::kafka::context::CapturingContext;
use crate::kafka::metadata::convert;
use crate::kafka::topics::fetch_bounds;
use crate::secrets::SecretStore;
use crate::storage::connections::ConnectionProfile;

pub trait EventSink: Send + 'static {
    /// false — получатель закрыт, сессию нужно завершить.
    fn send(&self, event: BrowseEvent) -> bool;
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowseParams {
    pub connection_id: String,
    pub topic: String,
    #[serde(default)]
    pub partitions: Option<Vec<i32>>,
    pub start: StartPosition,
    pub limit: usize,
    pub live: bool,
    pub key_format: DataFormat,
    pub value_format: DataFormat,
    #[serde(default)]
    pub filter: FilterSpec,
}

pub const MAX_LIMIT: usize = 100_000;
const BATCH_SIZE: usize = 200;
const BATCH_INTERVAL: Duration = Duration::from_millis(100);
const PROGRESS_INTERVAL: Duration = Duration::from_secs(1);
const POLL_TIMEOUT: Duration = Duration::from_millis(100);

pub fn validate(params: &BrowseParams) -> AppResult<()> {
    if params.topic.trim().is_empty() {
        return Err(AppError::invalid("Не указан топик"));
    }
    if params.limit == 0 || params.limit > MAX_LIMIT {
        return Err(AppError::invalid(format!("Лимит должен быть от 1 до {MAX_LIMIT}")));
    }
    match params.start {
        StartPosition::Latest { count } if count <= 0 => {
            Err(AppError::invalid("Количество последних сообщений должно быть больше 0"))
        }
        StartPosition::LastMinutes { minutes } if minutes <= 0 => {
            Err(AppError::invalid("Количество минут должно быть больше 0"))
        }
        _ => Ok(()),
    }
}

pub fn run_browse(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: BrowseParams,
    cancel: Arc<AtomicBool>,
    sink: &dyn EventSink,
) {
    if let Err(error) = run_inner(profile, secrets, &params, &cancel, sink) {
        sink.send(BrowseEvent::Error { error });
    }
}

fn run_inner(
    profile: &ConnectionProfile,
    secrets: &dyn SecretStore,
    params: &BrowseParams,
    cancel: &AtomicBool,
    sink: &dyn EventSink,
) -> AppResult<()> {
    validate(params)?;
    let filter = CompiledFilter::compile(&params.filter)?;
    let ctx = CapturingContext::default();
    let consumer = create_consumer(profile, secrets, &ctx)?;
    let kerr = |e: KafkaError| from_kafka(&e, ctx.last_error().as_deref());
    let timeout = Duration::from_millis(profile.request_timeout_ms);

    let bounds = load_bounds(&consumer, params, timeout, &kerr)?;
    let plans = resolve_plans(&consumer, params, &bounds, timeout).map_err(&kerr)?;
    assign(&consumer, &params.topic, &plans, params.live).map_err(&kerr)?;

    let mut tracker = EndTracker::new(&plans);
    let mut out = Output::new(sink);
    let reason = loop {
        if cancel.load(Ordering::Relaxed) || out.closed {
            break FinishReason::Cancelled;
        }
        if !params.live && out.matched >= params.limit as u64 {
            break FinishReason::Limit;
        }
        if !params.live && tracker.all_done() {
            break FinishReason::EndOfTopic;
        }
        match consumer.poll(POLL_TIMEOUT) {
            None => {}
            Some(Err(KafkaError::PartitionEOF(p))) => tracker.mark_done(p),
            Some(Err(e)) => return Err(kerr(e)),
            Some(Ok(msg)) => {
                let in_snapshot = tracker.observe(msg.partition(), msg.offset());
                if in_snapshot || params.live {
                    out.scanned += 1;
                    let m = to_browsed(&msg, params.key_format, params.value_format);
                    if filter.matches(&m.view()) {
                        out.push(m);
                    }
                }
            }
        }
        if params.live && !out.reached_end_sent && tracker.all_done() {
            out.flush();
            out.reached_end();
        }
        out.maybe_flush();
    };
    out.flush();
    out.emit(BrowseEvent::Finished { scanned: out.scanned, matched: out.matched, reason });
    Ok(())
}

fn create_consumer(
    p: &ConnectionProfile,
    secrets: &dyn SecretStore,
    ctx: &CapturingContext,
) -> AppResult<BaseConsumer<CapturingContext>> {
    let mut cfg = build_client_config(p, secrets)?;
    // group.id обязателен для librdkafka-консьюмера, но мы не подписываемся и не коммитим:
    // партиции назначаются через assign, поэтому группа на брокере не появляется.
    cfg.set("group.id", format!("kafka-explorer-browse-{}", uuid::Uuid::new_v4()))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "true")
        .set("auto.offset.reset", "earliest");
    cfg.create_with_context(ctx.clone()).map_err(|e| from_kafka(&e, None))
}

fn load_bounds(
    consumer: &BaseConsumer<CapturingContext>,
    params: &BrowseParams,
    timeout: Duration,
    kerr: &impl Fn(KafkaError) -> AppError,
) -> AppResult<Vec<PartitionBounds>> {
    let md = consumer.fetch_metadata(Some(&params.topic), timeout).map_err(kerr)?;
    let (_, topics) = convert(&md);
    let topic = topics
        .into_iter()
        .find(|t| t.name == params.topic && !t.partitions.is_empty())
        .ok_or_else(|| AppError::not_found(format!("Топик «{}» не найден", params.topic)))?;
    let all: Vec<i32> = topic.partitions.iter().map(|p| p.id).collect();
    let selected = match &params.partitions {
        Some(list) if !list.is_empty() => {
            if let Some(bad) = list.iter().find(|p| !all.contains(p)) {
                return Err(AppError::invalid(format!("В топике нет партиции {bad}")));
            }
            list.clone()
        }
        _ => all,
    };
    let keys: Vec<(String, i32)> = selected.iter().map(|p| (params.topic.clone(), *p)).collect();
    let bounds = fetch_bounds(consumer, &keys, timeout).map_err(kerr)?;
    let mut out = Vec::with_capacity(selected.len());
    for p in &selected {
        match bounds.get(&(params.topic.clone(), *p)) {
            Some(Ok((low, high))) => out.push(PartitionBounds { partition: *p, low: *low, high: *high }),
            other => {
                let reason = match other {
                    Some(Err(e)) => e.clone(),
                    _ => "нет ответа брокера".to_string(),
                };
                return Err(AppError::internal(format!("Не удалось получить оффсеты партиции {p}")).with_details(reason));
            }
        }
    }
    out.sort_by_key(|b| b.partition);
    Ok(out)
}

fn resolve_plans(
    consumer: &BaseConsumer<CapturingContext>,
    params: &BrowseParams,
    bounds: &[PartitionBounds],
    timeout: Duration,
) -> KafkaResult<Vec<PartitionPlan>> {
    Ok(match &params.start {
        StartPosition::Earliest => plan_earliest(bounds),
        StartPosition::Latest { count } => plan_latest(bounds, *count),
        StartPosition::Offset { offset } => plan_offset(bounds, *offset),
        StartPosition::Timestamp { timestamp_ms } => {
            plan_from_offsets(bounds, &offsets_for_timestamp(consumer, &params.topic, bounds, *timestamp_ms, timeout)?)
        }
        StartPosition::LastMinutes { minutes } => {
            let ts = now_ms() - minutes * 60_000;
            plan_from_offsets(bounds, &offsets_for_timestamp(consumer, &params.topic, bounds, ts, timeout)?)
        }
    })
}

fn offsets_for_timestamp(
    consumer: &BaseConsumer<CapturingContext>,
    topic: &str,
    bounds: &[PartitionBounds],
    ts: i64,
    timeout: Duration,
) -> KafkaResult<HashMap<i32, i64>> {
    let mut tpl = TopicPartitionList::new();
    for b in bounds {
        tpl.add_partition_offset(topic, b.partition, Offset::Offset(ts))?;
    }
    let res = consumer.offsets_for_times(tpl, timeout)?;
    Ok(res
        .elements()
        .iter()
        .filter_map(|e| match e.offset() {
            Offset::Offset(o) if o >= 0 => Some((e.partition(), o)),
            _ => None,
        })
        .collect())
}

fn assign(consumer: &BaseConsumer<CapturingContext>, topic: &str, plans: &[PartitionPlan], live: bool) -> KafkaResult<()> {
    let mut tpl = TopicPartitionList::new();
    for p in plans.iter().filter(|p| live || p.start < p.end) {
        tpl.add_partition_offset(topic, p.partition, Offset::Offset(p.start))?;
    }
    consumer.assign(&tpl)
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn to_browsed(m: &BorrowedMessage<'_>, key_format: DataFormat, value_format: DataFormat) -> BrowsedMessage {
    let headers = m
        .headers()
        .map(|hs| hs.iter().map(|h| HeaderView { name: h.key.to_string(), value: h.value.map(header_text) }).collect())
        .unwrap_or_default();
    BrowsedMessage {
        partition: m.partition(),
        offset: m.offset(),
        timestamp: m.timestamp().to_millis(),
        key: m.key().map(|k| decode(k, key_format)),
        value: m.payload().map(|v| decode(v, value_format)),
        headers,
        size: m.key().map_or(0, <[u8]>::len) + m.payload().map_or(0, <[u8]>::len),
    }
}

/// Буферизация: до BATCH_SIZE сообщений или раз в BATCH_INTERVAL; Progress — не реже раза в секунду.
struct Output<'a> {
    sink: &'a dyn EventSink,
    batch: Vec<BrowsedMessage>,
    scanned: u64,
    matched: u64,
    last_flush: Instant,
    last_progress: Instant,
    reached_end_sent: bool,
    closed: bool,
}

impl<'a> Output<'a> {
    fn new(sink: &'a dyn EventSink) -> Self {
        Self {
            sink,
            batch: Vec::with_capacity(BATCH_SIZE),
            scanned: 0,
            matched: 0,
            last_flush: Instant::now(),
            last_progress: Instant::now(),
            reached_end_sent: false,
            closed: false,
        }
    }

    fn push(&mut self, m: BrowsedMessage) {
        self.matched += 1;
        self.batch.push(m);
    }

    fn maybe_flush(&mut self) {
        if self.batch.len() >= BATCH_SIZE || (!self.batch.is_empty() && self.last_flush.elapsed() >= BATCH_INTERVAL) {
            self.flush();
        } else if self.last_progress.elapsed() >= PROGRESS_INTERVAL {
            self.progress();
        }
    }

    fn flush(&mut self) {
        if !self.batch.is_empty() {
            let messages = std::mem::take(&mut self.batch);
            self.emit(BrowseEvent::Messages { messages });
        }
        self.last_flush = Instant::now();
        self.progress();
    }

    fn progress(&mut self) {
        self.emit(BrowseEvent::Progress { scanned: self.scanned, matched: self.matched });
        self.last_progress = Instant::now();
    }

    fn reached_end(&mut self) {
        self.reached_end_sent = true;
        self.emit(BrowseEvent::ReachedEnd);
    }

    fn emit(&mut self, event: BrowseEvent) {
        if !self.sink.send(event) {
            self.closed = true;
        }
    }
}
```

- [ ] **Step 5: Unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib browse::`
Expected: PASS (все тесты модуля `browse`, включая 4 новых).

- [ ] **Step 6: Интеграционные тесты Browse**

`src-tauri/tests/it_browse.rs`:
```rust
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::TestMsg;
use kafka_explorer_lib::browse::filter::{FilterSpec, HeaderMatch, TextMatch};
use kafka_explorer_lib::browse::format::DataFormat;
use kafka_explorer_lib::browse::plan::StartPosition;
use kafka_explorer_lib::browse::session::{run_browse, BrowseParams, EventSink};
use kafka_explorer_lib::browse::types::{BrowseEvent, BrowsedMessage, FinishReason};
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::secrets::MemoryStore;

#[derive(Clone, Default)]
struct VecSink(Arc<Mutex<Vec<BrowseEvent>>>);

impl EventSink for VecSink {
    fn send(&self, event: BrowseEvent) -> bool {
        self.0.lock().unwrap().push(event);
        true
    }
}

impl VecSink {
    fn messages(&self) -> Vec<BrowsedMessage> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .flat_map(|e| match e {
                BrowseEvent::Messages { messages } => messages.clone(),
                _ => vec![],
            })
            .collect()
    }

    fn last(&self) -> BrowseEvent {
        self.0.lock().unwrap().last().cloned().unwrap()
    }

    fn has_reached_end(&self) -> bool {
        self.0.lock().unwrap().iter().any(|e| matches!(e, BrowseEvent::ReachedEnd))
    }
}

fn params(topic: &str, start: StartPosition, limit: usize, filter: FilterSpec) -> BrowseParams {
    BrowseParams {
        connection_id: "it".into(),
        topic: topic.into(),
        partitions: None,
        start,
        limit,
        live: false,
        key_format: DataFormat::Auto,
        value_format: DataFormat::Auto,
        filter,
    }
}

async fn browse(bs: &str, p: BrowseParams) -> VecSink {
    let sink = VecSink::default();
    let (s, profile) = (sink.clone(), common::plaintext_profile(bs));
    tokio::task::spawn_blocking(move || run_browse(&profile, &MemoryStore::default(), p, Arc::new(AtomicBool::new(false)), &s))
        .await
        .unwrap();
    sink
}

/// 30 сообщений по 3 партициям: key k{i}, value {"n":i,"status":"OK"|"FAILED"(i=7)}, header trace=t{i}.
async fn seeded_topic(bs: &str) -> String {
    let topic = common::unique_topic("it-browse");
    common::create_topic(bs, &topic, 3).await;
    let keys: Vec<String> = (0..30).map(|i| format!("k{i}")).collect();
    let values: Vec<String> =
        (0..30).map(|i| format!(r#"{{"n":{i},"status":"{}"}}"#, if i == 7 { "FAILED" } else { "OK" })).collect();
    let traces: Vec<String> = (0..30).map(|i| format!("t{i}")).collect();
    let headers: Vec<[(&str, &str); 1]> = traces.iter().map(|t| [("trace", t.as_str())]).collect();
    let msgs: Vec<TestMsg> = (0..30)
        .map(|i| TestMsg { partition: (i % 3) as i32, key: Some(keys[i].as_str()), value: &values[i], headers: &headers[i] })
        .collect();
    common::produce(bs, &topic, &msgs).await;
    topic
}

#[tokio::test(flavor = "multi_thread")]
async fn earliest_reads_everything_and_stops_at_end() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = seeded_topic(&bs).await;
    let sink = browse(&bs, params(&topic, StartPosition::Earliest, 1000, FilterSpec::default())).await;
    let msgs = sink.messages();
    assert_eq!(msgs.len(), 30);
    assert_eq!(msgs[0].value.as_ref().unwrap().format, DataFormat::Json);
    assert_eq!(msgs[0].headers[0].name, "trace");
    assert!(matches!(sink.last(), BrowseEvent::Finished { reason: FinishReason::EndOfTopic, scanned: 30, matched: 30 }));
}

#[tokio::test(flavor = "multi_thread")]
async fn latest_count_takes_tail_of_each_partition() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = seeded_topic(&bs).await;
    let msgs = browse(&bs, params(&topic, StartPosition::Latest { count: 6 }, 1000, FilterSpec::default())).await.messages();
    assert_eq!(msgs.len(), 6);
    assert!(msgs.iter().all(|m| m.offset >= 8)); // в каждой партиции 10 сообщений → offsets 8, 9
}

#[tokio::test(flavor = "multi_thread")]
async fn limit_stops_early() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = seeded_topic(&bs).await;
    let sink = browse(&bs, params(&topic, StartPosition::Earliest, 5, FilterSpec::default())).await;
    assert_eq!(sink.messages().len(), 5);
    assert!(matches!(sink.last(), BrowseEvent::Finished { reason: FinishReason::Limit, .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn filters_by_json_and_header() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = seeded_topic(&bs).await;
    let json = FilterSpec { json_path: Some(r#"$.status == "FAILED""#.into()), ..Default::default() };
    let msgs = browse(&bs, params(&topic, StartPosition::Earliest, 1000, json)).await.messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].key.as_ref().unwrap().text, "k7");

    let header = FilterSpec {
        header: Some(HeaderMatch {
            name: "trace".into(),
            value: Some(TextMatch { pattern: "^t3$".into(), regex: true, case_sensitive: true }),
        }),
        ..Default::default()
    };
    let msgs = browse(&bs, params(&topic, StartPosition::Earliest, 1000, header)).await.messages();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].key.as_ref().unwrap().text, "k3");
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_topic_reports_not_found() {
    let Some(bs) = common::bootstrap() else { return };
    let sink = browse(&bs, params("does-not-exist-xyz", StartPosition::Earliest, 10, FilterSpec::default())).await;
    match sink.last() {
        BrowseEvent::Error { error } => assert_eq!(error.code, ErrorCode::NotFound),
        other => panic!("ожидали Error, получили {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_tail_receives_new_messages_until_cancelled() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::unique_topic("it-live");
    common::create_topic(&bs, &topic, 1).await;
    common::produce(&bs, &topic, &[TestMsg { partition: 0, key: None, value: "old", headers: &[] }]).await;

    let sink = VecSink::default();
    let cancel = Arc::new(AtomicBool::new(false));
    let mut p = params(&topic, StartPosition::Latest { count: 1 }, 1000, FilterSpec::default());
    p.live = true;
    let (s, c, profile) = (sink.clone(), cancel.clone(), common::plaintext_profile(&bs));
    let handle = tokio::task::spawn_blocking(move || run_browse(&profile, &MemoryStore::default(), p, c, &s));

    let deadline = Instant::now() + Duration::from_secs(15);
    while !sink.has_reached_end() {
        assert!(Instant::now() < deadline, "не дождались ReachedEnd");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let new: Vec<TestMsg> = (0..3).map(|_| TestMsg { partition: 0, key: None, value: "new", headers: &[] }).collect();
    common::produce(&bs, &topic, &new).await;
    while sink.messages().len() < 4 {
        assert!(Instant::now() < deadline, "не дождались новых сообщений");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cancel.store(true, Ordering::Relaxed);
    handle.await.unwrap();
    assert!(matches!(sink.last(), BrowseEvent::Finished { reason: FinishReason::Cancelled, .. }));
}
```

- [ ] **Step 7: Запустить**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_browse`
Expected: PASS, 6 тестов.

- [ ] **Step 8: Commit**

```bash
git add src-tauri/src/browse src-tauri/tests
git commit -m "feat(browse): сессия чтения без коммитов, live tail, реестр сессий"
```

---

### Task 13: Состояние приложения и Tauri-команды

**Files:**
- Create: `src-tauri/src/state.rs`
- Create: `src-tauri/src/commands/mod.rs`, `src-tauri/src/commands/connections.rs`, `src-tauri/src/commands/cluster.rs`, `src-tauri/src/commands/topics.rs`, `src-tauri/src/commands/browse.rs`
- Modify: `src-tauri/src/lib.rs` (полная замена)

**Interfaces:**
- Consumes: всё из Tasks 2–12
- Produces (IPC; имена аргументов в JS — camelCase):
  - `list_connections() -> ConnectionProfile[]`
  - `save_connection({ input: { profile, saslPassword? } }) -> ConnectionProfile`
  - `delete_connection({ id }) -> void`, `disconnect({ connectionId }) -> void`
  - `test_connection({ input: { profile, saslPassword? } }) -> TestReport`
  - `get_cluster_overview({ connectionId }) -> ClusterOverview`, `get_broker_config({ connectionId, brokerId }) -> ConfigEntryDto[]`
  - `list_topics({ connectionId }) -> TopicSummary[]`, `get_topic_stats({ connectionId, topics }) -> TopicStats[]`, `get_topic({ connectionId, topic }) -> TopicDetails`, `get_topic_configs({ connectionId, topic }) -> ConfigEntryDto[]`
  - `start_browse({ params, onEvent: Channel<BrowseEvent> }) -> string` (session id), `stop_session({ sessionId }) -> boolean`
  - `struct AppState { connections: ConnectionStore, secrets: Arc<dyn SecretStore>, pool: ClientPool, sessions: Arc<SessionRegistry> }` с методами `new(data_dir, secrets)`, `profile(id)`, `handles(id)`

- [ ] **Step 1: Тест состояния** (в конец `state.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::sample_profile;

    #[test]
    fn handles_for_unknown_connection_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        assert_eq!(state.handles("nope").err().unwrap().code, ErrorCode::NotFound);
        state.connections.upsert(sample_profile("a")).unwrap();
        assert!(state.handles("a").is_ok());
    }
}
```

- [ ] **Step 2: Реализация `state.rs`** (над тестом)

```rust
use std::path::Path;
use std::sync::Arc;

use crate::browse::registry::SessionRegistry;
use crate::error::AppResult;
use crate::kafka::pool::{ClientPool, KafkaHandles};
use crate::secrets::SecretStore;
use crate::storage::connections::{ConnectionProfile, ConnectionStore};

pub struct AppState {
    pub connections: ConnectionStore,
    pub secrets: Arc<dyn SecretStore>,
    pub pool: ClientPool,
    pub sessions: Arc<SessionRegistry>,
}

impl AppState {
    pub fn new(data_dir: &Path, secrets: Arc<dyn SecretStore>) -> AppResult<Self> {
        Ok(Self {
            connections: ConnectionStore::open(data_dir)?,
            secrets,
            pool: ClientPool::default(),
            sessions: Arc::new(SessionRegistry::default()),
        })
    }

    pub fn profile(&self, id: &str) -> AppResult<ConnectionProfile> {
        self.connections.get(id)
    }

    pub fn handles(&self, id: &str) -> AppResult<Arc<KafkaHandles>> {
        let profile = self.connections.get(id)?;
        self.pool.get(&profile, self.secrets.as_ref())
    }
}
```

- [ ] **Step 3: `commands/mod.rs`**

```rust
pub mod browse;
pub mod cluster;
pub mod connections;
pub mod topics;

use crate::error::{AppError, AppResult};

/// Блокирующие вызовы librdkafka не выполняются на async-потоках Tauri.
pub(crate) async fn blocking<T, F>(f: F) -> AppResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> AppResult<T> + Send + 'static,
{
    tauri::async_runtime::spawn_blocking(f)
        .await
        .map_err(|e| AppError::internal(format!("Фоновая задача завершилась аварийно: {e}")))?
}
```

- [ ] **Step 4: `commands/connections.rs`**

```rust
use std::sync::Arc;

use serde::Deserialize;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::config::SASL_PASSWORD;
use crate::kafka::connectivity::{self, TestReport};
use crate::secrets::{secret_key, MemoryStore, SecretStore};
use crate::state::AppState;
use crate::storage::connections::ConnectionProfile;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInput {
    pub profile: ConnectionProfile,
    #[serde(default)]
    pub sasl_password: Option<String>,
}

#[tauri::command]
pub fn list_connections(state: State<'_, AppState>) -> Vec<ConnectionProfile> {
    state.connections.list()
}

#[tauri::command]
pub fn save_connection(state: State<'_, AppState>, input: ConnectionInput) -> AppResult<ConnectionProfile> {
    let mut profile = input.profile;
    if profile.id.is_empty() {
        profile.id = uuid::Uuid::new_v4().to_string();
    }
    // upsert валидирует профиль — секрет пишем только после успешного сохранения.
    let saved = state.connections.upsert(profile)?;
    if let Some(pw) = input.sasl_password.filter(|p| !p.is_empty()) {
        state.secrets.set(&secret_key(&saved.id, SASL_PASSWORD), &pw)?;
    }
    state.pool.evict(&saved.id);
    Ok(saved)
}

#[tauri::command]
pub fn delete_connection(state: State<'_, AppState>, id: String) -> AppResult<()> {
    state.connections.delete(&id)?;
    state.secrets.delete(&secret_key(&id, SASL_PASSWORD))?;
    state.pool.evict(&id);
    Ok(())
}

#[tauri::command]
pub fn disconnect(state: State<'_, AppState>, connection_id: String) {
    state.pool.evict(&connection_id);
}

/// Проверяет и несохранённый профиль: если пароль передан, берём его, а не из keychain.
#[tauri::command]
pub async fn test_connection(state: State<'_, AppState>, input: ConnectionInput) -> AppResult<TestReport> {
    let secrets: Arc<dyn SecretStore> = match input.sasl_password.filter(|p| !p.is_empty()) {
        Some(pw) => {
            let mem = MemoryStore::default();
            mem.set(&secret_key(&input.profile.id, SASL_PASSWORD), &pw)?;
            Arc::new(mem)
        }
        None => state.secrets.clone(),
    };
    let profile = input.profile;
    blocking(move || Ok(connectivity::test_connection(&profile, secrets.as_ref()))).await
}
```

- [ ] **Step 5: `commands/cluster.rs` и `commands/topics.rs`**

`commands/cluster.rs`:
```rust
use rdkafka::admin::ResourceSpecifier;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::cluster::{self, ClusterOverview};
use crate::kafka::configs::{describe_configs, ConfigEntryDto};
use crate::state::AppState;

#[tauri::command]
pub async fn get_cluster_overview(state: State<'_, AppState>, connection_id: String) -> AppResult<ClusterOverview> {
    let h = state.handles(&connection_id)?;
    blocking(move || cluster::cluster_overview(&h)).await
}

#[tauri::command]
pub async fn get_broker_config(
    state: State<'_, AppState>,
    connection_id: String,
    broker_id: i32,
) -> AppResult<Vec<ConfigEntryDto>> {
    let h = state.handles(&connection_id)?;
    describe_configs(&h, ResourceSpecifier::Broker(broker_id)).await
}
```

`commands/topics.rs`:
```rust
use rdkafka::admin::ResourceSpecifier;
use tauri::State;

use super::blocking;
use crate::error::AppResult;
use crate::kafka::configs::{describe_configs, ConfigEntryDto};
use crate::kafka::topics::{self, TopicDetails, TopicStats, TopicSummary};
use crate::state::AppState;

#[tauri::command]
pub async fn list_topics(state: State<'_, AppState>, connection_id: String) -> AppResult<Vec<TopicSummary>> {
    let h = state.handles(&connection_id)?;
    blocking(move || topics::list_topics(&h)).await
}

#[tauri::command]
pub async fn get_topic_stats(
    state: State<'_, AppState>,
    connection_id: String,
    topics: Vec<String>,
) -> AppResult<Vec<TopicStats>> {
    let h = state.handles(&connection_id)?;
    blocking(move || topics::topic_stats(&h, &topics)).await
}

#[tauri::command]
pub async fn get_topic(state: State<'_, AppState>, connection_id: String, topic: String) -> AppResult<TopicDetails> {
    let h = state.handles(&connection_id)?;
    blocking(move || topics::topic_details(&h, &topic)).await
}

#[tauri::command]
pub async fn get_topic_configs(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
) -> AppResult<Vec<ConfigEntryDto>> {
    let h = state.handles(&connection_id)?;
    describe_configs(&h, ResourceSpecifier::Topic(&topic)).await
}
```

- [ ] **Step 6: `commands/browse.rs`**

```rust
use tauri::ipc::Channel;
use tauri::State;

use crate::browse::session::{run_browse, BrowseParams, EventSink};
use crate::browse::types::BrowseEvent;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

impl EventSink for Channel<BrowseEvent> {
    fn send(&self, event: BrowseEvent) -> bool {
        Channel::<BrowseEvent>::send(self, event).is_ok()
    }
}

#[tauri::command]
pub fn start_browse(state: State<'_, AppState>, params: BrowseParams, on_event: Channel<BrowseEvent>) -> AppResult<String> {
    let profile = state.profile(&params.connection_id)?;
    let secrets = state.secrets.clone();
    let sessions = state.sessions.clone();
    let (id, cancel) = sessions.register();
    let session_id = id.clone();
    std::thread::Builder::new()
        .name(format!("browse-{id}"))
        .spawn(move || {
            run_browse(&profile, secrets.as_ref(), params, cancel, &on_event);
            sessions.remove(&session_id);
        })
        .map_err(|e| AppError::internal(format!("Не удалось запустить поток чтения: {e}")))?;
    Ok(id)
}

#[tauri::command]
pub fn stop_session(state: State<'_, AppState>, session_id: String) -> bool {
    state.sessions.cancel(&session_id)
}
```

- [ ] **Step 7: Полностью заменить `src-tauri/src/lib.rs`**

```rust
pub mod browse;
pub mod commands;
pub mod error;
pub mod kafka;
pub mod secrets;
pub mod state;
pub mod storage;

use std::sync::Arc;

use tauri::Manager;
use tracing_subscriber::EnvFilter;

use crate::secrets::KeyringStore;
use crate::state::AppState;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let dir = app.path().app_data_dir()?;
            app.manage(AppState::new(&dir, Arc::new(KeyringStore))?);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            commands::connections::list_connections,
            commands::connections::save_connection,
            commands::connections::delete_connection,
            commands::connections::disconnect,
            commands::connections::test_connection,
            commands::cluster::get_cluster_overview,
            commands::cluster::get_broker_config,
            commands::topics::list_topics,
            commands::topics::get_topic_stats,
            commands::topics::get_topic,
            commands::topics::get_topic_configs,
            commands::browse::start_browse,
            commands::browse::stop_session,
        ])
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app, event| {
            if let tauri::RunEvent::Exit = event {
                app.state::<AppState>().sessions.cancel_all();
            }
        });
}
```
`src-tauri/src/main.rs` из шаблона оставить как есть (он вызывает `kafka_explorer_lib::run()`).

- [ ] **Step 8: Сборка и все Rust-тесты**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib && cargo clippy --manifest-path src-tauri/Cargo.toml -- -D warnings`
Expected: все unit-тесты PASS, clippy без предупреждений.

- [ ] **Step 9: Commit**

```bash
git add src-tauri/src
git commit -m "feat(app): AppState и Tauri-команды для подключений, кластера, топиков, browse"
```

---

### Task 14: Фронтенд — типы и API-клиент

**Files:**
- Create: `src/api/types.ts`, `src/api/client.ts`, `src/api/client.test.ts`

**Interfaces:**
- Consumes: IPC из Task 13
- Produces: TS-типы (зеркало Rust DTO); `api` (объект с методами ниже); `toAppError(e: unknown): AppError`; `isAppError(e: unknown): e is AppError`
  - `api.listConnections()`, `api.saveConnection(profile, saslPassword?)`, `api.deleteConnection(id)`, `api.testConnection(profile, saslPassword?)`
  - `api.getClusterOverview(connectionId)`, `api.getBrokerConfig(connectionId, brokerId)`
  - `api.listTopics(connectionId)`, `api.getTopicStats(connectionId, topics)`, `api.getTopic(connectionId, topic)`, `api.getTopicConfigs(connectionId, topic)`
  - `api.startBrowse(params, onEvent) -> Promise<string>`, `api.stopSession(sessionId) -> Promise<boolean>`

- [ ] **Step 1: Типы `src/api/types.ts`**

```ts
export type ErrorCode =
  | "CONNECTION_FAILED"
  | "TLS_ERROR"
  | "AUTH_FAILED"
  | "NOT_AUTHORIZED"
  | "TIMEOUT"
  | "INVALID_INPUT"
  | "NOT_FOUND"
  | "INTERNAL";

export interface AppError {
  code: ErrorCode;
  message: string;
  details?: string;
  retryable: boolean;
}

export type Environment = "local" | "dev" | "test" | "stage" | "prod";
export type SaslMechanism = "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512";

export interface TlsConfig {
  caLocation: string | null;
  verifyHostname: boolean;
}

export interface SaslConfig {
  mechanism: SaslMechanism;
  username: string;
}

export type SecurityConfig =
  | { protocol: "PLAINTEXT" }
  | { protocol: "SSL"; tls: TlsConfig }
  | { protocol: "SASL_PLAINTEXT"; sasl: SaslConfig }
  | { protocol: "SASL_SSL"; tls: TlsConfig; sasl: SaslConfig };

export type Protocol = SecurityConfig["protocol"];

export interface ConnectionProfile {
  id: string;
  name: string;
  color: string;
  environment: Environment;
  readOnly: boolean;
  bootstrapServers: string;
  security: SecurityConfig;
  extraProperties: Record<string, string>;
  requestTimeoutMs: number;
}

export interface TestStep {
  name: "config" | "tcp" | "kafka";
  ok: boolean;
  message: string;
  error?: AppError;
}

export interface TestReport {
  ok: boolean;
  steps: TestStep[];
}

export interface BrokerInfo {
  id: number;
  host: string;
  port: number;
}

export interface ClusterOverview {
  clusterId: string | null;
  controllerId: number | null;
  brokers: BrokerInfo[];
  topicCount: number;
  internalTopicCount: number;
  partitionCount: number;
  underReplicatedPartitions: number;
  offlinePartitions: number;
}

export interface ConfigEntry {
  name: string;
  value: string | null;
  source: string;
  isDefault: boolean;
  isReadOnly: boolean;
  isSensitive: boolean;
}

export interface TopicSummary {
  name: string;
  internal: boolean;
  partitionCount: number;
  replicationFactor: number;
  underReplicated: number;
}

export interface TopicStats {
  name: string;
  /** null — для части партиций оффсеты недоступны */
  messageCount: number | null;
}

export interface PartitionDetails {
  id: number;
  leader: number;
  replicas: number[];
  isr: number[];
  low: number | null;
  high: number | null;
  messageCount: number | null;
  offsetsError?: string;
  underReplicated: boolean;
}

export interface TopicDetails {
  name: string;
  internal: boolean;
  replicationFactor: number;
  /** null — у какой-либо партиции оффсеты недоступны */
  messageCount: number | null;
  partitions: PartitionDetails[];
}

export type DataFormat = "auto" | "string" | "json" | "hex" | "base64";

export interface DecodedData {
  format: DataFormat;
  text: string;
  size: number;
  truncated: boolean;
  error?: string;
}

export interface HeaderView {
  name: string;
  value: string | null;
}

export interface BrowsedMessage {
  partition: number;
  offset: number;
  timestamp: number | null;
  key: DecodedData | null;
  value: DecodedData | null;
  headers: HeaderView[];
  size: number;
}

export type StartPosition =
  | { type: "earliest" }
  | { type: "latest"; count: number }
  | { type: "offset"; offset: number }
  | { type: "timestamp"; timestampMs: number }
  | { type: "lastMinutes"; minutes: number };

export interface TextMatch {
  pattern: string;
  regex: boolean;
  caseSensitive: boolean;
}

export interface FilterSpec {
  key?: TextMatch;
  value?: TextMatch;
  header?: { name: string; value?: TextMatch };
  jsonPath?: string;
  fromTimestamp?: number;
  toTimestamp?: number;
}

export interface BrowseParams {
  connectionId: string;
  topic: string;
  partitions: number[] | null;
  start: StartPosition;
  limit: number;
  live: boolean;
  keyFormat: DataFormat;
  valueFormat: DataFormat;
  filter: FilterSpec;
}

export type FinishReason = "limit" | "endOfTopic" | "cancelled";

export type BrowseEvent =
  | { type: "messages"; messages: BrowsedMessage[] }
  | { type: "progress"; scanned: number; matched: number }
  | { type: "reachedEnd" }
  | { type: "finished"; scanned: number; matched: number; reason: FinishReason }
  | { type: "error"; error: AppError };
```

- [ ] **Step 2: Тест клиента `src/api/client.test.ts`**

```ts
import { beforeEach, describe, expect, it, vi } from "vitest";

const invoke = vi.fn();
vi.mock("@tauri-apps/api/core", () => ({
  invoke: (...args: unknown[]) => invoke(...args),
  Channel: class {
    onmessage: (m: unknown) => void = () => {};
  },
}));

import { api, toAppError } from "./client";
import type { BrowseParams } from "./types";

beforeEach(() => invoke.mockReset());

describe("api client", () => {
  it("передаёт аргументы в camelCase", async () => {
    invoke.mockResolvedValue([]);
    await api.getTopicStats("c1", ["a", "b"]);
    expect(invoke).toHaveBeenCalledWith("get_topic_stats", { connectionId: "c1", topics: ["a", "b"] });
  });

  it("оборачивает профиль и пароль в input", async () => {
    invoke.mockResolvedValue({ ok: true, steps: [] });
    const profile = { id: "", name: "x" } as never;
    await api.testConnection(profile, "pw");
    expect(invoke).toHaveBeenCalledWith("test_connection", { input: { profile, saslPassword: "pw" } });
  });

  it("пробрасывает AppError как есть", async () => {
    const err = { code: "NOT_FOUND", message: "нет", retryable: false };
    invoke.mockRejectedValue(err);
    await expect(api.listTopics("c")).rejects.toEqual(err);
  });

  it("строковые ошибки превращает в INTERNAL", async () => {
    invoke.mockRejectedValue("boom");
    await expect(api.listTopics("c")).rejects.toEqual({ code: "INTERNAL", message: "boom", retryable: false });
  });

  it("startBrowse передаёт Channel и вызывает обработчик", async () => {
    invoke.mockResolvedValue("sid");
    const onEvent = vi.fn();
    const id = await api.startBrowse({ topic: "t" } as BrowseParams, onEvent);
    expect(id).toBe("sid");
    const [, args] = invoke.mock.calls[0];
    args.onEvent.onmessage({ type: "reachedEnd" });
    expect(onEvent).toHaveBeenCalledWith({ type: "reachedEnd" });
  });

  it("toAppError понимает Error", () => {
    expect(toAppError(new Error("x"))).toEqual({ code: "INTERNAL", message: "x", retryable: false });
  });
});
```

- [ ] **Step 3: Убедиться в падении**

Run: `pnpm vitest run src/api`
Expected: FAIL — `Failed to resolve import "./client"`.

- [ ] **Step 4: Реализация `src/api/client.ts`**

```ts
import { Channel, invoke } from "@tauri-apps/api/core";
import type {
  AppError,
  BrowseEvent,
  BrowseParams,
  ClusterOverview,
  ConfigEntry,
  ConnectionProfile,
  TestReport,
  TopicDetails,
  TopicStats,
  TopicSummary,
} from "./types";

export function isAppError(e: unknown): e is AppError {
  return typeof e === "object" && e !== null && "code" in e && "message" in e;
}

export function toAppError(e: unknown): AppError {
  if (isAppError(e)) return e;
  if (e instanceof Error) return { code: "INTERNAL", message: e.message, retryable: false };
  return { code: "INTERNAL", message: String(e), retryable: false };
}

async function call<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  try {
    return await invoke<T>(cmd, args);
  } catch (e) {
    throw toAppError(e);
  }
}

export const api = {
  listConnections: () => call<ConnectionProfile[]>("list_connections"),
  saveConnection: (profile: ConnectionProfile, saslPassword?: string) =>
    call<ConnectionProfile>("save_connection", { input: { profile, saslPassword: saslPassword ?? null } }),
  deleteConnection: (id: string) => call<void>("delete_connection", { id }),
  testConnection: (profile: ConnectionProfile, saslPassword?: string) =>
    call<TestReport>("test_connection", { input: { profile, saslPassword: saslPassword ?? null } }),

  getClusterOverview: (connectionId: string) => call<ClusterOverview>("get_cluster_overview", { connectionId }),
  getBrokerConfig: (connectionId: string, brokerId: number) =>
    call<ConfigEntry[]>("get_broker_config", { connectionId, brokerId }),

  listTopics: (connectionId: string) => call<TopicSummary[]>("list_topics", { connectionId }),
  getTopicStats: (connectionId: string, topics: string[]) =>
    call<TopicStats[]>("get_topic_stats", { connectionId, topics }),
  getTopic: (connectionId: string, topic: string) => call<TopicDetails>("get_topic", { connectionId, topic }),
  getTopicConfigs: (connectionId: string, topic: string) =>
    call<ConfigEntry[]>("get_topic_configs", { connectionId, topic }),

  startBrowse: (params: BrowseParams, onEvent: (e: BrowseEvent) => void) => {
    const channel = new Channel<BrowseEvent>();
    channel.onmessage = onEvent;
    return call<string>("start_browse", { params, onEvent: channel });
  },
  stopSession: (sessionId: string) => call<boolean>("stop_session", { sessionId }),
};
```

Тест про `saslPassword` ожидает `"pw"`: при переданном пароле `?? null` не срабатывает, так что тест корректен.

- [ ] **Step 5: Тесты проходят**

Run: `pnpm vitest run src/api`
Expected: PASS, 6 тестов.

- [ ] **Step 6: Commit**

```bash
git add src/api
git commit -m "feat(ui): TS-типы и API-клиент поверх Tauri invoke/Channel"
```

---

### Task 15: Фронтенд — вкладки, раскладка, дерево подключений

**Files:**
- Create: `src/state/workspace.ts`, `src/state/workspace.test.ts`
- Create: `src/lib/env.ts`, `src/lib/format.ts`, `src/lib/format.test.ts`
- Create: `src/components/common/ErrorBanner.tsx`, `src/components/Sidebar.tsx`, `src/components/Layout.tsx`, `src/components/TabContent.tsx`
- Create: `src/styles.css`
- Modify: `src/App.tsx`, `src/main.tsx` (полная замена); удалить `src/App.css` и `src/assets/` из шаблона

**Interfaces:**
- Consumes: `api`, `toAppError`, типы
- Produces:
  - `type TabSpec = { kind: "cluster"; connectionId } | { kind: "topics"; connectionId } | { kind: "topic"; connectionId; topic } | { kind: "connection-form"; connectionId: string | null }`, `type Tab = TabSpec & { id: string }`
  - `tabId(spec): string`, `useWorkspace` (Zustand) с `tabs`, `activeId`, `open(spec)`, `close(id)`, `activate(id)`
  - `ENV_COLORS: Record<Environment, string>`, `ENV_LABELS: Record<Environment, string>`
  - `formatTimestamp(ms: number | null, utc?: boolean): string`, `formatBytes(n: number): string`, `formatCount(n?: number | null): string` (undefined → «…», null → «—»)
  - `<ErrorBanner error={unknown} onRetry?={() => void} />`
  - `<TabContent tab={Tab} />` — роутер по `tab.kind`; в этом таске заглушки, в Tasks 16–20 их заменяют реальные компоненты

- [ ] **Step 1: Тесты store и форматирования**

`src/state/workspace.test.ts`:
```ts
import { beforeEach, describe, expect, it } from "vitest";
import { tabId, useWorkspace } from "./workspace";

beforeEach(() => useWorkspace.setState({ tabs: [], activeId: null }));

describe("workspace", () => {
  it("повторное открытие активирует существующую вкладку", () => {
    const { open } = useWorkspace.getState();
    open({ kind: "topics", connectionId: "c" });
    open({ kind: "topic", connectionId: "c", topic: "t" });
    open({ kind: "topics", connectionId: "c" });
    const s = useWorkspace.getState();
    expect(s.tabs).toHaveLength(2);
    expect(s.activeId).toBe("topics:c");
  });

  it("закрытие активной вкладки активирует соседнюю", () => {
    const { open, close, activate } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "a" });
    open({ kind: "cluster", connectionId: "b" });
    open({ kind: "cluster", connectionId: "c" });
    activate("cluster:b");
    close("cluster:b");
    expect(useWorkspace.getState().activeId).toBe("cluster:c");
    close("cluster:c");
    expect(useWorkspace.getState().activeId).toBe("cluster:a");
    close("cluster:a");
    expect(useWorkspace.getState().activeId).toBeNull();
  });

  it("id форм подключения", () => {
    expect(tabId({ kind: "connection-form", connectionId: null })).toBe("connection-form:new");
    expect(tabId({ kind: "topic", connectionId: "c", topic: "x" })).toBe("topic:c:x");
  });
});
```

`src/lib/format.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import { formatBytes, formatCount, formatTimestamp } from "./format";

describe("format", () => {
  it("timestamp в UTC с миллисекундами", () => {
    expect(formatTimestamp(Date.UTC(2026, 0, 2, 3, 4, 5, 6), true)).toBe("2026-01-02 03:04:05.006");
    expect(formatTimestamp(null)).toBe("—");
  });

  it("bytes", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(2048)).toBe("2.0 KB");
    expect(formatBytes(3 * 1024 * 1024)).toBe("3.0 MB");
  });

  it("count: загрузка и недоступно", () => {
    expect(formatCount(undefined)).toBe("…");
    expect(formatCount(null)).toBe("—");
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/state src/lib`
Expected: FAIL — модули не найдены.

- [ ] **Step 3: Реализация store и утилит**

`src/state/workspace.ts`:
```ts
import { create } from "zustand";

export type TabSpec =
  | { kind: "cluster"; connectionId: string }
  | { kind: "topics"; connectionId: string }
  | { kind: "topic"; connectionId: string; topic: string }
  | { kind: "connection-form"; connectionId: string | null };

export type Tab = TabSpec & { id: string };

export function tabId(spec: TabSpec): string {
  switch (spec.kind) {
    case "topic":
      return `topic:${spec.connectionId}:${spec.topic}`;
    case "connection-form":
      return `connection-form:${spec.connectionId ?? "new"}`;
    default:
      return `${spec.kind}:${spec.connectionId}`;
  }
}

interface WorkspaceState {
  tabs: Tab[];
  activeId: string | null;
  open: (spec: TabSpec) => void;
  close: (id: string) => void;
  activate: (id: string) => void;
}

export const useWorkspace = create<WorkspaceState>((set) => ({
  tabs: [],
  activeId: null,
  open: (spec) =>
    set((s) => {
      const id = tabId(spec);
      if (s.tabs.some((t) => t.id === id)) return { activeId: id };
      return { tabs: [...s.tabs, { ...spec, id } as Tab], activeId: id };
    }),
  close: (id) =>
    set((s) => {
      const idx = s.tabs.findIndex((t) => t.id === id);
      if (idx < 0) return s;
      const tabs = s.tabs.filter((t) => t.id !== id);
      const activeId = s.activeId !== id ? s.activeId : (tabs[idx] ?? tabs[idx - 1])?.id ?? null;
      return { tabs, activeId };
    }),
  activate: (id) => set({ activeId: id }),
}));
```

`src/lib/env.ts`:
```ts
import type { Environment } from "../api/types";

export const ENV_COLORS: Record<Environment, string> = {
  local: "#8a8f98",
  dev: "#3b82f6",
  test: "#16a34a",
  stage: "#f59e0b",
  prod: "#dc2626",
};

export const ENV_LABELS: Record<Environment, string> = {
  local: "LOCAL",
  dev: "DEV",
  test: "TEST",
  stage: "STAGE",
  prod: "PROD",
};
```

`src/lib/format.ts`:
```ts
const pad = (n: number, w = 2) => String(n).padStart(w, "0");

export function formatTimestamp(ms: number | null, utc = false): string {
  if (ms === null) return "—";
  const d = new Date(ms);
  const parts = utc
    ? [d.getUTCFullYear(), d.getUTCMonth() + 1, d.getUTCDate(), d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds(), d.getUTCMilliseconds()]
    : [d.getFullYear(), d.getMonth() + 1, d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds(), d.getMilliseconds()];
  const [y, mo, da, h, mi, s, milli] = parts;
  return `${y}-${pad(mo)}-${pad(da)} ${pad(h)}:${pad(mi)}:${pad(s)}.${pad(milli, 3)}`;
}

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

/** undefined — ещё загружается, null — недоступно. */
export function formatCount(n?: number | null): string {
  if (n === undefined) return "…";
  if (n === null) return "—";
  return n.toLocaleString("ru-RU");
}
```

- [ ] **Step 4: Тесты проходят**

Run: `pnpm vitest run src/state src/lib`
Expected: PASS, 6 тестов.

- [ ] **Step 5: Компоненты раскладки**

`src/components/common/ErrorBanner.tsx`:
```tsx
import { toAppError } from "../../api/client";

export function ErrorBanner({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const e = toAppError(error);
  return (
    <div className="error-banner" role="alert">
      <div className="error-title">
        <strong>{e.message}</strong> <span className="muted">({e.code})</span>
        {onRetry && (
          <button type="button" onClick={onRetry}>
            Повторить
          </button>
        )}
      </div>
      {e.details && (
        <details>
          <summary>Подробности</summary>
          <pre>{e.details}</pre>
        </details>
      )}
    </div>
  );
}
```

`src/components/Sidebar.tsx`:
```tsx
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { ENV_COLORS, ENV_LABELS } from "../lib/env";
import { useWorkspace } from "../state/workspace";
import { ErrorBanner } from "./common/ErrorBanner";

export function Sidebar() {
  const { data: connections = [], error } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const open = useWorkspace((s) => s.open);

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <span>Подключения</span>
        <button type="button" title="Новое подключение" onClick={() => open({ kind: "connection-form", connectionId: null })}>
          +
        </button>
      </div>
      {error && <ErrorBanner error={error} />}
      {connections.length === 0 && !error && (
        <p className="empty">
          Добавьте первое подключение. Для локальной Kafka: <code>docker compose -f dev/docker-compose.yml up -d</code> и
          bootstrap <code>localhost:9092</code>.
        </p>
      )}
      <ul className="tree">
        {connections.map((c) => (
          <li key={c.id}>
            <div className="tree-conn">
              <span className="dot" style={{ background: c.color }} />
              <span className="tree-name">{c.name}</span>
              <span className="env-badge" style={{ background: ENV_COLORS[c.environment] }}>
                {ENV_LABELS[c.environment]}
              </span>
              {c.readOnly && <span title="Только чтение">🔒</span>}
              <button type="button" className="icon" title="Изменить подключение" onClick={() => open({ kind: "connection-form", connectionId: c.id })}>
                ✎
              </button>
            </div>
            <ul>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "cluster", connectionId: c.id })}>
                  Обзор кластера
                </button>
              </li>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "topics", connectionId: c.id })}>
                  Топики
                </button>
              </li>
            </ul>
          </li>
        ))}
      </ul>
    </aside>
  );
}
```

`src/components/TabContent.tsx` (заглушки; Tasks 16–20 заменяют ветки на реальные компоненты):
```tsx
import type { Tab } from "../state/workspace";

export function TabContent({ tab }: { tab: Tab }) {
  switch (tab.kind) {
    case "connection-form":
      return <p>Форма подключения</p>;
    case "cluster":
      return <p>Обзор кластера</p>;
    case "topics":
      return <p>Топики</p>;
    case "topic":
      return <p>Топик {tab.topic}</p>;
  }
}
```

`src/components/Layout.tsx`:
```tsx
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import type { ConnectionProfile } from "../api/types";
import { ENV_COLORS } from "../lib/env";
import { type Tab, useWorkspace } from "../state/workspace";
import { Sidebar } from "./Sidebar";
import { TabContent } from "./TabContent";

function tabTitle(tab: Tab, byId: Map<string, ConnectionProfile>): string {
  const conn = tab.connectionId ? byId.get(tab.connectionId)?.name ?? "?" : "";
  switch (tab.kind) {
    case "connection-form":
      return tab.connectionId ? `Подключение: ${conn}` : "Новое подключение";
    case "cluster":
      return `${conn} · Обзор`;
    case "topics":
      return `${conn} · Топики`;
    case "topic":
      return `${conn} · ${tab.topic}`;
  }
}

export function Layout() {
  const { tabs, activeId, activate, close } = useWorkspace();
  const { data: connections = [] } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const byId = new Map(connections.map((c) => [c.id, c]));
  const active = tabs.find((t) => t.id === activeId);
  const activeConn = active?.connectionId ? byId.get(active.connectionId) : undefined;

  return (
    <div className="layout">
      <Sidebar />
      <main className="workspace">
        <div className="env-stripe" style={{ background: activeConn ? ENV_COLORS[activeConn.environment] : "transparent" }} />
        <nav className="tabbar">
          {tabs.map((t) => (
            <div key={t.id} className={t.id === activeId ? "tab active" : "tab"} onClick={() => activate(t.id)}>
              <span>{tabTitle(t, byId)}</span>
              <button
                type="button"
                className="icon"
                aria-label="Закрыть вкладку"
                onClick={(e) => {
                  e.stopPropagation();
                  close(t.id);
                }}
              >
                ×
              </button>
            </div>
          ))}
        </nav>
        {tabs.length === 0 && <p className="empty">Выберите подключение слева или добавьте новое.</p>}
        {/* Неактивные вкладки скрыты, но не размонтированы — browse-сессии и состояние форм сохраняются. */}
        {tabs.map((t) => (
          <section key={t.id} className="tab-content" hidden={t.id !== activeId}>
            <TabContent tab={t} />
          </section>
        ))}
      </main>
    </div>
  );
}
```

`src/App.tsx`:
```tsx
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Layout } from "./components/Layout";

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, refetchOnWindowFocus: false } },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Layout />
    </QueryClientProvider>
  );
}
```

`src/main.tsx`:
```tsx
import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
```

`src/styles.css`:
```css
:root {
  --bg: #ffffff;
  --bg-alt: #f4f5f7;
  --fg: #1d2129;
  --muted: #6b7280;
  --border: #e3e5e8;
  --accent: #3b82f6;
  --danger: #dc2626;
  --warn: #f59e0b;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  font-size: 13px;
  color: var(--fg);
  background: var(--bg);
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #1b1d21;
    --bg-alt: #23262b;
    --fg: #e6e8eb;
    --muted: #9aa0a6;
    --border: #33373d;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); }
button { font: inherit; color: inherit; background: var(--bg-alt); border: 1px solid var(--border); border-radius: 4px; padding: 3px 8px; cursor: pointer; }
button:disabled { opacity: 0.5; cursor: default; }
button.icon { background: none; border: none; padding: 0 4px; }
button.link { background: none; border: none; color: var(--accent); padding: 2px 0; }
button.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
button.danger { color: var(--danger); }
input, select, textarea { font: inherit; color: inherit; background: var(--bg); border: 1px solid var(--border); border-radius: 4px; padding: 3px 6px; }
code, pre { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
pre { white-space: pre-wrap; word-break: break-all; margin: 0; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--border); }
th { color: var(--muted); font-weight: 500; }
tr.clickable { cursor: pointer; }
tr.clickable:hover, .row.clickable:hover { background: var(--bg-alt); }
.muted { color: var(--muted); }
.warn { color: var(--warn); font-weight: 600; }
.empty { color: var(--muted); padding: 16px; }
.layout { display: flex; height: 100vh; }
.sidebar { width: 260px; flex-shrink: 0; border-right: 1px solid var(--border); background: var(--bg-alt); overflow: auto; }
.sidebar-header { display: flex; justify-content: space-between; align-items: center; padding: 8px 12px; font-weight: 600; }
.tree, .tree ul { list-style: none; margin: 0; padding: 0; }
.tree > li { padding: 4px 12px; }
.tree ul { padding-left: 18px; }
.tree-conn { display: flex; align-items: center; gap: 6px; }
.tree-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.dot { width: 8px; height: 8px; border-radius: 50%; }
.env-badge { color: #fff; font-size: 10px; padding: 0 4px; border-radius: 3px; }
.workspace { flex: 1; display: flex; flex-direction: column; min-width: 0; }
.env-stripe { height: 3px; flex-shrink: 0; }
.tabbar { display: flex; border-bottom: 1px solid var(--border); overflow-x: auto; flex-shrink: 0; }
.tab { display: flex; align-items: center; gap: 4px; padding: 6px 10px; border-right: 1px solid var(--border); cursor: pointer; white-space: nowrap; }
.tab.active { background: var(--bg-alt); font-weight: 600; }
.tab-content { flex: 1; overflow: auto; min-height: 0; }
.tab-content[hidden] { display: none; }
.page { padding: 12px 16px; display: flex; flex-direction: column; gap: 12px; min-height: 100%; }
.toolbar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
.toolbar h2 { margin: 0 auto 0 0; font-size: 16px; }
.stats { display: grid; grid-template-columns: repeat(auto-fill, minmax(160px, 1fr)); gap: 8px; margin: 0; }
.stats div { background: var(--bg-alt); border-radius: 6px; padding: 8px; }
.stats dt { color: var(--muted); font-size: 11px; }
.stats dd { margin: 2px 0 0; font-size: 16px; }
.form { display: grid; grid-template-columns: 180px minmax(240px, 520px); gap: 8px 12px; align-items: center; }
.form label { color: var(--muted); }
.form .full { grid-column: 1 / -1; }
.error-banner { border: 1px solid var(--danger); border-radius: 6px; padding: 8px; background: color-mix(in srgb, var(--danger) 8%, transparent); }
.error-title { display: flex; gap: 8px; align-items: center; }
.steps { list-style: none; padding: 0; margin: 0; }
.steps li { padding: 2px 0; }
.subtabs { display: flex; gap: 4px; border-bottom: 1px solid var(--border); }
.subtabs button { border: none; border-bottom: 2px solid transparent; border-radius: 0; background: none; }
.subtabs button.active { border-bottom-color: var(--accent); font-weight: 600; }
.vlist { flex: 1; overflow: auto; position: relative; border: 1px solid var(--border); border-radius: 4px; min-height: 200px; }
.row { display: grid; align-items: center; position: absolute; left: 0; right: 0; padding: 0 8px; border-bottom: 1px solid var(--border); white-space: nowrap; }
.row > span { overflow: hidden; text-overflow: ellipsis; padding-right: 8px; }
.row.header { position: sticky; top: 0; z-index: 1; background: var(--bg-alt); color: var(--muted); }
.row.selected { background: color-mix(in srgb, var(--accent) 15%, transparent); }
.topic-row { grid-template-columns: minmax(200px, 1fr) 90px 60px 120px 90px; }
.msg-row { grid-template-columns: 50px 90px 190px minmax(80px, 1fr) minmax(160px, 3fr) 50px 70px; }
.browse { display: flex; flex-direction: column; gap: 8px; height: 100%; }
.browse-body { display: flex; gap: 8px; flex: 1; min-height: 300px; }
.browse-body .vlist { flex: 3; }
.details-pane { flex: 2; border: 1px solid var(--border); border-radius: 4px; padding: 8px; overflow: auto; }
.browse-form { display: flex; flex-wrap: wrap; gap: 6px 12px; align-items: center; }
.browse-form fieldset { border: 1px solid var(--border); border-radius: 4px; display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
```

- [ ] **Step 6: Проверить сборку и тесты**

Run: `pnpm build && pnpm test`
Expected: сборка без ошибок TypeScript; тесты PASS.

- [ ] **Step 7: Commit**

```bash
git add -A src
git commit -m "feat(ui): раскладка с вкладками, дерево подключений, индикация окружения"
```

---

### Task 16: Фронтенд — форма подключения и проверка

**Files:**
- Create: `src/components/connections/profile.ts`, `src/components/connections/profile.test.ts`
- Create: `src/components/connections/ConnectionForm.tsx`, `src/components/connections/ConnectionForm.test.tsx`, `src/components/connections/ConnectionFormTab.tsx`
- Modify: `src/components/TabContent.tsx` (ветка `connection-form`)

**Interfaces:**
- Consumes: `api.saveConnection`, `api.testConnection`, `api.deleteConnection`, `api.listConnections`, `useWorkspace`, `ErrorBanner`
- Produces:
  - `newProfile(): ConnectionProfile`, `withProtocol(sec: SecurityConfig, protocol: Protocol): SecurityConfig`, `parseProps(text: string): Record<string,string>` (бросает Error на строке без `=`), `formatProps(p): string`
  - `<ConnectionForm initial={ConnectionProfile} tabId={string} />`, `<ConnectionFormTab connectionId={string | null} tabId={string} />`

- [ ] **Step 1: Тесты `profile.test.ts`**

```ts
import { describe, expect, it } from "vitest";
import { formatProps, newProfile, parseProps, withProtocol } from "./profile";

describe("profile helpers", () => {
  it("новый профиль — PLAINTEXT localhost", () => {
    const p = newProfile();
    expect(p.id).toBe("");
    expect(p.security).toEqual({ protocol: "PLAINTEXT" });
    expect(p.bootstrapServers).toBe("localhost:9092");
  });

  it("withProtocol сохраняет уже введённые tls/sasl", () => {
    const sasl = withProtocol({ protocol: "PLAINTEXT" }, "SASL_SSL");
    expect(sasl).toEqual({
      protocol: "SASL_SSL",
      tls: { caLocation: null, verifyHostname: true },
      sasl: { mechanism: "PLAIN", username: "" },
    });
    const edited = { ...sasl, sasl: { mechanism: "SCRAM-SHA-512" as const, username: "bob" } };
    const plain = withProtocol(edited, "SASL_PLAINTEXT");
    expect(plain).toEqual({ protocol: "SASL_PLAINTEXT", sasl: { mechanism: "SCRAM-SHA-512", username: "bob" } });
    expect(withProtocol(plain, "PLAINTEXT")).toEqual({ protocol: "PLAINTEXT" });
  });

  it("parseProps / formatProps", () => {
    expect(parseProps("a=1\n\n  b.c = x=y \n# коммент")).toEqual({ a: "1", "b.c": "x=y" });
    expect(formatProps({ a: "1", b: "2" })).toBe("a=1\nb=2");
    expect(() => parseProps("oops")).toThrow("Строка 1");
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/connections`
Expected: FAIL — модуль `./profile` не найден.

- [ ] **Step 3: Реализация `profile.ts`**

```ts
import type { ConnectionProfile, Protocol, SaslConfig, SecurityConfig, TlsConfig } from "../../api/types";

export function newProfile(): ConnectionProfile {
  return {
    id: "",
    name: "",
    color: "#3b82f6",
    environment: "local",
    readOnly: false,
    bootstrapServers: "localhost:9092",
    security: { protocol: "PLAINTEXT" },
    extraProperties: {},
    requestTimeoutMs: 30000,
  };
}

const DEFAULT_TLS: TlsConfig = { caLocation: null, verifyHostname: true };
const DEFAULT_SASL: SaslConfig = { mechanism: "PLAIN", username: "" };

export function withProtocol(sec: SecurityConfig, protocol: Protocol): SecurityConfig {
  const tls = "tls" in sec ? sec.tls : DEFAULT_TLS;
  const sasl = "sasl" in sec ? sec.sasl : DEFAULT_SASL;
  switch (protocol) {
    case "PLAINTEXT":
      return { protocol };
    case "SSL":
      return { protocol, tls };
    case "SASL_PLAINTEXT":
      return { protocol, sasl };
    case "SASL_SSL":
      return { protocol, tls, sasl };
  }
}

/** Формат: одна пара key=value на строку; пустые строки и строки с # пропускаются. */
export function parseProps(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  text.split("\n").forEach((raw, i) => {
    const line = raw.trim();
    if (!line || line.startsWith("#")) return;
    const eq = line.indexOf("=");
    if (eq <= 0) throw new Error(`Строка ${i + 1}: ожидается key=value`);
    out[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  });
  return out;
}

export function formatProps(props: Record<string, string>): string {
  return Object.entries(props)
    .map(([k, v]) => `${k}=${v}`)
    .join("\n");
}
```

- [ ] **Step 4: Тест формы `ConnectionForm.test.tsx`**

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      testConnection: vi.fn(),
      saveConnection: vi.fn(),
      deleteConnection: vi.fn(),
      listConnections: vi.fn().mockResolvedValue([]),
    },
  };
});

import { api } from "../../api/client";
import { ConnectionForm } from "./ConnectionForm";
import { newProfile } from "./profile";

beforeEach(() => vi.clearAllMocks());

describe("ConnectionForm", () => {
  it("собирает профиль SASL_SSL и показывает шаги проверки", async () => {
    vi.mocked(api.testConnection).mockResolvedValue({
      ok: false,
      steps: [
        { name: "config", ok: true, message: "Конфигурация корректна" },
        { name: "tcp", ok: true, message: "TCP-соединение установлено" },
        {
          name: "kafka",
          ok: false,
          message: "Ошибка аутентификации",
          error: { code: "AUTH_FAILED", message: "Ошибка аутентификации", details: "SASL authentication error", retryable: false },
        },
      ],
    });
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);

    await user.type(screen.getByLabelText("Имя"), "dev");
    await user.selectOptions(screen.getByLabelText("Протокол"), "SASL_SSL");
    await user.type(screen.getByLabelText("Пользователь"), "alice");
    await user.type(screen.getByLabelText("Пароль"), "secret");
    await user.click(screen.getByRole("button", { name: "Проверить подключение" }));

    expect(api.testConnection).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "dev",
        security: {
          protocol: "SASL_SSL",
          tls: { caLocation: null, verifyHostname: true },
          sasl: { mechanism: "PLAIN", username: "alice" },
        },
      }),
      "secret",
    );
    expect(await screen.findByText("Ошибка аутентификации")).toBeInTheDocument();
    expect(screen.getByText("SASL authentication error")).toBeInTheDocument();
  });

  it("для нового prod-подключения включает read-only", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={newProfile()} tabId="t" />);
    await user.selectOptions(screen.getByLabelText("Окружение"), "prod");
    expect(screen.getByLabelText("Только чтение")).toBeChecked();
  });

  it("невалидные доп. свойства показывают ошибку и не вызывают сохранение", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConnectionForm initial={{ ...newProfile(), name: "x" }} tabId="t" />);
    await user.type(screen.getByLabelText("Доп. свойства librdkafka"), "broken");
    await user.click(screen.getByRole("button", { name: "Сохранить" }));
    expect(await screen.findByText("Строка 1: ожидается key=value")).toBeInTheDocument();
    expect(api.saveConnection).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 5: Реализация `ConnectionForm.tsx`**

```tsx
import { useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { AppError, ConnectionProfile, Environment, Protocol, SaslMechanism, TestReport } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { formatProps, parseProps, withProtocol } from "./profile";

const ENVIRONMENTS: Environment[] = ["local", "dev", "test", "stage", "prod"];
const PROTOCOLS: Protocol[] = ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"];
const MECHANISMS: SaslMechanism[] = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"];
const STEP_LABELS = { config: "Конфигурация", tcp: "DNS / TCP", kafka: "Kafka (TLS, SASL, метаданные)" } as const;

export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile; tabId: string }) {
  const [profile, setProfile] = useState(initial);
  const [password, setPassword] = useState("");
  const [propsText, setPropsText] = useState(formatProps(initial.extraProperties));
  const [report, setReport] = useState<TestReport | null>(null);
  const [error, setError] = useState<AppError | null>(null);
  const [busy, setBusy] = useState(false);
  const queryClient = useQueryClient();
  const open = useWorkspace((s) => s.open);
  const close = useWorkspace((s) => s.close);
  const isNew = initial.id === "";
  const sec = profile.security;

  const update = (patch: Partial<ConnectionProfile>) => setProfile((p) => ({ ...p, ...patch }));
  const current = (): ConnectionProfile => ({ ...profile, extraProperties: parseProps(propsText) });

  async function run(fn: () => Promise<void>) {
    setBusy(true);
    setError(null);
    try {
      await fn();
    } catch (e) {
      setError(toAppError(e));
    } finally {
      setBusy(false);
    }
  }

  const onTest = () =>
    run(async () => {
      setReport(null);
      setReport(await api.testConnection(current(), password || undefined));
    });

  const onSave = () =>
    run(async () => {
      const saved = await api.saveConnection(current(), password || undefined);
      await queryClient.invalidateQueries({ queryKey: ["connections"] });
      queryClient.removeQueries({ predicate: (q) => q.queryKey[1] === saved.id });
      close(tabId);
      open({ kind: "cluster", connectionId: saved.id });
    });

  const onDelete = () =>
    run(async () => {
      if (!window.confirm(`Удалить подключение «${profile.name}»?`)) return;
      await api.deleteConnection(profile.id);
      await queryClient.invalidateQueries({ queryKey: ["connections"] });
      close(tabId);
    });

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{isNew ? "Новое подключение" : `Подключение «${initial.name}»`}</h2>
      </div>
      <div className="form">
        <label htmlFor="cf-name">Имя</label>
        <input id="cf-name" value={profile.name} onChange={(e) => update({ name: e.target.value })} />

        <label htmlFor="cf-env">Окружение</label>
        <select
          id="cf-env"
          value={profile.environment}
          onChange={(e) => {
            const environment = e.target.value as Environment;
            update({ environment, readOnly: isNew && environment === "prod" ? true : profile.readOnly });
          }}
        >
          {ENVIRONMENTS.map((env) => (
            <option key={env} value={env}>
              {env}
            </option>
          ))}
        </select>

        <label htmlFor="cf-ro">Только чтение</label>
        <input id="cf-ro" type="checkbox" checked={profile.readOnly} onChange={(e) => update({ readOnly: e.target.checked })} />

        <label htmlFor="cf-color">Цвет</label>
        <input id="cf-color" type="color" value={profile.color} onChange={(e) => update({ color: e.target.value })} />

        <label htmlFor="cf-bootstrap">Bootstrap-серверы</label>
        <input
          id="cf-bootstrap"
          placeholder="host1:9092,host2:9092"
          value={profile.bootstrapServers}
          onChange={(e) => update({ bootstrapServers: e.target.value })}
        />

        <label htmlFor="cf-protocol">Протокол</label>
        <select id="cf-protocol" value={sec.protocol} onChange={(e) => update({ security: withProtocol(sec, e.target.value as Protocol) })}>
          {PROTOCOLS.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>

        {"tls" in sec && (
          <>
            <label htmlFor="cf-ca">CA-сертификат (путь к PEM)</label>
            <input
              id="cf-ca"
              placeholder="пусто — системное хранилище"
              value={sec.tls.caLocation ?? ""}
              onChange={(e) => update({ security: { ...sec, tls: { ...sec.tls, caLocation: e.target.value || null } } })}
            />
            <label htmlFor="cf-verify">Проверять hostname</label>
            <input
              id="cf-verify"
              type="checkbox"
              checked={sec.tls.verifyHostname}
              onChange={(e) => update({ security: { ...sec, tls: { ...sec.tls, verifyHostname: e.target.checked } } })}
            />
          </>
        )}

        {"sasl" in sec && (
          <>
            <label htmlFor="cf-mech">Механизм SASL</label>
            <select
              id="cf-mech"
              value={sec.sasl.mechanism}
              onChange={(e) => update({ security: { ...sec, sasl: { ...sec.sasl, mechanism: e.target.value as SaslMechanism } } })}
            >
              {MECHANISMS.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
            <label htmlFor="cf-user">Пользователь</label>
            <input
              id="cf-user"
              value={sec.sasl.username}
              onChange={(e) => update({ security: { ...sec, sasl: { ...sec.sasl, username: e.target.value } } })}
            />
            <label htmlFor="cf-pass">Пароль</label>
            <input
              id="cf-pass"
              type="password"
              autoComplete="off"
              placeholder={isNew ? "" : "оставьте пустым, чтобы не менять"}
              value={password}
              onChange={(e) => setPassword(e.target.value)}
            />
          </>
        )}

        <label htmlFor="cf-timeout">Таймаут запроса, мс</label>
        <input
          id="cf-timeout"
          type="number"
          min={1000}
          step={1000}
          value={profile.requestTimeoutMs}
          onChange={(e) => update({ requestTimeoutMs: Number(e.target.value) })}
        />

        <label htmlFor="cf-props">Доп. свойства librdkafka</label>
        <textarea id="cf-props" rows={4} placeholder="key=value на строку" value={propsText} onChange={(e) => setPropsText(e.target.value)} />
      </div>

      <div className="toolbar">
        <button type="button" disabled={busy} onClick={onTest}>
          Проверить подключение
        </button>
        <button type="button" className="primary" disabled={busy} onClick={onSave}>
          Сохранить
        </button>
        {!isNew && (
          <button type="button" className="danger" disabled={busy} onClick={onDelete}>
            Удалить
          </button>
        )}
        {busy && <span className="muted">Выполняется…</span>}
      </div>

      {error && <ErrorBanner error={error} />}

      {report && (
        <ul className="steps">
          {report.steps.map((s) => (
            <li key={s.name}>
              {s.ok ? "✅" : "❌"} <strong>{STEP_LABELS[s.name]}:</strong> <span>{s.message}</span>
              {s.error?.details && <pre className="muted">{s.error.details}</pre>}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
```

`src/components/connections/ConnectionFormTab.tsx`:
```tsx
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { ErrorBanner } from "../common/ErrorBanner";
import { ConnectionForm } from "./ConnectionForm";
import { newProfile } from "./profile";

export function ConnectionFormTab({ connectionId, tabId }: { connectionId: string | null; tabId: string }) {
  const q = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  if (connectionId === null) return <ConnectionForm initial={newProfile()} tabId={tabId} />;
  if (q.isPending) return <p className="empty">Загрузка…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const profile = q.data.find((c) => c.id === connectionId);
  if (!profile) return <p className="empty">Подключение удалено.</p>;
  return <ConnectionForm key={profile.id} initial={profile} tabId={tabId} />;
}
```

В `TabContent.tsx` ветку `connection-form` заменить на:
```tsx
    case "connection-form":
      return <ConnectionFormTab connectionId={tab.connectionId} tabId={tab.id} />;
```
и добавить импорт `import { ConnectionFormTab } from "./connections/ConnectionFormTab";`.

- [ ] **Step 6: Тесты проходят**

Run: `pnpm vitest run src/components/connections`
Expected: PASS, 6 тестов.

- [ ] **Step 7: Commit**

```bash
git add src/components
git commit -m "feat(ui): форма подключения с пошаговой проверкой"
```

---

### Task 17: Фронтенд — обзор кластера и таблица конфигов

**Files:**
- Create: `src/components/common/ConfigTable.tsx`, `src/components/common/ConfigTable.test.tsx`
- Create: `src/components/cluster/ClusterOverview.tsx`
- Modify: `src/components/TabContent.tsx` (ветка `cluster`)

**Interfaces:**
- Consumes: `api.getClusterOverview`, `api.getBrokerConfig`, `ConfigEntry`, `ErrorBanner`, `formatCount`
- Produces: `filterConfigs(entries, query, onlyChanged): ConfigEntry[]`, `<ConfigTable entries={ConfigEntry[]} />`, `<ClusterOverview connectionId={string} />`

- [ ] **Step 1: Тест `ConfigTable.test.tsx`**

```tsx
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import type { ConfigEntry } from "../../api/types";
import { ConfigTable, filterConfigs } from "./ConfigTable";

const e = (name: string, value: string | null, isDefault: boolean, isSensitive = false): ConfigEntry => ({
  name,
  value,
  source: isDefault ? "Default" : "DynamicTopic",
  isDefault,
  isReadOnly: false,
  isSensitive,
});

const entries = [e("retention.ms", "1000", false), e("cleanup.policy", "delete", true), e("sasl.jaas.config", null, true, true)];

describe("ConfigTable", () => {
  it("фильтрует по подстроке и изменённым", () => {
    expect(filterConfigs(entries, "RET", false).map((x) => x.name)).toEqual(["retention.ms"]);
    expect(filterConfigs(entries, "", true).map((x) => x.name)).toEqual(["retention.ms"]);
  });

  it("скрывает sensitive и фильтрует через UI", async () => {
    const user = userEvent.setup();
    render(<ConfigTable entries={entries} />);
    expect(screen.getByText("••••••")).toBeInTheDocument();
    await user.click(screen.getByLabelText("Только изменённые"));
    expect(screen.queryByText("cleanup.policy")).not.toBeInTheDocument();
    expect(screen.getByText("retention.ms")).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/common`
Expected: FAIL — модуль `./ConfigTable` не найден.

- [ ] **Step 3: Реализация `ConfigTable.tsx`**

```tsx
import { useState } from "react";
import type { ConfigEntry } from "../../api/types";

export function filterConfigs(entries: ConfigEntry[], query: string, onlyChanged: boolean): ConfigEntry[] {
  const q = query.trim().toLowerCase();
  return entries.filter((e) => (!onlyChanged || !e.isDefault) && (!q || e.name.toLowerCase().includes(q)));
}

export function ConfigTable({ entries }: { entries: ConfigEntry[] }) {
  const [query, setQuery] = useState("");
  const [onlyChanged, setOnlyChanged] = useState(false);
  const rows = filterConfigs(entries, query, onlyChanged);
  return (
    <div>
      <div className="toolbar">
        <input placeholder="Поиск параметра" value={query} onChange={(e) => setQuery(e.target.value)} />
        <label>
          <input type="checkbox" checked={onlyChanged} onChange={(e) => setOnlyChanged(e.target.checked)} /> Только изменённые
        </label>
        <span className="muted">
          {rows.length} из {entries.length}
        </span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Параметр</th>
            <th>Значение</th>
            <th>Источник</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((e) => (
            <tr key={e.name}>
              <td style={{ fontWeight: e.isDefault ? 400 : 600 }}>
                {e.name}
                {e.isReadOnly && <span className="muted" title="Только чтение"> 🔒</span>}
              </td>
              <td>
                <code>{e.isSensitive ? "••••••" : (e.value ?? "—")}</code>
              </td>
              <td className="muted">{e.source}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 4: Тесты проходят**

Run: `pnpm vitest run src/components/common`
Expected: PASS, 2 теста.

- [ ] **Step 5: Реализация `ClusterOverview.tsx`**

```tsx
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { formatCount } from "../../lib/format";
import { ConfigTable } from "../common/ConfigTable";
import { ErrorBanner } from "../common/ErrorBanner";

export function ClusterOverview({ connectionId }: { connectionId: string }) {
  const q = useQuery({ queryKey: ["cluster", connectionId], queryFn: () => api.getClusterOverview(connectionId) });
  const [brokerId, setBrokerId] = useState<number | null>(null);

  if (q.isPending) return <p className="empty">Загрузка метаданных…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const o = q.data;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>Обзор кластера</h2>
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          Обновить
        </button>
      </div>
      <dl className="stats">
        <div>
          <dt>Cluster ID</dt>
          <dd>
            <code>{o.clusterId ?? "—"}</code>
          </dd>
        </div>
        <div>
          <dt>Контроллер</dt>
          <dd>{o.controllerId ?? "—"}</dd>
        </div>
        <div>
          <dt>Брокеры</dt>
          <dd>{o.brokers.length}</dd>
        </div>
        <div>
          <dt>Топики (внутренние)</dt>
          <dd>
            {formatCount(o.topicCount)} ({o.internalTopicCount})
          </dd>
        </div>
        <div>
          <dt>Партиции</dt>
          <dd>{formatCount(o.partitionCount)}</dd>
        </div>
        <div>
          <dt>Under-replicated</dt>
          <dd className={o.underReplicatedPartitions > 0 ? "warn" : ""}>{o.underReplicatedPartitions}</dd>
        </div>
        <div>
          <dt>Offline</dt>
          <dd className={o.offlinePartitions > 0 ? "warn" : ""}>{o.offlinePartitions}</dd>
        </div>
      </dl>

      <h3>Брокеры</h3>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>Адрес</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {o.brokers.map((b) => (
            <tr key={b.id} className="clickable" onClick={() => setBrokerId(b.id)}>
              <td>{b.id}</td>
              <td>
                {b.host}:{b.port}
              </td>
              <td>{b.id === o.controllerId && <span className="muted">контроллер</span>}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {brokerId !== null && <BrokerConfig connectionId={connectionId} brokerId={brokerId} />}
    </div>
  );
}

function BrokerConfig({ connectionId, brokerId }: { connectionId: string; brokerId: number }) {
  const q = useQuery({
    queryKey: ["brokerConfig", connectionId, brokerId],
    queryFn: () => api.getBrokerConfig(connectionId, brokerId),
  });
  return (
    <div>
      <h3>Конфигурация брокера {brokerId}</h3>
      {q.isPending && <p className="empty">Загрузка…</p>}
      {q.error && <ErrorBanner error={q.error} onRetry={() => q.refetch()} />}
      {q.data && <ConfigTable entries={q.data} />}
    </div>
  );
}
```

В `TabContent.tsx` ветку `cluster` заменить на `return <ClusterOverview connectionId={tab.connectionId} />;` и добавить импорт `import { ClusterOverview } from "./cluster/ClusterOverview";`.

- [ ] **Step 6: Проверить**

Run: `pnpm build && pnpm test`
Expected: всё PASS.

- [ ] **Step 7: Commit**

```bash
git add src/components
git commit -m "feat(ui): обзор кластера и таблица конфигураций"
```

---

### Task 18: Фронтенд — список топиков

**Files:**
- Create: `src/components/topics/topicFilter.ts`, `src/components/topics/topicFilter.test.ts`, `src/components/topics/TopicList.tsx`
- Modify: `src/components/TabContent.tsx` (ветка `topics`)

**Interfaces:**
- Consumes: `api.listTopics`, `api.getTopicStats`, `useWorkspace.open`, `formatCount`
- Produces:
  - `filterTopics(topics, query, showInternal): TopicSummary[]` (запрос `/.../` — regex, без учёта регистра)
  - `STATS_CHUNK = 200`, `visibleChunks(first: number, last: number, chunk?: number): number[]`
  - `<TopicList connectionId={string} />`

- [ ] **Step 1: Тест `topicFilter.test.ts`**

```ts
import { describe, expect, it } from "vitest";
import type { TopicSummary } from "../../api/types";
import { filterTopics, visibleChunks } from "./topicFilter";

const t = (name: string, internal = false): TopicSummary => ({
  name,
  internal,
  partitionCount: 1,
  replicationFactor: 1,
  underReplicated: 0,
});
const topics = [t("orders"), t("orders-dlq"), t("payments"), t("__consumer_offsets", true)];

describe("filterTopics", () => {
  it("подстрока без учёта регистра, внутренние скрыты", () => {
    expect(filterTopics(topics, "ORD", false).map((x) => x.name)).toEqual(["orders", "orders-dlq"]);
    expect(filterTopics(topics, "", false)).toHaveLength(3);
    expect(filterTopics(topics, "", true)).toHaveLength(4);
  });

  it("regex в слэшах; невалидный regex → пусто", () => {
    expect(filterTopics(topics, "/-dlq$/", false).map((x) => x.name)).toEqual(["orders-dlq"]);
    expect(filterTopics(topics, "/(/", false)).toEqual([]);
  });
});

describe("visibleChunks", () => {
  it("номера чанков для видимого диапазона", () => {
    expect(visibleChunks(0, 10)).toEqual([0]);
    expect(visibleChunks(190, 410)).toEqual([0, 1, 2]);
    expect(visibleChunks(0, -1)).toEqual([]);
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/topics`
Expected: FAIL — модуль `./topicFilter` не найден.

- [ ] **Step 3: Реализация `topicFilter.ts`**

```ts
import type { TopicSummary } from "../../api/types";

export const STATS_CHUNK = 200;

export function filterTopics(topics: TopicSummary[], query: string, showInternal: boolean): TopicSummary[] {
  const q = query.trim();
  let match: (name: string) => boolean;
  if (q.length > 2 && q.startsWith("/") && q.endsWith("/")) {
    let re: RegExp;
    try {
      re = new RegExp(q.slice(1, -1), "i");
    } catch {
      return [];
    }
    match = (n) => re.test(n);
  } else {
    const lower = q.toLowerCase();
    match = (n) => n.toLowerCase().includes(lower);
  }
  return topics.filter((t) => (showInternal || !t.internal) && match(t.name));
}

export function visibleChunks(first: number, last: number, chunk = STATS_CHUNK): number[] {
  if (last < first) return [];
  const out: number[] = [];
  for (let c = Math.floor(first / chunk); c <= Math.floor(last / chunk); c++) out.push(c);
  return out;
}
```

- [ ] **Step 4: Тесты проходят**

Run: `pnpm vitest run src/components/topics`
Expected: PASS, 3 теста.

- [ ] **Step 5: Реализация `TopicList.tsx`**

```tsx
import { useMemo, useRef, useState } from "react";
import { useQueries, useQuery } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { api } from "../../api/client";
import { formatCount } from "../../lib/format";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { filterTopics, STATS_CHUNK, visibleChunks } from "./topicFilter";

const ROW_HEIGHT = 30;
const REFRESH_OPTIONS = [
  { label: "Автообновление: выкл.", value: 0 },
  { label: "каждые 10 с", value: 10_000 },
  { label: "каждые 30 с", value: 30_000 },
];

export function TopicList({ connectionId }: { connectionId: string }) {
  const [query, setQuery] = useState("");
  const [showInternal, setShowInternal] = useState(false);
  const [refreshMs, setRefreshMs] = useState(0);
  const open = useWorkspace((s) => s.open);

  const q = useQuery({
    queryKey: ["topics", connectionId],
    queryFn: () => api.listTopics(connectionId),
    refetchInterval: refreshMs || false,
  });
  const rows = useMemo(() => filterTopics(q.data ?? [], query, showInternal), [q.data, query, showInternal]);
  const names = useMemo(() => rows.map((r) => r.name), [rows]);

  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });
  const items = virtualizer.getVirtualItems();
  const chunks = visibleChunks(items[0]?.index ?? 0, items[items.length - 1]?.index ?? -1);

  // Счётчики сообщений грузим лениво: только для чанков по 200 топиков, попавших в видимую область.
  const statsResults = useQueries({
    queries: chunks.map((c) => {
      const slice = names.slice(c * STATS_CHUNK, (c + 1) * STATS_CHUNK);
      return {
        queryKey: ["topicStats", connectionId, slice],
        queryFn: () => api.getTopicStats(connectionId, slice),
        staleTime: 30_000,
        refetchInterval: refreshMs || false,
      };
    }),
  });
  const counts = new Map<string, number | null>();
  for (const r of statsResults) for (const s of r.data ?? []) counts.set(s.name, s.messageCount);

  if (q.isPending) return <p className="empty">Загрузка списка топиков…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>Топики</h2>
        <input placeholder="Поиск (или /regex/)" value={query} onChange={(e) => setQuery(e.target.value)} />
        <label>
          <input type="checkbox" checked={showInternal} onChange={(e) => setShowInternal(e.target.checked)} /> Внутренние
        </label>
        <select value={refreshMs} onChange={(e) => setRefreshMs(Number(e.target.value))}>
          {REFRESH_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          Обновить
        </button>
        <span className="muted">
          {rows.length} из {q.data.length}
        </span>
      </div>
      <div className="vlist" ref={parentRef}>
        <div className="row header topic-row" style={{ height: ROW_HEIGHT }}>
          <span>Имя</span>
          <span>Партиции</span>
          <span>RF</span>
          <span>Сообщений (≈)</span>
          <span>Under-repl.</span>
        </div>
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {items.map((item) => {
            const t = rows[item.index];
            return (
              <div
                key={t.name}
                className="row clickable topic-row"
                style={{ top: item.start, height: ROW_HEIGHT }}
                onClick={() => open({ kind: "topic", connectionId, topic: t.name })}
              >
                <span title={t.name}>
                  {t.name}
                  {t.internal && <span className="muted"> (внутр.)</span>}
                </span>
                <span>{t.partitionCount}</span>
                <span>{t.replicationFactor}</span>
                <span>{formatCount(counts.get(t.name))}</span>
                <span className={t.underReplicated > 0 ? "warn" : ""}>{t.underReplicated}</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
```

В `TabContent.tsx` ветку `topics` заменить на `return <TopicList connectionId={tab.connectionId} />;` и добавить импорт `import { TopicList } from "./topics/TopicList";`.

- [ ] **Step 6: Проверить**

Run: `pnpm build && pnpm test`
Expected: всё PASS.

- [ ] **Step 7: Commit**

```bash
git add src/components
git commit -m "feat(ui): виртуализированный список топиков с ленивыми счётчиками"
```

---

### Task 19: Фронтенд — детали топика

**Files:**
- Create: `src/components/topics/TopicDetails.tsx`, `src/components/topics/TopicDetails.test.tsx`
- Modify: `src/components/TabContent.tsx` (ветка `topic`)

**Interfaces:**
- Consumes: `api.getTopic`, `api.getTopicConfigs`, `ConfigTable`, `ErrorBanner`, `formatCount`
- Produces: `<TopicDetails connectionId={string} topic={string} />` с подвкладками `messages` / `partitions` / `configs`. Вкладка `messages` в этом таске — заглушка, Task 20 подключает `BrowsePanel`.

- [ ] **Step 1: Тест**

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getTopic: vi.fn().mockResolvedValue({
        name: "orders",
        internal: false,
        replicationFactor: 2,
        messageCount: 15,
        partitions: [
          { id: 0, leader: 1, replicas: [1, 2], isr: [1, 2], low: 0, high: 10, messageCount: 10, underReplicated: false },
          { id: 1, leader: 2, replicas: [1, 2], isr: [2], low: 5, high: 10, messageCount: 5, underReplicated: true },
        ],
      }),
      getTopicConfigs: vi.fn().mockResolvedValue([
        { name: "cleanup.policy", value: "compact", source: "DynamicTopic", isDefault: false, isReadOnly: false, isSensitive: false },
      ]),
    },
  };
});

import { TopicDetails } from "./TopicDetails";

describe("TopicDetails", () => {
  it("показывает партиции и конфиги", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicDetails connectionId="c" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Партиции" }));
    expect(screen.getAllByText("1, 2")).toHaveLength(3); // реплики обеих партиций + ISR партиции 0
    expect(screen.getByText("2", { selector: "td.warn" })).toBeInTheDocument(); // ISR под-реплицированной партиции
    await user.click(screen.getByRole("button", { name: "Конфигурация" }));
    expect(await screen.findByText("compact")).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/topics/TopicDetails.test.tsx`
Expected: FAIL — модуль `./TopicDetails` не найден.

- [ ] **Step 3: Реализация `TopicDetails.tsx`**

```tsx
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { TopicDetails as TopicDetailsDto } from "../../api/types";
import { formatCount } from "../../lib/format";
import { ConfigTable } from "../common/ConfigTable";
import { ErrorBanner } from "../common/ErrorBanner";

type SubTab = "messages" | "partitions" | "configs";
const SUBTABS: { id: SubTab; label: string }[] = [
  { id: "messages", label: "Сообщения" },
  { id: "partitions", label: "Партиции" },
  { id: "configs", label: "Конфигурация" },
];

export function TopicDetails({ connectionId, topic }: { connectionId: string; topic: string }) {
  const [sub, setSub] = useState<SubTab>("messages");
  const q = useQuery({ queryKey: ["topic", connectionId, topic], queryFn: () => api.getTopic(connectionId, topic) });

  if (q.isPending) return <p className="empty">Загрузка…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const t = q.data;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{t.name}</h2>
        <span className="muted">
          партиций: {t.partitions.length} · RF: {t.replicationFactor} · сообщений ≈ {formatCount(t.messageCount)}
        </span>
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          Обновить
        </button>
      </div>
      <div className="subtabs">
        {SUBTABS.map((s) => (
          <button key={s.id} type="button" className={sub === s.id ? "active" : ""} onClick={() => setSub(s.id)}>
            {s.label}
          </button>
        ))}
      </div>
      {/* Сообщения не размонтируем при переключении, чтобы не терять результаты browse. */}
      <div hidden={sub !== "messages"} style={{ flex: 1, display: sub === "messages" ? "flex" : "none", flexDirection: "column" }}>
        <p className="empty">Просмотр сообщений</p>
      </div>
      {sub === "partitions" && <PartitionsTable topic={t} />}
      {sub === "configs" && <TopicConfigs connectionId={connectionId} topic={topic} />}
    </div>
  );
}

function PartitionsTable({ topic }: { topic: TopicDetailsDto }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Партиция</th>
          <th>Лидер</th>
          <th>Реплики</th>
          <th>ISR</th>
          <th>Low</th>
          <th>High</th>
          <th>Сообщений</th>
        </tr>
      </thead>
      <tbody>
        {topic.partitions.map((p) => (
          <tr key={p.id}>
            <td>{p.id}</td>
            <td className={p.leader < 0 ? "warn" : ""}>{p.leader < 0 ? "нет" : p.leader}</td>
            <td>{p.replicas.join(", ")}</td>
            <td className={p.underReplicated ? "warn" : ""}>{p.isr.join(", ")}</td>
            <td>{p.low ?? "—"}</td>
            <td>{p.high ?? "—"}</td>
            <td className={p.offsetsError ? "warn" : ""} title={p.offsetsError}>
              {formatCount(p.messageCount)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function TopicConfigs({ connectionId, topic }: { connectionId: string; topic: string }) {
  const q = useQuery({
    queryKey: ["topicConfigs", connectionId, topic],
    queryFn: () => api.getTopicConfigs(connectionId, topic),
  });
  if (q.isPending) return <p className="empty">Загрузка…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  return <ConfigTable entries={q.data} />;
}
```

В `TabContent.tsx` ветку `topic` заменить на `return <TopicDetails connectionId={tab.connectionId} topic={tab.topic} />;` и добавить импорт `import { TopicDetails } from "./topics/TopicDetails";`.

- [ ] **Step 4: Тесты проходят**

Run: `pnpm vitest run src/components/topics`
Expected: PASS, 4 теста.

- [ ] **Step 5: Commit**

```bash
git add src/components
git commit -m "feat(ui): детали топика — партиции и конфигурация"
```

---

### Task 20: Фронтенд — просмотр сообщений (Browse)

**Files:**
- Create: `src/components/browse/state.ts`, `src/components/browse/state.test.ts`
- Create: `src/components/browse/params.ts`, `src/components/browse/params.test.ts`
- Create: `src/components/browse/useBrowseSession.ts`
- Create: `src/components/browse/BrowseForm.tsx`, `src/components/browse/MessageTable.tsx`, `src/components/browse/MessageDetails.tsx`, `src/components/browse/BrowsePanel.tsx`
- Modify: `src/components/topics/TopicDetails.tsx` (вкладка `messages` → `BrowsePanel`)

**Interfaces:**
- Consumes: `api.startBrowse`, `api.stopSession`, `toAppError`, типы Browse, `formatTimestamp`, `formatBytes`
- Produces:
  - `BrowseState`, `initialBrowseState`, `browseReducer(state, action)`. Действия: `{ type: "start"; limit }`, `{ type: "started"; sessionId }`, `{ type: "event"; event }`, `{ type: "failed"; error }`
  - `BrowseFormState`, `defaultBrowseForm`, `buildBrowseParams(form, connectionId, topic): BrowseParams` (бросает `Error` с понятным текстом)
  - `prettyText(d: DecodedData | null): string`, `previewText(d: DecodedData | null, max?: number): string`
  - `useBrowseSession()` → `{ state, start(params), stop() }`
  - `<BrowsePanel connectionId topic />`

- [ ] **Step 1: Тесты reducer и params**

`src/components/browse/state.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import type { BrowsedMessage } from "../../api/types";
import { browseReducer, initialBrowseState } from "./state";

const msg = (offset: number): BrowsedMessage => ({ partition: 0, offset, timestamp: null, key: null, value: null, headers: [], size: 0 });

describe("browseReducer", () => {
  it("кольцевой буфер держит последние limit сообщений", () => {
    let s = browseReducer(initialBrowseState, { type: "start", limit: 3 });
    s = browseReducer(s, { type: "event", event: { type: "messages", messages: [msg(1), msg(2)] } });
    s = browseReducer(s, { type: "event", event: { type: "messages", messages: [msg(3), msg(4)] } });
    expect(s.messages.map((m) => m.offset)).toEqual([2, 3, 4]);
    expect(s.status).toBe("running");
  });

  it("progress, reachedEnd, finished, error", () => {
    let s = browseReducer(initialBrowseState, { type: "start", limit: 10 });
    s = browseReducer(s, { type: "event", event: { type: "progress", scanned: 5, matched: 2 } });
    expect([s.scanned, s.matched]).toEqual([5, 2]);
    s = browseReducer(s, { type: "event", event: { type: "reachedEnd" } });
    expect(s.reachedEnd).toBe(true);
    s = browseReducer(s, { type: "event", event: { type: "finished", scanned: 9, matched: 3, reason: "limit" } });
    expect(s.status).toBe("finished");
    expect(s.finishReason).toBe("limit");
    const err = { code: "NOT_FOUND" as const, message: "нет", retryable: false };
    s = browseReducer(s, { type: "event", event: { type: "error", error: err } });
    expect(s.status).toBe("error");
    expect(s.error).toEqual(err);
  });

  it("start сбрасывает предыдущие результаты", () => {
    let s = browseReducer(initialBrowseState, { type: "start", limit: 10 });
    s = browseReducer(s, { type: "event", event: { type: "messages", messages: [msg(1)] } });
    s = browseReducer(s, { type: "start", limit: 10 });
    expect(s.messages).toEqual([]);
    expect(s.scanned).toBe(0);
  });
});
```

`src/components/browse/params.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import { buildBrowseParams, defaultBrowseForm, prettyText, previewText } from "./params";

describe("buildBrowseParams", () => {
  it("значения по умолчанию — последние 100", () => {
    const p = buildBrowseParams(defaultBrowseForm, "c", "t");
    expect(p).toEqual({
      connectionId: "c",
      topic: "t",
      partitions: null,
      start: { type: "latest", count: 100 },
      limit: 500,
      live: false,
      keyFormat: "auto",
      valueFormat: "auto",
      filter: {},
    });
  });

  it("партиции, фильтры, время", () => {
    const p = buildBrowseParams(
      {
        ...defaultBrowseForm,
        startType: "timestamp",
        timestamp: "2026-01-01T10:00",
        partitions: "0, 2",
        valuePattern: "FAILED",
        regex: true,
        headerName: "trace",
        headerValue: "t1",
        jsonPath: "$.a == 1",
        fromTime: "2026-01-01T09:00",
      },
      "c",
      "t",
    );
    expect(p.partitions).toEqual([0, 2]);
    expect(p.start).toEqual({ type: "timestamp", timestampMs: new Date("2026-01-01T10:00").getTime() });
    expect(p.filter).toEqual({
      value: { pattern: "FAILED", regex: true, caseSensitive: false },
      header: { name: "trace", value: { pattern: "t1", regex: true, caseSensitive: false } },
      jsonPath: "$.a == 1",
      fromTimestamp: new Date("2026-01-01T09:00").getTime(),
    });
  });

  it("ошибки ввода", () => {
    expect(() => buildBrowseParams({ ...defaultBrowseForm, partitions: "a" }, "c", "t")).toThrow("Партиции");
    expect(() => buildBrowseParams({ ...defaultBrowseForm, startType: "timestamp", timestamp: "" }, "c", "t")).toThrow("время");
    expect(() => buildBrowseParams({ ...defaultBrowseForm, limit: 0 }, "c", "t")).toThrow("Лимит");
  });
});

describe("text helpers", () => {
  it("prettyText форматирует JSON", () => {
    expect(prettyText({ format: "json", text: '{"a":1}', size: 7, truncated: false })).toBe('{\n  "a": 1\n}');
    expect(prettyText({ format: "string", text: "x", size: 1, truncated: false })).toBe("x");
    expect(prettyText(null)).toBe("");
  });

  it("previewText в одну строку с обрезкой", () => {
    expect(previewText({ format: "string", text: "a\n  b", size: 5, truncated: false })).toBe("a b");
    expect(previewText({ format: "string", text: "x".repeat(300), size: 300, truncated: false }, 10)).toBe("xxxxxxxxxx…");
    expect(previewText(null)).toBe("null");
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/browse`
Expected: FAIL — модули `./state`, `./params` не найдены.

- [ ] **Step 3: Реализация `state.ts`**

```ts
import type { AppError, BrowseEvent, BrowsedMessage, FinishReason } from "../../api/types";

export interface BrowseState {
  status: "idle" | "running" | "finished" | "error";
  sessionId: string | null;
  messages: BrowsedMessage[];
  scanned: number;
  matched: number;
  reachedEnd: boolean;
  finishReason: FinishReason | null;
  error: AppError | null;
  limit: number;
}

export const initialBrowseState: BrowseState = {
  status: "idle",
  sessionId: null,
  messages: [],
  scanned: 0,
  matched: 0,
  reachedEnd: false,
  finishReason: null,
  error: null,
  limit: 500,
};

export type BrowseAction =
  | { type: "start"; limit: number }
  | { type: "started"; sessionId: string }
  | { type: "event"; event: BrowseEvent }
  | { type: "failed"; error: AppError };

export function browseReducer(s: BrowseState, a: BrowseAction): BrowseState {
  switch (a.type) {
    case "start":
      return { ...initialBrowseState, status: "running", limit: a.limit };
    case "started":
      return { ...s, sessionId: a.sessionId };
    case "failed":
      return { ...s, status: "error", error: a.error };
    case "event": {
      const e = a.event;
      switch (e.type) {
        case "messages": {
          const merged = s.messages.concat(e.messages);
          const messages = merged.length > s.limit ? merged.slice(merged.length - s.limit) : merged;
          return { ...s, messages };
        }
        case "progress":
          return { ...s, scanned: e.scanned, matched: e.matched };
        case "reachedEnd":
          return { ...s, reachedEnd: true };
        case "finished":
          return { ...s, status: "finished", scanned: e.scanned, matched: e.matched, finishReason: e.reason };
        case "error":
          return { ...s, status: "error", error: e.error };
      }
    }
  }
}
```

- [ ] **Step 4: Реализация `params.ts`**

```ts
import type { BrowseParams, DataFormat, DecodedData, FilterSpec, StartPosition, TextMatch } from "../../api/types";

export interface BrowseFormState {
  startType: StartPosition["type"];
  count: number;
  offset: number;
  timestamp: string; // значение <input type="datetime-local">, локальное время
  minutes: number;
  partitions: string;
  limit: number;
  live: boolean;
  keyFormat: DataFormat;
  valueFormat: DataFormat;
  keyPattern: string;
  valuePattern: string;
  regex: boolean;
  caseSensitive: boolean;
  headerName: string;
  headerValue: string;
  jsonPath: string;
  fromTime: string;
  toTime: string;
}

export const defaultBrowseForm: BrowseFormState = {
  startType: "latest",
  count: 100,
  offset: 0,
  timestamp: "",
  minutes: 15,
  partitions: "",
  limit: 500,
  live: false,
  keyFormat: "auto",
  valueFormat: "auto",
  keyPattern: "",
  valuePattern: "",
  regex: false,
  caseSensitive: false,
  headerName: "",
  headerValue: "",
  jsonPath: "",
  fromTime: "",
  toTime: "",
};

const MAX_LIMIT = 100_000;

function parseLocalTime(value: string, what: string): number {
  const ms = new Date(value).getTime();
  if (!value || Number.isNaN(ms)) throw new Error(`Укажите корректное время: ${what}`);
  return ms;
}

function parsePartitions(text: string): number[] | null {
  const t = text.trim();
  if (!t) return null;
  return t.split(",").map((part) => {
    const n = Number(part.trim());
    if (!Number.isInteger(n) || n < 0) throw new Error(`Партиции: «${part.trim()}» — не номер партиции`);
    return n;
  });
}

export function buildBrowseParams(f: BrowseFormState, connectionId: string, topic: string): BrowseParams {
  if (!Number.isInteger(f.limit) || f.limit < 1 || f.limit > MAX_LIMIT) {
    throw new Error(`Лимит должен быть от 1 до ${MAX_LIMIT}`);
  }
  let start: StartPosition;
  switch (f.startType) {
    case "earliest":
      start = { type: "earliest" };
      break;
    case "latest":
      start = { type: "latest", count: f.count };
      break;
    case "offset":
      start = { type: "offset", offset: f.offset };
      break;
    case "timestamp":
      start = { type: "timestamp", timestampMs: parseLocalTime(f.timestamp, "время начала") };
      break;
    case "lastMinutes":
      start = { type: "lastMinutes", minutes: f.minutes };
      break;
    default:
      throw new Error(`Неизвестная начальная позиция: ${String(f.startType)}`);
  }

  const text = (pattern: string): TextMatch | undefined =>
    pattern ? { pattern, regex: f.regex, caseSensitive: f.caseSensitive } : undefined;
  const filter: FilterSpec = {};
  const key = text(f.keyPattern);
  if (key) filter.key = key;
  const value = text(f.valuePattern);
  if (value) filter.value = value;
  if (f.headerName.trim()) {
    const hv = text(f.headerValue);
    filter.header = hv ? { name: f.headerName.trim(), value: hv } : { name: f.headerName.trim() };
  }
  if (f.jsonPath.trim()) filter.jsonPath = f.jsonPath.trim();
  if (f.fromTime) filter.fromTimestamp = parseLocalTime(f.fromTime, "фильтр «с»");
  if (f.toTime) filter.toTimestamp = parseLocalTime(f.toTime, "фильтр «по»");

  return {
    connectionId,
    topic,
    partitions: parsePartitions(f.partitions),
    start,
    limit: f.limit,
    live: f.live,
    keyFormat: f.keyFormat,
    valueFormat: f.valueFormat,
    filter,
  };
}

export function prettyText(d: DecodedData | null): string {
  if (!d) return "";
  if (d.format === "json" && !d.error && !d.truncated) {
    try {
      return JSON.stringify(JSON.parse(d.text), null, 2);
    } catch {
      return d.text;
    }
  }
  return d.text;
}

export function previewText(d: DecodedData | null, max = 200): string {
  if (!d) return "null";
  const flat = d.text.replace(/\s+/g, " ");
  return flat.length > max ? `${flat.slice(0, max)}…` : flat;
}
```

- [ ] **Step 5: Тесты проходят**

Run: `pnpm vitest run src/components/browse`
Expected: PASS, 8 тестов.

- [ ] **Step 6: Хук `useBrowseSession.ts`**

```ts
import { useCallback, useEffect, useReducer, useRef } from "react";
import { api, toAppError } from "../../api/client";
import type { BrowseParams } from "../../api/types";
import { browseReducer, initialBrowseState } from "./state";

export function useBrowseSession() {
  const [state, dispatch] = useReducer(browseReducer, initialBrowseState);
  const sessionRef = useRef<string | null>(null);
  // Токен запуска: события от предыдущей (остановленной) сессии игнорируются.
  const tokenRef = useRef(0);

  const stop = useCallback(async () => {
    const id = sessionRef.current;
    sessionRef.current = null;
    if (id) await api.stopSession(id).catch(() => false);
  }, []);

  const start = useCallback(
    async (params: BrowseParams) => {
      await stop();
      const token = ++tokenRef.current;
      dispatch({ type: "start", limit: params.limit });
      try {
        const id = await api.startBrowse(params, (event) => {
          if (tokenRef.current === token) dispatch({ type: "event", event });
        });
        if (tokenRef.current !== token) {
          await api.stopSession(id).catch(() => false);
          return;
        }
        sessionRef.current = id;
        dispatch({ type: "started", sessionId: id });
      } catch (e) {
        if (tokenRef.current === token) dispatch({ type: "failed", error: toAppError(e) });
      }
    },
    [stop],
  );

  useEffect(() => () => void stop(), [stop]);

  return { state, start, stop };
}
```

- [ ] **Step 7: Компоненты**

`src/components/browse/BrowseForm.tsx`:
```tsx
import type { DataFormat, StartPosition } from "../../api/types";
import type { BrowseFormState } from "./params";

const FORMATS: DataFormat[] = ["auto", "string", "json", "hex", "base64"];
const STARTS: { id: StartPosition["type"]; label: string }[] = [
  { id: "latest", label: "Последние N" },
  { id: "earliest", label: "С начала" },
  { id: "offset", label: "С оффсета" },
  { id: "timestamp", label: "С времени" },
  { id: "lastMinutes", label: "За последние X минут" },
];

interface Props {
  form: BrowseFormState;
  onChange: (f: BrowseFormState) => void;
}

export function BrowseForm({ form, onChange }: Props) {
  const set = <K extends keyof BrowseFormState>(k: K, v: BrowseFormState[K]) => onChange({ ...form, [k]: v });
  return (
    <div className="browse-form">
      <fieldset>
        <legend>Откуда</legend>
        <select aria-label="Начальная позиция" value={form.startType} onChange={(e) => set("startType", e.target.value as StartPosition["type"])}>
          {STARTS.map((s) => (
            <option key={s.id} value={s.id}>
              {s.label}
            </option>
          ))}
        </select>
        {form.startType === "latest" && (
          <input aria-label="N" type="number" min={1} value={form.count} onChange={(e) => set("count", Number(e.target.value))} style={{ width: 80 }} />
        )}
        {form.startType === "offset" && (
          <input aria-label="Оффсет" type="number" min={0} value={form.offset} onChange={(e) => set("offset", Number(e.target.value))} style={{ width: 100 }} />
        )}
        {form.startType === "timestamp" && (
          <input aria-label="Время" type="datetime-local" step={1} value={form.timestamp} onChange={(e) => set("timestamp", e.target.value)} />
        )}
        {form.startType === "lastMinutes" && (
          <input aria-label="Минут" type="number" min={1} value={form.minutes} onChange={(e) => set("minutes", Number(e.target.value))} style={{ width: 70 }} />
        )}
        <input aria-label="Партиции" placeholder="партиции: все" value={form.partitions} onChange={(e) => set("partitions", e.target.value)} style={{ width: 110 }} />
        <label>
          Лимит <input type="number" min={1} max={100000} value={form.limit} onChange={(e) => set("limit", Number(e.target.value))} style={{ width: 80 }} />
        </label>
        <label>
          <input type="checkbox" checked={form.live} onChange={(e) => set("live", e.target.checked)} /> Live tail
        </label>
      </fieldset>
      <fieldset>
        <legend>Формат</legend>
        <label>
          ключ{" "}
          <select value={form.keyFormat} onChange={(e) => set("keyFormat", e.target.value as DataFormat)}>
            {FORMATS.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
        </label>
        <label>
          значение{" "}
          <select value={form.valueFormat} onChange={(e) => set("valueFormat", e.target.value as DataFormat)}>
            {FORMATS.map((f) => (
              <option key={f}>{f}</option>
            ))}
          </select>
        </label>
      </fieldset>
      <fieldset>
        <legend>Фильтры</legend>
        <input placeholder="ключ содержит" value={form.keyPattern} onChange={(e) => set("keyPattern", e.target.value)} />
        <input placeholder="значение содержит" value={form.valuePattern} onChange={(e) => set("valuePattern", e.target.value)} />
        <label>
          <input type="checkbox" checked={form.regex} onChange={(e) => set("regex", e.target.checked)} /> regex
        </label>
        <label>
          <input type="checkbox" checked={form.caseSensitive} onChange={(e) => set("caseSensitive", e.target.checked)} /> Aa
        </label>
        <input placeholder="заголовок" value={form.headerName} onChange={(e) => set("headerName", e.target.value)} style={{ width: 110 }} />
        <input placeholder="= значение" value={form.headerValue} onChange={(e) => set("headerValue", e.target.value)} style={{ width: 110 }} />
        <input
          placeholder={'JSONPath: $.order.status == "FAILED"'}
          value={form.jsonPath}
          onChange={(e) => set("jsonPath", e.target.value)}
          style={{ width: 260 }}
        />
        <label>
          с <input type="datetime-local" step={1} value={form.fromTime} onChange={(e) => set("fromTime", e.target.value)} />
        </label>
        <label>
          по <input type="datetime-local" step={1} value={form.toTime} onChange={(e) => set("toTime", e.target.value)} />
        </label>
      </fieldset>
    </div>
  );
}
```

`src/components/browse/MessageTable.tsx`:
```tsx
import { useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { BrowsedMessage } from "../../api/types";
import { formatBytes, formatTimestamp } from "../../lib/format";
import { previewText } from "./params";

const ROW_HEIGHT = 28;

export const messageKey = (m: BrowsedMessage) => `${m.partition}:${m.offset}`;

interface Props {
  messages: BrowsedMessage[];
  selectedKey: string | null;
  onSelect: (m: BrowsedMessage) => void;
  utc: boolean;
}

export function MessageTable({ messages, selectedKey, onSelect, utc }: Props) {
  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: messages.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 30,
  });
  return (
    <div className="vlist" ref={parentRef}>
      <div className="row header msg-row" style={{ height: ROW_HEIGHT }}>
        <span>P</span>
        <span>Offset</span>
        <span>Время{utc ? " (UTC)" : ""}</span>
        <span>Ключ</span>
        <span>Значение</span>
        <span>Hdr</span>
        <span>Размер</span>
      </div>
      <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
        {virtualizer.getVirtualItems().map((item) => {
          const m = messages[item.index];
          const key = messageKey(m);
          return (
            <div
              key={key}
              className={`row clickable msg-row${key === selectedKey ? " selected" : ""}`}
              style={{ top: item.start, height: ROW_HEIGHT }}
              onClick={() => onSelect(m)}
            >
              <span>{m.partition}</span>
              <span>{m.offset}</span>
              <span>{formatTimestamp(m.timestamp, utc)}</span>
              <span title={m.key?.text}>{previewText(m.key, 60)}</span>
              <span>{previewText(m.value)}</span>
              <span>{m.headers.length || ""}</span>
              <span>{formatBytes(m.size)}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
```

`src/components/browse/MessageDetails.tsx`:
```tsx
import type { BrowsedMessage, DecodedData } from "../../api/types";
import { formatBytes, formatTimestamp } from "../../lib/format";
import { prettyText } from "./params";

function DataBlock({ title, data }: { title: string; data: DecodedData | null }) {
  return (
    <section>
      <h4>
        {title} {data && <span className="muted">({data.format}, {formatBytes(data.size)})</span>}
        {data && (
          <button type="button" className="link" onClick={() => void navigator.clipboard.writeText(data.text)}>
            копировать
          </button>
        )}
      </h4>
      {data?.error && <p className="warn">{data.error}</p>}
      {data?.truncated && <p className="warn">Показан первый 1 МБ значения.</p>}
      <pre>{data ? prettyText(data) : <span className="muted">null</span>}</pre>
    </section>
  );
}

export function MessageDetails({ message, utc }: { message: BrowsedMessage | null; utc: boolean }) {
  if (!message) return <div className="details-pane muted">Выберите сообщение</div>;
  return (
    <div className="details-pane">
      <p className="muted">
        Партиция {message.partition} · offset {message.offset} · {formatTimestamp(message.timestamp, utc)}
        <button
          type="button"
          className="link"
          onClick={() => void navigator.clipboard.writeText(JSON.stringify(message, null, 2))}
        >
          {" "}
          копировать всё как JSON
        </button>
      </p>
      <DataBlock title="Значение" data={message.value} />
      <DataBlock title="Ключ" data={message.key} />
      <section>
        <h4>Заголовки ({message.headers.length})</h4>
        {message.headers.length > 0 && (
          <table>
            <tbody>
              {message.headers.map((h, i) => (
                <tr key={`${h.name}-${i}`}>
                  <td>{h.name}</td>
                  <td>
                    <code>{h.value ?? "null"}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>
    </div>
  );
}
```

`src/components/browse/BrowsePanel.tsx`:
```tsx
import { useState } from "react";
import type { BrowsedMessage } from "../../api/types";
import { formatCount } from "../../lib/format";
import { ErrorBanner } from "../common/ErrorBanner";
import { BrowseForm } from "./BrowseForm";
import { MessageDetails } from "./MessageDetails";
import { messageKey, MessageTable } from "./MessageTable";
import { buildBrowseParams, defaultBrowseForm } from "./params";
import { useBrowseSession } from "./useBrowseSession";

const FINISH_TEXT = { limit: "достигнут лимит", endOfTopic: "дочитано до конца", cancelled: "остановлено" } as const;

export function BrowsePanel({ connectionId, topic }: { connectionId: string; topic: string }) {
  const [form, setForm] = useState(defaultBrowseForm);
  const [formError, setFormError] = useState<string | null>(null);
  const [selected, setSelected] = useState<BrowsedMessage | null>(null);
  const [utc, setUtc] = useState(false);
  const { state, start, stop } = useBrowseSession();
  const running = state.status === "running";

  const onStart = () => {
    try {
      const params = buildBrowseParams(form, connectionId, topic);
      setFormError(null);
      setSelected(null);
      void start(params);
    } catch (e) {
      setFormError((e as Error).message);
    }
  };

  return (
    <div className="browse">
      <BrowseForm form={form} onChange={setForm} />
      <div className="toolbar">
        <button type="button" className="primary" onClick={onStart}>
          {running ? "Перезапустить" : "Читать"}
        </button>
        <button type="button" disabled={!running} onClick={() => void stop()}>
          Остановить
        </button>
        <label>
          <input type="checkbox" checked={utc} onChange={(e) => setUtc(e.target.checked)} /> UTC
        </label>
        <span className="muted">
          просмотрено {formatCount(state.scanned)} · найдено {formatCount(state.matched)} · в таблице {state.messages.length}
          {running && (state.reachedEnd ? " · ждём новые сообщения…" : " · читаем…")}
          {state.status === "finished" && state.finishReason && ` · ${FINISH_TEXT[state.finishReason]}`}
        </span>
      </div>
      {formError && <ErrorBanner error={{ code: "INVALID_INPUT", message: formError, retryable: false }} />}
      {state.error && <ErrorBanner error={state.error} onRetry={onStart} />}
      <div className="browse-body">
        <MessageTable
          messages={state.messages}
          selectedKey={selected ? messageKey(selected) : null}
          onSelect={setSelected}
          utc={utc}
        />
        <MessageDetails message={selected} utc={utc} />
      </div>
    </div>
  );
}
```

В `TopicDetails.tsx` заменить заглушку `<p className="empty">Просмотр сообщений</p>` на `<BrowsePanel connectionId={connectionId} topic={topic} />` и добавить импорт `import { BrowsePanel } from "../browse/BrowsePanel";`.

- [ ] **Step 8: Проверить**

Run: `pnpm build && pnpm test`
Expected: сборка без ошибок, все тесты PASS (в `TopicDetails.test.tsx` вкладка «Сообщения» рендерит `BrowsePanel` без вызова `api`, пока не нажата кнопка «Читать»).

- [ ] **Step 9: Commit**

```bash
git add src/components
git commit -m "feat(ui): просмотр сообщений — форма, фильтры, live tail, таблица и детали"
```

---

### Task 21: Сквозная проверка M1 и README

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: всё приложение
- Produces: README с инструкцией по запуску и тестам; ручной смоук-чек-лист M1 выполнен

- [ ] **Step 1: README**

````markdown
# Kafka Explorer

Desktop-приложение (Tauri 2 + Rust + React) для интерактивной работы с Kafka-кластерами.
Спецификация: `docs/superpowers/specs/2026-10-14-kafka-explorer-design.md`.

## Требования
- Rust stable (`rustup`), cmake, Xcode Command Line Tools (macOS)
- Node 22+, pnpm
- Docker — для локальной Kafka и интеграционных тестов

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

## Где хранятся данные
- Профили подключений: `connections.json` в каталоге данных приложения (macOS: `~/Library/Application Support/dev.kafkaexplorer.app/`).
- Пароли — в системном хранилище секретов (Keychain / Credential Manager / Secret Service), сервис `kafka-explorer`.
- Логи: `RUST_LOG=debug pnpm tauri dev`.
````

- [ ] **Step 2: Полный прогон автоматических тестов**

Run:
```bash
pnpm test && pnpm build && \
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && \
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --lib --tests
```
Expected: всё зелёное.

- [ ] **Step 3: Ручной смоук в `pnpm tauri dev`**

Подготовить данные:
```bash
docker exec ke-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic smoke --partitions 3
for i in $(seq 1 50); do echo "k$i:{\"n\":$i,\"status\":\"$([ $i = 7 ] && echo FAILED || echo OK)\"}"; done | \
  docker exec -i ke-kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic smoke \
  --property parse.key=true --property key.separator=:
```
Пройти чек-лист:
1. «+» → имя `local`, bootstrap `localhost:9092` → «Проверить подключение» → три ✅ → «Сохранить» → открылся «Обзор кластера», брокер 1 в списке, клик по брокеру показывает конфиг.
2. Второе подключение: `localhost:9094`, протокол `SASL_PLAINTEXT`, PLAIN, `tester` / `wrong` → «Проверить» → ❌ на шаге Kafka с текстом «Ошибка аутентификации». Исправить пароль на `tester-secret` → ✅ → «Сохранить». Перезапустить приложение и убедиться, что подключение работает (пароль взят из keychain), а в `connections.json` пароля нет.
3. Окружение `prod` для нового подключения → флажок «Только чтение» включился; при открытии вкладки этого подключения полоса над вкладками красная.
4. «Топики» → `smoke` виден, сообщений ≈ 50; поиск `/^sm/` находит его; переключатель «Внутренние» показывает `__consumer_offsets`.
5. Открыть `smoke` → «Партиции»: 3 партиции, сумма = 50; «Конфигурация»: `cleanup.policy = delete`, «Только изменённые» сокращает список.
6. «Сообщения» → «Читать» (последние 100) → 50 сообщений, статус «дочитано до конца». Значение `{"n":7,...}` в деталях отформатировано как JSON.
7. Фильтр JSONPath `$.status == "FAILED"` → 1 сообщение с ключом `k7`. Невалидный regex `(` с включённым regex → баннер `INVALID_INPUT`.
8. Live tail: включить, «Читать» → «ждём новые сообщения…»; отправить сообщение через console-producer → оно появляется в таблице за ~1 с; «Остановить» → статус «остановлено».
9. Переключиться на другую вкладку и обратно во время live tail → результаты не пропали.
10. Остановить Kafka (`docker compose -f dev/docker-compose.yml stop`) → «Обновить» в обзоре кластера → баннер с кодом `CONNECTION_FAILED` или `TIMEOUT` и кнопкой «Повторить»; UI не зависает.

Каждую найденную проблему фиксировать отдельным коммитом с тестом, который её воспроизводит.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: README с запуском, тестами и расположением данных"
```

---

## Что сознательно не входит в M1 (по спецификации — следующие этапы)

- M2: создание, изменение и удаление топиков, purge, Producer, consumer groups и сброс оффсетов, consumer-сессии, проверка read-only и prod-защиты в Rust. Сюда же: подвкладка «Consumer groups» в деталях топика, действия над сообщением «Отправить повторно» и «Сбросить группу сюда», размер топика на диске (DescribeLogDirs).
- M3: Schema Registry (Avro, Protobuf, JSON Schema), auto-детект wire format, локальные `.proto`, подвкладка «Schema» в деталях топика, числовые форматы int/long/double.
- M4: mTLS (PEM, PKCS#12, JKS), Kerberos, OAUTHBEARER и MSK IAM; экспорт сообщений, полная загрузка значений больше 1 МБ, command palette, горячие клавиши, локализация ru/en, логи в файл, автообновление, подписание, E2E на `tauri-driver`, восстановление вкладок после перезапуска, избранные топики, rack брокеров и определение режима KRaft/ZK в обзоре кластера, Monaco Editor в панели деталей (в M1 — `<pre>` с pretty JSON).
