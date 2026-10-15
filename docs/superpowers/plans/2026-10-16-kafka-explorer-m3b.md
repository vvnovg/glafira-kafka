# M3b «Релиз» Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Установщики для macOS, Windows и Linux собираются в CI по тегу и публикуются в публичный репозиторий релизов. Есть автообновление с учётом корпоративной политики, логи в файл, диагностика, экран «О программе» и документация для администраторов.

**Architecture:** Имя и идентификаторы продукта живут в одном месте (`product.rs` + `tauri.conf.json`). При смене идентификатора данные и секреты один раз мигрируют со старого. Логи пишутся собственным писателем с ротацией по размеру. Обновления проверяет Rust (`tauri-plugin-updater`) с учётом политики (`updatesEnabled`, `updateUrl`). Workflow `release` по тегу `v*`:
- проверяет версии и ключи;
- собирает матрицу ОС через `tauri-action`;
- создаёт `.pkg`;
- собирает `latest.json`;
- публикует всё в отдельный публичный репозиторий релизов.

**Tech Stack:** Rust (Tauri 2, `tauri-plugin-updater` 2, `zip`), React 19, GitHub Actions (`tauri-apps/tauri-action@v0`), Node-скрипты (`scripts/*.mjs`).

**Спецификация:** `docs/superpowers/specs/2026-10-16-kafka-explorer-m3-distribution-design.md` (§6). Начинается от `main` со слитым M3a и использует его `product.rs`, `policy`, `license`.

## Решения владельца перед стартом (спрашивает контроллер)

Контроллер получает их у пользователя до Task 1 и подставляет в задачи. Без ответа действуют значения по умолчанию.

| Параметр | Обозначение в плане | По умолчанию |
|---|---|---|
| Название продукта | `<NAME>` | `Kafka Explorer` (без переименования) |
| Короткое имя (файлы, сервис keychain) | `<SLUG>` | `kafka-explorer` |
| Идентификатор | `<IDENTIFIER>` | `dev.kafkaexplorer.app` (без миграции) |
| Вендор и продукт для реестра Windows | `<VENDOR>`, `<PRODUCT>` | `KafkaExplorer`, `KafkaExplorer` |
| Публичный репозиторий релизов | `<RELEASES_REPO>` | `vvnovg/kafka-explorer-releases` |

## Global Constraints

- Ошибки никогда не превращаются в 0 или в успех. Неизвестное значение — «—» с причиной в title.
- Блокирующие вызовы (файлы, keychain, сеть) не выполняются в async-рантайме: только через `commands::blocking` или в отдельном потоке.
- Подтверждения показываются только на странице. У каждого поля видимая подпись.
- Пароли, ключи, содержимое сертификатов и лицензионный ключ **не попадают в логи и диагностику**.
- Телеметрии и отчётов о сбоях нет. Наружу приложение ходит только к брокерам и, если обновления включены, к серверу обновлений.
- `updatesEnabled = false` в политике означает, что к серверу обновлений не уходит ни одного запроса.
- Ключ updater обязателен с первого релиза. Без него сборка релиза падает.
- Релиз нельзя собрать с dev-ключом лицензий (`src-tauri/license-public.key` ≠ `crates/license-format/testdata/dev-public.key`).
- Подпись Apple и Windows включается, только если в CI заданы соответствующие секреты. Без них установщики собираются неподписанными, а в журнале появляется предупреждение.
- Логи пишутся в стандартную папку логов ОС: ротация 5 файлов по 5 МБ, уровень `info` (`debug` через `RUST_LOG`).
- Миграция со старого идентификатора идемпотентна и ничего не удаляет из старого места.
- Версия в теге `vX.Y.Z[-pre]`, в `package.json`, `src-tauri/Cargo.toml` и `src-tauri/tauri.conf.json` совпадает.
- Коммиты заканчиваются пустой строкой и `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Push, создание репозиториев и секретов, теги — только с согласия пользователя (через контроллера).

## Структура файлов

| Файл | Ответственность |
|---|---|
| `src-tauri/src/product.rs`, `src-tauri/tauri.conf.json` | имя, идентификатор, список прежних идентификаторов |
| `src-tauri/src/migrate.rs` | перенос данных и секретов со старого идентификатора |
| `src-tauri/src/logging.rs` | писатель логов с ротацией, инициализация tracing |
| `src-tauri/src/diagnostics.rs` | архив диагностики |
| `src-tauri/build.rs` | `GIT_COMMIT` для «О программе» |
| `src-tauri/src/commands/app.rs` | `get_app_info`, `export_diagnostics`, `check_update`, `install_update` |
| `src/components/about/AboutPanel.tsx` | «О программе», обновления, диагностика |
| `scripts/check-release.mjs` | версии, ключ лицензий, CHANGELOG |
| `scripts/make-latest-json.mjs` | `latest.json` для updater |
| `scripts/make-pkg.sh` | `.pkg` для macOS |
| `.github/workflows/release.yml` | сборка и публикация релиза |
| `CHANGELOG.md`, `docs/admin.md`, `docs/admin/*`, `docs/release-checklist.md`, `docs/releases-repo/README.md` | документация |

---

### Task 1: Имя продукта и миграция со старого идентификатора

**Files:**
- Modify: `src-tauri/src/product.rs`, `src-tauri/tauri.conf.json`, `package.json` (`name` не трогать), `index.html` (`<title>`)
- Create: `src-tauri/src/migrate.rs`
- Modify: `src-tauri/src/lib.rs` (`pub mod migrate;`, вызов в `setup`)
- Modify: `src-tauri/src/secrets.rs` (`KeyringStore` с явным именем сервиса)

**Interfaces:**
- Consumes: `CONNECTION_SECRET_FIELDS`, `secret_key` (M3a), `ConnectionStore`.
- Produces:
  ```rust
  // product.rs
  pub const PRODUCT_NAME: &str = "<NAME>";
  pub const IDENTIFIER: &str = "<IDENTIFIER>";
  pub const KEYCHAIN_SERVICE: &str = "<SLUG>";
  pub const POLICY_VENDOR: &str = "<VENDOR>";
  pub const POLICY_PRODUCT: &str = "<PRODUCT>";
  /// Прежние (идентификатор, сервис keychain), от новых к старым.
  pub const LEGACY: &[(&str, &str)] = &[("dev.kafkaexplorer.app", "kafka-explorer")];

  // secrets.rs
  pub struct KeyringStore { pub service: &'static str }   // KeyringStore::default() — текущий сервис

  // migrate.rs
  pub struct MigrationReport { pub from: Option<String>, pub files: Vec<String>, pub secrets: usize, pub errors: Vec<String> }
  pub fn migrate(new_dir: &Path, legacy: &[(PathBuf, Arc<dyn SecretStore>)], target: &dyn SecretStore) -> MigrationReport;
  pub fn legacy_sources(data_dir_parent: &Path) -> Vec<(PathBuf, Arc<dyn SecretStore>)>;
  ```
  Правило: миграция выполняется, если в `new_dir` нет `connections.json`, а в первом найденном старом каталоге он есть. Копируются `connections.json`, `templates.json`, `license.json` и `history/`. Для каждого подключения и каждого поля из `CONNECTION_SECRET_FIELDS` секрет копируется из старого хранилища, если в новом его нет. Старое не удаляется. Если идентификатор не меняется (`LEGACY` содержит текущий `IDENTIFIER`), запись пропускается.

- [ ] **Step 1: Подставить имя** — в `product.rs` значения из таблицы решений владельца и константу `LEGACY` (без записи, равной текущему `IDENTIFIER`). В `tauri.conf.json`:
  - `productName` = `<NAME>`;
  - `identifier` = `<IDENTIFIER>`;
  - `app.windows[0].title` = `<NAME>`.

  В `index.html` — `<title><NAME></title>`. Тест `product::tests::matches_tauri_conf` (M3a) проверяет совпадение.

- [ ] **Step 2: `KeyringStore` с сервисом** — в `secrets.rs`:

```rust
/// OS keychain: macOS Keychain, Windows Credential Manager, Secret Service.
pub struct KeyringStore {
    pub service: &'static str,
}

impl Default for KeyringStore {
    fn default() -> Self {
        Self { service: crate::product::KEYCHAIN_SERVICE }
    }
}
```

В трёх методах `keyring::Entry::new(SERVICE, key)` заменить на `keyring::Entry::new(self.service, key)`. В `lib.rs` — `Arc::new(KeyringStore::default())`.

- [ ] **Step 3: Тесты миграции** — `src-tauri/src/migrate.rs` (сначала тесты):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::config::SASL_PASSWORD;
    use crate::secrets::{MemoryStore, secret_key};
    use crate::storage::connections::{ConnectionStore, sample_profile};

    fn old_install(dir: &Path) -> Arc<dyn SecretStore> {
        let store = ConnectionStore::open(dir).unwrap();
        store.upsert(sample_profile("a")).unwrap();
        std::fs::create_dir_all(dir.join("history")).unwrap();
        std::fs::write(dir.join("history").join("a.json"), "{}").unwrap();
        std::fs::write(dir.join("templates.json"), r#"{"schemaVersion":1,"templates":[]}"#).unwrap();
        let secrets = Arc::new(MemoryStore::default());
        secrets.set(&secret_key("a", SASL_PASSWORD), "pw").unwrap();
        secrets
    }

    #[test]
    fn copies_data_and_secrets_once() {
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("old");
        let new = root.path().join("new");
        let old_secrets = old_install(&old);
        let target = MemoryStore::default();
        let r = migrate(&new, &[(old.clone(), old_secrets.clone())], &target);
        assert_eq!(r.from.as_deref(), Some(old.to_string_lossy().as_ref()));
        assert_eq!(r.secrets, 1);
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        assert!(new.join("connections.json").exists());
        assert!(new.join("history").join("a.json").exists());
        assert_eq!(target.get(&secret_key("a", SASL_PASSWORD)).unwrap().as_deref(), Some("pw"));
        assert!(old.join("connections.json").exists(), "старое не удаляется");

        // Повторный запуск ничего не делает: новые данные уже есть.
        let again = migrate(&new, &[(old, old_secrets)], &target);
        assert_eq!(again.from, None);
    }

    #[test]
    fn nothing_to_migrate() {
        let root = tempfile::tempdir().unwrap();
        let r = migrate(&root.path().join("new"), &[(root.path().join("absent"), Arc::new(MemoryStore::default()))], &MemoryStore::default());
        assert_eq!(r.from, None);
    }

    #[test]
    fn existing_new_secret_is_not_overwritten() {
        let root = tempfile::tempdir().unwrap();
        let old = root.path().join("old");
        let old_secrets = old_install(&old);
        let target = MemoryStore::default();
        target.set(&secret_key("a", SASL_PASSWORD), "newer").unwrap();
        migrate(&root.path().join("new"), &[(old, old_secrets)], &target);
        assert_eq!(target.get(&secret_key("a", SASL_PASSWORD)).unwrap().as_deref(), Some("newer"));
    }
}
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib migrate`
Expected: FAIL.

- [ ] **Step 4: Реализация** — начало `migrate.rs`:

```rust
//! Однократный перенос данных и секретов со старого идентификатора приложения (переименование).
//! Ничего не удаляет: к старой версии можно вернуться.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::secrets::{CONNECTION_SECRET_FIELDS, KeyringStore, SecretStore, secret_key};
use crate::storage::connections::ConnectionStore;

#[derive(Debug, Default)]
pub struct MigrationReport {
    pub from: Option<String>,
    pub files: Vec<String>,
    pub secrets: usize,
    pub errors: Vec<String>,
}

const FILES: [&str; 3] = ["connections.json", "templates.json", "license.json"];

/// Каталоги данных прежних идентификаторов рядом с текущим (`<app data>/<identifier>`).
pub fn legacy_sources(data_dir_parent: &Path) -> Vec<(PathBuf, Arc<dyn SecretStore>)> {
    crate::product::LEGACY
        .iter()
        .filter(|(id, _)| *id != crate::product::IDENTIFIER)
        .map(|(id, service)| {
            let store: Arc<dyn SecretStore> = Arc::new(KeyringStore { service });
            (data_dir_parent.join(id), store)
        })
        .collect()
}

pub fn migrate(new_dir: &Path, legacy: &[(PathBuf, Arc<dyn SecretStore>)], target: &dyn SecretStore) -> MigrationReport {
    let mut report = MigrationReport::default();
    if new_dir.join("connections.json").exists() {
        return report;
    }
    let Some((old_dir, old_secrets)) = legacy.iter().find(|(d, _)| d.join("connections.json").exists()) else {
        return report;
    };
    report.from = Some(old_dir.to_string_lossy().into_owned());
    if let Err(e) = std::fs::create_dir_all(new_dir) {
        report.errors.push(format!("{}: {e}", new_dir.display()));
        return report;
    }
    for name in FILES {
        let src = old_dir.join(name);
        if src.exists() {
            match std::fs::copy(&src, new_dir.join(name)) {
                Ok(_) => report.files.push(name.to_string()),
                Err(e) => report.errors.push(format!("{name}: {e}")),
            }
        }
    }
    copy_dir(&old_dir.join("history"), &new_dir.join("history"), &mut report);

    let connections = match ConnectionStore::open(old_dir) {
        Ok(s) => s.list(),
        Err(e) => {
            report.errors.push(format!("connections.json: {}", e.message));
            Vec::new()
        }
    };
    for c in connections {
        for field in CONNECTION_SECRET_FIELDS {
            let key = secret_key(&c.id, field);
            let copy = || -> crate::error::AppResult<bool> {
                if target.get(&key)?.is_some() {
                    return Ok(false);
                }
                match old_secrets.get(&key)? {
                    Some(v) => target.set(&key, &v).map(|_| true),
                    None => Ok(false),
                }
            };
            match copy() {
                Ok(true) => report.secrets += 1,
                Ok(false) => {}
                Err(e) => report.errors.push(format!("секрет {key}: {}", e.message)),
            }
        }
    }
    report
}

fn copy_dir(src: &Path, dst: &Path, report: &mut MigrationReport) {
    let Ok(entries) = std::fs::read_dir(src) else { return };
    if let Err(e) = std::fs::create_dir_all(dst) {
        report.errors.push(format!("{}: {e}", dst.display()));
        return;
    }
    for entry in entries.flatten() {
        let name = entry.file_name();
        match std::fs::copy(entry.path(), dst.join(&name)) {
            Ok(_) => report.files.push(format!("history/{}", name.to_string_lossy())),
            Err(e) => report.errors.push(format!("history/{}: {e}", name.to_string_lossy())),
        }
    }
}
```

`lib.rs` → `setup`, до создания `AppState`:

```rust
            let dir = app.path().app_data_dir()?;
            if let Some(parent) = dir.parent() {
                let report = crate::migrate::migrate(&dir, &crate::migrate::legacy_sources(parent), &KeyringStore::default());
                if let Some(from) = &report.from {
                    tracing::info!(from, files = report.files.len(), secrets = report.secrets, "миграция данных");
                }
                for e in &report.errors {
                    tracing::warn!("миграция: {e}");
                }
            }
```

Каталог данных Tauri на всех ОС — `<базовый каталог>/<identifier>`, поэтому старый каталог лежит рядом: `parent.join(old_identifier)`.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Ручная проверка (если идентификатор меняется)** — запустить `pnpm tauri dev`. Подключения, шаблоны, история и пароли SASL прежней установки должны быть на месте. В логе — `миграция данных`.

- [ ] **Step 6: Commit**

```bash
git add src-tauri index.html
git commit -m "feat: имя продукта и миграция данных со старого идентификатора

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Логи в файл с ротацией

**Files:**
- Create: `src-tauri/src/logging.rs`
- Modify: `src-tauri/src/lib.rs` (инициализация в `setup` вместо `tracing_subscriber::fmt().init()` в начале `run`)
- Create: `src-tauri/tests/it_log_redaction.rs`

**Interfaces:**
- Produces:
  ```rust
  pub const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024;
  pub const MAX_FILES: usize = 5;
  pub const LOG_FILE: &str = "app.log";       // app.log, app.1.log … app.4.log
  pub struct RotatingWriter;                   // RotatingWriter::new(dir, base, max_bytes, keep) -> io::Result<Self>; impl io::Write; impl MakeWriter через Mutex
  pub fn init(log_dir: &Path);                 // stderr + файл; уровень из RUST_LOG или info
  pub fn log_files(log_dir: &Path) -> Vec<PathBuf>;  // существующие файлы, от новых к старым
  ```

- [ ] **Step 1: Тесты** — `logging.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn rotates_by_size_and_keeps_n_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = RotatingWriter::new(dir.path(), "app", 100, 3).unwrap();
        for i in 0..20 {
            writeln!(w, "{i:03} {}", "x".repeat(40)).unwrap();
        }
        w.flush().unwrap();
        let files = log_files_named(dir.path(), "app");
        assert_eq!(files.len(), 3, "{files:?}");
        assert!(files[0].ends_with("app.log"));
        assert!(files.iter().all(|f| std::fs::metadata(f).unwrap().len() <= 100 + 50));
        let newest = std::fs::read_to_string(&files[0]).unwrap();
        assert!(newest.contains("019"), "последняя запись в текущем файле");
    }

    #[test]
    fn appends_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        writeln!(RotatingWriter::new(dir.path(), "app", 1000, 3).unwrap(), "first").unwrap();
        writeln!(RotatingWriter::new(dir.path(), "app", 1000, 3).unwrap(), "second").unwrap();
        let text = std::fs::read_to_string(dir.path().join("app.log")).unwrap();
        assert!(text.contains("first") && text.contains("second"));
    }
}
```

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib logging`
Expected: FAIL.

- [ ] **Step 2: Реализация** — начало `logging.rs`:

```rust
//! Логи: stderr и файл в папке логов ОС с ротацией по размеру (5 × 5 МБ).
//! Секреты в логи не пишутся нигде в коде; it_log_redaction.rs это проверяет.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024;
pub const MAX_FILES: usize = 5;
pub const LOG_BASE: &str = "app";

pub struct RotatingWriter {
    dir: PathBuf,
    base: String,
    max_bytes: u64,
    keep: usize,
    file: File,
    size: u64,
}

fn file_name(base: &str, n: usize) -> String {
    if n == 0 { format!("{base}.log") } else { format!("{base}.{n}.log") }
}

impl RotatingWriter {
    pub fn new(dir: &Path, base: &str, max_bytes: u64, keep: usize) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(file_name(base, 0));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let size = file.metadata()?.len();
        Ok(Self { dir: dir.to_path_buf(), base: base.to_string(), max_bytes, keep: keep.max(1), file, size })
    }

    fn rotate(&mut self) -> io::Result<()> {
        let oldest = self.dir.join(file_name(&self.base, self.keep - 1));
        let _ = std::fs::remove_file(&oldest);
        for n in (0..self.keep - 1).rev() {
            let from = self.dir.join(file_name(&self.base, n));
            if from.exists() {
                std::fs::rename(&from, self.dir.join(file_name(&self.base, n + 1)))?;
            }
        }
        self.file = OpenOptions::new().create(true).append(true).open(self.dir.join(file_name(&self.base, 0)))?;
        self.size = 0;
        Ok(())
    }
}

impl Write for RotatingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.size > 0 && self.size + buf.len() as u64 > self.max_bytes {
            self.rotate()?;
        }
        let n = self.file.write(buf)?;
        self.size += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

struct SharedWriter(Mutex<RotatingWriter>);

impl<'a> MakeWriter<'a> for SharedWriter {
    type Writer = SharedGuard<'a>;
    fn make_writer(&'a self) -> Self::Writer {
        SharedGuard(&self.0)
    }
}

struct SharedGuard<'a>(&'a Mutex<RotatingWriter>);

impl Write for SharedGuard<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Отравленный мьютекс (паника во время записи) не должен глушить логи.
        let mut w = self.0.lock().unwrap_or_else(|e| e.into_inner());
        w.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).flush()
    }
}

/// Существующие файлы логов, от новых к старым.
pub fn log_files_named(dir: &Path, base: &str) -> Vec<PathBuf> {
    (0..MAX_FILES.max(16)).map(|n| dir.join(file_name(base, n))).filter(|p| p.exists()).collect()
}

pub fn log_files(dir: &Path) -> Vec<PathBuf> {
    log_files_named(dir, LOG_BASE)
}

/// stderr + файл. Если файл открыть нельзя, остаётся только stderr (и предупреждение в нём).
pub fn init(log_dir: &Path) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stderr = tracing_subscriber::fmt::layer().with_writer(io::stderr);
    match RotatingWriter::new(log_dir, LOG_BASE, MAX_FILE_BYTES, MAX_FILES) {
        Ok(w) => {
            let file = tracing_subscriber::fmt::layer().with_ansi(false).with_writer(SharedWriter(Mutex::new(w)));
            let _ = tracing_subscriber::registry().with(filter).with(stderr).with(file).try_init();
        }
        Err(e) => {
            let _ = tracing_subscriber::registry().with(filter).with(stderr).try_init();
            tracing::warn!("лог-файл недоступен ({}): {e}", log_dir.display());
        }
    }
}
```

`lib.rs`:
- удалить инициализацию `tracing_subscriber::fmt()…init()` в начале `run()`;
- первой строкой `setup` добавить `crate::logging::init(&app.path().app_log_dir()?);`, затем `tracing::info!(version = env!("CARGO_PKG_VERSION"), "запуск")`;
- добавить `pub mod logging;`.

Если `tracing_subscriber` без фичи `registry`, в `Cargo.toml` сделать `features = ["env-filter", "registry", "fmt"]`.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib logging`
Expected: PASS.

- [ ] **Step 3: Проверка, что секреты не попадают в лог** — `src-tauri/tests/it_log_redaction.rs`:

```rust
mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};

use kafka_explorer_lib::kafka::config::{SASL_PASSWORD, SSL_KEY_PASSWORD};
use kafka_explorer_lib::kafka::connectivity::test_connection;
use kafka_explorer_lib::secrets::{MemoryStore, SecretStore, secret_key};
use kafka_explorer_lib::storage::connections::{
    CertFormat, ClientCert, SaslConfig, SaslMechanism, SecurityConfig, TlsConfig,
};

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn passwords_and_keys_never_reach_logs() {
    let buf = Buf::default();
    let sink = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(move || sink.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        let secrets = MemoryStore::default();
        secrets.set(&secret_key("it", SASL_PASSWORD), "SASL-SECRET-123").unwrap();
        secrets.set(&secret_key("it", SSL_KEY_PASSWORD), "keypass").unwrap();
        let mut p = common::plaintext_profile("127.0.0.1:1");
        p.security = SecurityConfig::SaslSsl {
            tls: TlsConfig {
                ca_location: Some(common::cert_path("ca.pem")),
                ca_format: CertFormat::Pem,
                verify_hostname: true,
                client_cert: Some(ClientCert {
                    format: CertFormat::Pem,
                    certificate_path: common::cert_path("client.pem"),
                    key_path: Some(common::cert_path("client-enc.key")),
                    alias: None,
                }),
            },
            sasl: SaslConfig { mechanism: SaslMechanism::Plain, username: "u".into() },
        };
        let _ = test_connection(&p, &secrets);
    });
    let text = String::from_utf8_lossy(&buf.0.lock().unwrap()).to_string();
    for secret in ["SASL-SECRET-123", "keypass", "PRIVATE KEY", "BEGIN CERTIFICATE"] {
        assert!(!text.contains(secret), "в логе найдено «{secret}»:\n{text}");
    }
}
```

Тест не требует Kafka (адрес `127.0.0.1:1` недоступен). В `Cargo.toml` `[dev-dependencies]` добавить `tracing-subscriber` с теми же фичами, если он не виден в интеграционных тестах.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --test it_log_redaction`
Expected: PASS. Если тест нашёл секрет, найти место в коде, которое его логирует, и убрать вывод. Ослаблять тест нельзя.

- [ ] **Step 4: Commit**

```bash
git add src-tauri
git commit -m "feat: логи в файл с ротацией 5×5 МБ; тест, что секреты не попадают в лог

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: «О программе» и диагностика

**Files:**
- Modify: `src-tauri/build.rs` (`GIT_COMMIT`)
- Create: `src-tauri/src/diagnostics.rs`
- Create: `src-tauri/src/commands/app.rs` (`get_app_info`, `export_diagnostics`)
- Modify: `src-tauri/Cargo.toml` (`zip`), `src-tauri/capabilities/default.json` (`dialog:allow-save`), `src-tauri/src/lib.rs`, `src-tauri/src/commands/mod.rs`
- Modify: `src/api/types.ts`, `src/api/client.ts`
- Create: `src/components/about/AboutPanel.tsx`, `AboutPanel.test.tsx`
- Modify: `src/state/workspace.ts`, `src/components/TabContent.tsx`, `src/components/Layout.tsx`, `src/components/Sidebar.tsx`

**Interfaces:**
- Produces:
  ```rust
  #[derive(Serialize)] #[serde(rename_all = "camelCase")]
  pub struct AppInfo { pub name: String, pub version: String, pub commit: String, pub os: String, pub arch: String,
                       pub data_dir: String, pub log_dir: String }
  pub fn write_archive(out: &Path, info: &AppInfo, profiles: &[ConnectionProfile], policy: &PolicyStatus,
                       license: &LicenseStatus, log_files: &[PathBuf]) -> AppResult<()>;   // diagnostics.rs
  #[tauri::command] get_app_info() -> AppInfo
  #[tauri::command] async export_diagnostics(path: String) -> ()
  ```
  Состав архива:
  - `info.json`: `AppInfo`, `policy`, `license`;
  - `connections.json`: профили, в которых нет секретов, но пути к сертификатам остаются;
  - `logs/app*.log`.
- Produces (TS): `AppInfo`, `api.getAppInfo()`, `api.exportDiagnostics(path)`; `TabSpec += { kind: "about" }`.

- [ ] **Step 1: Коммит в сборке** — `src-tauri/build.rs`:

```rust
fn main() {
    let commit = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=GIT_COMMIT={commit}");
    println!("cargo:rerun-if-changed=../.git/HEAD");
    tauri_build::build()
}
```

- [ ] **Step 2: Тест архива** — `diagnostics.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::license::{LicenseState, LicenseStatus, Tier};
    use crate::policy::PolicyStatus;
    use crate::storage::connections::sample_profile;
    use std::io::Read;

    #[test]
    fn archive_contains_info_profiles_and_logs_without_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("app.log");
        std::fs::write(&log, "запуск\n").unwrap();
        let info = AppInfo {
            name: "X".into(), version: "0.2.0".into(), commit: "abc".into(), os: "macos".into(),
            arch: "aarch64".into(), data_dir: "/d".into(), log_dir: "/l".into(),
        };
        let license = LicenseStatus {
            tier: Tier::Pro, state: LicenseState::Valid, source: None, licensee: Some("a@b".into()),
            kind: None, expires: None, days_left: None, error: None,
        };
        let out = dir.path().join("diag.zip");
        write_archive(&out, &info, &[sample_profile("a")], &PolicyStatus::default(), &license, &[log]).unwrap();

        let mut zip = zip::ZipArchive::new(std::fs::File::open(&out).unwrap()).unwrap();
        let mut names: Vec<_> = (0..zip.len()).map(|i| zip.by_index(i).unwrap().name().to_string()).collect();
        names.sort();
        assert_eq!(names, ["connections.json", "info.json", "logs/app.log"]);
        let mut info_json = String::new();
        zip.by_name("info.json").unwrap().read_to_string(&mut info_json).unwrap();
        assert!(info_json.contains("\"version\": \"0.2.0\"") && info_json.contains("\"tier\": \"pro\""));
    }
}
```

`Cargo.toml`: `zip = { version = "2", default-features = false, features = ["deflate"] }`.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib diagnostics`
Expected: FAIL.

- [ ] **Step 3: Реализация** — начало `diagnostics.rs`:

```rust
//! Архив диагностики: пользователь сам прикладывает его к обращению. Наружу ничего не отправляется.
//! Секретов в источниках нет: профили не содержат паролей (они в keychain), лицензия — без ключа.

use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;
use zip::write::SimpleFileOptions;

use crate::error::{AppError, AppResult};
use crate::license::LicenseStatus;
use crate::policy::PolicyStatus;
use crate::storage::connections::ConnectionProfile;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AppInfo {
    pub name: String,
    pub version: String,
    pub commit: String,
    pub os: String,
    pub arch: String,
    pub data_dir: String,
    pub log_dir: String,
}

#[derive(Serialize)]
struct InfoFile<'a> {
    app: &'a AppInfo,
    policy: &'a PolicyStatus,
    license: &'a LicenseStatus,
}

fn zip_err(e: impl std::fmt::Display) -> AppError {
    AppError::internal("Не удалось записать архив диагностики").with_details(e.to_string())
}

pub fn write_archive(
    out: &Path,
    info: &AppInfo,
    profiles: &[ConnectionProfile],
    policy: &PolicyStatus,
    license: &LicenseStatus,
    log_files: &[PathBuf],
) -> AppResult<()> {
    let file = std::fs::File::create(out)?;
    let mut zip = zip::ZipWriter::new(file);
    let opts = SimpleFileOptions::default();
    zip.start_file("info.json", opts).map_err(zip_err)?;
    zip.write_all(&serde_json::to_vec_pretty(&InfoFile { app: info, policy, license })?)?;
    zip.start_file("connections.json", opts).map_err(zip_err)?;
    zip.write_all(&serde_json::to_vec_pretty(profiles)?)?;
    for path in log_files {
        let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
        let bytes = std::fs::read(path)?;
        zip.start_file(format!("logs/{name}"), opts).map_err(zip_err)?;
        zip.write_all(&bytes)?;
    }
    zip.finish().map_err(zip_err)?;
    Ok(())
}
```

`commands/app.rs`:

```rust
use tauri::{AppHandle, Manager, State};

use super::blocking;
use crate::diagnostics::{AppInfo, write_archive};
use crate::error::{AppError, AppResult};
use crate::state::AppState;

fn dir(app: &AppHandle, which: &str) -> String {
    let p = match which {
        "data" => app.path().app_data_dir(),
        _ => app.path().app_log_dir(),
    };
    p.map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|_| "—".into())
}

pub fn app_info(app: &AppHandle) -> AppInfo {
    AppInfo {
        name: crate::product::PRODUCT_NAME.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        commit: env!("GIT_COMMIT").into(),
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
        data_dir: dir(app, "data"),
        log_dir: dir(app, "log"),
    }
}

#[tauri::command]
pub fn get_app_info(app: AppHandle) -> AppResult<AppInfo> {
    Ok(app_info(&app))
}

#[tauri::command]
pub async fn export_diagnostics(app: AppHandle, state: State<'_, AppState>, path: String) -> AppResult<()> {
    if path.trim().is_empty() {
        return Err(AppError::invalid("Не выбран файл для архива"));
    }
    let st = state.inner().clone();
    let info = app_info(&app);
    let log_dir = app.path().app_log_dir().map_err(|e| AppError::internal(e.to_string()))?;
    blocking(move || {
        write_archive(
            std::path::Path::new(&path),
            &info,
            &st.connections.list(),
            &st.policy.status,
            &st.license.status(),
            &crate::logging::log_files(&log_dir),
        )
    })
    .await
}
```

Зарегистрировать `pub mod diagnostics;`, `commands::app` и обе команды. В `capabilities/default.json` добавить `"dialog:allow-save"`.

- [ ] **Step 4: Экран «О программе» — тест** — `src/components/about/AboutPanel.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-dialog", () => ({ save: vi.fn().mockResolvedValue("/tmp/diag.zip") }));
vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getAppInfo: vi.fn().mockResolvedValue({ name: "X", version: "0.2.0", commit: "abc123", os: "macos", arch: "aarch64", dataDir: "/d", logDir: "/l" }),
      getLicenseStatus: vi.fn().mockResolvedValue({ tier: "free", state: "none", source: null, licensee: null, kind: null, expires: null, daysLeft: null, error: null }),
      getPolicyStatus: vi.fn().mockResolvedValue({ managed: false, source: null, updatesEnabled: true, updateUrl: null, hasLicenseKey: false, errors: [] }),
      exportDiagnostics: vi.fn().mockResolvedValue(undefined),
      checkUpdate: vi.fn().mockResolvedValue({ status: "upToDate" }),
    },
  };
});

import { api } from "../../api/client";
import { AboutPanel } from "./AboutPanel";

describe("AboutPanel", () => {
  it("версия, сборка, папка логов и диагностика", async () => {
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    expect(await screen.findByText("0.2.0")).toBeInTheDocument();
    expect(screen.getByText("abc123")).toBeInTheDocument();
    expect(screen.getByText("/l")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Собрать диагностику" }));
    expect(api.exportDiagnostics).toHaveBeenCalledWith("/tmp/diag.zip");
    expect(await screen.findByText(/Архив сохранён/)).toBeInTheDocument();
  });
});
```

Run: `pnpm vitest run src/components/about`
Expected: FAIL.

- [ ] **Step 5: Реализация экрана**
  - Типы: `AppInfo { name; version; commit; os; arch; dataDir; logDir }`.
  - `api.getAppInfo = () => call<AppInfo>("get_app_info")`, `api.exportDiagnostics = (path: string) => call<void>("export_diagnostics", { path })`.
  - Вкладка `{ kind: "about" }`: id `about`, заголовок «О программе». В `Sidebar` рядом со ссылкой «Лицензия» — ссылка «О программе».
  - `src/components/about/AboutPanel.tsx` (секцию обновлений добавит Task 4):

```tsx
import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { save } from "@tauri-apps/plugin-dialog";
import { api } from "../../api/client";
import { ErrorBanner } from "../common/ErrorBanner";

export function AboutPanel() {
  const info = useQuery({ queryKey: ["app-info"], queryFn: api.getAppInfo });
  const license = useQuery({ queryKey: ["license"], queryFn: api.getLicenseStatus });
  const policy = useQuery({ queryKey: ["policy"], queryFn: api.getPolicyStatus });
  const [saved, setSaved] = useState<string | null>(null);
  const diag = useMutation({
    mutationFn: async () => {
      const date = new Date().toISOString().slice(0, 10);
      const path = await save({ defaultPath: `diagnostics-${date}.zip`, filters: [{ name: "ZIP", extensions: ["zip"] }] });
      if (!path) return null;
      await api.exportDiagnostics(path);
      return path;
    },
    onSuccess: (path) => setSaved(path),
  });

  if (info.error) return <ErrorBanner error={info.error} onRetry={() => info.refetch()} />;
  const i = info.data;
  if (!i) return <p className="empty">Загрузка…</p>;
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{i.name}</h2>
      </div>
      <dl className="stats">
        <div><dt>Версия</dt><dd>{i.version}</dd></div>
        <div><dt>Сборка</dt><dd>{i.commit}</dd></div>
        <div><dt>Система</dt><dd>{i.os} / {i.arch}</dd></div>
        <div><dt>Тариф</dt><dd>{license.data ? (license.data.tier === "pro" ? "Pro" : "Free") : "…"}</dd></div>
        <div><dt>Политика организации</dt><dd>{policy.data ? (policy.data.managed ? "задана" : "нет") : "…"}</dd></div>
      </dl>
      <p className="muted">Данные: <code>{i.dataDir}</code></p>
      <p className="muted">Логи: <code>{i.logDir}</code></p>
      <section className="panel">
        <h3>Диагностика</h3>
        <p className="muted">
          Архив с логами, версией и профилями подключений без паролей. Приложите его к обращению — само приложение ничего не отправляет.
        </p>
        <div className="toolbar">
          <button type="button" disabled={diag.isPending} onClick={() => diag.mutate()}>
            Собрать диагностику
          </button>
        </div>
        {saved && <p role="status">Архив сохранён: {saved}</p>}
        {diag.error && <ErrorBanner error={diag.error} />}
      </section>
    </div>
  );
}
```

Run: `pnpm vitest run && pnpm build && cargo test --manifest-path src-tauri/Cargo.toml --lib && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src src-tauri
git commit -m "feat: «О программе» и архив диагностики

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Автообновление

**Files:**
- Modify: `src-tauri/Cargo.toml` (`tauri-plugin-updater = "2"`), `src-tauri/tauri.conf.json` (`plugins.updater`, `bundle.createUpdaterArtifacts`)
- Modify: `src-tauri/src/commands/app.rs` (`check_update`, `install_update`), `src-tauri/src/lib.rs`
- Modify: `src/api/types.ts`, `src/api/client.ts`, `src/components/about/AboutPanel.tsx`, `AboutPanel.test.tsx`
- Create: `src/components/about/UpdateNotice.tsx` (проверка при запуске), подключить в `Layout.tsx`

**Interfaces:**
- Consumes: `AppState.policy.status.{updates_enabled, update_url}`, `AppState.sessions.len()`.
- Produces:
  ```rust
  #[derive(Serialize)] #[serde(tag = "status", rename_all = "camelCase")]
  pub enum UpdateCheck {
      Disabled { reason: String },                      // политика выключила
      UpToDate,
      Available { version: String, notes: Option<String>, date: Option<String> },
  }
  #[tauri::command] async check_update() -> UpdateCheck
  #[tauri::command] async install_update(force: bool) -> ()   // без force при активных сессиях → CONFIRMATION_REQUIRED
  ```
  JSON: `{"status":"available","version":"0.3.0","notes":"…","date":"2026-11-01T…"}`, `{"status":"disabled","reason":"Обновления управляются вашей организацией"}`.
- Produces (TS): `type UpdateCheck = { status: "disabled"; reason: string } | { status: "upToDate" } | { status: "available"; version: string; notes: string | null; date: string | null }`; `api.checkUpdate()`, `api.installUpdate(force: boolean)`.

- [ ] **Step 1: Ключ updater (владелец, через контроллера)** — **исполнитель сам ключ не создаёт.** В отчёте попросить контроллера получить у пользователя открытый ключ. Пользователь запускает у себя:

```bash
pnpm tauri signer generate -w ~/.tauri/<SLUG>-updater.key
```

Команда спросит пароль. Нужен открытый ключ: содержимое `~/.tauri/<SLUG>-updater.key.pub`. Закрытый ключ и пароль позже станут секретами `TAURI_SIGNING_PRIVATE_KEY` и `TAURI_SIGNING_PRIVATE_KEY_PASSWORD` (Task 6). Пока ключа нет, в `pubkey` временно стоит строка `"REPLACE_WITH_UPDATER_PUBKEY"`. Проверка релиза (Task 6) не пропустит её в сборку.

- [ ] **Step 2: Конфигурация** — `tauri.conf.json`:

```json
  "bundle": {
    "createUpdaterArtifacts": true
  },
  "plugins": {
    "updater": {
      "pubkey": "<открытый ключ из Step 1>",
      "endpoints": ["https://github.com/<RELEASES_REPO>/releases/latest/download/latest.json"]
    }
  }
```

(Объединить с существующим `bundle`, не заменяя `icon` и прочее.) `lib.rs`: `.plugin(tauri_plugin_updater::Builder::new().build())`. Права в capabilities не нужны: плагин вызывается только из Rust.

- [ ] **Step 3: Команды — тесты** — логика решения вынесена в чистую функцию, чтобы тестировать её без сети:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::PolicyStatus;

    #[test]
    fn policy_disables_check_without_network() {
        let st = PolicyStatus { managed: true, updates_enabled: false, ..PolicyStatus::default() };
        match update_plan(&st) {
            UpdatePlan::Disabled(reason) => assert!(reason.contains("организац")),
            _ => panic!("ожидалось Disabled"),
        }
    }

    #[test]
    fn policy_url_overrides_endpoint() {
        let st = PolicyStatus { managed: true, update_url: Some("https://mirror.corp/latest.json".into()), ..PolicyStatus::default() };
        match update_plan(&st) {
            UpdatePlan::Check(Some(url)) => assert_eq!(url, "https://mirror.corp/latest.json"),
            _ => panic!(),
        }
        assert!(matches!(update_plan(&PolicyStatus::default()), UpdatePlan::Check(None)));
    }

    #[test]
    fn update_check_json_shape() {
        let v = serde_json::to_value(UpdateCheck::Available { version: "0.3.0".into(), notes: None, date: None }).unwrap();
        assert_eq!(v["status"], "available");
        assert_eq!(serde_json::to_value(UpdateCheck::UpToDate).unwrap()["status"], "upToDate");
    }
}
```

- [ ] **Step 4: Команды — реализация** — в `commands/app.rs`:

```rust
use serde::Serialize;
use tauri_plugin_updater::UpdaterExt;

use crate::error::ErrorCode;
use crate::policy::PolicyStatus;

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum UpdateCheck {
    Disabled { reason: String },
    UpToDate,
    Available { version: String, notes: Option<String>, date: Option<String> },
}

enum UpdatePlan {
    Disabled(String),
    /// None — адрес из tauri.conf.json, Some — зеркало из политики.
    Check(Option<String>),
}

fn update_plan(policy: &PolicyStatus) -> UpdatePlan {
    if !policy.updates_enabled {
        return UpdatePlan::Disabled("Обновления управляются вашей организацией".into());
    }
    UpdatePlan::Check(policy.update_url.clone())
}

fn updater(app: &AppHandle, url: Option<String>) -> AppResult<tauri_plugin_updater::Updater> {
    let mut b = app.updater_builder();
    if let Some(u) = url {
        let parsed = u.parse().map_err(|_| AppError::invalid(format!("Неверный адрес обновлений в политике: {u}")))?;
        b = b.endpoints(vec![parsed]).map_err(|e| AppError::internal(e.to_string()))?;
    }
    b.build().map_err(|e| AppError::internal("Обновления недоступны").with_details(e.to_string()))
}

fn net_err(e: tauri_plugin_updater::Error) -> AppError {
    AppError::new(ErrorCode::ConnectionFailed, "Не удалось проверить обновления").with_details(e.to_string())
}

#[tauri::command]
pub async fn check_update(app: AppHandle, state: State<'_, AppState>) -> AppResult<UpdateCheck> {
    let url = match update_plan(&state.policy.status) {
        UpdatePlan::Disabled(reason) => return Ok(UpdateCheck::Disabled { reason }),
        UpdatePlan::Check(url) => url,
    };
    match updater(&app, url)?.check().await.map_err(net_err)? {
        None => Ok(UpdateCheck::UpToDate),
        Some(u) => Ok(UpdateCheck::Available { version: u.version.clone(), notes: u.body.clone(), date: u.date.map(|d| d.to_string()) }),
    }
}

#[tauri::command]
pub async fn install_update(app: AppHandle, state: State<'_, AppState>, force: bool) -> AppResult<()> {
    let url = match update_plan(&state.policy.status) {
        UpdatePlan::Disabled(reason) => return Err(AppError::invalid(reason)),
        UpdatePlan::Check(url) => url,
    };
    let active = state.sessions.len();
    if active > 0 && !force {
        return Err(AppError::new(
            ErrorCode::ConfirmationRequired,
            format!("Идёт сессий: {active} (чтение, consumer или пакетная отправка). Обновление их остановит."),
        ));
    }
    let Some(update) = updater(&app, url)?.check().await.map_err(net_err)? else {
        return Err(AppError::invalid("Обновлений нет"));
    };
    update
        .download_and_install(|_, _| {}, || {})
        .await
        .map_err(|e| AppError::internal("Не удалось установить обновление").with_details(e.to_string()))?;
    app.restart();
}
```

(`app.restart()` возвращает `!`. Если сигнатура в вашей версии Tauri другая, после него вернуть `Ok(())`. При выходе `RunEvent::Exit` (M2b) корректно завершает сессии.)

Зарегистрировать обе команды.

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib commands::app && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Интерфейс** — типы и клиент из Interfaces (`checkUpdate: () => call<UpdateCheck>("check_update")`, `installUpdate: (force: boolean) => call<void>("install_update", { force })`). В `AboutPanel` секция «Обновления»:
  - кнопка «Проверить обновления» (`useMutation(api.checkUpdate)`);
  - `disabled` → текст причины без кнопки установки;
  - `upToDate` → «Установлена последняя версия»;
  - `available` → «Доступна версия X», описание (`<pre>`) и кнопка «Установить и перезапустить». Если ответ `CONFIRMATION_REQUIRED`, в странице появляется подтверждение с текстом ошибки и кнопками «Остановить сессии и обновить» (`installUpdate(true)`) и «Отмена»;
  - ошибка сети → `ErrorBanner` с «Повторить».

Тесты в `AboutPanel.test.tsx`:

```tsx
  it("обновление доступно; при активных сессиях — подтверждение", async () => {
    vi.mocked(api.checkUpdate).mockResolvedValue({ status: "available", version: "0.3.0", notes: "Исправления", date: null });
    vi.mocked(api.installUpdate)
      .mockRejectedValueOnce({ code: "CONFIRMATION_REQUIRED", message: "Идёт сессий: 1 …", retryable: false })
      .mockResolvedValueOnce(undefined);
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Проверить обновления" }));
    expect(await screen.findByText("Доступна версия 0.3.0")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Установить и перезапустить" }));
    expect(await screen.findByText(/Идёт сессий: 1/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Остановить сессии и обновить" }));
    expect(api.installUpdate).toHaveBeenLastCalledWith(true);
  });

  it("политика выключила обновления", async () => {
    vi.mocked(api.checkUpdate).mockResolvedValue({ status: "disabled", reason: "Обновления управляются вашей организацией" });
    const user = userEvent.setup();
    renderWithClient(<AboutPanel />);
    await user.click(await screen.findByRole("button", { name: "Проверить обновления" }));
    expect(await screen.findByText("Обновления управляются вашей организацией")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Установить и перезапустить" })).not.toBeInTheDocument();
  });
```

(В моке `api` добавить `installUpdate: vi.fn()`.)

`src/components/about/UpdateNotice.tsx` — проверка один раз при запуске, без блокировки интерфейса. Ошибки проверки показываются тихой меткой, ничего не всплывает:

```tsx
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../../api/client";
import { useWorkspace } from "../../state/workspace";

export function UpdateNotice() {
  const q = useQuery({ queryKey: ["update-check"], queryFn: api.checkUpdate, staleTime: Infinity, retry: false });
  const open = useWorkspace((s) => s.open);
  const [hidden, setHidden] = useState(false);
  if (hidden || q.data?.status !== "available") return null;
  return (
    <div className="notice" role="status">
      <span>Доступна версия {q.data.version}.</span>
      <button type="button" className="link" onClick={() => open({ kind: "about" })}>
        Подробнее
      </button>
      <button type="button" className="icon" aria-label="Скрыть уведомление" onClick={() => setHidden(true)}>
        ×
      </button>
    </div>
  );
}
```

В `Layout.tsx` — рядом с `<LicenseNotice />`.

Run: `pnpm vitest run && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src src-tauri
git commit -m "feat: автообновление с учётом политики организации

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Установщики

**Files:**
- Modify: `src-tauri/tauri.conf.json` (`bundle.targets`, метаданные, Linux-зависимости)
- Create: `scripts/make-pkg.sh`

**Interfaces:**
- Produces: `bundle.targets = ["app", "dmg", "msi", "deb", "rpm", "appimage"]`. MSI ставится на машину и поддерживает тихую установку `msiexec /i <файл>.msi /qn`.
- Produces: `scripts/make-pkg.sh <path/to/App.app> <out.pkg> [installer-identity]` → `.pkg` (с подписью, если передана identity «Developer ID Installer: …»).

- [ ] **Step 1: Конфигурация** — в `tauri.conf.json` → `bundle`:

```json
    "targets": ["app", "dmg", "msi", "deb", "rpm", "appimage"],
    "publisher": "<NAME>",
    "category": "DeveloperTool",
    "shortDescription": "<NAME> — desktop client for Apache Kafka",
    "copyright": "© 2026 <NAME>",
    "linux": {
      "deb": { "depends": ["libsecret-1-0"] },
      "rpm": { "depends": ["libsecret"] }
    },
    "macOS": { "minimumSystemVersion": "11.0" },
    "windows": { "wix": { "language": ["en-US"] } }
```

Шаблон WiX в Tauri ставит приложение на машину (`InstallScope="perMachine"`). Проверить это в собранном MSI: `msiexec /i … /qn` из административной консоли устанавливает в `C:\Program Files\<NAME>`. Если установка идёт в профиль пользователя, задать свой шаблон WiX (`bundle.windows.wix.template`) с `InstallScope="perMachine"`.

- [ ] **Step 2: `.pkg`** — `scripts/make-pkg.sh` (исполняемый):

```bash
#!/usr/bin/env bash
# .pkg для раздачи через Jamf/Intune: ставит приложение в /Applications.
# Использование: make-pkg.sh <App.app> <out.pkg> [Developer ID Installer identity]
set -euo pipefail
APP="$1"
OUT="$2"
IDENTITY="${3:-}"
ARGS=(--component "$APP" /Applications)
if [ -n "$IDENTITY" ]; then
  ARGS+=(--sign "$IDENTITY")
else
  echo "warning: .pkg без подписи (нет Developer ID Installer)" >&2
fi
productbuild "${ARGS[@]}" "$OUT"
```

- [ ] **Step 3: Локальная сборка (macOS)**

Run: `pnpm tauri build --bundles app,dmg && scripts/make-pkg.sh "src-tauri/target/release/bundle/macos/<NAME>.app" /tmp/test.pkg && pkgutil --payload-files /tmp/test.pkg | head -3`
Expected:
- `.dmg` и `.app` собраны;
- `pkgutil` показывает `./<NAME>.app/...`.

Подпись updater: без `TAURI_SIGNING_PRIVATE_KEY` Tauri предупредит или упадёт на `createUpdaterArtifacts`. Для локальной проверки задайте `TAURI_SIGNING_PRIVATE_KEY` из `~/.tauri/<SLUG>-updater.key` (с согласия пользователя: файл его) или временно соберите с `--config '{"bundle":{"createUpdaterArtifacts":false}}'`. Опишите в отчёте, какой вариант использован.

- [ ] **Step 4: Commit**

```bash
git add src-tauri/tauri.conf.json scripts/make-pkg.sh
git commit -m "build: установщики dmg/pkg, msi на машину, deb/rpm/AppImage

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Workflow релиза

**Files:**
- Create: `scripts/check-release.mjs`, `scripts/check-release.test.mjs`
- Create: `scripts/make-latest-json.mjs`, `scripts/make-latest-json.test.mjs`
- Create: `.github/workflows/release.yml`
- Create: `CHANGELOG.md`
- Modify: `package.json` (`"test:scripts": "node --test scripts/"`)

**Interfaces:**
- Produces: `node scripts/check-release.mjs <tag>`. Код 0 — можно выпускать, 1 — список проблем в stderr. Проверяется:
  - версия тега совпадает с `package.json`, `src-tauri/Cargo.toml` и `src-tauri/tauri.conf.json`;
  - `src-tauri/license-public.key` ≠ `crates/license-format/testdata/dev-public.key`;
  - `plugins.updater.pubkey` ≠ `REPLACE_WITH_UPDATER_PUBKEY`;
  - в `CHANGELOG.md` есть раздел `## <версия>`.
- Produces: `node scripts/make-latest-json.mjs --version <v> --notes-file <f> --base-url <url> --dir <artifacts>` → `latest.json` в stdout. Платформы определяются по именам файлов:
  - `*.app.tar.gz` с `aarch64` → `darwin-aarch64`, с `x64`/`x86_64` → `darwin-x86_64`;
  - `*.msi` → `windows-x86_64`;
  - `*.AppImage` → `linux-x86_64`;
  - подпись — содержимое соседнего `<файл>.sig`.
- Produces: функции `export function checkRelease({ tag, files }) -> string[]` и `export function buildLatest({ version, notes, baseUrl, files, pubDate }) -> object` для тестов.

- [ ] **Step 1: Тесты скриптов** — `scripts/check-release.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { checkRelease } from "./check-release.mjs";

const files = (o = {}) => ({
  packageJson: JSON.stringify({ version: "0.2.0" }),
  cargoToml: '[package]\nname = "x"\nversion = "0.2.0"\n',
  tauriConf: JSON.stringify({ version: "0.2.0", plugins: { updater: { pubkey: "REALKEY" } } }),
  licensePublic: "REAL\n",
  devPublic: "DEV\n",
  changelog: "# Changelog\n\n## 0.2.0\n- первое\n",
  ...o,
});

test("всё в порядке", () => assert.deepEqual(checkRelease({ tag: "v0.2.0", files: files() }), []));
test("разные версии", () => {
  const p = checkRelease({ tag: "v0.2.1", files: files() });
  assert.equal(p.length, 4); // три файла + CHANGELOG
});
test("dev-ключ лицензий и заглушка updater", () => {
  const p = checkRelease({
    tag: "v0.2.0",
    files: files({ licensePublic: "DEV\n", tauriConf: JSON.stringify({ version: "0.2.0", plugins: { updater: { pubkey: "REPLACE_WITH_UPDATER_PUBKEY" } } }) }),
  });
  assert.ok(p.some((x) => x.includes("лицензий")));
  assert.ok(p.some((x) => x.includes("updater")));
});
test("предрелиз", () => {
  const f = files({
    packageJson: JSON.stringify({ version: "0.2.0-beta.1" }),
    cargoToml: 'version = "0.2.0-beta.1"\n',
    tauriConf: JSON.stringify({ version: "0.2.0-beta.1", plugins: { updater: { pubkey: "K" } } }),
    changelog: "## 0.2.0-beta.1\n",
  });
  assert.deepEqual(checkRelease({ tag: "v0.2.0-beta.1", files: f }), []);
});
```

`scripts/make-latest-json.test.mjs`:

```js
import { test } from "node:test";
import assert from "node:assert/strict";
import { buildLatest } from "./make-latest-json.mjs";

test("платформы по именам файлов", () => {
  const files = {
    "App_aarch64.app.tar.gz": "SIG-A",
    "App_x64.app.tar.gz": "SIG-X",
    "App_0.2.0_x64_en-US.msi": "SIG-W",
    "app_0.2.0_amd64.AppImage": "SIG-L",
  };
  const latest = buildLatest({ version: "0.2.0", notes: "n", baseUrl: "https://g/r/releases/download/v0.2.0", files, pubDate: "2026-11-01T00:00:00Z" });
  assert.equal(latest.version, "0.2.0");
  assert.deepEqual(Object.keys(latest.platforms).sort(), ["darwin-aarch64", "darwin-x86_64", "linux-x86_64", "windows-x86_64"]);
  assert.equal(latest.platforms["windows-x86_64"].url, "https://g/r/releases/download/v0.2.0/App_0.2.0_x64_en-US.msi");
  assert.equal(latest.platforms["darwin-aarch64"].signature, "SIG-A");
});

test("нет подписи — ошибка", () => {
  assert.throws(() => buildLatest({ version: "1", notes: "", baseUrl: "u", files: { "a.msi": "" }, pubDate: "d" }), /подпис/);
});
```

Run: `node --test scripts/`
Expected: FAIL (нет модулей).

- [ ] **Step 2: Скрипты** — `scripts/check-release.mjs`:

```js
#!/usr/bin/env node
// Проверки перед релизом: версии, боевые ключи, раздел CHANGELOG.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export function checkRelease({ tag, files }) {
  const problems = [];
  const version = tag.replace(/^v/, "");
  const pkg = JSON.parse(files.packageJson).version;
  const cargo = /^version\s*=\s*"([^"]+)"/m.exec(files.cargoToml)?.[1];
  const tauri = JSON.parse(files.tauriConf);
  for (const [name, v] of [["package.json", pkg], ["src-tauri/Cargo.toml", cargo], ["src-tauri/tauri.conf.json", tauri.version]]) {
    if (v !== version) problems.push(`${name}: версия ${v ?? "не найдена"}, а тег ${tag}`);
  }
  if (files.licensePublic.trim() === files.devPublic.trim()) {
    problems.push("src-tauri/license-public.key — тестовый dev-ключ лицензий: сгенерируйте боевой (docs/licensing.md)");
  }
  const pubkey = tauri.plugins?.updater?.pubkey;
  if (!pubkey || pubkey === "REPLACE_WITH_UPDATER_PUBKEY") {
    problems.push("tauri.conf.json: не задан открытый ключ updater (plugins.updater.pubkey)");
  }
  if (!new RegExp(`^## ${version.replace(/[.]/g, "\\.")}\\s*$`, "m").test(files.changelog)) {
    problems.push(`CHANGELOG.md: нет раздела «## ${version}»`);
  }
  return problems;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const tag = process.argv[2];
  if (!tag) {
    console.error("использование: check-release.mjs <tag>");
    process.exit(2);
  }
  const read = (p) => readFileSync(p, "utf8");
  const problems = checkRelease({
    tag,
    files: {
      packageJson: read("package.json"),
      cargoToml: read("src-tauri/Cargo.toml"),
      tauriConf: read("src-tauri/tauri.conf.json"),
      licensePublic: read("src-tauri/license-public.key"),
      devPublic: read("crates/license-format/testdata/dev-public.key"),
      changelog: read("CHANGELOG.md"),
    },
  });
  if (problems.length) {
    console.error(problems.map((p) => `✗ ${p}`).join("\n"));
    process.exit(1);
  }
  console.log(`✓ ${tag}: можно выпускать`);
}
```

`scripts/make-latest-json.mjs`:

```js
#!/usr/bin/env node
// latest.json для tauri-plugin-updater из собранных артефактов и их .sig.
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

function platformOf(name) {
  if (name.endsWith(".app.tar.gz")) return /aarch64|arm64/.test(name) ? "darwin-aarch64" : "darwin-x86_64";
  if (name.endsWith(".msi")) return "windows-x86_64";
  if (name.endsWith(".AppImage")) return "linux-x86_64";
  return null;
}

/** files: { имя артефакта: содержимое .sig } */
export function buildLatest({ version, notes, baseUrl, files, pubDate }) {
  const platforms = {};
  for (const [name, signature] of Object.entries(files)) {
    const platform = platformOf(name);
    if (!platform) continue;
    if (!signature?.trim()) throw new Error(`нет подписи для ${name}`);
    platforms[platform] = { signature: signature.trim(), url: `${baseUrl}/${encodeURIComponent(name)}` };
  }
  return { version, notes, pub_date: pubDate, platforms };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({ options: { version: { type: "string" }, "notes-file": { type: "string" }, "base-url": { type: "string" }, dir: { type: "string" } } });
  const dir = values.dir;
  const names = readdirSync(dir);
  const files = Object.fromEntries(
    names.filter((n) => platformOf(n) && names.includes(`${n}.sig`)).map((n) => [n, readFileSync(join(dir, `${n}.sig`), "utf8")]),
  );
  const latest = buildLatest({
    version: values.version,
    notes: readFileSync(values["notes-file"], "utf8"),
    baseUrl: values["base-url"],
    files,
    pubDate: new Date().toISOString(),
  });
  process.stdout.write(JSON.stringify(latest, null, 2) + "\n");
}
```

`package.json` → `scripts`: `"test:scripts": "node --test scripts/"`. В `.github/workflows/ci.yml` (job `linux`) добавить шаг `- run: pnpm test:scripts`.

Run: `node --test scripts/`
Expected: PASS.

- [ ] **Step 3: CHANGELOG** — `CHANGELOG.md`:

```markdown
# Changelog

## 0.2.0
- Клиентские сертификаты (mTLS): PEM, PKCS#12, JKS; пароли в системном хранилище секретов.
- Лицензии Free / Pro: consumer-сессии и режим prod — в Pro.
- Корпоративная политика (MDM, групповые политики, /etc): обновления, адрес зеркала, ключ организации.
- Автообновление, логи в файл, архив диагностики, «О программе».
- Установщики: macOS (.dmg, .pkg), Windows (.msi), Linux (.deb, .rpm, .AppImage).
```

Версию `0.2.0` выставить во всех трёх файлах (`package.json`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`).

- [ ] **Step 4: Workflow** — `.github/workflows/release.yml`:

```yaml
name: release

on:
  push:
    tags: ["v*"]

permissions:
  contents: read

env:
  RELEASES_REPO: <RELEASES_REPO>

jobs:
  check:
    runs-on: ubuntu-22.04
    outputs:
      version: ${{ steps.v.outputs.version }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
      - run: node scripts/check-release.mjs "${GITHUB_REF_NAME}"
      - id: v
        run: echo "version=${GITHUB_REF_NAME#v}" >> "$GITHUB_OUTPUT"
      - name: Секреты подписи updater заданы
        env:
          KEY: ${{ secrets.TAURI_SIGNING_PRIVATE_KEY }}
        run: |
          if [ -z "$KEY" ]; then echo "::error::нет секрета TAURI_SIGNING_PRIVATE_KEY"; exit 1; fi

  build:
    needs: check
    strategy:
      fail-fast: false
      matrix:
        include:
          - os: macos-14
            target: aarch64-apple-darwin
          - os: macos-13
            target: x86_64-apple-darwin
          - os: windows-latest
            target: x86_64-pc-windows-msvc
          - os: ubuntu-22.04
            target: x86_64-unknown-linux-gnu
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with:
          version: 11
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: pnpm
      - uses: dtolnay/rust-toolchain@stable
        with:
          targets: ${{ matrix.target }}
      - uses: Swatinem/rust-cache@v2
        with:
          workspaces: src-tauri -> target
      - name: Системные зависимости (Linux)
        if: runner.os == 'Linux'
        run: |
          sudo apt-get update
          sudo apt-get install -y libwebkit2gtk-4.1-dev libappindicator3-dev librsvg2-dev patchelf libsecret-1-dev cmake rpm
      - run: pnpm install --frozen-lockfile
      - name: Подпись Apple (если заданы секреты)
        if: runner.os == 'macOS'
        env:
          APPLE_CERTIFICATE: ${{ secrets.APPLE_CERTIFICATE }}
        run: |
          if [ -z "$APPLE_CERTIFICATE" ]; then echo "::warning::macOS-сборка без подписи и нотаризации"; fi
      - uses: tauri-apps/tauri-action@v0
        id: tauri
        env:
          TAURI_SIGNING_PRIVATE_KEY: ${{ secrets.TAURI_SIGNING_PRIVATE_KEY }}
          TAURI_SIGNING_PRIVATE_KEY_PASSWORD: ${{ secrets.TAURI_SIGNING_PRIVATE_KEY_PASSWORD }}
          APPLE_CERTIFICATE: ${{ secrets.APPLE_CERTIFICATE }}
          APPLE_CERTIFICATE_PASSWORD: ${{ secrets.APPLE_CERTIFICATE_PASSWORD }}
          APPLE_SIGNING_IDENTITY: ${{ secrets.APPLE_SIGNING_IDENTITY }}
          APPLE_ID: ${{ secrets.APPLE_ID }}
          APPLE_PASSWORD: ${{ secrets.APPLE_PASSWORD }}
          APPLE_TEAM_ID: ${{ secrets.APPLE_TEAM_ID }}
        with:
          args: --target ${{ matrix.target }}
      - name: .pkg (macOS)
        if: runner.os == 'macOS'
        env:
          INSTALLER_IDENTITY: ${{ secrets.APPLE_INSTALLER_IDENTITY }}
        run: |
          APP=$(ls -d src-tauri/target/${{ matrix.target }}/release/bundle/macos/*.app | head -1)
          NAME=$(basename "$APP" .app)
          ARCH=$([ "${{ matrix.target }}" = "aarch64-apple-darwin" ] && echo aarch64 || echo x64)
          scripts/make-pkg.sh "$APP" "src-tauri/target/${{ matrix.target }}/release/bundle/macos/${NAME}_${{ needs.check.outputs.version }}_${ARCH}.pkg" "$INSTALLER_IDENTITY"
      - name: Тихая установка MSI (проверка)
        if: runner.os == 'Windows'
        shell: pwsh
        run: |
          $msi = Get-ChildItem src-tauri/target/${{ matrix.target }}/release/bundle/msi/*.msi | Select-Object -First 1
          $p = Start-Process msiexec -ArgumentList "/i `"$($msi.FullName)`" /qn /l*v install.log" -Wait -PassThru
          if ($p.ExitCode -ne 0) { Get-Content install.log -Tail 50; exit 1 }
      - uses: actions/upload-artifact@v4
        with:
          name: bundle-${{ matrix.target }}
          path: |
            src-tauri/target/${{ matrix.target }}/release/bundle/dmg/*.dmg
            src-tauri/target/${{ matrix.target }}/release/bundle/macos/*.pkg
            src-tauri/target/${{ matrix.target }}/release/bundle/macos/*.app.tar.gz
            src-tauri/target/${{ matrix.target }}/release/bundle/macos/*.app.tar.gz.sig
            src-tauri/target/${{ matrix.target }}/release/bundle/msi/*.msi
            src-tauri/target/${{ matrix.target }}/release/bundle/msi/*.msi.sig
            src-tauri/target/${{ matrix.target }}/release/bundle/deb/*.deb
            src-tauri/target/${{ matrix.target }}/release/bundle/rpm/*.rpm
            src-tauri/target/${{ matrix.target }}/release/bundle/appimage/*.AppImage
            src-tauri/target/${{ matrix.target }}/release/bundle/appimage/*.AppImage.sig
          if-no-files-found: error

  publish:
    needs: [check, build]
    runs-on: ubuntu-22.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
      - uses: actions/download-artifact@v4
        with:
          path: dist-release
          merge-multiple: true
      - name: Описание и latest.json
        env:
          VERSION: ${{ needs.check.outputs.version }}
        run: |
          awk -v v="## $VERSION" '$0==v{f=1;next} /^## /{f=0} f' CHANGELOG.md > notes.md
          node scripts/make-latest-json.mjs --version "$VERSION" --notes-file notes.md \
            --base-url "https://github.com/${RELEASES_REPO}/releases/download/v${VERSION}" --dir dist-release > dist-release/latest.json
          cat dist-release/latest.json
      - name: Публикация в репозиторий релизов
        env:
          GH_TOKEN: ${{ secrets.RELEASES_TOKEN }}
          VERSION: ${{ needs.check.outputs.version }}
        run: |
          # Бета публикуется обычным релизом: updater берёт releases/latest, а предрелизы туда не попадают.
          gh release create "v${VERSION}" dist-release/* --repo "$RELEASES_REPO" --title "v${VERSION}" --notes-file notes.md
```

Секреты, которые владелец задаёт в приватном репозитории:
- обязательные: `TAURI_SIGNING_PRIVATE_KEY`, `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`, `RELEASES_TOKEN` (fine-grained PAT с правом `contents: write` только на `<RELEASES_REPO>`);
- необязательные: `APPLE_*`, `APPLE_INSTALLER_IDENTITY`.

Подпись Windows: когда появится сертификат или Azure Trusted Signing, добавить в `tauri.conf.json` `bundle.windows.signCommand` по документации Tauri. Это отдельный шаг, не блокирующий бету.

Run: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('ok')" && node --test scripts/ && node scripts/check-release.mjs v0.2.0; echo "exit $?"`
Expected: `ok`, тесты зелёные. `check-release` для `v0.2.0` пока падает: dev-ключ лицензий и заглушка updater. Так и должно быть до шагов владельца из Task 7.

- [ ] **Step 5: Commit**

```bash
git add scripts .github/workflows CHANGELOG.md package.json src-tauri/Cargo.toml src-tauri/tauri.conf.json
git commit -m "ci: релиз по тегу — проверки, сборка на 4 платформах, latest.json, публикация

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Документация для администраторов и первый релиз

**Files:**
- Create: `docs/admin.md`, `docs/admin/policy.mobileconfig`, `docs/admin/policy.reg`, `docs/admin/policy.json`
- Create: `docs/release-checklist.md`
- Create: `docs/releases-repo/README.md` (README публичного репозитория релизов)
- Modify: `README.md` (раздел «Установка»)

- [ ] **Step 1: Примеры политики**

`docs/admin/policy.json` (Linux, `/etc/<IDENTIFIER>/policy.json`):

```json
{
  "updatesEnabled": false,
  "updateUrl": "https://mirror.example.corp/<SLUG>/latest.json",
  "licenseKey": "<ключ организации>"
}
```

`docs/admin/policy.reg` (Windows):

```reg
Windows Registry Editor Version 5.00

[HKEY_LOCAL_MACHINE\SOFTWARE\Policies\<VENDOR>\<PRODUCT>]
"updatesEnabled"=dword:00000000
"updateUrl"="https://mirror.example.corp/<SLUG>/latest.json"
"licenseKey"="<ключ организации>"
```

`docs/admin/policy.mobileconfig` (macOS, профиль для MDM; `PayloadUUID` заменить на свои `uuidgen`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>PayloadContent</key>
  <array>
    <dict>
      <key>PayloadType</key><string><IDENTIFIER></string>
      <key>PayloadIdentifier</key><string><IDENTIFIER>.policy</string>
      <key>PayloadUUID</key><string>00000000-0000-0000-0000-000000000001</string>
      <key>PayloadVersion</key><integer>1</integer>
      <key>updatesEnabled</key><false/>
      <key>updateUrl</key><string>https://mirror.example.corp/<SLUG>/latest.json</string>
      <key>licenseKey</key><string>&lt;ключ организации&gt;</string>
    </dict>
  </array>
  <key>PayloadDisplayName</key><string><NAME> policy</string>
  <key>PayloadIdentifier</key><string><IDENTIFIER>.profile</string>
  <key>PayloadType</key><string>Configuration</string>
  <key>PayloadUUID</key><string>00000000-0000-0000-0000-000000000002</string>
  <key>PayloadVersion</key><integer>1</integer>
</dict>
</plist>
```

Проверить, что пример разбирается кодом приложения: тест в `src-tauri/src/policy/sources.rs`:

```rust
    #[test]
    fn documented_examples_parse() {
        let base = concat!(env!("CARGO_MANIFEST_DIR"), "/../docs/admin");
        let json = read_json_file(std::path::Path::new(&format!("{base}/policy.json")));
        let p = crate::policy::resolve("doc", json);
        assert!(p.status.errors.is_empty(), "{:?}", p.status.errors);
        assert!(!p.status.updates_enabled);
    }
```

(Плейсхолдеры `<…>` внутри строк JSON на разбор не влияют.)

- [ ] **Step 2: `docs/admin.md`**

````markdown
# <NAME>: руководство администратора

## Установка
| ОС | Файл | Тихая установка |
|---|---|---|
| macOS | `<NAME>_<версия>_aarch64.pkg` / `_x64.pkg` | `sudo installer -pkg <файл>.pkg -target /` (Jamf, Intune, Kandji) |
| Windows | `<NAME>_<версия>_x64_en-US.msi` | `msiexec /i <файл>.msi /qn` (SCCM, Intune) — установка на машину |
| Linux | `.deb`, `.rpm`, `.AppImage` | `sudo apt install ./<файл>.deb`, `sudo dnf install ./<файл>.rpm` |

Установщики — в разделе релизов `https://github.com/<RELEASES_REPO>/releases`.

## Политика
Приложение читает политику при запуске. Пользователь не может её изменить.

| ОС | Где |
|---|---|
| macOS | профиль конфигурации MDM, домен `<IDENTIFIER>` (файл попадает в `/Library/Managed Preferences/<IDENTIFIER>.plist`) — пример `admin/policy.mobileconfig` |
| Windows | `HKLM\SOFTWARE\Policies\<VENDOR>\<PRODUCT>` (GPO, Intune) — пример `admin/policy.reg` |
| Linux | `/etc/<IDENTIFIER>/policy.json` — пример `admin/policy.json` |

| Ключ | Тип | По умолчанию | Смысл |
|---|---|---|---|
| `updatesEnabled` | bool (`dword` 0/1 в реестре) | `true` | `false` — приложение не обращается к серверу обновлений |
| `updateUrl` | строка | публичный `latest.json` | зеркало с одобренными версиями (формат — `latest.json` Tauri) |
| `licenseKey` | строка | — | ключ организации: Pro у всех пользователей |

Если политика есть, но не читается, обновления выключаются, а в «О программе» и «Лицензия» показывается ошибка политики.

## Внутреннее зеркало обновлений
Скопируйте из релиза установщики, `*.sig` и `latest.json`, замените в `latest.json` адреса `url` на адреса зеркала
и укажите адрес `latest.json` в `updateUrl`. Подписи остаются прежними: приложение проверяет их встроенным ключом.

## Сеть
Приложение подключается только к:
- брокерам Kafka из профилей пользователя;
- серверу обновлений (`github.com`, `objects.githubusercontent.com` или `updateUrl`), если обновления не выключены.

Телеметрии и отчётов о сбоях нет.

## Данные и логи
| | macOS | Windows | Linux |
|---|---|---|---|
| Данные | `~/Library/Application Support/<IDENTIFIER>` | `%APPDATA%\<IDENTIFIER>` | `~/.local/share/<IDENTIFIER>` |
| Логи | `~/Library/Logs/<IDENTIFIER>` | `%LOCALAPPDATA%\<IDENTIFIER>\logs` | `~/.local/share/<IDENTIFIER>/logs` |
| Пароли | Keychain, сервис `<SLUG>` | Credential Manager | Secret Service |

«О программе» → «Собрать диагностику» сохраняет архив с логами и профилями без паролей.
````

(Пути к данным и логам проверить в «О программе» на каждой ОС при смоуке и поправить таблицу.)

- [ ] **Step 3: README релизов и установка** — `docs/releases-repo/README.md`. Это README публичного репозитория `<RELEASES_REPO>`, владелец копирует его туда при создании:

````markdown
# <NAME> — releases

Desktop client for Apache Kafka. Download the installer for your OS from [Releases](../../releases/latest).

## Unsigned builds (beta)
Beta builds may be unsigned. To open them:
- **macOS:** right-click the app → Open → Open. Or System Settings → Privacy & Security → "Open Anyway".
- **Windows:** SmartScreen → "More info" → "Run anyway".

## Pro license
During the beta, Pro keys are free — contact the author.

Apache Kafka is a trademark of the Apache Software Foundation. This project is not affiliated with the ASF.
````

В основной `README.md` добавить раздел «Установка» со ссылкой на `https://github.com/<RELEASES_REPO>/releases` и тем же текстом про неподписанные сборки по-русски, плюс ссылки на `docs/admin.md` и `docs/licensing.md`.

- [ ] **Step 4: Чек-лист релиза** — `docs/release-checklist.md`:

````markdown
# Чек-лист релиза

## Один раз (владелец)
1. Боевые ключи лицензий: `docs/licensing.md` → «Один раз». Заменить `src-tauri/license-public.key`, закоммитить.
2. Ключ updater: `pnpm tauri signer generate -w ~/.tauri/<SLUG>-updater.key`. Открытый — в `tauri.conf.json` → `plugins.updater.pubkey`.
3. Публичный репозиторий `<RELEASES_REPO>` с README из `docs/releases-repo/README.md`.
4. Секреты приватного репозитория (Settings → Secrets and variables → Actions):
   `TAURI_SIGNING_PRIVATE_KEY` (содержимое файла ключа), `TAURI_SIGNING_PRIVATE_KEY_PASSWORD`,
   `RELEASES_TOKEN` (fine-grained PAT: только `<RELEASES_REPO>`, Contents: Read and write).
   Позже, при наличии: `APPLE_CERTIFICATE`, `APPLE_CERTIFICATE_PASSWORD`, `APPLE_SIGNING_IDENTITY`,
   `APPLE_ID`, `APPLE_PASSWORD`, `APPLE_TEAM_ID`, `APPLE_INSTALLER_IDENTITY`.
5. Резервные копии закрытых ключей (лицензии и updater) вне компьютера разработки.

## Каждый релиз
1. Версия в `package.json`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`; раздел в `CHANGELOG.md`.
2. `node scripts/check-release.mjs vX.Y.Z` → «можно выпускать».
3. `git tag vX.Y.Z && git push origin vX.Y.Z` → workflow `release`.
4. Смоук на трёх ОС (ниже) по установщикам из релиза.

## Смоук установщиков
- [ ] macOS (`.dmg` и `.pkg`): установка, запуск, подключение PLAINTEXT и mTLS, «О программе» — версия.
- [ ] Windows (`.msi`, `msiexec /qn` из администраторской консоли): то же.
- [ ] Linux (`.deb` или `.AppImage`): то же.
- [ ] Обновление: установить предыдущую версию → «Проверить обновления» → «Установить и перезапустить» → новая версия, данные на месте.
- [ ] Политика `updatesEnabled = false` (по `docs/admin.md`): «Обновления управляются вашей организацией», в логе нет запросов к серверу обновлений.
- [ ] «Собрать диагностику» → архив открывается, паролей нет.
````

- [ ] **Step 5: Полный прогон**

Run:
```bash
docker compose -f dev/docker-compose.yml up -d && scripts/wait-kafka.sh
source "$HOME/.cargo/env"
pnpm test && pnpm test:scripts && pnpm build && \
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && \
cargo fmt --manifest-path src-tauri/Cargo.toml --check && \
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 KE_IT_MTLS_BOOTSTRAP=localhost:9095 \
  cargo test --manifest-path src-tauri/Cargo.toml --lib --tests
```
Expected: всё зелёное.

- [ ] **Step 6: Commit**

```bash
git add docs README.md src-tauri/src/policy/sources.rs
git commit -m "docs: руководство администратора, примеры политики, чек-лист релиза

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 7: Первый релиз (контроллер с пользователем)** — исполнитель этот шаг не выполняет. Контроллер:
  1. проводит пользователя по разделу «Один раз» чек-листа (ключи, репозиторий релизов, секреты);
  2. после слияния ветки, с согласия пользователя, ставит тег `v0.2.0` и пушит его;
  3. следит за workflow `release` (`gh run watch`). Падения разбираются отдельными задачами-исправлениями;
  4. просит пользователя пройти «Смоук установщиков».
