# Kafka Explorer — M2a «Защита, топики, группы»: план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Добавить в Kafka Explorer защиту мутаций (read-only и prod) на уровне Rust, операции с топиками (создание, изменение конфигов, добавление партиций, purge, удаление), колонку `cleanup.policy`, просмотр consumer groups с лагом, сброс оффсетов с обязательным предпросмотром и удаление групп.

**Architecture:** Каждая мутирующая команда первым делом вызывает `guard::ensure_mutation_allowed`. Операции с топиками используют safe-API rust-rdkafka (`kafka/admin.rs`). Чего в rust-rdkafka 0.37 нет (IncrementalAlterConfigs, ListConsumerGroups, DescribeConsumerGroups, List/AlterConsumerGroupOffsets), вызывается через FFI librdkafka в отдельном модуле `kafka/admin_ext.rs` — весь новый `unsafe` только там. Логика групп (лаг, план сброса) — чистые функции в `kafka/groups.rs`, сетевые операции — в `kafka/group_ops.rs`. Фронтенд получает общий `ConfirmDialog`, вкладки групп и действия над топиками.

**Tech Stack:** Rust stable (edition 2024), Tauri 2, rdkafka 0.37 (librdkafka 2.12, C API через `rdkafka::bindings`), serde; React 19, TypeScript, TanStack Query 5, TanStack Virtual 3, Zustand 5, Vitest + Testing Library; Docker (confluentinc/cp-kafka:7.6.1) для интеграционных тестов.

**Спецификации:** `docs/superpowers/specs/2026-10-14-kafka-explorer-design.md` (§4.3, §4.4, §5, §8), `docs/superpowers/specs/2026-10-15-kafka-explorer-m2-design.md` (решения M2).

## Global Constraints

- Платформы: macOS (arm64, x64), Windows x64, Linux x64; Kafka 2.8+ (ZooKeeper и KRaft), Redpanda, AWS MSK, Confluent Cloud.
- Вся работа с Kafka — в Rust; фронтенд не знает о librdkafka.
- Все команды возвращают `Result<T, AppError>`; `AppError { code, message, details?, retryable }` сериализуется в camelCase, `code` — в SCREAMING_SNAKE_CASE.
- **Каждая мутирующая команда первой строкой вызывает guard** (`AppState::guarded`): `readOnly` → `READ_ONLY`; окружение `prod` без `confirm`, равного имени подключения, → `CONFIRMATION_REQUIRED`. Удаление топиков, удаление группы и purge дополнительно требуют ввод имени объекта (`ensure_name_confirmed`); при удалении нескольких топиков вводится их число.
- Мутирующие команды M2a: `create_topic`, `alter_topic_configs`, `add_partitions`, `purge_topic`, `delete_topics`, `reset_offsets`, `delete_group`. `preview_offset_reset` — не мутирующая.
- Новый `unsafe`-код — только в `src-tauri/src/kafka/admin_ext.rs`; наружу из него выходят только безопасные функции с Rust-типами.
- Ошибки получения оффсетов и лага **никогда не превращаются в 0**: значение — `Option`, в UI — «—» с причиной в `title`.
- Ни одна операция не блокирует UI: блокирующие вызовы librdkafka (в том числе все функции `admin_ext`) выполняются в `spawn_blocking` (`commands::blocking`); у каждой сетевой операции есть таймаут (`request_timeout_ms` профиля).
- Сброс оффсетов и удаление группы — только для группы в состоянии `Empty` (сброс — ещё и для несуществующей группы, состояние `Dead`); иначе `GROUP_NOT_EMPTY` со списком участников в `details`. Сброс применяет ровно тот план, что показан в предпросмотре.
- JSON-файлы приложения пишутся только через `storage::fs::write_json` (временный файл → fsync → rename).
- Блокировки берутся через `sync::MutexExt::lock_safe` / `RwLockExt::{read_safe, write_safe}` — без паники на отравленном мьютексе.
- Тексты UI — по-русски прямо в коде (локализация — M4). Подтверждения — внутри страницы (`ConfirmDialog`), нативный `window.confirm` в Tauri 2 не показывается.
- Клавиатурная доступность строк таблиц — через общий `activateProps` (`src/lib/activate.ts`); интерактивные элементы внутри строки гасят всплытие `onClick` и `onKeyDown`.
- Для read-only подключений кнопки изменений скрыты (`useConnection(id)?.readOnly`), но защита всё равно в Rust.
- Нет телеметрии и удалённого контента.

---

## Структура файлов M2a

```
src-tauri/
├── src/
│   ├── sync.rs                    # NEW: lock_safe / read_safe / write_safe
│   ├── guard.rs                   # NEW: ensure_mutation_allowed, ensure_name_confirmed
│   ├── error.rs                   # + READ_ONLY, CONFIRMATION_REQUIRED, GROUP_NOT_EMPTY
│   ├── state.rs                   # + AppState::guarded
│   ├── lib.rs                     # регистрация модулей и команд
│   ├── secrets.rs                 # MemoryStore на lock_safe
│   ├── storage/{mod.rs, fs.rs (NEW), connections.rs}
│   ├── browse/registry.rs         # lock_safe
│   ├── kafka/
│   │   ├── pool.rs                # KeyedPool: создание вне мьютекса, поколения
│   │   ├── errors.rs              # + item_error, admin_error (перенесён из configs.rs)
│   │   ├── configs.rs             # + describe_topic_policies
│   │   ├── admin_ext.rs           # NEW: FFI (unsafe) — IncrementalAlterConfigs, группы
│   │   ├── admin.rs               # NEW: валидация имени, create/alter/partitions/purge/delete
│   │   ├── groups.rs              # NEW: DTO, лаг, plan_reset (чистые функции)
│   │   └── group_ops.rs           # NEW: list/describe/offsets/lag/preview/reset/delete
│   └── commands/{mod.rs, connections.rs, topics.rs, topic_ops.rs (NEW), groups.rs (NEW)}
└── tests/
    ├── common/mod.rs              # TempTopic, TempGroup, wait_for_topic, commit_group_offsets
    ├── it_admin_ext.rs (NEW), it_topic_ops.rs (NEW), it_groups.rs (NEW)
    └── it_cluster.rs, it_topics.rs, it_browse.rs  # переход на TempTopic

src/
├── api/types.ts, api/client.ts    # новые DTO и команды
├── lib/useConnection.ts (NEW)
├── state/workspace.ts             # вкладки groups / group, open() обновляет spec
├── components/
│   ├── Layout.tsx, Sidebar.tsx, TabContent.tsx
│   ├── common/ConfirmDialog.tsx (NEW), common/ConfigEditor.tsx (NEW), common/configDiff.ts (NEW)
│   ├── topics/
│   │   ├── TopicList.tsx          # «Создать топик», выбор и удаление, колонка cleanup.policy
│   │   ├── topicName.ts (NEW), CreateTopicForm.tsx (NEW), DeleteTopicsPanel.tsx (NEW)
│   │   ├── TopicActions.tsx (NEW) # добавить партиции, purge, удалить
│   │   ├── TopicDetails.tsx       # действия, редактор конфигов, подвкладка «Consumer groups»
│   │   └── TopicGroups.tsx (NEW)
│   ├── groups/
│   │   ├── groupFilter.ts (NEW), GroupList.tsx (NEW)
│   │   ├── groupLag.ts (NEW), GroupDetails.tsx (NEW)
│   │   └── reset.ts (NEW), ResetOffsetsPanel.tsx (NEW)
│   └── browse/MessageDetails.tsx  # «Сбросить группу сюда»
```

**Как запускать тесты** (из корня репозитория):
- Unit-тесты Rust: `cargo test --manifest-path src-tauri/Cargo.toml --lib <фильтр>`
- Интеграционные тесты Rust (нужен Docker): `docker compose -f dev/docker-compose.yml up -d`, затем `KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --test <имя>`. Без переменных окружения интеграционные тесты пропускаются (печатают `skip` и проходят).
- Фронтенд: `pnpm vitest run <путь>`
- Линт: `cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
- Внутри контейнера Kafka CLI вызывается без `.sh`: `docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 …`

**Ветка:** `feature/m2a` от `main`.

---

### Task 1: Атомарная запись файлов и блокировки без паники

Бэклог M1: запись `connections.json` без fsync, `unwrap()` на отравленных мьютексах в хранилище, реестре сессий и `MemoryStore`.

**Files:**
- Create: `src-tauri/src/sync.rs`
- Create: `src-tauri/src/storage/fs.rs`
- Modify: `src-tauri/src/lib.rs` (добавить `pub mod sync;`)
- Modify: `src-tauri/src/storage/mod.rs` (добавить `pub mod fs;`)
- Modify: `src-tauri/src/storage/connections.rs` (open/persist/блокировки)
- Modify: `src-tauri/src/browse/registry.rs` (все `.lock().unwrap()` → `.lock_safe()`)
- Modify: `src-tauri/src/secrets.rs` (`MemoryStore`: `.lock().unwrap()` → `.lock_safe()`)

**Interfaces:**
- Produces:
  - `trait sync::MutexExt<T> { fn lock_safe(&self) -> MutexGuard<'_, T> }` для `std::sync::Mutex<T>`
  - `trait sync::RwLockExt<T> { fn read_safe(&self) -> RwLockReadGuard<'_, T>; fn write_safe(&self) -> RwLockWriteGuard<'_, T> }` для `std::sync::RwLock<T>`
  - `fn storage::fs::atomic_write(path: &Path, bytes: &[u8]) -> AppResult<()>`
  - `fn storage::fs::read_json<T: DeserializeOwned>(path: &Path) -> AppResult<Option<T>>` (`None` — файла нет)
  - `fn storage::fs::write_json<T: Serialize>(path: &Path, value: &T) -> AppResult<()>` (pretty JSON через `atomic_write`)

- [ ] **Step 1: Тесты блокировок** — создать `src-tauri/src/sync.rs` только с тестом:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, RwLock};

    #[test]
    fn poisoned_locks_are_still_usable() {
        let m = Arc::new(Mutex::new(1));
        let rw = Arc::new(RwLock::new(1));
        let (m2, rw2) = (m.clone(), rw.clone());
        let _ = std::thread::spawn(move || {
            let _g = m2.lock().unwrap();
            let _w = rw2.write().unwrap();
            panic!("отравляем блокировки");
        })
        .join();
        assert!(m.is_poisoned() && rw.is_poisoned());
        *m.lock_safe() += 1;
        *rw.write_safe() += 1;
        assert_eq!(*m.lock_safe(), 2);
        assert_eq!(*rw.read_safe(), 2);
    }
}
```

И добавить `pub mod sync;` в `src-tauri/src/lib.rs` (после `pub mod storage;`).

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib sync`
Expected: FAIL — `no method named lock_safe`.

- [ ] **Step 3: Реализация** — над тестами в `sync.rs`:

```rust
//! Блокировки без паники на отравлении. Если поток упал с паникой под блокировкой, остальные
//! продолжают работать с последним записанным значением: во всём проекте значение под
//! блокировкой заменяется целиком, поэтому оно остаётся согласованным.

use std::sync::{Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub trait MutexExt<T> {
    fn lock_safe(&self) -> MutexGuard<'_, T>;
}

impl<T> MutexExt<T> for Mutex<T> {
    fn lock_safe(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub trait RwLockExt<T> {
    fn read_safe(&self) -> RwLockReadGuard<'_, T>;
    fn write_safe(&self) -> RwLockWriteGuard<'_, T>;
}

impl<T> RwLockExt<T> for RwLock<T> {
    fn read_safe(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write_safe(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap_or_else(PoisonError::into_inner)
    }
}
```

- [ ] **Step 4: Тесты атомарной записи** — создать `src-tauri/src/storage/fs.rs` с тестами и добавить `pub mod fs;` в `src-tauri/src/storage/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn writes_replaces_and_leaves_no_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.json");
        atomic_write(&p, b"1").unwrap();
        atomic_write(&p, b"2").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"2");
        assert!(!tmp_path(&p).exists());
    }

    #[test]
    fn creates_missing_directories() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("history").join("c1.json");
        atomic_write(&p, b"[]").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"[]");
    }

    #[test]
    fn failure_keeps_original_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.json");
        atomic_write(&p, b"old").unwrap();
        // Каталог на месте временного файла: создать его не получится.
        std::fs::create_dir(tmp_path(&p)).unwrap();
        let err = atomic_write(&p, b"new").unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(std::fs::read(&p).unwrap(), b"old");
    }

    #[test]
    fn json_roundtrip_and_missing_file_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("v.json");
        assert_eq!(read_json::<Vec<i32>>(&p).unwrap(), None);
        write_json(&p, &vec![1, 2]).unwrap();
        assert_eq!(read_json::<Vec<i32>>(&p).unwrap(), Some(vec![1, 2]));
    }
}
```

- [ ] **Step 5: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib storage::fs`
Expected: FAIL — `cannot find function atomic_write`.

- [ ] **Step 6: Реализация** — над тестами в `storage/fs.rs`:

```rust
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{AppError, AppResult};

fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".tmp");
    path.with_file_name(name)
}

/// Атомарная запись: временный файл → fsync → rename → fsync каталога.
/// При сбое на любом шаге исходный файл остаётся нетронутым.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> AppResult<()> {
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let tmp = tmp_path(path);
    let result = (|| -> std::io::Result<()> {
        fs::create_dir_all(dir)?;
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, path)?;
        sync_dir(dir);
        Ok(())
    })();
    result.map_err(|e| {
        let _ = fs::remove_file(&tmp);
        AppError::internal(format!("Не удалось записать {}: {e}", path.display()))
    })
}

/// fsync каталога фиксирует rename на диске (POSIX). На Windows каталог так не открыть — пропускаем.
fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    #[cfg(not(unix))]
    let _ = dir;
}

pub fn read_json<T: DeserializeOwned>(path: &Path) -> AppResult<Option<T>> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> AppResult<()> {
    atomic_write(path, &serde_json::to_vec_pretty(value)?)
}
```

- [ ] **Step 7: Перевести `ConnectionStore` на `fs` и `RwLockExt`** — в `src-tauri/src/storage/connections.rs`:

Импорты: удалить `use std::fs;` (оставшийся вызов пишется полным путём `std::fs::create_dir_all`), добавить:
```rust
use crate::storage::fs::{read_json, write_json};
use crate::sync::RwLockExt;
```

Заменить `open` и `persist`:
```rust
    pub fn open(dir: &Path) -> AppResult<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join("connections.json");
        let connections = match read_json::<ConnectionsFile>(&path)? {
            Some(file) if file.schema_version > CURRENT_SCHEMA_VERSION => {
                return Err(AppError::internal(format!(
                    "connections.json создан более новой версией приложения (schemaVersion {})",
                    file.schema_version
                )));
            }
            Some(file) => file.connections,
            None => Vec::new(),
        };
        Ok(Self {
            path,
            cache: RwLock::new(connections),
        })
    }
```
```rust
    fn persist(&self, list: &[ConnectionProfile]) -> AppResult<()> {
        write_json(
            &self.path,
            &ConnectionsFile {
                schema_version: CURRENT_SCHEMA_VERSION,
                connections: list.to_vec(),
            },
        )
    }
```
В `list`, `get` заменить `.read().unwrap()` на `.read_safe()`, в `upsert`, `delete` — `.write().unwrap()` на `.write_safe()`.

- [ ] **Step 8: Реестр сессий и `MemoryStore`**

В `src-tauri/src/browse/registry.rs` добавить `use crate::sync::MutexExt;` и заменить все шесть `self.sessions.lock().unwrap()` на `self.sessions.lock_safe()`.
В `src-tauri/src/secrets.rs` добавить `use crate::sync::MutexExt;` и в `MemoryStore` заменить три `self.values.lock().unwrap()` на `self.values.lock_safe()`.

- [ ] **Step 9: Все unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib`
Expected: PASS (новые `sync::tests`, `storage::fs::tests`; старые `storage::connections::tests` без изменений).
Run: `grep -rn "lock().unwrap()\|read().unwrap()\|write().unwrap()" src-tauri/src`
Expected: пусто, кроме `src-tauri/src/kafka/pool.rs` (переделывается в Task 2) и тестового кода в `sync.rs`.

- [ ] **Step 10: Commit**

```bash
git add src-tauri/src
git commit -m "refactor(storage): атомарная запись с fsync и блокировки без паники"
```

---

### Task 2: Пул клиентов без блокировки на время создания

Бэклог M1: мьютекс пула удерживается во время чтения keychain и создания клиента; освобождение клиента под мьютексом. Обобщённый `KeyedPool<T>` в M2b используется и для producer'ов.

**Files:**
- Modify: `src-tauri/src/kafka/pool.rs`

**Interfaces:**
- Consumes: `sync::MutexExt`
- Produces:
  - `struct KeyedPool<T>` (`Default`): `fn get_or_create(&self, id: &str, create: impl FnOnce() -> AppResult<T>) -> AppResult<Arc<T>>`, `fn evict(&self, id: &str)`
  - `ClientPool` — прежний API: `fn get(&self, p: &ConnectionProfile, secrets: &dyn SecretStore) -> AppResult<Arc<KafkaHandles>>`, `fn evict(&self, id: &str)`

- [ ] **Step 1: Тесты** — в `mod tests` файла `pool.rs` добавить (существующий `pool_reuses_handles_until_evicted` не трогать):

```rust
    #[test]
    fn keyed_pool_reuses_until_evicted() {
        let pool = KeyedPool::<u32>::default();
        let a = pool.get_or_create("c", || Ok(1)).unwrap();
        let b = pool.get_or_create("c", || Ok(2)).unwrap();
        assert!(Arc::ptr_eq(&a, &b));
        pool.evict("c");
        assert_eq!(*pool.get_or_create("c", || Ok(3)).unwrap(), 3);
    }

    #[test]
    fn creation_errors_are_not_cached() {
        let pool = KeyedPool::<u32>::default();
        let e = pool
            .get_or_create("c", || Err(AppError::internal("нет")))
            .unwrap_err();
        assert_eq!(e.message, "нет");
        assert_eq!(*pool.get_or_create("c", || Ok(5)).unwrap(), 5);
    }

    #[test]
    fn create_runs_without_holding_the_lock() {
        let pool = Arc::new(KeyedPool::<u32>::default());
        let p2 = pool.clone();
        // Если бы мьютекс удерживался во время create, вложенный вызов зависал бы.
        let a = pool
            .get_or_create("a", || {
                p2.get_or_create("b", || Ok(7)).unwrap();
                Ok(1)
            })
            .unwrap();
        assert_eq!(*a, 1);
        assert_eq!(*pool.get_or_create("b", || Ok(0)).unwrap(), 7);
    }

    #[test]
    fn client_created_before_eviction_is_not_cached() {
        let pool = Arc::new(KeyedPool::<u32>::default());
        let p2 = pool.clone();
        // Вытеснение посреди создания (например, сохранили новый пароль): клиент отдаётся
        // вызывающему, но в пул не попадает — следующий вызов создаёт новый.
        let a = pool
            .get_or_create("c", || {
                p2.evict("c");
                Ok(1)
            })
            .unwrap();
        assert_eq!(*a, 1);
        assert_eq!(*pool.get_or_create("c", || Ok(2)).unwrap(), 2);
    }
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::pool`
Expected: FAIL — `cannot find type KeyedPool`.

- [ ] **Step 3: Реализация** — заменить `ClientPool` и его `impl` в `pool.rs` (структура `KafkaHandles` без изменений):

```rust
/// Кэш «id → клиент». Клиент создаётся и освобождается вне мьютекса: чтение keychain и
/// создание/уничтожение клиента librdkafka занимают до секунд. Поколение id растёт при каждом
/// вытеснении: клиент, начатый до вытеснения (возможно, со старыми учётными данными),
/// возвращается вызывающему, но в пул не попадает.
pub struct KeyedPool<T> {
    inner: Mutex<PoolState<T>>,
}

struct PoolState<T> {
    items: HashMap<String, Arc<T>>,
    generations: HashMap<String, u64>,
}

impl<T> Default for KeyedPool<T> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(PoolState {
                items: HashMap::new(),
                generations: HashMap::new(),
            }),
        }
    }
}

impl<T> KeyedPool<T> {
    pub fn get_or_create(
        &self,
        id: &str,
        create: impl FnOnce() -> AppResult<T>,
    ) -> AppResult<Arc<T>> {
        let generation = {
            let st = self.inner.lock_safe();
            if let Some(item) = st.items.get(id) {
                return Ok(item.clone());
            }
            st.generations.get(id).copied().unwrap_or(0)
        };
        let created = Arc::new(create()?);
        let mut st = self.inner.lock_safe();
        if st.generations.get(id).copied().unwrap_or(0) != generation {
            return Ok(created);
        }
        if let Some(existing) = st.items.get(id).cloned() {
            // Параллельный вызов успел раньше: наш экземпляр освобождаем уже вне мьютекса.
            drop(st);
            drop(created);
            return Ok(existing);
        }
        st.items.insert(id.to_string(), created.clone());
        Ok(created)
    }

    pub fn evict(&self, id: &str) {
        let removed = {
            let mut st = self.inner.lock_safe();
            *st.generations.entry(id.to_string()).or_insert(0) += 1;
            st.items.remove(id)
        };
        // Последняя ссылка (если она здесь) освобождается вне мьютекса.
        drop(removed);
    }
}

#[derive(Default)]
pub struct ClientPool {
    handles: KeyedPool<KafkaHandles>,
}

impl ClientPool {
    pub fn get(
        &self,
        p: &ConnectionProfile,
        secrets: &dyn SecretStore,
    ) -> AppResult<Arc<KafkaHandles>> {
        self.handles
            .get_or_create(&p.id, || KafkaHandles::create(p, secrets))
    }

    pub fn evict(&self, id: &str) {
        self.handles.evict(id);
    }
}
```

Импорты `pool.rs`: убрать неиспользуемое, добавить `use crate::sync::MutexExt;`. `AppError` в тестах — `use crate::error::AppError;` внутри `mod tests` (если не импортирован выше).

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::pool state`
Expected: PASS (5 тестов `kafka::pool`, 2 теста `state`).

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/kafka/pool.rs
git commit -m "refactor(pool): создание и освобождение клиентов вне мьютекса пула"
```

---

### Task 3: Секрет SASL при сохранении подключения

Бэклог M1: при переключении с SASL на протокол без SASL пароль остаётся в keychain; если запись в keychain упала, профиль уже сохранён (частичная запись).

**Files:**
- Modify: `src-tauri/src/storage/connections.rs` (метод `SecurityConfig::uses_sasl`)
- Modify: `src-tauri/src/commands/connections.rs`

**Interfaces:**
- Produces:
  - `fn SecurityConfig::uses_sasl(&self) -> bool`
  - `fn commands::connections::save_connection_blocking(st: &AppState, input: ConnectionInput) -> AppResult<ConnectionProfile>` — логика команды `save_connection`, синхронная (вызывается в `blocking`)

- [ ] **Step 1: Тесты** — в конец `src-tauri/src/commands/connections.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{AppError, ErrorCode};
    use crate::storage::connections::{SaslConfig, SaslMechanism, SecurityConfig, sample_profile};

    /// Keychain, в который ничего нельзя записать.
    struct BrokenKeychain;

    impl SecretStore for BrokenKeychain {
        fn get(&self, _: &str) -> AppResult<Option<String>> {
            Ok(None)
        }
        fn set(&self, _: &str, _: &str) -> AppResult<()> {
            Err(AppError::internal("keychain недоступен"))
        }
        fn delete(&self, _: &str) -> AppResult<()> {
            Err(AppError::internal("keychain недоступен"))
        }
    }

    fn sasl(mut p: ConnectionProfile) -> ConnectionProfile {
        p.security = SecurityConfig::SaslPlaintext {
            sasl: SaslConfig {
                mechanism: SaslMechanism::Plain,
                username: "u".into(),
            },
        };
        p
    }

    fn input(profile: ConnectionProfile, pw: Option<&str>) -> ConnectionInput {
        ConnectionInput {
            profile,
            sasl_password: pw.map(String::from),
        }
    }

    fn state(secrets: Arc<dyn SecretStore>) -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), secrets).unwrap();
        (dir, st)
    }

    #[test]
    fn switching_off_sasl_deletes_secret() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw"))).unwrap();
        let key = secret_key("a", SASL_PASSWORD);
        assert_eq!(st.secrets.get(&key).unwrap().as_deref(), Some("pw"));
        // Форма может прислать старый пароль и для PLAINTEXT — он не должен сохраниться.
        save_connection_blocking(&st, input(sample_profile("a"), Some("pw"))).unwrap();
        assert_eq!(st.secrets.get(&key).unwrap(), None);
    }

    #[test]
    fn empty_password_keeps_existing_secret() {
        let (_d, st) = state(Arc::new(MemoryStore::default()));
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw"))).unwrap();
        save_connection_blocking(&st, input(sasl(sample_profile("a")), Some(""))).unwrap();
        let key = secret_key("a", SASL_PASSWORD);
        assert_eq!(st.secrets.get(&key).unwrap().as_deref(), Some("pw"));
    }

    #[test]
    fn keychain_failure_does_not_leave_new_profile() {
        let (_d, st) = state(Arc::new(BrokenKeychain));
        let err =
            save_connection_blocking(&st, input(sasl(sample_profile("a")), Some("pw"))).unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert_eq!(st.connections.get("a").unwrap_err().code, ErrorCode::NotFound);
    }

    #[test]
    fn keychain_failure_restores_previous_profile() {
        let (_d, st) = state(Arc::new(BrokenKeychain));
        st.connections.upsert(sample_profile("a")).unwrap();
        let mut changed = sasl(sample_profile("a"));
        changed.name = "новое имя".into();
        assert!(save_connection_blocking(&st, input(changed, Some("pw"))).is_err());
        assert_eq!(st.connections.get("a").unwrap(), sample_profile("a"));
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib commands::connections`
Expected: FAIL — `cannot find function save_connection_blocking`.

- [ ] **Step 3: Реализация**

В `src-tauri/src/storage/connections.rs` после `enum SecurityConfig` добавить:
```rust
impl SecurityConfig {
    pub fn uses_sasl(&self) -> bool {
        matches!(
            self,
            SecurityConfig::SaslPlaintext { .. } | SecurityConfig::SaslSsl { .. }
        )
    }
}
```

В `src-tauri/src/commands/connections.rs` заменить тело команды `save_connection` и добавить функцию:
```rust
#[tauri::command]
pub async fn save_connection(
    state: State<'_, AppState>,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let st = state.inner().clone();
    blocking(move || save_connection_blocking(&st, input)).await
}

/// Сохраняет профиль и секрет согласованно: если keychain не принял изменение,
/// профиль возвращается к прежнему состоянию (или удаляется, если он новый).
pub fn save_connection_blocking(
    st: &AppState,
    input: ConnectionInput,
) -> AppResult<ConnectionProfile> {
    let mut profile = input.profile;
    if profile.id.is_empty() {
        profile.id = uuid::Uuid::new_v4().to_string();
    }
    let previous = st.connections.get(&profile.id).ok();
    // upsert валидирует профиль — секрет трогаем только после успешного сохранения.
    let saved = st.connections.upsert(profile)?;
    let key = secret_key(&saved.id, SASL_PASSWORD);
    let secret_result = if !saved.security.uses_sasl() {
        st.secrets.delete(&key)
    } else if let Some(pw) = input.sasl_password.filter(|p| !p.is_empty()) {
        st.secrets.set(&key, &pw)
    } else {
        Ok(())
    };
    if let Err(e) = secret_result {
        let rollback = match previous {
            Some(p) => st.connections.upsert(p).map(|_| ()),
            None => st.connections.delete(&saved.id),
        };
        if let Err(r) = rollback {
            tracing::error!("не удалось откатить профиль {}: {r}", saved.id);
        }
        return Err(e);
    }
    st.pool.evict(&saved.id);
    Ok(saved)
}
```

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib commands::connections`
Expected: PASS, 4 теста.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src
git commit -m "fix(connections): удаление секрета SASL при смене протокола и откат при ошибке keychain"
```

---

### Task 4: Инфраструктура интеграционных тестов

Бэклог M1: тесты не удаляют топики и ждут фиксированные 500 мс после создания.

**Files:**
- Modify: `src-tauri/tests/common/mod.rs`
- Modify: `src-tauri/tests/it_cluster.rs`, `src-tauri/tests/it_topics.rs`, `src-tauri/tests/it_browse.rs`

**Interfaces:**
- Produces (в `tests/common`):
  - `fn admin(bs: &str) -> AdminClient<DefaultClientContext>`
  - `fn block_on<F: Future + Send>(f: F) -> F::Output where F::Output: Send` — выполняет future в отдельном потоке со своим runtime (можно звать из `Drop` внутри tokio)
  - `fn wait_for_topic_sync(bs: &str, name: &str, partitions: usize)` — ждёт до 15 с, пока у топика в метаданных нужное число партиций с лидерами
  - `async fn create_topic(bs: &str, name: &str, partitions: i32)` — создаёт и ждёт метаданных (без паузы 500 мс)
  - `struct TempTopic { pub name: String }`: `async fn create(bs: &str, prefix: &str, partitions: i32) -> Self`; `fn adopt(bs: &str, name: String) -> Self` (берёт под уборку топик, который создаёт сам тест); `Deref<Target = str>`; удаляет топик в `Drop`
  - `struct TempGroup { pub id: String }`: `fn new(bs: &str, prefix: &str) -> Self`; `Deref<Target = str>`; удаляет группу в `Drop`
  - `fn commit_group_offsets(bs: &str, group: &str, topic: &str, offsets: &[(i32, i64)])` — коммит от имени группы без вступления в неё (группа остаётся `Empty`)
  - `fn delete_topics_sync(bs: &str, names: &[String])`, `fn delete_group_sync(bs: &str, group: &str)` — ошибки уборки игнорируются

- [ ] **Step 1: Хелперы** — в `src-tauri/tests/common/mod.rs`.

Импорты в начале файла заменить на:
```rust
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Deref;
use std::time::{Duration, Instant};

use kafka_explorer_lib::storage::connections::{ConnectionProfile, Environment, SecurityConfig};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};
```

Функцию `create_topic` заменить, а после неё добавить новые хелперы:
```rust
pub fn admin(bs: &str) -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", bs)
        .create()
        .unwrap()
}

/// Выполняет future в отдельном потоке со своим runtime: так её можно дождаться и из `Drop`,
/// который вызывается внутри tokio-runtime теста.
pub fn block_on<F>(f: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    std::thread::scope(|s| {
        s.spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f)
        })
        .join()
        .unwrap()
    })
}

/// Ждёт, пока топик появится в метаданных с нужным числом партиций и у всех есть лидер.
pub fn wait_for_topic_sync(bs: &str, name: &str, partitions: usize) {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .create()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(md) = c.fetch_metadata(Some(name), Duration::from_secs(2)) {
            let ready = md.topics().iter().any(|t| {
                t.name() == name
                    && t.error().is_none()
                    && t.partitions().len() == partitions
                    && t.partitions().iter().all(|p| p.leader() >= 0)
            });
            if ready {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "топик {name} не появился в метаданных за 15 с"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

pub async fn create_topic(bs: &str, name: &str, partitions: i32) {
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin(bs)
        .create_topics(
            &[NewTopic::new(name, partitions, TopicReplication::Fixed(1))],
            &opts,
        )
        .await
        .unwrap();
    for r in results {
        r.unwrap();
    }
    let (bs, name) = (bs.to_string(), name.to_string());
    tokio::task::spawn_blocking(move || wait_for_topic_sync(&bs, &name, partitions as usize))
        .await
        .unwrap();
}

pub fn delete_topics_sync(bs: &str, names: &[String]) {
    let admin = admin(bs);
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    // Ошибки уборки не должны ронять тест (топик мог удалить сам тест).
    let _ = block_on(admin.delete_topics(&refs, &opts));
}

pub fn delete_group_sync(bs: &str, group: &str) {
    let admin = admin(bs);
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let _ = block_on(admin.delete_groups(&[group], &opts));
}

/// Топик теста; удаляется в Drop, даже если тест упал.
pub struct TempTopic {
    bs: String,
    pub name: String,
}

impl TempTopic {
    pub async fn create(bs: &str, prefix: &str, partitions: i32) -> Self {
        let name = unique_topic(prefix);
        create_topic(bs, &name, partitions).await;
        Self {
            bs: bs.to_string(),
            name,
        }
    }

    /// Уборка топика, который создаёт сам тест (например, проверяемой командой create_topic).
    pub fn adopt(bs: &str, name: String) -> Self {
        Self {
            bs: bs.to_string(),
            name,
        }
    }
}

impl Deref for TempTopic {
    type Target = str;
    fn deref(&self) -> &str {
        &self.name
    }
}

impl Drop for TempTopic {
    fn drop(&mut self) {
        delete_topics_sync(&self.bs, &[self.name.clone()]);
    }
}

/// Consumer group теста; удаляется в Drop.
pub struct TempGroup {
    bs: String,
    pub id: String,
}

impl TempGroup {
    pub fn new(bs: &str, prefix: &str) -> Self {
        Self {
            bs: bs.to_string(),
            id: unique_topic(prefix),
        }
    }
}

impl Deref for TempGroup {
    type Target = str;
    fn deref(&self) -> &str {
        &self.id
    }
}

impl Drop for TempGroup {
    fn drop(&mut self) {
        delete_group_sync(&self.bs, &self.id);
    }
}

/// Коммит от имени группы без вступления в неё: группа остаётся в состоянии Empty.
pub fn commit_group_offsets(bs: &str, group: &str, topic: &str, offsets: &[(i32, i64)]) {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for (p, o) in offsets {
        tpl.add_partition_offset(topic, *p, Offset::Offset(*o))
            .unwrap();
    }
    c.commit(&tpl, CommitMode::Sync).unwrap();
}
```

- [ ] **Step 2: Перевести существующие тесты на `TempTopic`**

`src-tauri/tests/it_cluster.rs`: строки
```rust
    let topic = common::unique_topic("it-cluster");
    common::create_topic(&bs, &topic, 2).await;
```
заменить на
```rust
    let _topic = common::TempTopic::create(&bs, "it-cluster", 2).await;
```

`src-tauri/tests/it_topics.rs`:
- `let topic = common::unique_topic("it-topics");` + `common::create_topic(&bs, &topic, 3).await;` → `let topic = common::TempTopic::create(&bs, "it-topics", 3).await;`
- `let (h1, t1) = (h.clone(), topic.clone());` → `let (h1, t1) = (h.clone(), topic.to_string());`
- `.find(|t| t.name == topic)` → `.find(|t| t.name == *topic)`

`src-tauri/tests/it_browse.rs`:
- `async fn seeded_topic(bs: &str) -> String {` → `async fn seeded_topic(bs: &str) -> common::TempTopic {`; первые две строки тела → `let topic = common::TempTopic::create(bs, "it-browse", 3).await;`
- в `live_tail_receives_new_messages_until_cancelled`: `let topic = common::unique_topic("it-live");` + `common::create_topic(&bs, &topic, 1).await;` → `let topic = common::TempTopic::create(&bs, "it-live", 1).await;`

Остальные места передают `&topic` в параметры `&str` — работает через `Deref`.

- [ ] **Step 3: Проверить на живой Kafka**

Run:
```bash
docker compose -f dev/docker-compose.yml up -d
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --tests
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -c '^it-'
```
Expected: все тесты PASS; последняя команда печатает `0` (тестовые топики удалены).

- [ ] **Step 4: Commit**

```bash
git add src-tauri/tests
git commit -m "test(it): уборка топиков и групп, ожидание метаданных вместо паузы"
```

---

### Task 5: Новые коды ошибок и guard мутаций

**Files:**
- Create: `src-tauri/src/guard.rs`
- Modify: `src-tauri/src/error.rs` (коды `ReadOnly`, `ConfirmationRequired`, `GroupNotEmpty`)
- Modify: `src-tauri/src/state.rs` (`AppState::guarded`)
- Modify: `src-tauri/src/lib.rs` (`pub mod guard;`)
- Modify: `src/api/types.ts` (тип `ErrorCode`)

**Interfaces:**
- Produces:
  - `ErrorCode::{ReadOnly, ConfirmationRequired, GroupNotEmpty}` → `"READ_ONLY"`, `"CONFIRMATION_REQUIRED"`, `"GROUP_NOT_EMPTY"`, все `retryable: false`
  - `fn guard::ensure_mutation_allowed(p: &ConnectionProfile, confirm: Option<&str>) -> AppResult<()>`
  - `fn guard::ensure_name_confirmed(expected: &str, typed: Option<&str>) -> AppResult<()>`
  - `fn AppState::guarded(&self, connection_id: &str, confirm: Option<&str>) -> AppResult<ConnectionProfile>` — профиль + `ensure_mutation_allowed`; дешёвый (без keychain и сети), можно звать на async-потоке

- [ ] **Step 1: Тесты guard** — создать `src-tauri/src/guard.rs` с тестами и добавить `pub mod guard;` в `lib.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::storage::connections::{Environment, sample_profile};

    #[test]
    fn read_only_blocks_even_with_confirmation() {
        let mut p = sample_profile("a");
        p.read_only = true;
        let e = ensure_mutation_allowed(&p, Some(&p.name.clone())).unwrap_err();
        assert_eq!(e.code, ErrorCode::ReadOnly);
    }

    #[test]
    fn prod_requires_connection_name() {
        let mut p = sample_profile("a");
        p.environment = Environment::Prod;
        p.name = "kafka-prod".into();
        for confirm in [None, Some(""), Some("kafka-pro"), Some("KAFKA-PROD")] {
            let e = ensure_mutation_allowed(&p, confirm).unwrap_err();
            assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{confirm:?}");
            assert!(e.message.contains("kafka-prod"), "{}", e.message);
        }
        assert!(ensure_mutation_allowed(&p, Some("kafka-prod")).is_ok());
        assert!(ensure_mutation_allowed(&p, Some(" kafka-prod ")).is_ok());
    }

    #[test]
    fn other_environments_need_no_confirmation() {
        for env in [Environment::Local, Environment::Dev, Environment::Test, Environment::Stage] {
            let mut p = sample_profile("a");
            p.environment = env;
            assert!(ensure_mutation_allowed(&p, None).is_ok(), "{env:?}");
        }
    }

    #[test]
    fn object_name_must_match_exactly() {
        assert!(ensure_name_confirmed("orders", Some("orders")).is_ok());
        assert!(ensure_name_confirmed("orders", Some(" orders ")).is_ok());
        for typed in [None, Some("order"), Some("ORDERS")] {
            let e = ensure_name_confirmed("orders", typed).unwrap_err();
            assert_eq!(e.code, ErrorCode::ConfirmationRequired, "{typed:?}");
        }
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib guard`
Expected: FAIL — `no variant ReadOnly` / `cannot find function ensure_mutation_allowed`.

- [ ] **Step 3: Коды ошибок** — в `src-tauri/src/error.rs` в `enum ErrorCode` после `NotFound`:

```rust
    ReadOnly,
    ConfirmationRequired,
    GroupNotEmpty,
```

И в `mod tests` файла `error.rs`:
```rust
    #[test]
    fn mutation_codes_serialize_and_are_not_retryable() {
        for (code, name) in [
            (ErrorCode::ReadOnly, "READ_ONLY"),
            (ErrorCode::ConfirmationRequired, "CONFIRMATION_REQUIRED"),
            (ErrorCode::GroupNotEmpty, "GROUP_NOT_EMPTY"),
        ] {
            assert_eq!(serde_json::to_value(code).unwrap(), name);
            assert!(!AppError::new(code, "x").retryable);
        }
    }
```

- [ ] **Step 4: Реализация guard** — над тестами в `guard.rs`:

```rust
//! Защита мутирующих операций (спец. §5): проверяется в Rust, а не только в UI.

use crate::error::{AppError, AppResult, ErrorCode};
use crate::storage::connections::{ConnectionProfile, Environment};

/// read-only запрещает любые изменения; prod без read-only требует ввести имя подключения.
pub fn ensure_mutation_allowed(p: &ConnectionProfile, confirm: Option<&str>) -> AppResult<()> {
    if p.read_only {
        return Err(AppError::new(
            ErrorCode::ReadOnly,
            format!("Подключение «{}» открыто только для чтения", p.name),
        ));
    }
    if p.environment == Environment::Prod && confirm.map(str::trim) != Some(p.name.trim()) {
        return Err(AppError::new(
            ErrorCode::ConfirmationRequired,
            format!(
                "Для изменений в prod-подключении введите его имя «{}»",
                p.name
            ),
        ));
    }
    Ok(())
}

/// Удаление и очистка требуют ввести имя объекта (при удалении нескольких топиков — их число).
pub fn ensure_name_confirmed(expected: &str, typed: Option<&str>) -> AppResult<()> {
    if typed.map(str::trim) == Some(expected) {
        Ok(())
    } else {
        Err(AppError::new(
            ErrorCode::ConfirmationRequired,
            format!("Для подтверждения введите «{expected}»"),
        ))
    }
}
```

- [ ] **Step 5: `AppState::guarded`** — в `src-tauri/src/state.rs` в `impl AppState` после `profile`:

```rust
    /// Профиль подключения, если мутирующая операция на нём разрешена (см. `guard`).
    /// Не читает keychain и не ходит в сеть — можно вызывать на async-потоке.
    pub fn guarded(&self, connection_id: &str, confirm: Option<&str>) -> AppResult<ConnectionProfile> {
        let p = self.connections.get(connection_id)?;
        crate::guard::ensure_mutation_allowed(&p, confirm)?;
        Ok(p)
    }
```

И тест в `mod tests` файла `state.rs`:
```rust
    #[test]
    fn guarded_checks_profile() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        assert_eq!(state.guarded("nope", None).unwrap_err().code, ErrorCode::NotFound);
        let mut p = sample_profile("a");
        p.read_only = true;
        state.connections.upsert(p).unwrap();
        assert_eq!(state.guarded("a", None).unwrap_err().code, ErrorCode::ReadOnly);
        state.connections.upsert(sample_profile("b")).unwrap();
        assert_eq!(state.guarded("b", None).unwrap().id, "b");
    }
```

- [ ] **Step 6: Тип на фронтенде** — в `src/api/types.ts` расширить `ErrorCode`:

```ts
export type ErrorCode =
  | "CONNECTION_FAILED"
  | "TLS_ERROR"
  | "AUTH_FAILED"
  | "NOT_AUTHORIZED"
  | "TIMEOUT"
  | "INVALID_INPUT"
  | "NOT_FOUND"
  | "READ_ONLY"
  | "CONFIRMATION_REQUIRED"
  | "GROUP_NOT_EMPTY"
  | "INTERNAL";
```

- [ ] **Step 7: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib guard error state && pnpm build`
Expected: PASS; `tsc` без ошибок.

- [ ] **Step 8: Commit**

```bash
git add src-tauri/src src/api/types.ts
git commit -m "feat(guard): защита мутаций read-only и prod на уровне Rust"
```

---

### Task 6: FFI-модуль admin_ext — каркас и IncrementalAlterConfigs

**Files:**
- Create: `src-tauri/src/kafka/admin_ext.rs`
- Modify: `src-tauri/src/kafka/mod.rs` (`pub mod admin_ext;`)
- Modify: `src-tauri/src/kafka/errors.rs` (`item_error`, перенос `admin_error`)
- Modify: `src-tauri/src/kafka/configs.rs` (использовать `errors::admin_error`)
- Create: `src-tauri/tests/it_admin_ext.rs`

**Interfaces:**
- Consumes: `CapturingContext`, `KafkaHandles` (`h.admin`, `h.timeout`, `h.kafka_err`)
- Produces:
  - `struct admin_ext::ItemError { pub code: RDKafkaErrorCode, pub message: String }`, `type ItemResult<T> = Result<T, ItemError>`
  - `struct admin_ext::RawPartition { pub topic: String, pub partition: i32, pub offset: i64, pub error: Option<ItemError> }`
  - `fn admin_ext::incremental_alter_topic_configs(admin: &AdminClient<CapturingContext>, timeout: Duration, topic: &str, changes: &[(String, Option<String>)]) -> KafkaResult<ItemResult<()>>` — `Some(value)` = SET, `None` = DELETE (сброс к default); **блокирующая**
  - `fn kafka::errors::item_error(code: RDKafkaErrorCode, message: &str, context: &str) -> AppError` — ошибка отдельного элемента admin-ответа: `message` = `context`, `details` = код + текст брокера
  - `async fn kafka::errors::admin_error(h: &Arc<KafkaHandles>, started: Instant, e: KafkaError) -> AppError` (перенесена из `configs.rs` без изменений, теперь `pub`)

- [ ] **Step 1: Тесты `item_error`** — в `mod tests` файла `src-tauri/src/kafka/errors.rs`:

```rust
    #[test]
    fn item_errors_map_to_app_codes() {
        use crate::error::ErrorCode as E;
        let cases = [
            (C::TopicAlreadyExists, E::InvalidInput),
            (C::InvalidConfig, E::InvalidInput),
            (C::InvalidPartitions, E::InvalidInput),
            (C::InvalidReplicationFactor, E::InvalidInput),
            (C::PolicyViolation, E::InvalidInput),
            (C::TopicAuthorizationFailed, E::NotAuthorized),
            (C::GroupAuthorizationFailed, E::NotAuthorized),
            (C::UnknownTopicOrPartition, E::NotFound),
            (C::GroupIdNotFound, E::NotFound),
            (C::NonEmptyGroup, E::GroupNotEmpty),
            (C::RequestTimedOut, E::Timeout),
            (C::Unknown, E::Internal),
        ];
        for (code, expected) in cases {
            assert_eq!(item_error(code, "", "ctx").code, expected, "{code:?}");
        }
        let e = item_error(
            C::InvalidConfig,
            "Invalid value abc for configuration retention.ms",
            "Не удалось изменить конфигурацию топика",
        );
        assert_eq!(e.message, "Не удалось изменить конфигурацию топика");
        assert!(e.details.unwrap().contains("retention.ms"));
    }
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::errors`
Expected: FAIL — `cannot find function item_error`.

- [ ] **Step 3: `item_error` и перенос `admin_error`** — в `src-tauri/src/kafka/errors.rs`.

Импорты дополнить:
```rust
use std::sync::Arc;

use rdkafka::types::RDKafkaErrorCode;

use crate::kafka::pool::KafkaHandles;
```

Добавить после `op_error`:
```rust
/// Ошибка отдельного элемента admin-ответа (топика, ресурса, группы, партиции).
/// `message` — контекст для пользователя, в `details` — код и текст брокера.
pub fn item_error(code: RDKafkaErrorCode, message: &str, context: &str) -> AppError {
    use RDKafkaErrorCode as C;
    let app_code = match code {
        C::TopicAlreadyExists
        | C::InvalidConfig
        | C::InvalidRequest
        | C::PolicyViolation
        | C::InvalidPartitions
        | C::InvalidReplicationFactor
        | C::InvalidReplicaAssignment
        | C::InvalidTopic
        | C::OffsetOutOfRange => ErrorCode::InvalidInput,
        C::TopicAuthorizationFailed
        | C::GroupAuthorizationFailed
        | C::ClusterAuthorizationFailed => ErrorCode::NotAuthorized,
        C::UnknownTopicOrPartition | C::GroupIdNotFound => ErrorCode::NotFound,
        C::NonEmptyGroup => ErrorCode::GroupNotEmpty,
        C::RequestTimedOut | C::OperationTimedOut => ErrorCode::Timeout,
        _ => classify_message(message).unwrap_or(ErrorCode::Internal),
    };
    let details = if message.is_empty() {
        code.to_string()
    } else {
        format!("{code}: {message}")
    };
    AppError::new(app_code, context).with_details(details)
}

/// Колбэки ошибок admin-клиента в rust-rdkafka не доставляются, поэтому причину (TLS/SASL)
/// берём у pooled-консьюмера того же подключения; его опрос блокирующий — уводим с async-потока.
pub async fn admin_error(h: &Arc<KafkaHandles>, started: Instant, e: KafkaError) -> AppError {
    let h = h.clone();
    let fallback = e.clone();
    tauri::async_runtime::spawn_blocking(move || h.kafka_err(started, e))
        .await
        .unwrap_or_else(|_| from_kafka(&fallback, None))
}
```

В `src-tauri/src/kafka/configs.rs` удалить функцию `admin_error` и импортировать `use crate::kafka::errors::admin_error;` (неиспользуемые импорты убрать).

- [ ] **Step 4: Тест проходит**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka`
Expected: PASS.

- [ ] **Step 5: Интеграционный тест IncrementalAlterConfigs** — создать `src-tauri/tests/it_admin_ext.rs`:

```rust
mod common;

use std::sync::Arc;

use kafka_explorer_lib::kafka::admin_ext::incremental_alter_topic_configs;
use kafka_explorer_lib::kafka::configs::describe_configs;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::admin::ResourceSpecifier;
use rdkafka::types::RDKafkaErrorCode;

fn handles(bs: &str) -> Arc<KafkaHandles> {
    Arc::new(KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap())
}

fn alter(h: &Arc<KafkaHandles>, topic: &str, changes: &[(&str, Option<&str>)]) -> Result<(), RDKafkaErrorCode> {
    let changes: Vec<(String, Option<String>)> = changes
        .iter()
        .map(|(k, v)| (k.to_string(), v.map(String::from)))
        .collect();
    incremental_alter_topic_configs(&h.admin, h.timeout, topic, &changes)
        .expect("запрос выполнен")
        .map_err(|e| e.code)
}

async fn config(h: &Arc<KafkaHandles>, topic: &str, name: &str) -> (Option<String>, bool) {
    let entries = describe_configs(h, ResourceSpecifier::Topic(topic)).await.unwrap();
    let e = entries.iter().find(|e| e.name == name).unwrap();
    (e.value.clone(), e.is_default)
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_alter_sets_and_deletes_without_touching_other_configs() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-alter", 1).await;
    let h = handles(&bs);

    let (h1, t1) = (h.clone(), topic.to_string());
    tokio::task::spawn_blocking(move || {
        alter(&h1, &t1, &[("retention.ms", Some("123456")), ("cleanup.policy", Some("compact"))]).unwrap();
    })
    .await
    .unwrap();
    assert_eq!(config(&h, &topic, "retention.ms").await, (Some("123456".into()), false));
    assert_eq!(config(&h, &topic, "cleanup.policy").await.0.as_deref(), Some("compact"));

    // DELETE возвращает только retention.ms к default; cleanup.policy остаётся (в отличие от AlterConfigs).
    let (h1, t1) = (h.clone(), topic.to_string());
    tokio::task::spawn_blocking(move || alter(&h1, &t1, &[("retention.ms", None)]).unwrap())
        .await
        .unwrap();
    assert!(config(&h, &topic, "retention.ms").await.1, "retention.ms снова default");
    assert_eq!(config(&h, &topic, "cleanup.policy").await.0.as_deref(), Some("compact"));
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_alter_reports_item_errors() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-alter-bad", 1).await;
    let h = handles(&bs);
    let t1 = topic.to_string();
    let (bad_value, missing) = tokio::task::spawn_blocking(move || {
        (
            alter(&h, &t1, &[("retention.ms", Some("abc"))]),
            alter(&h, "does-not-exist-topic", &[("retention.ms", Some("1000"))]),
        )
    })
    .await
    .unwrap();
    assert_eq!(bad_value, Err(RDKafkaErrorCode::InvalidConfig));
    assert_eq!(missing, Err(RDKafkaErrorCode::UnknownTopicOrPartition));
}
```

- [ ] **Step 6: Убедиться в падении**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_admin_ext`
Expected: FAIL — нет модуля `admin_ext`.

- [ ] **Step 7: Реализация** — создать `src-tauri/src/kafka/admin_ext.rs` и добавить `pub mod admin_ext;` в `src-tauri/src/kafka/mod.rs`:

```rust
//! FFI-обёртки над Admin API librdkafka, которых нет в rust-rdkafka 0.37:
//! IncrementalAlterConfigs, ListConsumerGroups, DescribeConsumerGroups,
//! ListConsumerGroupOffsets, AlterConsumerGroupOffsets.
//!
//! Весь новый `unsafe` M2 — только здесь; наружу выходят безопасные функции с Rust-типами.
//! Все функции **блокирующие** (ждут результат в собственной очереди событий) — вызывать
//! только из blocking-контекста. Ошибка запроса целиком — `KafkaError`; ошибка отдельного
//! ресурса, группы или партиции — `ItemError`.

use std::ffi::{CStr, CString, c_char};
use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::bindings as rd;
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::types::RDKafkaErrorCode;

use crate::kafka::context::CapturingContext;

type Admin = AdminClient<CapturingContext>;

const NO_ERROR: rd::rd_kafka_resp_err_t = rd::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR;
/// Запас сверх таймаута запроса на доставку события результата в очередь.
const POLL_MARGIN_MS: i32 = 5_000;

#[derive(Debug, Clone, PartialEq)]
pub struct ItemError {
    pub code: RDKafkaErrorCode,
    pub message: String,
}

pub type ItemResult<T> = Result<T, ItemError>;

/// Партиция из ответа librdkafka. `offset` — как вернул брокер (отрицательный = нет значения).
#[derive(Debug, Clone, PartialEq)]
pub struct RawPartition {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<ItemError>,
}

// ---- RAII-владельцы объектов librdkafka ----

struct Queue(*mut rd::rd_kafka_queue_t);
impl Drop for Queue {
    fn drop(&mut self) {
        // SAFETY: указатель получен из rd_kafka_queue_new и освобождается один раз.
        unsafe { rd::rd_kafka_queue_destroy(self.0) }
    }
}

struct Options(*mut rd::rd_kafka_AdminOptions_t);
impl Drop for Options {
    fn drop(&mut self) {
        // SAFETY: указатель получен из rd_kafka_AdminOptions_new.
        unsafe { rd::rd_kafka_AdminOptions_destroy(self.0) }
    }
}

struct Event(*mut rd::rd_kafka_event_t);
impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: событие получено из rd_kafka_queue_poll; результаты, прочитанные из него,
        // к этому моменту уже скопированы в Rust-типы.
        unsafe { rd::rd_kafka_event_destroy(self.0) }
    }
}

struct ConfigResource(*mut rd::rd_kafka_ConfigResource_t);
impl Drop for ConfigResource {
    fn drop(&mut self) {
        // SAFETY: admin-вызов копирует ресурс, владение остаётся у нас.
        unsafe { rd::rd_kafka_ConfigResource_destroy(self.0) }
    }
}

// ---- вспомогательные функции ----

fn cstring(s: &str) -> KafkaResult<CString> {
    CString::new(s)
        .map_err(|_| KafkaError::AdminOpCreation(format!("Недопустимый символ NUL в «{s}»")))
}

/// # Safety
/// `p` — NULL или указатель на C-строку, живую на время вызова.
unsafe fn string(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        // SAFETY: см. контракт функции.
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }
}

/// # Safety
/// `p` — NULL или массив из `n` элементов, живой, пока используется срез.
unsafe fn slice<'a, T>(p: *const T, n: usize) -> &'a [T] {
    if p.is_null() || n == 0 {
        &[]
    } else {
        // SAFETY: см. контракт функции.
        unsafe { std::slice::from_raw_parts(p, n) }
    }
}

/// # Safety
/// `e` — NULL или живой `rd_kafka_error_t` (принадлежит событию результата).
unsafe fn error_obj(e: *const rd::rd_kafka_error_t) -> Option<ItemError> {
    if e.is_null() {
        return None;
    }
    // SAFETY: см. контракт функции.
    let code = unsafe { rd::rd_kafka_error_code(e) };
    if code == NO_ERROR {
        return None;
    }
    Some(ItemError {
        code: code.into(),
        // SAFETY: строка принадлежит объекту ошибки.
        message: unsafe { string(rd::rd_kafka_error_string(e)) },
    })
}

fn unexpected_event() -> KafkaError {
    KafkaError::AdminOpCreation("librdkafka вернула событие неожиданного типа".into())
}

fn timeout_ms(timeout: Duration) -> i32 {
    timeout.as_millis().min(i32::MAX as u128) as i32
}

/// Выполняет admin-запрос: своя очередь и опции с таймаутом, вызов `call`, ожидание события.
/// Блокирует до таймаута запроса + POLL_MARGIN_MS.
fn run(
    admin: &Admin,
    op: rd::rd_kafka_admin_op_t,
    timeout: Duration,
    call: impl FnOnce(*mut rd::rd_kafka_t, *const rd::rd_kafka_AdminOptions_t, *mut rd::rd_kafka_queue_t),
) -> KafkaResult<Event> {
    let rk = admin.inner().native_ptr();
    let ms = timeout_ms(timeout);
    // SAFETY: rk — живой клиент: `admin` заимствован на всё время вызова.
    let queue = Queue(unsafe { rd::rd_kafka_queue_new(rk) });
    // SAFETY: то же.
    let opts = Options(unsafe { rd::rd_kafka_AdminOptions_new(rk, op) });
    if opts.0.is_null() {
        return Err(KafkaError::AdminOpCreation(
            "Операция не поддерживается этой версией librdkafka".into(),
        ));
    }
    let mut errstr = [0 as c_char; 512];
    // SAFETY: буфер errstr живёт до конца вызова, размер передан.
    let err = unsafe {
        rd::rd_kafka_AdminOptions_set_request_timeout(opts.0, ms, errstr.as_mut_ptr(), errstr.len())
    };
    if err != NO_ERROR {
        // SAFETY: librdkafka записала в errstr C-строку.
        return Err(KafkaError::AdminOpCreation(unsafe { string(errstr.as_ptr()) }));
    }
    call(rk, opts.0, queue.0);
    // SAFETY: очередь жива; NULL означает таймаут.
    let ev = unsafe { rd::rd_kafka_queue_poll(queue.0, ms.saturating_add(POLL_MARGIN_MS)) };
    if ev.is_null() {
        return Err(KafkaError::AdminOp(RDKafkaErrorCode::OperationTimedOut));
    }
    let ev = Event(ev);
    // SAFETY: событие живо до Drop.
    let code = unsafe { rd::rd_kafka_event_error(ev.0) };
    if code != NO_ERROR {
        // SAFETY: строка принадлежит событию.
        let text = unsafe { string(rd::rd_kafka_event_error_string(ev.0)) };
        tracing::warn!("admin-запрос завершился ошибкой: {text}");
        return Err(KafkaError::AdminOp(code.into()));
    }
    Ok(ev)
}

// ---- IncrementalAlterConfigs ----

/// Изменяет конфиги топика: `Some(value)` — SET, `None` — DELETE (возврат к default).
/// Остальные параметры топика не затрагиваются (в отличие от устаревшего AlterConfigs).
pub fn incremental_alter_topic_configs(
    admin: &Admin,
    timeout: Duration,
    topic: &str,
    changes: &[(String, Option<String>)],
) -> KafkaResult<ItemResult<()>> {
    let name = cstring(topic)?;
    // SAFETY: name живёт до конца функции; ресурс освобождается в Drop.
    let resource = ConfigResource(unsafe {
        rd::rd_kafka_ConfigResource_new(rd::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC, name.as_ptr())
    });
    for (key, value) in changes {
        let key = cstring(key)?;
        let value = value.as_deref().map(cstring).transpose()?;
        let (op, value_ptr) = match &value {
            Some(v) => (rd::rd_kafka_AlterConfigOpType_t::RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET, v.as_ptr()),
            None => (rd::rd_kafka_AlterConfigOpType_t::RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE, std::ptr::null()),
        };
        // SAFETY: строки живут до конца итерации, librdkafka их копирует.
        let err = unsafe {
            rd::rd_kafka_ConfigResource_add_incremental_config(resource.0, key.as_ptr(), op, value_ptr)
        };
        if !err.is_null() {
            // SAFETY: err — живой объект ошибки, которым мы владеем.
            let message = unsafe { string(rd::rd_kafka_error_string(err)) };
            // SAFETY: освобождаем принадлежащий нам объект ошибки.
            unsafe { rd::rd_kafka_error_destroy(err) };
            return Err(KafkaError::AdminOpCreation(message));
        }
    }
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS,
        timeout,
        |rk, opts, queue| {
            let mut arr = [resource.0];
            // SAFETY: массив из одного живого ресурса; librdkafka копирует его.
            unsafe { rd::rd_kafka_IncrementalAlterConfigs(rk, arr.as_mut_ptr(), 1, opts, queue) }
        },
    )?;
    // SAFETY: событие живо; результат принадлежит событию.
    let result = unsafe { rd::rd_kafka_event_IncrementalAlterConfigs_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let mut n = 0usize;
    // SAFETY: result жив, пока жив ev.
    let resources = unsafe { rd::rd_kafka_IncrementalAlterConfigs_result_resources(result, &mut n) };
    // SAFETY: массив из n ресурсов, принадлежащих событию.
    for r in unsafe { slice(resources, n) } {
        // SAFETY: r — живой ресурс результата.
        let code = unsafe { rd::rd_kafka_ConfigResource_error(*r) };
        if code != NO_ERROR {
            return Ok(Err(ItemError {
                code: code.into(),
                // SAFETY: строка принадлежит ресурсу результата.
                message: unsafe { string(rd::rd_kafka_ConfigResource_error_string(*r)) },
            }));
        }
    }
    Ok(Ok(()))
}
```

Если компилятор не находит `PartialEq` для `rd_kafka_resp_err_t` (сравнение с `NO_ERROR`), сравнивайте через `RDKafkaErrorCode::from(code) == RDKafkaErrorCode::NoError`.

- [ ] **Step 8: Тесты проходят**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_admin_ext && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS (2 теста), clippy без предупреждений.

- [ ] **Step 9: Commit**

```bash
git add src-tauri/src/kafka src-tauri/tests/it_admin_ext.rs
git commit -m "feat(kafka): FFI IncrementalAlterConfigs и ошибки элементов admin-ответа"
```

---

### Task 7: FFI-функции для consumer groups

**Files:**
- Modify: `src-tauri/src/kafka/admin_ext.rs`
- Modify: `src-tauri/tests/it_admin_ext.rs`

**Interfaces:**
- Consumes: `run`, `slice`, `string`, `error_obj`, `cstring`, `ItemError`, `RawPartition` (Task 6)
- Produces (все **блокирующие**):
  - `struct GroupListing { pub group_id: String, pub state: String, pub is_simple: bool }` — `state` — имя librdkafka: `Stable`, `Empty`, `PreparingRebalance`, `CompletingRebalance`, `Dead`, `Unknown`
  - `fn list_consumer_groups(admin, timeout) -> KafkaResult<(Vec<GroupListing>, Vec<ItemError>)>` — второй элемент — частичные ошибки отдельных брокеров
  - `struct MemberRaw { pub member_id: String, pub client_id: String, pub host: String, pub assignment: Vec<(String, i32)> }`
  - `struct GroupDescriptionRaw { pub group_id: String, pub state: String, pub assignor: String, pub members: Vec<MemberRaw>, pub error: Option<ItemError> }`
  - `fn describe_consumer_groups(admin, timeout, ids: &[String]) -> KafkaResult<Vec<GroupDescriptionRaw>>`
  - `fn list_consumer_group_offsets(admin, timeout, group: &str) -> KafkaResult<ItemResult<Vec<RawPartition>>>` — все закоммиченные партиции группы
  - `fn alter_consumer_group_offsets(admin, timeout, group: &str, offsets: &[(String, i32, i64)]) -> KafkaResult<ItemResult<Vec<RawPartition>>>` — результат по каждой партиции

- [ ] **Step 1: Интеграционные тесты** — дописать в `src-tauri/tests/it_admin_ext.rs`:

```rust
use std::time::{Duration, Instant};

use kafka_explorer_lib::kafka::admin_ext::{
    alter_consumer_group_offsets, describe_consumer_groups, list_consumer_group_offsets,
    list_consumer_groups,
};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};

/// Участник группы: подписан на топик и получил назначение.
fn join_group(bs: &str, group: &str, topic: &str) -> BaseConsumer {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bs)
        .set("group.id", group)
        .set("client.id", "it-member")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    c.subscribe(&[topic]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while c.assignment().unwrap().count() == 0 {
        assert!(Instant::now() < deadline, "участник не получил назначение");
        let _ = c.poll(Duration::from_millis(100));
    }
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn groups_list_describe_offsets_alter() {
    let Some(bs) = common::bootstrap() else { return };
    let topic = common::TempTopic::create(&bs, "it-groups-ffi", 2).await;
    let group = common::TempGroup::new(&bs, "it-group-ffi");
    common::commit_group_offsets(&bs, &group, &topic, &[(0, 3), (1, 5)]);
    let h = handles(&bs);
    let (t, g, bs2) = (topic.to_string(), group.to_string(), bs.clone());

    tokio::task::spawn_blocking(move || {
        let (groups, errors) = list_consumer_groups(&h.admin, h.timeout).unwrap();
        assert!(errors.is_empty(), "{errors:?}");
        let listing = groups.iter().find(|l| l.group_id == g).expect("группа в списке");
        assert_eq!(listing.state, "Empty");

        let d = describe_consumer_groups(&h.admin, h.timeout, std::slice::from_ref(&g)).unwrap();
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].state, "Empty");
        assert!(d[0].members.is_empty());
        assert!(d[0].error.is_none());

        let mut offsets = list_consumer_group_offsets(&h.admin, h.timeout, &g).unwrap().unwrap();
        offsets.sort_by_key(|p| p.partition);
        let pairs: Vec<(i32, i64)> = offsets.iter().map(|p| (p.partition, p.offset)).collect();
        assert_eq!(pairs, [(0, 3), (1, 5)]);
        assert!(offsets.iter().all(|p| p.topic == t && p.error.is_none()));

        let altered = alter_consumer_group_offsets(&h.admin, h.timeout, &g, &[(t.clone(), 0, 1), (t.clone(), 1, 0)])
            .unwrap()
            .unwrap();
        assert!(altered.iter().all(|p| p.error.is_none()), "{altered:?}");
        let mut after = list_consumer_group_offsets(&h.admin, h.timeout, &g).unwrap().unwrap();
        after.sort_by_key(|p| p.partition);
        assert_eq!(after.iter().map(|p| p.offset).collect::<Vec<_>>(), [1, 0]);

        // Активная группа: участник и его назначение видны.
        let member = join_group(&bs2, &g, &t);
        let d = describe_consumer_groups(&h.admin, h.timeout, std::slice::from_ref(&g)).unwrap();
        assert_eq!(d[0].state, "Stable");
        assert_eq!(d[0].members.len(), 1);
        assert_eq!(d[0].members[0].client_id, "it-member");
        let mut assigned = d[0].members[0].assignment.clone();
        assigned.sort();
        assert_eq!(assigned, [(t.clone(), 0), (t.clone(), 1)]);
        assert!(!d[0].assignor.is_empty());
        member.unsubscribe();
        drop(member);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn describe_unknown_group_is_dead() {
    let Some(bs) = common::bootstrap() else { return };
    let h = handles(&bs);
    tokio::task::spawn_blocking(move || {
        let d = describe_consumer_groups(&h.admin, h.timeout, &["it-never-existed-group".to_string()]).unwrap();
        assert_eq!(d[0].state, "Dead");
        assert!(d[0].members.is_empty());
    })
    .await
    .unwrap();
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_admin_ext`
Expected: FAIL — не найдены `list_consumer_groups` и др.

- [ ] **Step 3: Реализация** — дописать в `admin_ext.rs` (после `incremental_alter_topic_configs`):

```rust
// ---- consumer groups ----

#[derive(Debug, Clone, PartialEq)]
pub struct GroupListing {
    pub group_id: String,
    /// Stable, Empty, PreparingRebalance, CompletingRebalance, Dead, Unknown.
    pub state: String,
    pub is_simple: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemberRaw {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub assignment: Vec<(String, i32)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupDescriptionRaw {
    pub group_id: String,
    pub state: String,
    pub assignor: String,
    pub members: Vec<MemberRaw>,
    pub error: Option<ItemError>,
}

struct ListOffsetsRequest(*mut rd::rd_kafka_ListConsumerGroupOffsets_t);
impl Drop for ListOffsetsRequest {
    fn drop(&mut self) {
        // SAFETY: объект создан rd_kafka_ListConsumerGroupOffsets_new; admin-вызов его копирует.
        unsafe { rd::rd_kafka_ListConsumerGroupOffsets_destroy(self.0) }
    }
}

struct AlterOffsetsRequest(*mut rd::rd_kafka_AlterConsumerGroupOffsets_t);
impl Drop for AlterOffsetsRequest {
    fn drop(&mut self) {
        // SAFETY: объект создан rd_kafka_AlterConsumerGroupOffsets_new; admin-вызов его копирует.
        unsafe { rd::rd_kafka_AlterConsumerGroupOffsets_destroy(self.0) }
    }
}

struct PartitionList(*mut rd::rd_kafka_topic_partition_list_t);
impl Drop for PartitionList {
    fn drop(&mut self) {
        // SAFETY: список создан rd_kafka_topic_partition_list_new.
        unsafe { rd::rd_kafka_topic_partition_list_destroy(self.0) }
    }
}

fn state_name(s: rd::rd_kafka_consumer_group_state_t) -> String {
    // SAFETY: функция возвращает статическую строку.
    unsafe { string(rd::rd_kafka_consumer_group_state_name(s)) }
}

/// # Safety
/// `list` — NULL или живой список партиций (принадлежит событию результата).
unsafe fn read_partitions(list: *const rd::rd_kafka_topic_partition_list_t) -> Vec<RawPartition> {
    if list.is_null() {
        return Vec::new();
    }
    // SAFETY: см. контракт функции.
    let l = unsafe { &*list };
    // SAFETY: elems — массив из cnt элементов.
    unsafe { slice(l.elems, l.cnt.max(0) as usize) }
        .iter()
        .map(|e| RawPartition {
            // SAFETY: имя топика принадлежит элементу списка.
            topic: unsafe { string(e.topic) },
            partition: e.partition,
            offset: e.offset,
            error: (e.err != NO_ERROR).then(|| {
                let code: RDKafkaErrorCode = e.err.into();
                ItemError {
                    message: code.to_string(),
                    code,
                }
            }),
        })
        .collect()
}

fn partition_list(items: &[(String, i32, i64)]) -> KafkaResult<PartitionList> {
    // SAFETY: создаём пустой список нужной ёмкости, владение — у PartitionList.
    let list = PartitionList(unsafe { rd::rd_kafka_topic_partition_list_new(items.len() as i32) });
    for (topic, partition, offset) in items {
        let t = cstring(topic)?;
        // SAFETY: список жив; librdkafka копирует имя топика и возвращает указатель на элемент.
        let e = unsafe { rd::rd_kafka_topic_partition_list_add(list.0, t.as_ptr(), *partition) };
        // SAFETY: e указывает на только что добавленный элемент списка.
        unsafe { (*e).offset = *offset };
    }
    Ok(list)
}

/// # Safety
/// `groups` — массив из `n` живых group_result, принадлежащих событию.
unsafe fn single_group_result(
    groups: *mut *const rd::rd_kafka_group_result_t,
    n: usize,
) -> KafkaResult<ItemResult<Vec<RawPartition>>> {
    // SAFETY: см. контракт функции.
    let Some(g) = unsafe { slice(groups, n) }.first().copied() else {
        return Err(unexpected_event());
    };
    // SAFETY: g — живой group_result.
    if let Some(e) = unsafe { error_obj(rd::rd_kafka_group_result_error(g)) } {
        return Ok(Err(e));
    }
    // SAFETY: список партиций принадлежит group_result.
    Ok(Ok(unsafe { read_partitions(rd::rd_kafka_group_result_partitions(g)) }))
}

pub fn list_consumer_groups(
    admin: &Admin,
    timeout: Duration,
) -> KafkaResult<(Vec<GroupListing>, Vec<ItemError>)> {
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPS,
        timeout,
        // SAFETY: аргументы — живые объекты, созданные в `run`.
        |rk, opts, queue| unsafe { rd::rd_kafka_ListConsumerGroups(rk, opts, queue) },
    )?;
    // SAFETY: событие живо до конца функции.
    let result = unsafe { rd::rd_kafka_event_ListConsumerGroups_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let (mut n, mut m) = (0usize, 0usize);
    // SAFETY: result принадлежит событию.
    let valid = unsafe { rd::rd_kafka_ListConsumerGroups_result_valid(result, &mut n) };
    // SAFETY: массив из n листингов события.
    let groups = unsafe { slice(valid, n) }
        .iter()
        .map(|g| GroupListing {
            // SAFETY: g — живой листинг события.
            group_id: unsafe { string(rd::rd_kafka_ConsumerGroupListing_group_id(*g)) },
            // SAFETY: то же.
            state: state_name(unsafe { rd::rd_kafka_ConsumerGroupListing_state(*g) }),
            // SAFETY: то же.
            is_simple: unsafe { rd::rd_kafka_ConsumerGroupListing_is_simple_consumer_group(*g) } != 0,
        })
        .collect();
    // SAFETY: result принадлежит событию.
    let errs = unsafe { rd::rd_kafka_ListConsumerGroups_result_errors(result, &mut m) };
    // SAFETY: массив из m ошибок события.
    let errors = unsafe { slice(errs, m) }
        .iter()
        // SAFETY: e — живой объект ошибки события.
        .filter_map(|e| unsafe { error_obj(*e) })
        .collect();
    Ok((groups, errors))
}

pub fn describe_consumer_groups(
    admin: &Admin,
    timeout: Duration,
    ids: &[String],
) -> KafkaResult<Vec<GroupDescriptionRaw>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let names = ids.iter().map(|s| cstring(s)).collect::<KafkaResult<Vec<_>>>()?;
    let mut ptrs: Vec<*const c_char> = names.iter().map(|c| c.as_ptr()).collect();
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONSUMERGROUPS,
        timeout,
        |rk, opts, queue| {
            // SAFETY: ptrs указывают на живые CString из names; librdkafka их копирует.
            unsafe { rd::rd_kafka_DescribeConsumerGroups(rk, ptrs.as_mut_ptr(), ptrs.len(), opts, queue) }
        },
    )?;
    // SAFETY: событие живо до конца функции.
    let result = unsafe { rd::rd_kafka_event_DescribeConsumerGroups_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let mut n = 0usize;
    // SAFETY: result принадлежит событию.
    let arr = unsafe { rd::rd_kafka_DescribeConsumerGroups_result_groups(result, &mut n) };
    // SAFETY: массив из n описаний события.
    let out = unsafe { slice(arr, n) }
        .iter()
        .map(|g| {
            let g = *g;
            // SAFETY: g — живое описание группы; все строки и вложенные объекты принадлежат ему.
            unsafe {
                let members = (0..rd::rd_kafka_ConsumerGroupDescription_member_count(g))
                    .map(|i| {
                        let m = rd::rd_kafka_ConsumerGroupDescription_member(g, i);
                        let assignment = rd::rd_kafka_MemberDescription_assignment(m);
                        let parts = if assignment.is_null() {
                            Vec::new()
                        } else {
                            read_partitions(rd::rd_kafka_MemberAssignment_partitions(assignment))
                        };
                        MemberRaw {
                            member_id: string(rd::rd_kafka_MemberDescription_consumer_id(m)),
                            client_id: string(rd::rd_kafka_MemberDescription_client_id(m)),
                            host: string(rd::rd_kafka_MemberDescription_host(m)),
                            assignment: parts.into_iter().map(|p| (p.topic, p.partition)).collect(),
                        }
                    })
                    .collect();
                GroupDescriptionRaw {
                    group_id: string(rd::rd_kafka_ConsumerGroupDescription_group_id(g)),
                    state: state_name(rd::rd_kafka_ConsumerGroupDescription_state(g)),
                    assignor: string(rd::rd_kafka_ConsumerGroupDescription_partition_assignor(g)),
                    members,
                    error: error_obj(rd::rd_kafka_ConsumerGroupDescription_error(g)),
                }
            }
        })
        .collect();
    Ok(out)
}

/// Все закоммиченные партиции группы (список партиций запроса — NULL).
pub fn list_consumer_group_offsets(
    admin: &Admin,
    timeout: Duration,
    group: &str,
) -> KafkaResult<ItemResult<Vec<RawPartition>>> {
    let g = cstring(group)?;
    // SAFETY: g живёт до конца функции; NULL-список = все партиции.
    let req = ListOffsetsRequest(unsafe {
        rd::rd_kafka_ListConsumerGroupOffsets_new(g.as_ptr(), std::ptr::null())
    });
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_LISTCONSUMERGROUPOFFSETS,
        timeout,
        |rk, opts, queue| {
            let mut arr = [req.0];
            // SAFETY: массив из одного живого запроса; librdkafka его копирует.
            unsafe { rd::rd_kafka_ListConsumerGroupOffsets(rk, arr.as_mut_ptr(), 1, opts, queue) }
        },
    )?;
    // SAFETY: событие живо до конца функции.
    let result = unsafe { rd::rd_kafka_event_ListConsumerGroupOffsets_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let mut n = 0usize;
    // SAFETY: result принадлежит событию.
    let groups = unsafe { rd::rd_kafka_ListConsumerGroupOffsets_result_groups(result, &mut n) };
    // SAFETY: массив из n результатов события.
    unsafe { single_group_result(groups, n) }
}

pub fn alter_consumer_group_offsets(
    admin: &Admin,
    timeout: Duration,
    group: &str,
    offsets: &[(String, i32, i64)],
) -> KafkaResult<ItemResult<Vec<RawPartition>>> {
    let g = cstring(group)?;
    let list = partition_list(offsets)?;
    // SAFETY: g и list живы до конца функции; librdkafka копирует их.
    let req = AlterOffsetsRequest(unsafe { rd::rd_kafka_AlterConsumerGroupOffsets_new(g.as_ptr(), list.0) });
    let ev = run(
        admin,
        rd::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_ALTERCONSUMERGROUPOFFSETS,
        timeout,
        |rk, opts, queue| {
            let mut arr = [req.0];
            // SAFETY: массив из одного живого запроса; librdkafka его копирует.
            unsafe { rd::rd_kafka_AlterConsumerGroupOffsets(rk, arr.as_mut_ptr(), 1, opts, queue) }
        },
    )?;
    // SAFETY: событие живо до конца функции.
    let result = unsafe { rd::rd_kafka_event_AlterConsumerGroupOffsets_result(ev.0) };
    if result.is_null() {
        return Err(unexpected_event());
    }
    let mut n = 0usize;
    // SAFETY: result принадлежит событию.
    let groups = unsafe { rd::rd_kafka_AlterConsumerGroupOffsets_result_groups(result, &mut n) };
    // SAFETY: массив из n результатов события.
    unsafe { single_group_result(groups, n) }
}
```

- [ ] **Step 4: Тесты проходят**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_admin_ext && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS (4 теста), clippy чистый.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/kafka/admin_ext.rs src-tauri/tests/it_admin_ext.rs
git commit -m "feat(kafka): FFI для списка, описания и оффсетов consumer groups"
```

---

### Task 8: Операции с топиками в Rust

**Files:**
- Create: `src-tauri/src/kafka/admin.rs`
- Modify: `src-tauri/src/kafka/mod.rs` (`pub mod admin;`)
- Create: `src-tauri/tests/it_topic_ops.rs`

**Interfaces:**
- Consumes: `KafkaHandles`, `fetch_metadata_fast_fail`, `convert`, `admin_ext::incremental_alter_topic_configs`, `errors::{item_error, admin_error}`
- Produces:
  - `fn validate_topic_name(name: &str) -> AppResult<()>` — 1–249 символов `[a-zA-Z0-9._-]`, не `.` и не `..`
  - `struct NewTopicSpec { pub name: String, pub partitions: i32, pub replication_factor: i32, pub configs: BTreeMap<String, String> }` (serde camelCase, `configs` по умолчанию пуст); `replication_factor == -1` — значение брокера по умолчанию
  - `fn validate_new_topic(spec: &NewTopicSpec) -> AppResult<()>`
  - `async fn create_topic(h: &Arc<KafkaHandles>, spec: &NewTopicSpec) -> AppResult<()>`
  - `fn topic_partition_ids(h: &KafkaHandles, topic: &str) -> AppResult<Vec<i32>>` (блокирующая; `NOT_FOUND`, если топика нет)
  - `fn validate_partition_increase(current: usize, new_total: i32) -> AppResult<()>`
  - `async fn add_partitions(h: &Arc<KafkaHandles>, topic: &str, new_total: usize) -> AppResult<()>`
  - `fn resolve_partitions(all: &[i32], requested: Option<&[i32]>) -> AppResult<Vec<i32>>` (отсортированные; пустой или несуществующий выбор — `INVALID_INPUT`)
  - `struct PurgedPartition { pub partition: i32, pub low_watermark: Option<i64>, pub error: Option<String> }` (Serialize camelCase)
  - `async fn purge_topic(h: &Arc<KafkaHandles>, topic: &str, partitions: &[i32]) -> AppResult<Vec<PurgedPartition>>` — `DeleteRecords` до high watermark
  - `struct TopicDeleteResult { pub name: String, pub error: Option<AppError> }` (Serialize camelCase)
  - `async fn delete_topics(h: &Arc<KafkaHandles>, names: &[String]) -> AppResult<Vec<TopicDeleteResult>>`
  - `enum ConfigChange { Set { name: String, value: String }, Delete { name: String } }` — JSON `{"op":"set","name":…,"value":…}` / `{"op":"delete","name":…}`
  - `fn validate_config_changes(changes: &[ConfigChange]) -> AppResult<()>`
  - `fn alter_topic_configs(h: &KafkaHandles, topic: &str, changes: &[ConfigChange]) -> AppResult<()>` (блокирующая)

- [ ] **Step 1: Unit-тесты** — создать `src-tauri/src/kafka/admin.rs` с тестами и добавить `pub mod admin;` в `kafka/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn topic_names_follow_kafka_rules() {
        for ok in ["orders", "a.b-c_d", "X1", &"a".repeat(249)] {
            assert!(validate_topic_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", ".", "..", "with space", "заказы", "a/b", &"a".repeat(250)] {
            let e = validate_topic_name(bad).unwrap_err();
            assert_eq!(e.code, ErrorCode::InvalidInput, "{bad}");
        }
    }

    fn spec(partitions: i32, rf: i32) -> NewTopicSpec {
        NewTopicSpec {
            name: "t".into(),
            partitions,
            replication_factor: rf,
            configs: BTreeMap::new(),
        }
    }

    #[test]
    fn new_topic_validation() {
        assert!(validate_new_topic(&spec(3, 1)).is_ok());
        assert!(validate_new_topic(&spec(1, -1)).is_ok(), "-1 — RF брокера по умолчанию");
        for bad in [spec(0, 1), spec(-1, 1), spec(1, 0), spec(1, -2)] {
            assert_eq!(validate_new_topic(&bad).unwrap_err().code, ErrorCode::InvalidInput);
        }
        let mut s = spec(1, 1);
        s.configs.insert(" ".into(), "x".into());
        assert_eq!(validate_new_topic(&s).unwrap_err().code, ErrorCode::InvalidInput);
    }

    #[test]
    fn new_topic_spec_json() {
        let s: NewTopicSpec = serde_json::from_value(serde_json::json!({
            "name": "t", "partitions": 3, "replicationFactor": 1
        }))
        .unwrap();
        assert_eq!((s.partitions, s.replication_factor), (3, 1));
        assert!(s.configs.is_empty());
    }

    #[test]
    fn partitions_can_only_grow() {
        assert!(validate_partition_increase(3, 4).is_ok());
        for bad in [3, 2, 0, -1] {
            assert_eq!(
                validate_partition_increase(3, bad).unwrap_err().code,
                ErrorCode::InvalidInput,
                "{bad}"
            );
        }
    }

    #[test]
    fn resolve_partitions_validates_selection() {
        assert_eq!(resolve_partitions(&[0, 1, 2], None).unwrap(), [0, 1, 2]);
        assert_eq!(resolve_partitions(&[0, 1, 2], Some(&[2, 0])).unwrap(), [0, 2]);
        assert_eq!(resolve_partitions(&[0, 1, 2], Some(&[2, 2])).unwrap(), [2]);
        for bad in [&[][..], &[5][..]] {
            assert_eq!(
                resolve_partitions(&[0, 1, 2], Some(bad)).unwrap_err().code,
                ErrorCode::InvalidInput
            );
        }
    }

    #[test]
    fn config_changes_json_and_validation() {
        let changes: Vec<ConfigChange> = serde_json::from_value(serde_json::json!([
            {"op": "set", "name": "retention.ms", "value": "1000"},
            {"op": "delete", "name": "cleanup.policy"}
        ]))
        .unwrap();
        assert_eq!(
            changes,
            [
                ConfigChange::Set { name: "retention.ms".into(), value: "1000".into() },
                ConfigChange::Delete { name: "cleanup.policy".into() },
            ]
        );
        assert!(validate_config_changes(&changes).is_ok());
        assert!(validate_config_changes(&[]).is_err());
        let dup = [
            ConfigChange::Delete { name: "a".into() },
            ConfigChange::Set { name: "a".into(), value: "1".into() },
        ];
        assert_eq!(validate_config_changes(&dup).unwrap_err().code, ErrorCode::InvalidInput);
        assert!(validate_config_changes(&[ConfigChange::Delete { name: " ".into() }]).is_err());
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::admin`
Expected: FAIL — `cannot find function validate_topic_name`.

- [ ] **Step 3: Реализация** — над тестами в `kafka/admin.rs`:

```rust
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use rdkafka::admin::{AdminOptions, NewPartitions, NewTopic, TopicReplication, TopicResult};
use rdkafka::{Offset, TopicPartitionList};
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::kafka::admin_ext;
use crate::kafka::errors::{admin_error, fetch_metadata_fast_fail, item_error};
use crate::kafka::metadata::convert;
use crate::kafka::pool::KafkaHandles;

const MAX_TOPIC_NAME: usize = 249;

pub fn validate_topic_name(name: &str) -> AppResult<()> {
    if name.is_empty() {
        return Err(AppError::invalid("Имя топика не может быть пустым"));
    }
    if name == "." || name == ".." {
        return Err(AppError::invalid("Имя топика не может быть «.» или «..»"));
    }
    if name.len() > MAX_TOPIC_NAME {
        return Err(AppError::invalid(format!(
            "Имя топика длиннее {MAX_TOPIC_NAME} символов"
        )));
    }
    if let Some(c) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(AppError::invalid(format!(
            "Недопустимый символ «{c}»: разрешены латинские буквы, цифры, «.», «_» и «-»"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewTopicSpec {
    pub name: String,
    pub partitions: i32,
    /// -1 — значение брокера по умолчанию (default.replication.factor).
    pub replication_factor: i32,
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

pub fn validate_new_topic(spec: &NewTopicSpec) -> AppResult<()> {
    validate_topic_name(&spec.name)?;
    if spec.partitions < 1 {
        return Err(AppError::invalid("Число партиций должно быть не меньше 1"));
    }
    if spec.replication_factor < 1 && spec.replication_factor != -1 {
        return Err(AppError::invalid(
            "Фактор репликации должен быть не меньше 1",
        ));
    }
    if spec.configs.keys().any(|k| k.trim().is_empty()) {
        return Err(AppError::invalid("Пустое имя параметра конфигурации"));
    }
    Ok(())
}

fn admin_opts(h: &KafkaHandles) -> AdminOptions {
    AdminOptions::new()
        .request_timeout(Some(h.timeout))
        .operation_timeout(Some(h.timeout))
}

fn topic_result(r: TopicResult, context: &str) -> AppResult<String> {
    r.map_err(|(name, code)| item_error(code, &name, context))
}

pub async fn create_topic(h: &Arc<KafkaHandles>, spec: &NewTopicSpec) -> AppResult<()> {
    validate_new_topic(spec)?;
    let mut topic = NewTopic::new(
        &spec.name,
        spec.partitions,
        TopicReplication::Fixed(spec.replication_factor),
    );
    for (k, v) in &spec.configs {
        topic = topic.set(k, v);
    }
    let started = Instant::now();
    let results = match h.admin.create_topics([&topic], &admin_opts(h)).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    for r in results {
        topic_result(r, "Не удалось создать топик")?;
    }
    Ok(())
}

/// Партиции топика по метаданным. Блокирующая.
pub fn topic_partition_ids(h: &KafkaHandles, topic: &str) -> AppResult<Vec<i32>> {
    let md = fetch_metadata_fast_fail(&h.consumer, Some(topic), h.timeout)?;
    let (_, topics) = convert(&md);
    let t = topics
        .into_iter()
        .find(|t| t.name == topic && !t.partitions.is_empty())
        .ok_or_else(|| AppError::not_found(format!("Топик «{topic}» не найден")))?;
    let mut ids: Vec<i32> = t.partitions.iter().map(|p| p.id).collect();
    ids.sort_unstable();
    Ok(ids)
}

pub fn validate_partition_increase(current: usize, new_total: i32) -> AppResult<()> {
    if new_total <= 0 || (new_total as usize) <= current {
        return Err(AppError::invalid(format!(
            "Новое число партиций должно быть больше текущего ({current})"
        )));
    }
    Ok(())
}

pub async fn add_partitions(h: &Arc<KafkaHandles>, topic: &str, new_total: usize) -> AppResult<()> {
    let np = NewPartitions::new(topic, new_total);
    let started = Instant::now();
    let results = match h.admin.create_partitions([&np], &admin_opts(h)).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    for r in results {
        topic_result(r, "Не удалось добавить партиции")?;
    }
    Ok(())
}

pub fn resolve_partitions(all: &[i32], requested: Option<&[i32]>) -> AppResult<Vec<i32>> {
    let Some(req) = requested else {
        return Ok(all.to_vec());
    };
    if req.is_empty() {
        return Err(AppError::invalid("Не выбраны партиции"));
    }
    if let Some(bad) = req.iter().find(|p| !all.contains(p)) {
        return Err(AppError::invalid(format!("В топике нет партиции {bad}")));
    }
    let mut out = req.to_vec();
    out.sort_unstable();
    out.dedup();
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgedPartition {
    pub partition: i32,
    /// Новый low watermark; None — партицию очистить не удалось (см. error).
    pub low_watermark: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// DeleteRecords до high watermark (Offset::End) по выбранным партициям.
pub async fn purge_topic(
    h: &Arc<KafkaHandles>,
    topic: &str,
    partitions: &[i32],
) -> AppResult<Vec<PurgedPartition>> {
    let mut tpl = TopicPartitionList::with_capacity(partitions.len());
    for p in partitions {
        tpl.add_partition_offset(topic, *p, Offset::End)
            .map_err(|e| AppError::internal(e.to_string()))?;
    }
    let started = Instant::now();
    let res = match h.admin.delete_records(&tpl, &admin_opts(h)).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    let mut out: Vec<PurgedPartition> = res
        .elements()
        .iter()
        .map(|e| match (e.error(), e.offset()) {
            (Ok(()), Offset::Offset(o)) => PurgedPartition {
                partition: e.partition(),
                low_watermark: Some(o),
                error: None,
            },
            (Ok(()), other) => PurgedPartition {
                partition: e.partition(),
                low_watermark: None,
                error: Some(format!("Неожиданный оффсет {other:?}")),
            },
            (Err(err), _) => PurgedPartition {
                partition: e.partition(),
                low_watermark: None,
                error: Some(err.to_string()),
            },
        })
        .collect();
    out.sort_by_key(|p| p.partition);
    Ok(out)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicDeleteResult {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AppError>,
}

pub async fn delete_topics(
    h: &Arc<KafkaHandles>,
    names: &[String],
) -> AppResult<Vec<TopicDeleteResult>> {
    if names.is_empty() {
        return Err(AppError::invalid("Не выбраны топики"));
    }
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let started = Instant::now();
    let results = match h.admin.delete_topics(&refs, &admin_opts(h)).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    Ok(results
        .into_iter()
        .map(|r| match r {
            Ok(name) => TopicDeleteResult { name, error: None },
            Err((name, code)) => TopicDeleteResult {
                error: Some(item_error(code, &name, "Не удалось удалить топик")),
                name,
            },
        })
        .collect())
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ConfigChange {
    Set { name: String, value: String },
    /// Сброс к значению по умолчанию.
    Delete { name: String },
}

impl ConfigChange {
    fn name(&self) -> &str {
        match self {
            ConfigChange::Set { name, .. } | ConfigChange::Delete { name } => name,
        }
    }
}

pub fn validate_config_changes(changes: &[ConfigChange]) -> AppResult<()> {
    if changes.is_empty() {
        return Err(AppError::invalid("Нет изменений конфигурации"));
    }
    let mut seen = HashSet::new();
    for c in changes {
        let name = c.name().trim();
        if name.is_empty() {
            return Err(AppError::invalid("Пустое имя параметра конфигурации"));
        }
        if !seen.insert(name) {
            return Err(AppError::invalid(format!(
                "Параметр «{name}» изменяется дважды"
            )));
        }
    }
    Ok(())
}

/// IncrementalAlterConfigs через FFI. Блокирующая.
pub fn alter_topic_configs(h: &KafkaHandles, topic: &str, changes: &[ConfigChange]) -> AppResult<()> {
    validate_config_changes(changes)?;
    let raw: Vec<(String, Option<String>)> = changes
        .iter()
        .map(|c| match c {
            ConfigChange::Set { name, value } => (name.trim().to_string(), Some(value.clone())),
            ConfigChange::Delete { name } => (name.trim().to_string(), None),
        })
        .collect();
    let started = Instant::now();
    match admin_ext::incremental_alter_topic_configs(&h.admin, h.timeout, topic, &raw) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(item)) => Err(item_error(
            item.code,
            &item.message,
            "Не удалось изменить конфигурацию топика",
        )),
        Err(e) => Err(h.kafka_err(started, e)),
    }
}
```

- [ ] **Step 4: Unit-тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::admin`
Expected: PASS, 6 тестов.

- [ ] **Step 5: Интеграционный тест жизненного цикла** — создать `src-tauri/tests/it_topic_ops.rs`:

```rust
mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::TestMsg;
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::admin::{
    ConfigChange, NewTopicSpec, add_partitions, alter_topic_configs, create_topic, delete_topics,
    purge_topic, topic_partition_ids,
};
use kafka_explorer_lib::kafka::configs::describe_configs;
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::kafka::topics::topic_details;
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::admin::ResourceSpecifier;

fn handles(bs: &str) -> Arc<KafkaHandles> {
    Arc::new(KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap())
}

async fn config_value(h: &Arc<KafkaHandles>, topic: &str, name: &str) -> (Option<String>, bool) {
    let entries = describe_configs(h, ResourceSpecifier::Topic(topic)).await.unwrap();
    let e = entries.iter().find(|e| e.name == name).unwrap();
    (e.value.clone(), e.is_default)
}

async fn wait_topic(bs: &str, name: &str, partitions: usize) {
    let (bs, name) = (bs.to_string(), name.to_string());
    tokio::task::spawn_blocking(move || common::wait_for_topic_sync(&bs, &name, partitions))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn topic_lifecycle() {
    let Some(bs) = common::bootstrap() else { return };
    let h = handles(&bs);
    let name = common::unique_topic("it-ops");
    let _cleanup = common::TempTopic::adopt(&bs, name.clone());

    // create
    let spec = NewTopicSpec {
        name: name.clone(),
        partitions: 1,
        replication_factor: 1,
        configs: BTreeMap::from([("retention.ms".to_string(), "600000".to_string())]),
    };
    create_topic(&h, &spec).await.unwrap();
    wait_topic(&bs, &name, 1).await;
    assert_eq!(create_topic(&h, &spec).await.unwrap_err().code, ErrorCode::InvalidInput);
    assert_eq!(config_value(&h, &name, "retention.ms").await.0.as_deref(), Some("600000"));

    // alter: set + delete
    let (h1, n1) = (h.clone(), name.clone());
    tokio::task::spawn_blocking(move || {
        alter_topic_configs(&h1, &n1, &[ConfigChange::Set { name: "cleanup.policy".into(), value: "compact,delete".into() }])
            .unwrap();
        alter_topic_configs(&h1, &n1, &[ConfigChange::Delete { name: "retention.ms".into() }]).unwrap();
        let bad = alter_topic_configs(&h1, &n1, &[ConfigChange::Set { name: "retention.ms".into(), value: "abc".into() }])
            .unwrap_err();
        assert_eq!(bad.code, ErrorCode::InvalidInput);
    })
    .await
    .unwrap();
    assert_eq!(config_value(&h, &name, "cleanup.policy").await.0.as_deref(), Some("compact,delete"));
    assert!(config_value(&h, &name, "retention.ms").await.1);

    // add partitions 1 → 3
    add_partitions(&h, &name, 3).await.unwrap();
    wait_topic(&bs, &name, 3).await;
    let (h1, n1) = (h.clone(), name.clone());
    let ids = tokio::task::spawn_blocking(move || topic_partition_ids(&h1, &n1)).await.unwrap().unwrap();
    assert_eq!(ids, [0, 1, 2]);

    // purge
    let msgs: Vec<TestMsg> = (0..6)
        .map(|i| TestMsg { partition: i % 3, key: None, value: "v", headers: &[] })
        .collect();
    common::produce(&bs, &name, &msgs).await;
    let purged = purge_topic(&h, &name, &[0, 1, 2]).await.unwrap();
    assert_eq!(
        purged.iter().map(|p| (p.partition, p.low_watermark)).collect::<Vec<_>>(),
        [(0, Some(2)), (1, Some(2)), (2, Some(2))]
    );
    let (h1, n1) = (h.clone(), name.clone());
    let details = tokio::task::spawn_blocking(move || topic_details(&h1, &n1)).await.unwrap().unwrap();
    assert_eq!(details.message_count, Some(0));

    // delete: существующий и несуществующий топики — результат по каждому
    let results = delete_topics(&h, &[name.clone(), "it-ops-missing-topic".into()]).await.unwrap();
    assert!(results[0].error.is_none(), "{:?}", results[0]);
    assert_eq!(results[1].error.as_ref().unwrap().code, ErrorCode::NotFound);
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (h1, n1) = (h.clone(), name.clone());
        let r = tokio::task::spawn_blocking(move || topic_details(&h1, &n1)).await.unwrap();
        if matches!(&r, Err(e) if e.code == ErrorCode::NotFound) {
            break;
        }
        assert!(Instant::now() < deadline, "топик не исчез из метаданных");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
```

- [ ] **Step 6: Тест проходит**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_topic_ops`
Expected: PASS.
Если `results[1].error` приходит с кодом `INTERNAL` (брокер вернул для несуществующего топика другой код), выведите `details` в сообщении ассерта и добавьте этот код брокера в ветку `NotFound` функции `item_error` (Task 6) с отдельным unit-тестом.

- [ ] **Step 7: Commit**

```bash
git add src-tauri/src/kafka src-tauri/tests/it_topic_ops.rs
git commit -m "feat(kafka): создание, изменение конфигов, партиции, purge и удаление топиков"
```

---

### Task 9: Команды для топиков и колонка cleanup.policy

**Files:**
- Modify: `src-tauri/src/commands/mod.rs` (`with_pooled`, `pooled_configs` через него)
- Modify: `src-tauri/src/kafka/configs.rs` (`describe_topic_policies`)
- Create: `src-tauri/src/commands/topic_ops.rs`
- Modify: `src-tauri/src/lib.rs` (регистрация команд)
- Modify: `src-tauri/tests/it_topic_ops.rs` (проверка `describe_topic_policies`)

**Interfaces:**
- Consumes: `AppState::guarded`, `guard::ensure_name_confirmed`, всё из `kafka::admin` (Task 8)
- Produces:
  - `async fn commands::with_pooled<T, F, Fut>(state: &AppState, connection_id: String, op: F) -> AppResult<T>` где `F: FnOnce(Arc<KafkaHandles>) -> Fut`, `Fut: Future<Output = AppResult<T>>` — handles берутся и освобождаются на blocking-потоке, при AUTH/TLS-ошибке вытесняются
  - `struct TopicPolicy { pub name: String, pub cleanup_policy: Option<String> }` (Serialize camelCase), `async fn configs::describe_topic_policies(h: &Arc<KafkaHandles>, topics: &[String]) -> AppResult<Vec<TopicPolicy>>`
  - Tauri-команды (аргументы — camelCase на стороне JS):
    - `create_topic(connectionId, spec: NewTopicSpec, confirm: string|null) -> ()`
    - `alter_topic_configs(connectionId, topic, changes: ConfigChange[], confirm) -> ()`
    - `add_partitions(connectionId, topic, newTotal: number, confirm) -> ()`
    - `purge_topic(connectionId, topic, partitions: number[]|null, confirm, typedName: string|null) -> PurgedPartition[]`
    - `delete_topics(connectionId, topics: string[], confirm, typedConfirmation: string|null) -> TopicDeleteResult[]`
    - `get_topic_policies(connectionId, topics: string[]) -> TopicPolicy[]`
  - Для каждой мутирующей команды — функция `*_impl(st: &AppState, …)` с той же логикой (для тестов без Tauri)

- [ ] **Step 1: Тесты защиты команд** — создать `src-tauri/src/commands/topic_ops.rs` с тестами и добавить `pub mod topic_ops;` в `commands/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{Environment, sample_profile};

    /// Подключения: ro (read-only), prod (без read-only), dev. Bootstrap недоступен:
    /// если проверка не сработает до сети, тест увидит CONNECTION_FAILED вместо ожидаемого кода.
    fn state() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        for (id, env, ro) in [("ro", Environment::Dev, true), ("prod", Environment::Prod, false), ("dev", Environment::Dev, false)] {
            let mut p = sample_profile(id);
            p.environment = env;
            p.read_only = ro;
            p.bootstrap_servers = "127.0.0.1:1".into();
            st.connections.upsert(p).unwrap();
        }
        (dir, st)
    }

    fn spec() -> NewTopicSpec {
        NewTopicSpec { name: "t".into(), partitions: 1, replication_factor: 1, configs: Default::default() }
    }

    async fn all_mutations(st: &AppState, id: &str, confirm: Option<String>) -> Vec<ErrorCode> {
        let s = |s: &str| Some(s.to_string());
        vec![
            create_topic_impl(st, id.into(), spec(), confirm.clone()).await.unwrap_err().code,
            alter_topic_configs_impl(st, id.into(), "t".into(), vec![ConfigChange::Delete { name: "x".into() }], confirm.clone())
                .await
                .unwrap_err()
                .code,
            add_partitions_impl(st, id.into(), "t".into(), 5, confirm.clone()).await.unwrap_err().code,
            purge_topic_impl(st, id.into(), "t".into(), None, confirm.clone(), s("t")).await.unwrap_err().code,
            delete_topics_impl(st, id.into(), vec!["t".into()], confirm.clone(), s("t")).await.unwrap_err().code,
        ]
    }

    #[tokio::test]
    async fn read_only_blocks_every_topic_mutation() {
        let (_d, st) = state();
        assert_eq!(all_mutations(&st, "ro", Some("conn-ro".into())).await, vec![ErrorCode::ReadOnly; 5]);
    }

    #[tokio::test]
    async fn prod_requires_connection_name() {
        let (_d, st) = state();
        assert_eq!(all_mutations(&st, "prod", None).await, vec![ErrorCode::ConfirmationRequired; 5]);
    }

    #[tokio::test]
    async fn purge_and_delete_require_object_name() {
        let (_d, st) = state();
        let wrong = Some("другое".to_string());
        let purge = purge_topic_impl(&st, "dev".into(), "t".into(), None, None, wrong.clone()).await;
        assert_eq!(purge.unwrap_err().code, ErrorCode::ConfirmationRequired);
        let one = delete_topics_impl(&st, "dev".into(), vec!["t".into()], None, wrong).await;
        assert_eq!(one.unwrap_err().code, ErrorCode::ConfirmationRequired);
        // Несколько топиков — подтверждается их число.
        let many = delete_topics_impl(&st, "dev".into(), vec!["a".into(), "b".into()], None, Some("a".into())).await;
        assert_eq!(many.unwrap_err().code, ErrorCode::ConfirmationRequired);
    }

    #[tokio::test]
    async fn invalid_input_is_rejected_before_network() {
        let (_d, st) = state();
        let mut bad = spec();
        bad.name = "bad name".into();
        let e = create_topic_impl(&st, "dev".into(), bad, None).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
        let e = alter_topic_configs_impl(&st, "dev".into(), "t".into(), vec![], None).await.unwrap_err();
        assert_eq!(e.code, ErrorCode::InvalidInput);
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib commands::topic_ops`
Expected: FAIL — не найдены `create_topic_impl` и др.

- [ ] **Step 3: `with_pooled`** — в `src-tauri/src/commands/mod.rs` добавить `pub mod topic_ops;`, импорты `use std::future::Future; use std::sync::Arc; use crate::kafka::pool::KafkaHandles;` и заменить `pooled_configs`:

```rust
/// Async-операция на pooled-handles подключения: handles берутся (keychain, создание клиента) и
/// освобождаются на blocking-потоке; при AUTH/TLS-ошибке вытесняются из пула
/// (как в `AppState::with_handles`).
pub(crate) async fn with_pooled<T, F, Fut>(
    state: &AppState,
    connection_id: String,
    op: F,
) -> AppResult<T>
where
    F: FnOnce(Arc<KafkaHandles>) -> Fut,
    Fut: Future<Output = AppResult<T>>,
{
    let (st, id) = (state.clone(), connection_id.clone());
    let h = blocking(move || st.handles(&id)).await?;
    let r = op(h.clone()).await;
    let (st, err) = (state.clone(), r.as_ref().err().cloned());
    let _ = blocking(move || {
        if let Some(e) = &err {
            st.evict_if_client_broken(&connection_id, e);
        }
        drop(h);
        Ok(())
    })
    .await;
    r
}

/// DescribeConfigs на pooled-handles.
pub(crate) async fn pooled_configs(
    state: &AppState,
    connection_id: String,
    spec: ResourceSpecifier<'_>,
) -> AppResult<Vec<ConfigEntryDto>> {
    with_pooled(state, connection_id, |h| async move { describe_configs(&h, spec).await }).await
}
```

- [ ] **Step 4: `describe_topic_policies`** — в `src-tauri/src/kafka/configs.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicPolicy {
    pub name: String,
    /// None — значение не получено (ошибка ресурса или нет прав).
    pub cleanup_policy: Option<String>,
}

/// cleanup.policy для набора топиков одним пакетным DescribeConfigs.
pub async fn describe_topic_policies(
    h: &Arc<KafkaHandles>,
    topics: &[String],
) -> AppResult<Vec<TopicPolicy>> {
    if topics.is_empty() {
        return Ok(Vec::new());
    }
    let specs: Vec<ResourceSpecifier> = topics.iter().map(|t| ResourceSpecifier::Topic(t)).collect();
    let started = Instant::now();
    let opts = AdminOptions::new().request_timeout(Some(h.timeout));
    let results = match h.admin.describe_configs(&specs, &opts).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    // Ответ сопоставляется по имени ресурса: порядок librdkafka не гарантирует.
    let mut found: std::collections::HashMap<String, Option<String>> = Default::default();
    for r in results.into_iter().flatten() {
        if let OwnedResourceSpecifier::Topic(name) = &r.specifier {
            let value = r.get("cleanup.policy").and_then(|e| e.value.clone());
            found.insert(name.clone(), value);
        }
    }
    Ok(topics
        .iter()
        .map(|t| TopicPolicy {
            name: t.clone(),
            cleanup_policy: found.get(t).cloned().flatten(),
        })
        .collect())
}
```
Импорты `configs.rs`: `use rdkafka::admin::OwnedResourceSpecifier;` (рядом с `AdminOptions`, `ResourceSpecifier`).

- [ ] **Step 5: Команды** — над тестами в `commands/topic_ops.rs`:

```rust
use tauri::State;

use super::{blocking, with_pooled};
use crate::error::AppResult;
use crate::guard::ensure_name_confirmed;
use crate::kafka::admin::{
    self, ConfigChange, NewTopicSpec, PurgedPartition, TopicDeleteResult,
};
use crate::kafka::configs::{TopicPolicy, describe_topic_policies};
use crate::state::AppState;

#[tauri::command]
pub async fn create_topic(
    state: State<'_, AppState>,
    connection_id: String,
    spec: NewTopicSpec,
    confirm: Option<String>,
) -> AppResult<()> {
    create_topic_impl(state.inner(), connection_id, spec, confirm).await
}

pub async fn create_topic_impl(
    st: &AppState,
    connection_id: String,
    spec: NewTopicSpec,
    confirm: Option<String>,
) -> AppResult<()> {
    st.guarded(&connection_id, confirm.as_deref())?;
    admin::validate_new_topic(&spec)?;
    with_pooled(st, connection_id, |h| async move { admin::create_topic(&h, &spec).await }).await
}

#[tauri::command]
pub async fn alter_topic_configs(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
    changes: Vec<ConfigChange>,
    confirm: Option<String>,
) -> AppResult<()> {
    alter_topic_configs_impl(state.inner(), connection_id, topic, changes, confirm).await
}

pub async fn alter_topic_configs_impl(
    st: &AppState,
    connection_id: String,
    topic: String,
    changes: Vec<ConfigChange>,
    confirm: Option<String>,
) -> AppResult<()> {
    st.guarded(&connection_id, confirm.as_deref())?;
    admin::validate_config_changes(&changes)?;
    let st = st.clone();
    blocking(move || {
        st.with_handles(&connection_id, |h| admin::alter_topic_configs(h, &topic, &changes))
    })
    .await
}

#[tauri::command]
pub async fn add_partitions(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
    new_total: i32,
    confirm: Option<String>,
) -> AppResult<()> {
    add_partitions_impl(state.inner(), connection_id, topic, new_total, confirm).await
}

pub async fn add_partitions_impl(
    st: &AppState,
    connection_id: String,
    topic: String,
    new_total: i32,
    confirm: Option<String>,
) -> AppResult<()> {
    st.guarded(&connection_id, confirm.as_deref())?;
    let current = partition_ids(st, &connection_id, &topic).await?.len();
    admin::validate_partition_increase(current, new_total)?;
    with_pooled(st, connection_id, |h| async move {
        admin::add_partitions(&h, &topic, new_total as usize).await
    })
    .await
}

#[tauri::command]
pub async fn purge_topic(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
    partitions: Option<Vec<i32>>,
    confirm: Option<String>,
    typed_name: Option<String>,
) -> AppResult<Vec<PurgedPartition>> {
    purge_topic_impl(state.inner(), connection_id, topic, partitions, confirm, typed_name).await
}

pub async fn purge_topic_impl(
    st: &AppState,
    connection_id: String,
    topic: String,
    partitions: Option<Vec<i32>>,
    confirm: Option<String>,
    typed_name: Option<String>,
) -> AppResult<Vec<PurgedPartition>> {
    st.guarded(&connection_id, confirm.as_deref())?;
    ensure_name_confirmed(&topic, typed_name.as_deref())?;
    let all = partition_ids(st, &connection_id, &topic).await?;
    let selected = admin::resolve_partitions(&all, partitions.as_deref())?;
    with_pooled(st, connection_id, |h| async move {
        admin::purge_topic(&h, &topic, &selected).await
    })
    .await
}

#[tauri::command]
pub async fn delete_topics(
    state: State<'_, AppState>,
    connection_id: String,
    topics: Vec<String>,
    confirm: Option<String>,
    typed_confirmation: Option<String>,
) -> AppResult<Vec<TopicDeleteResult>> {
    delete_topics_impl(state.inner(), connection_id, topics, confirm, typed_confirmation).await
}

pub async fn delete_topics_impl(
    st: &AppState,
    connection_id: String,
    topics: Vec<String>,
    confirm: Option<String>,
    typed_confirmation: Option<String>,
) -> AppResult<Vec<TopicDeleteResult>> {
    st.guarded(&connection_id, confirm.as_deref())?;
    // Один топик подтверждается его именем, несколько — их числом.
    let expected = match topics.as_slice() {
        [one] => one.clone(),
        many => many.len().to_string(),
    };
    ensure_name_confirmed(&expected, typed_confirmation.as_deref())?;
    with_pooled(st, connection_id, |h| async move { admin::delete_topics(&h, &topics).await }).await
}

#[tauri::command]
pub async fn get_topic_policies(
    state: State<'_, AppState>,
    connection_id: String,
    topics: Vec<String>,
) -> AppResult<Vec<TopicPolicy>> {
    with_pooled(state.inner(), connection_id, |h| async move {
        describe_topic_policies(&h, &topics).await
    })
    .await
}

async fn partition_ids(st: &AppState, connection_id: &str, topic: &str) -> AppResult<Vec<i32>> {
    let (st, id, topic) = (st.clone(), connection_id.to_string(), topic.to_string());
    blocking(move || st.with_handles(&id, |h| admin::topic_partition_ids(h, &topic))).await
}
```

- [ ] **Step 6: Регистрация** — в `src-tauri/src/lib.rs` в `generate_handler![…]` после `commands::topics::get_topic_configs,`:

```rust
            commands::topic_ops::create_topic,
            commands::topic_ops::alter_topic_configs,
            commands::topic_ops::add_partitions,
            commands::topic_ops::purge_topic,
            commands::topic_ops::delete_topics,
            commands::topic_ops::get_topic_policies,
```

- [ ] **Step 7: IT для `describe_topic_policies`** — в `tests/it_topic_ops.rs` добавить:

```rust
#[tokio::test(flavor = "multi_thread")]
async fn cleanup_policies_in_one_request() {
    let Some(bs) = common::bootstrap() else { return };
    let a = common::TempTopic::create(&bs, "it-policy-a", 1).await;
    let b = common::TempTopic::create(&bs, "it-policy-b", 1).await;
    let h = handles(&bs);
    let (h1, b1) = (h.clone(), b.to_string());
    tokio::task::spawn_blocking(move || {
        alter_topic_configs(&h1, &b1, &[ConfigChange::Set { name: "cleanup.policy".into(), value: "compact".into() }])
    })
    .await
    .unwrap()
    .unwrap();
    let policies = kafka_explorer_lib::kafka::configs::describe_topic_policies(
        &h,
        &[a.to_string(), b.to_string()],
    )
    .await
    .unwrap();
    assert_eq!(policies[0].cleanup_policy.as_deref(), Some("delete"));
    assert_eq!(policies[1].cleanup_policy.as_deref(), Some("compact"));
}
```

- [ ] **Step 8: Тесты проходят**

Run:
```bash
cargo test --manifest-path src-tauri/Cargo.toml --lib commands
KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_topic_ops --test it_topics --test it_cluster
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
```
Expected: PASS (4 новых unit-теста `commands::topic_ops`), clippy чистый.

- [ ] **Step 9: Commit**

```bash
git add src-tauri/src src-tauri/tests/it_topic_ops.rs
git commit -m "feat(commands): команды операций с топиками с защитой и cleanup.policy"
```

---

### Task 10: Модель групп — лаг и план сброса (чистые функции)

**Files:**
- Create: `src-tauri/src/kafka/groups.rs`
- Modify: `src-tauri/src/kafka/mod.rs` (`pub mod groups;`)
- Modify: `src-tauri/src/kafka/errors.rs` (`UnknownMemberId`, `RebalanceInProgress` → `GROUP_NOT_EMPTY` в `item_error`)

**Interfaces:**
- Consumes: `admin_ext::{RawPartition, GroupDescriptionRaw}`, `topics::PartitionBoundsMap`
- Produces (все DTO — `Serialize` camelCase; входные — `Deserialize` camelCase):
  - `struct TopicPartitionRef { pub topic: String, pub partition: i32 }` (Serialize + Deserialize, `Ord`)
  - `struct GroupSummary { pub group_id: String, pub state: String, pub is_simple: bool }`
  - `struct GroupList { pub groups: Vec<GroupSummary>, pub errors: Vec<String> }`
  - `struct GroupMember { pub member_id: String, pub client_id: String, pub host: String, pub assignment: Vec<TopicPartitionRef> }`
  - `struct GroupDescription { pub group_id: String, pub state: String, pub assignor: String, pub members: Vec<GroupMember>, pub error: Option<String> }`, `impl From<GroupDescriptionRaw>`
  - `struct GroupPartitionOffset { pub topic, pub partition, pub committed: Option<i64>, pub log_end: Option<i64>, pub lag: Option<i64>, pub member_client_id: Option<String>, pub error: Option<String> }`
  - `struct GroupLag { pub group_id: String, pub total_lag: Option<i64>, pub topics: Vec<String>, pub error: Option<String> }`
  - `struct TopicGroupLag { pub group_id: String, pub state: String, pub lag: Option<i64>, pub partitions: usize }`, `struct TopicGroups { pub groups: Vec<TopicGroupLag>, pub incomplete: bool }`
  - `enum ResetStrategy { Earliest, Latest, Timestamp { timestamp_ms: i64 }, Offset { offset: i64 }, Shift { delta: i64 } }` — JSON `{"type":"shift","delta":-5}`
  - `enum ResetScope { All, Topics { topics: Vec<String> }, Partitions { partitions: Vec<TopicPartitionRef> } }` — JSON `{"type":"topics","topics":[…]}`
  - `struct ResetRequest { pub group_id: String, pub strategy: ResetStrategy, pub scope: ResetScope }`
  - `struct ResetPreviewRow { pub topic, pub partition, pub current: Option<i64>, pub target: Option<i64>, pub low: Option<i64>, pub high: Option<i64>, pub error: Option<String> }`
  - `struct OffsetTarget { pub topic: String, pub partition: i32, pub offset: i64 }` (Serialize + Deserialize)
  - `struct ResetResult { pub topic: String, pub partition: i32, pub offset: i64, pub error: Option<String> }`
  - `type CommittedMap = BTreeMap<(String, i32), Result<Option<i64>, String>>` — `Ok(None)` = нет коммита
  - `struct PartitionState { pub topic: String, pub partition: i32, pub current: Option<i64>, pub bounds: Result<(i64, i64), String> }`
  - `type TimeLookup = HashMap<(String, i32), Result<Option<i64>, String>>` — `Ok(None)` = нет сообщений новее метки
  - `fn lag(committed: Option<i64>, log_end: Option<i64>) -> Option<i64>`
  - `fn committed_from_raw(raw: &[RawPartition]) -> CommittedMap`
  - `fn build_partition_offsets(committed: &CommittedMap, assigned: &HashMap<(String, i32), String>, bounds: &PartitionBoundsMap) -> Vec<GroupPartitionOffset>`
  - `fn total_lag(rows: &[GroupPartitionOffset]) -> Option<i64>`
  - `fn validate_reset_request(req: &ResetRequest) -> AppResult<()>`
  - `fn plan_reset(states: &[PartitionState], strategy: &ResetStrategy, by_time: &TimeLookup) -> Vec<ResetPreviewRow>`
  - `fn validate_targets(targets: &[OffsetTarget]) -> AppResult<()>`
  - `fn check_resettable(d: &GroupDescription) -> AppResult<()>` — `Empty`/`Dead` разрешены, иначе `GROUP_NOT_EMPTY` (участники в `details`)
  - `fn check_deletable(d: &GroupDescription) -> AppResult<()>` — только `Empty`; `Dead` → `NOT_FOUND`

- [ ] **Step 1: Unit-тесты** — создать `src-tauri/src/kafka/groups.rs` с тестами, добавить `pub mod groups;` в `kafka/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::kafka::admin_ext::{ItemError, RawPartition};
    use rdkafka::types::RDKafkaErrorCode;

    fn key(t: &str, p: i32) -> (String, i32) {
        (t.to_string(), p)
    }

    #[test]
    fn lag_is_unknown_without_both_sides_and_never_negative() {
        assert_eq!(lag(Some(3), Some(10)), Some(7));
        assert_eq!(lag(Some(12), Some(10)), Some(0));
        assert_eq!(lag(None, Some(10)), None);
        assert_eq!(lag(Some(3), None), None);
    }

    #[test]
    fn committed_from_raw_marks_missing_and_errors() {
        let raw = vec![
            RawPartition { topic: "t".into(), partition: 0, offset: 5, error: None },
            RawPartition { topic: "t".into(), partition: 1, offset: -1001, error: None },
            RawPartition {
                topic: "t".into(),
                partition: 2,
                offset: -1001,
                error: Some(ItemError { code: RDKafkaErrorCode::UnknownTopicOrPartition, message: "нет".into() }),
            },
        ];
        let m = committed_from_raw(&raw);
        assert_eq!(m[&key("t", 0)], Ok(Some(5)));
        assert_eq!(m[&key("t", 1)], Ok(None));
        assert!(m[&key("t", 2)].is_err());
    }

    #[test]
    fn partition_offsets_union_committed_and_assigned() {
        let committed: CommittedMap = BTreeMap::from([(key("t", 0), Ok(Some(3))), (key("t", 2), Ok(Some(1)))]);
        let assigned = HashMap::from([(key("t", 1), "client-a".to_string()), (key("t", 0), "client-a".to_string())]);
        let bounds: PartitionBoundsMap = HashMap::from([
            (key("t", 0), Ok((0, 10))),
            (key("t", 1), Ok((0, 4))),
            (key("t", 2), Err("leader not available".to_string())),
        ]);
        let rows = build_partition_offsets(&committed, &assigned, &bounds);
        assert_eq!(rows.len(), 3);
        assert_eq!((rows[0].committed, rows[0].log_end, rows[0].lag), (Some(3), Some(10), Some(7)));
        assert_eq!(rows[0].member_client_id.as_deref(), Some("client-a"));
        // назначена, но без коммита: лаг неизвестен, но это не ошибка
        assert_eq!((rows[1].committed, rows[1].lag, rows[1].error.as_deref()), (None, None, None));
        // ошибка оффсетов не превращается в 0
        assert_eq!((rows[2].log_end, rows[2].lag), (None, None));
        assert!(rows[2].error.as_deref().unwrap().contains("leader not available"));
        assert_eq!(total_lag(&rows), None, "ошибка в любой партиции → итог неизвестен");
        assert_eq!(total_lag(&rows[..2]), Some(7), "партиция без коммита не входит в сумму");
    }

    fn state(p: i32, current: Option<i64>, bounds: Result<(i64, i64), String>) -> PartitionState {
        PartitionState { topic: "t".into(), partition: p, current, bounds }
    }

    fn targets(rows: &[ResetPreviewRow]) -> Vec<Option<i64>> {
        rows.iter().map(|r| r.target).collect()
    }

    #[test]
    fn plan_reset_strategies() {
        let states = vec![state(0, Some(5), Ok((2, 10))), state(1, None, Ok((0, 4)))];
        let none = TimeLookup::new();
        assert_eq!(targets(&plan_reset(&states, &ResetStrategy::Earliest, &none)), [Some(2), Some(0)]);
        assert_eq!(targets(&plan_reset(&states, &ResetStrategy::Latest, &none)), [Some(10), Some(4)]);
        assert_eq!(targets(&plan_reset(&states, &ResetStrategy::Offset { offset: 7 }, &none)), [Some(7), Some(4)]);
        assert_eq!(targets(&plan_reset(&states, &ResetStrategy::Offset { offset: 0 }, &none)), [Some(2), Some(0)]);

        let shifted = plan_reset(&states, &ResetStrategy::Shift { delta: -4 }, &none);
        assert_eq!(shifted[0].target, Some(2), "5 − 4 = 1 → прижато к low = 2");
        assert_eq!(shifted[1].target, None);
        assert!(shifted[1].error.as_deref().unwrap().contains("Нет закоммиченного"));
        let forward = plan_reset(&states, &ResetStrategy::Shift { delta: 100 }, &none);
        assert_eq!(forward[0].target, Some(10));

        let by_time = TimeLookup::from([(key("t", 0), Ok(Some(6))), (key("t", 1), Ok(None))]);
        let ts = plan_reset(&states, &ResetStrategy::Timestamp { timestamp_ms: 1 }, &by_time);
        assert_eq!(targets(&ts), [Some(6), Some(4)], "нет сообщений новее метки → high");
    }

    #[test]
    fn plan_reset_reports_partition_errors() {
        let states = vec![state(0, Some(1), Err("нет лидера".into()))];
        let rows = plan_reset(&states, &ResetStrategy::Earliest, &TimeLookup::new());
        assert_eq!((rows[0].target, rows[0].low, rows[0].high), (None, None, None));
        assert_eq!(rows[0].current, Some(1));
        assert_eq!(rows[0].error.as_deref(), Some("нет лидера"));
        let by_time = TimeLookup::from([(key("t", 0), Err("timeout".to_string()))]);
        let ok_bounds = vec![state(0, Some(1), Ok((0, 5)))];
        let rows = plan_reset(&ok_bounds, &ResetStrategy::Timestamp { timestamp_ms: 1 }, &by_time);
        assert_eq!(rows[0].target, None);
        assert_eq!(rows[0].error.as_deref(), Some("timeout"));
    }

    #[test]
    fn reset_request_json_and_validation() {
        let r: ResetRequest = serde_json::from_value(serde_json::json!({
            "groupId": "g",
            "strategy": {"type": "shift", "delta": -5},
            "scope": {"type": "partitions", "partitions": [{"topic": "t", "partition": 1}]}
        }))
        .unwrap();
        assert!(matches!(r.strategy, ResetStrategy::Shift { delta: -5 }));
        assert!(validate_reset_request(&r).is_ok());
        let all: ResetRequest = serde_json::from_value(serde_json::json!({
            "groupId": "g", "strategy": {"type": "timestamp", "timestampMs": 5}, "scope": {"type": "all"}
        }))
        .unwrap();
        assert!(validate_reset_request(&all).is_ok());

        let bad = |strategy, scope| ResetRequest { group_id: "g".into(), strategy, scope };
        for r in [
            ResetRequest { group_id: " ".into(), strategy: ResetStrategy::Earliest, scope: ResetScope::All },
            bad(ResetStrategy::Offset { offset: -1 }, ResetScope::All),
            bad(ResetStrategy::Timestamp { timestamp_ms: -1 }, ResetScope::All),
            bad(ResetStrategy::Earliest, ResetScope::Topics { topics: vec![] }),
            bad(ResetStrategy::Earliest, ResetScope::Partitions { partitions: vec![] }),
        ] {
            assert_eq!(validate_reset_request(&r).unwrap_err().code, ErrorCode::InvalidInput);
        }
    }

    #[test]
    fn targets_validation() {
        let t = |p: i32, o: i64| OffsetTarget { topic: "t".into(), partition: p, offset: o };
        assert!(validate_targets(&[t(0, 0), t(1, 5)]).is_ok());
        assert!(validate_targets(&[]).is_err());
        assert!(validate_targets(&[t(0, -1)]).is_err());
        assert!(validate_targets(&[t(0, 1), t(0, 2)]).is_err());
    }

    fn desc(state: &str, members: usize) -> GroupDescription {
        GroupDescription {
            group_id: "g".into(),
            state: state.into(),
            assignor: "range".into(),
            members: (0..members)
                .map(|i| GroupMember {
                    member_id: format!("m{i}"),
                    client_id: format!("client-{i}"),
                    host: "/10.0.0.1".into(),
                    assignment: vec![],
                })
                .collect(),
            error: None,
        }
    }

    #[test]
    fn resettable_and_deletable_states() {
        assert!(check_resettable(&desc("Empty", 0)).is_ok());
        assert!(check_resettable(&desc("Dead", 0)).is_ok(), "новая группа — сбросить можно");
        let e = check_resettable(&desc("Stable", 2)).unwrap_err();
        assert_eq!(e.code, ErrorCode::GroupNotEmpty);
        assert!(e.details.as_deref().unwrap().contains("client-1"));
        assert!(check_deletable(&desc("Empty", 0)).is_ok());
        assert_eq!(check_deletable(&desc("Dead", 0)).unwrap_err().code, ErrorCode::NotFound);
        assert_eq!(check_deletable(&desc("PreparingRebalance", 1)).unwrap_err().code, ErrorCode::GroupNotEmpty);
    }

    #[test]
    fn description_error_blocks_checks() {
        let mut d = desc("Empty", 0);
        d.error = Some("нет прав".into());
        assert!(check_resettable(&d).is_err());
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka::groups`
Expected: FAIL — `cannot find function lag`.

- [ ] **Step 3: Реализация** — над тестами в `kafka/groups.rs`:

```rust
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult, ErrorCode};
use crate::kafka::admin_ext::{GroupDescriptionRaw, RawPartition};
use crate::kafka::topics::PartitionBoundsMap;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicPartitionRef {
    pub topic: String,
    pub partition: i32,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupSummary {
    pub group_id: String,
    pub state: String,
    pub is_simple: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupList {
    pub groups: Vec<GroupSummary>,
    /// Частичные ошибки (например, один из брокеров не ответил) — список может быть неполным.
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMember {
    pub member_id: String,
    pub client_id: String,
    pub host: String,
    pub assignment: Vec<TopicPartitionRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDescription {
    pub group_id: String,
    pub state: String,
    pub assignor: String,
    pub members: Vec<GroupMember>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl From<GroupDescriptionRaw> for GroupDescription {
    fn from(r: GroupDescriptionRaw) -> Self {
        Self {
            group_id: r.group_id,
            state: r.state,
            assignor: r.assignor,
            members: r
                .members
                .into_iter()
                .map(|m| {
                    let mut assignment: Vec<TopicPartitionRef> = m
                        .assignment
                        .into_iter()
                        .map(|(topic, partition)| TopicPartitionRef { topic, partition })
                        .collect();
                    assignment.sort();
                    GroupMember {
                        member_id: m.member_id,
                        client_id: m.client_id,
                        host: m.host,
                        assignment,
                    }
                })
                .collect(),
            error: r.error.map(|e| format!("{}: {}", e.code, e.message)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPartitionOffset {
    pub topic: String,
    pub partition: i32,
    /// None — коммита нет (или см. error).
    pub committed: Option<i64>,
    pub log_end: Option<i64>,
    pub lag: Option<i64>,
    pub member_client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupLag {
    pub group_id: String,
    pub total_lag: Option<i64>,
    pub topics: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicGroupLag {
    pub group_id: String,
    pub state: String,
    pub lag: Option<i64>,
    /// Партиций топика с коммитами этой группы.
    pub partitions: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicGroups {
    pub groups: Vec<TopicGroupLag>,
    /// Не все группы проверены за таймаут запроса.
    pub incomplete: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ResetStrategy {
    Earliest,
    Latest,
    Timestamp { timestamp_ms: i64 },
    Offset { offset: i64 },
    Shift { delta: i64 },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum ResetScope {
    /// Все партиции, в которые у группы есть коммиты.
    All,
    Topics { topics: Vec<String> },
    Partitions { partitions: Vec<TopicPartitionRef> },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetRequest {
    pub group_id: String,
    pub strategy: ResetStrategy,
    pub scope: ResetScope,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetPreviewRow {
    pub topic: String,
    pub partition: i32,
    pub current: Option<i64>,
    pub target: Option<i64>,
    pub low: Option<i64>,
    pub high: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OffsetTarget {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub type CommittedMap = BTreeMap<(String, i32), Result<Option<i64>, String>>;
pub type TimeLookup = HashMap<(String, i32), Result<Option<i64>, String>>;

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionState {
    pub topic: String,
    pub partition: i32,
    pub current: Option<i64>,
    pub bounds: Result<(i64, i64), String>,
}

pub fn lag(committed: Option<i64>, log_end: Option<i64>) -> Option<i64> {
    Some((log_end? - committed?).max(0))
}

pub fn committed_from_raw(raw: &[RawPartition]) -> CommittedMap {
    raw.iter()
        .map(|p| {
            let value = match &p.error {
                Some(e) => Err(format!("{}: {}", e.code, e.message)),
                None => Ok((p.offset >= 0).then_some(p.offset)),
            };
            ((p.topic.clone(), p.partition), value)
        })
        .collect()
}

pub fn build_partition_offsets(
    committed: &CommittedMap,
    assigned: &HashMap<(String, i32), String>,
    bounds: &PartitionBoundsMap,
) -> Vec<GroupPartitionOffset> {
    let keys: BTreeSet<&(String, i32)> = committed.keys().chain(assigned.keys()).collect();
    keys.into_iter()
        .map(|k| {
            let (committed, commit_err) = match committed.get(k) {
                Some(Ok(c)) => (*c, None),
                Some(Err(e)) => (None, Some(format!("коммит: {e}"))),
                None => (None, None),
            };
            let (log_end, bounds_err) = match bounds.get(k) {
                Some(Ok((_, high))) => (Some(*high), None),
                Some(Err(e)) => (None, Some(format!("оффсеты: {e}"))),
                None => (None, Some("оффсеты: нет ответа брокера".to_string())),
            };
            let error = match (commit_err, bounds_err) {
                (None, None) => None,
                (a, b) => Some([a, b].into_iter().flatten().collect::<Vec<_>>().join("; ")),
            };
            GroupPartitionOffset {
                topic: k.0.clone(),
                partition: k.1,
                committed,
                log_end,
                lag: lag(committed, log_end),
                member_client_id: assigned.get(k).cloned(),
                error,
            }
        })
        .collect()
}

/// Сумма лагов партиций с коммитом; None, если хоть одна партиция с ошибкой.
pub fn total_lag(rows: &[GroupPartitionOffset]) -> Option<i64> {
    if rows.iter().any(|r| r.error.is_some()) {
        return None;
    }
    Some(rows.iter().filter_map(|r| r.lag).sum())
}

pub fn validate_reset_request(req: &ResetRequest) -> AppResult<()> {
    if req.group_id.trim().is_empty() {
        return Err(AppError::invalid("Не указана группа"));
    }
    match req.strategy {
        ResetStrategy::Offset { offset } if offset < 0 => {
            return Err(AppError::invalid("Оффсет не может быть отрицательным"));
        }
        ResetStrategy::Timestamp { timestamp_ms } if timestamp_ms < 0 => {
            return Err(AppError::invalid("Метка времени не может быть отрицательной"));
        }
        _ => {}
    }
    match &req.scope {
        ResetScope::Topics { topics } if topics.is_empty() => Err(AppError::invalid("Не выбраны топики")),
        ResetScope::Partitions { partitions } if partitions.is_empty() => {
            Err(AppError::invalid("Не выбраны партиции"))
        }
        _ => Ok(()),
    }
}

pub fn plan_reset(
    states: &[PartitionState],
    strategy: &ResetStrategy,
    by_time: &TimeLookup,
) -> Vec<ResetPreviewRow> {
    let mut rows: Vec<ResetPreviewRow> = states
        .iter()
        .map(|s| {
            let row = |target: Result<i64, String>, low: Option<i64>, high: Option<i64>| {
                let (target, error) = match target {
                    Ok(t) => (Some(t), None),
                    Err(e) => (None, Some(e)),
                };
                ResetPreviewRow {
                    topic: s.topic.clone(),
                    partition: s.partition,
                    current: s.current,
                    target,
                    low,
                    high,
                    error,
                }
            };
            let (low, high) = match &s.bounds {
                Ok(b) => *b,
                Err(e) => return row(Err(e.clone()), None, None),
            };
            let clamp = |o: i64| o.clamp(low, high.max(low));
            let target = match strategy {
                ResetStrategy::Earliest => Ok(low),
                ResetStrategy::Latest => Ok(high),
                ResetStrategy::Offset { offset } => Ok(clamp(*offset)),
                ResetStrategy::Shift { delta } => s
                    .current
                    .map(|c| clamp(c.saturating_add(*delta)))
                    .ok_or_else(|| "Нет закоммиченного оффсета — сдвиг невозможен".to_string()),
                ResetStrategy::Timestamp { .. } => match by_time.get(&(s.topic.clone(), s.partition)) {
                    Some(Ok(Some(o))) => Ok(clamp(*o)),
                    // Нет сообщений новее метки — встаём в конец.
                    Some(Ok(None)) | None => Ok(high),
                    Some(Err(e)) => Err(e.clone()),
                },
            };
            row(target, Some(low), Some(high))
        })
        .collect();
    rows.sort_by(|a, b| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    rows
}

pub fn validate_targets(targets: &[OffsetTarget]) -> AppResult<()> {
    if targets.is_empty() {
        return Err(AppError::invalid("Нечего применять: план сброса пуст"));
    }
    let mut seen = HashSet::new();
    for t in targets {
        if t.offset < 0 {
            return Err(AppError::invalid(format!(
                "Отрицательный оффсет для {}/{}",
                t.topic, t.partition
            )));
        }
        if !seen.insert((&t.topic, t.partition)) {
            return Err(AppError::invalid(format!(
                "Партиция {}/{} указана дважды",
                t.topic, t.partition
            )));
        }
    }
    Ok(())
}

fn not_empty_error(d: &GroupDescription, action: &str) -> AppError {
    let members = d
        .members
        .iter()
        .map(|m| format!("{} ({})", m.client_id, m.host))
        .collect::<Vec<_>>()
        .join(", ");
    AppError::new(
        ErrorCode::GroupNotEmpty,
        format!(
            "{action} возможно только для группы без участников (состояние сейчас: {})",
            d.state
        ),
    )
    .with_details(format!("Активные участники: {members}"))
}

fn description_ok(d: &GroupDescription) -> AppResult<()> {
    match &d.error {
        Some(e) => Err(AppError::internal(format!("Не удалось получить состояние группы «{}»", d.group_id))
            .with_details(e.clone())),
        None => Ok(()),
    }
}

pub fn check_resettable(d: &GroupDescription) -> AppResult<()> {
    description_ok(d)?;
    match d.state.as_str() {
        "Empty" | "Dead" => Ok(()),
        _ => Err(not_empty_error(d, "Сброс оффсетов")),
    }
}

pub fn check_deletable(d: &GroupDescription) -> AppResult<()> {
    description_ok(d)?;
    match d.state.as_str() {
        "Empty" => Ok(()),
        "Dead" => Err(AppError::not_found(format!("Группа «{}» не найдена", d.group_id))),
        _ => Err(not_empty_error(d, "Удаление")),
    }
}
```

В `src-tauri/src/kafka/errors.rs` в `item_error` заменить ветку `C::NonEmptyGroup => …` на:
```rust
        C::NonEmptyGroup | C::UnknownMemberId | C::RebalanceInProgress => ErrorCode::GroupNotEmpty,
```
и добавить в тест `item_errors_map_to_app_codes` пары `(C::UnknownMemberId, E::GroupNotEmpty)` и `(C::RebalanceInProgress, E::GroupNotEmpty)` (брокер так отвечает на коммит от не-участника активной группы).

- [ ] **Step 4: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib kafka`
Expected: PASS (8 новых тестов `kafka::groups`).

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/kafka
git commit -m "feat(groups): модель групп, расчёт лага и план сброса оффсетов"
```

---

### Task 11: Сетевые операции с группами

**Files:**
- Create: `src-tauri/src/kafka/group_ops.rs`
- Modify: `src-tauri/src/kafka/mod.rs` (`pub mod group_ops;`)
- Modify: `src-tauri/tests/common/mod.rs` (перенести `join_group` из `it_admin_ext.rs`)
- Modify: `src-tauri/tests/it_admin_ext.rs` (использовать `common::join_group`)
- Create: `src-tauri/tests/it_groups.rs`

**Interfaces:**
- Consumes: `admin_ext::*` (Task 7), `groups::*` (Task 10), `topics::fetch_bounds`, `admin::topic_partition_ids`, `errors::item_error`
- Produces (все **блокирующие**, кроме `delete_group`):
  - `fn list_groups(h: &KafkaHandles) -> AppResult<GroupList>` (сортировка по id)
  - `fn describe_groups(h: &KafkaHandles, ids: &[String]) -> AppResult<Vec<GroupDescription>>`
  - `fn describe_group(h: &KafkaHandles, id: &str) -> AppResult<GroupDescription>`
  - `fn group_offsets(h: &KafkaHandles, group: &str) -> AppResult<Vec<GroupPartitionOffset>>`
  - `fn groups_lag(h: &KafkaHandles, ids: &[String]) -> AppResult<Vec<GroupLag>>`
  - `fn topic_groups(h: &KafkaHandles, topic: &str) -> AppResult<TopicGroups>` (общий дедлайн — `h.timeout`)
  - `fn preview_reset(h: &KafkaHandles, req: &ResetRequest) -> AppResult<Vec<ResetPreviewRow>>`
  - `fn reset_offsets(h: &KafkaHandles, group: &str, targets: &[OffsetTarget]) -> AppResult<Vec<ResetResult>>` — перед применением `check_resettable`
  - `async fn delete_group(h: &Arc<KafkaHandles>, group: &str) -> AppResult<()>` — вызывающий заранее проверяет `check_deletable(&describe_group(..))`
  - в `tests/common`: `fn join_group(bs: &str, group: &str, topic: &str) -> BaseConsumer` — участник с назначением (client.id `it-member`)

- [ ] **Step 1: Перенести `join_group` в `tests/common/mod.rs`**

Вырезать функцию `join_group` из `tests/it_admin_ext.rs`, вставить в `tests/common/mod.rs` как `pub fn join_group(...)` (импорт `use std::time::Instant;` уже есть, `BaseConsumer`, `Consumer`, `ClientConfig` — тоже). В `it_admin_ext.rs` вызвать `common::join_group(&bs2, &g, &t)` и удалить ставшие ненужными импорты `BaseConsumer`, `Consumer`, `ClientConfig`, `Instant`.

- [ ] **Step 2: Интеграционные тесты** — создать `src-tauri/tests/it_groups.rs`:

```rust
mod common;

use std::sync::Arc;

use common::TestMsg;
use kafka_explorer_lib::error::ErrorCode;
use kafka_explorer_lib::kafka::group_ops::{
    delete_group, describe_group, group_offsets, groups_lag, list_groups, preview_reset,
    reset_offsets, topic_groups,
};
use kafka_explorer_lib::kafka::groups::{
    OffsetTarget, ResetRequest, ResetScope, ResetStrategy, check_deletable,
};
use kafka_explorer_lib::kafka::pool::KafkaHandles;
use kafka_explorer_lib::secrets::MemoryStore;
use rdkafka::consumer::Consumer;

fn handles(bs: &str) -> Arc<KafkaHandles> {
    Arc::new(KafkaHandles::create(&common::plaintext_profile(bs), &MemoryStore::default()).unwrap())
}

/// Топик с 2 партициями: 5 сообщений в p0, 3 в p1; группа закоммитила (p0: 2, p1: 3) → лаг 3.
async fn seeded(bs: &str) -> (common::TempTopic, common::TempGroup) {
    let topic = common::TempTopic::create(bs, "it-grp", 2).await;
    let msgs: Vec<TestMsg> = (0..8)
        .map(|i| TestMsg { partition: if i < 5 { 0 } else { 1 }, key: None, value: "v", headers: &[] })
        .collect();
    common::produce(bs, &topic, &msgs).await;
    let group = common::TempGroup::new(bs, "it-grp");
    common::commit_group_offsets(bs, &group, &topic, &[(0, 2), (1, 3)]);
    (topic, group)
}

fn request(group: &str, strategy: ResetStrategy) -> ResetRequest {
    ResetRequest { group_id: group.into(), strategy, scope: ResetScope::All }
}

#[tokio::test(flavor = "multi_thread")]
async fn offsets_lag_and_topic_groups() {
    let Some(bs) = common::bootstrap() else { return };
    let (topic, group) = seeded(&bs).await;
    let h = handles(&bs);
    let (t, g) = (topic.to_string(), group.to_string());
    tokio::task::spawn_blocking(move || {
        let list = list_groups(&h).unwrap();
        assert!(list.groups.iter().any(|s| s.group_id == g && s.state == "Empty"));

        let rows = group_offsets(&h, &g).unwrap();
        let view: Vec<_> = rows.iter().map(|r| (r.partition, r.committed, r.log_end, r.lag)).collect();
        assert_eq!(view, [(0, Some(2), Some(5), Some(3)), (1, Some(3), Some(3), Some(0))]);

        let lag = groups_lag(&h, std::slice::from_ref(&g)).unwrap();
        assert_eq!(lag[0].total_lag, Some(3));
        assert_eq!(lag[0].topics, [t.clone()]);

        let tg = topic_groups(&h, &t).unwrap();
        assert!(!tg.incomplete);
        let mine = tg.groups.iter().find(|x| x.group_id == g).expect("группа топика");
        assert_eq!((mine.lag, mine.partitions), (Some(3), 2));
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn preview_and_reset() {
    let Some(bs) = common::bootstrap() else { return };
    let (topic, group) = seeded(&bs).await;
    let h = handles(&bs);
    let (t, g) = (topic.to_string(), group.to_string());
    tokio::task::spawn_blocking(move || {
        let targets = |s: ResetStrategy| -> Vec<Option<i64>> {
            preview_reset(&h, &request(&g, s)).unwrap().iter().map(|r| r.target).collect()
        };
        assert_eq!(targets(ResetStrategy::Earliest), [Some(0), Some(0)]);
        assert_eq!(targets(ResetStrategy::Latest), [Some(5), Some(3)]);
        assert_eq!(targets(ResetStrategy::Shift { delta: -1 }), [Some(1), Some(2)]);
        assert_eq!(targets(ResetStrategy::Offset { offset: 4 }), [Some(4), Some(3)]);
        assert_eq!(targets(ResetStrategy::Timestamp { timestamp_ms: 32_503_680_000_000 }), [Some(5), Some(3)]);
        assert_eq!(targets(ResetStrategy::Timestamp { timestamp_ms: 0 }), [Some(0), Some(0)]);

        // Область «топики» включает и партиции без коммитов.
        let scoped = preview_reset(
            &h,
            &ResetRequest { group_id: g.clone(), strategy: ResetStrategy::Earliest, scope: ResetScope::Topics { topics: vec![t.clone()] } },
        )
        .unwrap();
        assert_eq!(scoped.len(), 2);

        let plan = preview_reset(&h, &request(&g, ResetStrategy::Earliest)).unwrap();
        let apply: Vec<OffsetTarget> = plan
            .iter()
            .map(|r| OffsetTarget { topic: r.topic.clone(), partition: r.partition, offset: r.target.unwrap() })
            .collect();
        let res = reset_offsets(&h, &g, &apply).unwrap();
        assert!(res.iter().all(|r| r.error.is_none()), "{res:?}");
        let committed: Vec<_> = group_offsets(&h, &g).unwrap().iter().map(|r| r.committed).collect();
        assert_eq!(committed, [Some(0), Some(0)]);
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn active_group_cannot_be_reset_or_deleted() {
    let Some(bs) = common::bootstrap() else { return };
    let (topic, group) = seeded(&bs).await;
    let h = handles(&bs);
    let (t, g, bs2) = (topic.to_string(), group.to_string(), bs.clone());
    let h2 = h.clone();
    tokio::task::spawn_blocking(move || {
        let member = common::join_group(&bs2, &g, &t);
        let target = [OffsetTarget { topic: t.clone(), partition: 0, offset: 0 }];
        let e = reset_offsets(&h2, &g, &target).unwrap_err();
        assert_eq!(e.code, ErrorCode::GroupNotEmpty);
        assert!(e.details.unwrap().contains("it-member"));
        let d = describe_group(&h2, &g).unwrap();
        assert_eq!(check_deletable(&d).unwrap_err().code, ErrorCode::GroupNotEmpty);
        member.unsubscribe();
        drop(member);
    })
    .await
    .unwrap();

    // Участник ушёл — группа Empty, удаляется.
    let g = group.to_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let (h1, g1) = (h.clone(), g.clone());
        let d = tokio::task::spawn_blocking(move || describe_group(&h1, &g1)).await.unwrap().unwrap();
        if d.state == "Empty" {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "группа не опустела: {}", d.state);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    delete_group(&h, &g).await.unwrap();
    let (h1, g1) = (h.clone(), g.clone());
    let list = tokio::task::spawn_blocking(move || list_groups(&h1)).await.unwrap().unwrap();
    assert!(!list.groups.iter().any(|s| s.group_id == g1));
}
```

- [ ] **Step 3: Убедиться в падении**

Run: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_groups`
Expected: FAIL — нет модуля `group_ops`.

- [ ] **Step 4: Реализация** — создать `src-tauri/src/kafka/group_ops.rs`, добавить `pub mod group_ops;` в `kafka/mod.rs`:

```rust
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::admin::AdminOptions;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaResult;
use rdkafka::{Offset, TopicPartitionList};

use crate::error::{AppError, AppResult};
use crate::kafka::admin::topic_partition_ids;
use crate::kafka::admin_ext;
use crate::kafka::context::CapturingContext;
use crate::kafka::errors::{admin_error, item_error};
use crate::kafka::groups::*;
use crate::kafka::pool::KafkaHandles;
use crate::kafka::topics::{PartitionBoundsMap, fetch_bounds};

type Key = (String, i32);

pub fn list_groups(h: &KafkaHandles) -> AppResult<GroupList> {
    let started = Instant::now();
    let (groups, errors) =
        admin_ext::list_consumer_groups(&h.admin, h.timeout).map_err(|e| h.kafka_err(started, e))?;
    let mut groups: Vec<GroupSummary> = groups
        .into_iter()
        .map(|g| GroupSummary {
            group_id: g.group_id,
            state: g.state,
            is_simple: g.is_simple,
        })
        .collect();
    groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    Ok(GroupList {
        groups,
        errors: errors
            .into_iter()
            .map(|e| format!("{}: {}", e.code, e.message))
            .collect(),
    })
}

pub fn describe_groups(h: &KafkaHandles, ids: &[String]) -> AppResult<Vec<GroupDescription>> {
    let started = Instant::now();
    let raw = admin_ext::describe_consumer_groups(&h.admin, h.timeout, ids)
        .map_err(|e| h.kafka_err(started, e))?;
    Ok(raw.into_iter().map(GroupDescription::from).collect())
}

pub fn describe_group(h: &KafkaHandles, id: &str) -> AppResult<GroupDescription> {
    describe_groups(h, &[id.to_string()])?
        .into_iter()
        .next()
        .ok_or_else(|| AppError::internal("Пустой ответ DescribeConsumerGroups"))
}

fn committed(h: &KafkaHandles, group: &str) -> AppResult<CommittedMap> {
    let started = Instant::now();
    match admin_ext::list_consumer_group_offsets(&h.admin, h.timeout, group) {
        Ok(Ok(raw)) => Ok(committed_from_raw(&raw)),
        Ok(Err(item)) => Err(item_error(
            item.code,
            &item.message,
            "Не удалось получить оффсеты группы",
        )),
        Err(e) => Err(h.kafka_err(started, e)),
    }
}

fn bounds(h: &KafkaHandles, keys: &[Key]) -> AppResult<PartitionBoundsMap> {
    let started = Instant::now();
    fetch_bounds(&h.consumer, keys, h.timeout).map_err(|e| h.kafka_err(started, e))
}

pub fn group_offsets(h: &KafkaHandles, group: &str) -> AppResult<Vec<GroupPartitionOffset>> {
    let desc = describe_group(h, group)?;
    let committed = committed(h, group)?;
    let assigned: HashMap<Key, String> = desc
        .members
        .iter()
        .flat_map(|m| {
            m.assignment
                .iter()
                .map(move |a| ((a.topic.clone(), a.partition), m.client_id.clone()))
        })
        .collect();
    let keys: Vec<Key> = committed
        .keys()
        .chain(assigned.keys())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let b = bounds(h, &keys)?;
    Ok(build_partition_offsets(&committed, &assigned, &b))
}

pub fn groups_lag(h: &KafkaHandles, ids: &[String]) -> AppResult<Vec<GroupLag>> {
    let per_group: Vec<(String, AppResult<CommittedMap>)> =
        ids.iter().map(|g| (g.clone(), committed(h, g))).collect();
    let keys: Vec<Key> = per_group
        .iter()
        .filter_map(|(_, c)| c.as_ref().ok())
        .flat_map(|c| c.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let b = bounds(h, &keys)?;
    Ok(per_group
        .into_iter()
        .map(|(group_id, c)| match c {
            Ok(c) => {
                let rows = build_partition_offsets(&c, &HashMap::new(), &b);
                let topics: BTreeSet<String> = rows.iter().map(|r| r.topic.clone()).collect();
                GroupLag {
                    group_id,
                    total_lag: total_lag(&rows),
                    topics: topics.into_iter().collect(),
                    error: None,
                }
            }
            Err(e) => GroupLag {
                group_id,
                total_lag: None,
                topics: Vec::new(),
                error: Some(e.message),
            },
        })
        .collect())
}

/// Группы с коммитами в топик и их лаг по нему. Перебор групп ограничен таймаутом запроса.
pub fn topic_groups(h: &KafkaHandles, topic: &str) -> AppResult<TopicGroups> {
    let deadline = Instant::now() + h.timeout;
    let list = list_groups(h)?;
    let mut found: Vec<(GroupSummary, CommittedMap)> = Vec::new();
    let mut incomplete = !list.errors.is_empty();
    for g in list.groups {
        if Instant::now() >= deadline {
            incomplete = true;
            break;
        }
        // Ошибка одной группы (например, нет прав) не мешает остальным.
        let Ok(c) = committed(h, &g.group_id) else {
            incomplete = true;
            continue;
        };
        let mine: CommittedMap = c.into_iter().filter(|((t, _), _)| t == topic).collect();
        if !mine.is_empty() {
            found.push((g, mine));
        }
    }
    let keys: Vec<Key> = found
        .iter()
        .flat_map(|(_, c)| c.keys().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let b = bounds(h, &keys)?;
    Ok(TopicGroups {
        groups: found
            .into_iter()
            .map(|(g, c)| {
                let rows = build_partition_offsets(&c, &HashMap::new(), &b);
                TopicGroupLag {
                    group_id: g.group_id,
                    state: g.state,
                    lag: total_lag(&rows),
                    partitions: rows.len(),
                }
            })
            .collect(),
        incomplete,
    })
}

fn scope_keys(h: &KafkaHandles, scope: &ResetScope, committed: &CommittedMap) -> AppResult<Vec<Key>> {
    let keys: BTreeSet<Key> = match scope {
        ResetScope::All => {
            if committed.is_empty() {
                return Err(AppError::invalid(
                    "У группы нет закоммиченных оффсетов — выберите топики для сброса",
                ));
            }
            committed.keys().cloned().collect()
        }
        ResetScope::Topics { topics } => {
            let mut out = BTreeSet::new();
            for t in topics {
                for p in topic_partition_ids(h, t)? {
                    out.insert((t.clone(), p));
                }
            }
            out
        }
        ResetScope::Partitions { partitions } => partitions
            .iter()
            .map(|p| (p.topic.clone(), p.partition))
            .collect(),
    };
    Ok(keys.into_iter().collect())
}

/// Оффсеты по времени для каждой партиции: Ok(None) — сообщений новее метки нет.
fn offsets_for_time(
    c: &BaseConsumer<CapturingContext>,
    keys: &[Key],
    ts: i64,
    timeout: Duration,
) -> KafkaResult<TimeLookup> {
    let mut tpl = TopicPartitionList::with_capacity(keys.len());
    for (t, p) in keys {
        tpl.add_partition_offset(t, *p, Offset::Offset(ts))?;
    }
    let res = c.offsets_for_times(tpl, timeout)?;
    Ok(res
        .elements()
        .iter()
        .map(|e| {
            let v = match (e.error(), e.offset()) {
                (Err(err), _) => Err(err.to_string()),
                (Ok(()), Offset::Offset(o)) if o >= 0 => Ok(Some(o)),
                (Ok(()), _) => Ok(None),
            };
            ((e.topic().to_string(), e.partition()), v)
        })
        .collect())
}

pub fn preview_reset(h: &KafkaHandles, req: &ResetRequest) -> AppResult<Vec<ResetPreviewRow>> {
    validate_reset_request(req)?;
    let committed = committed(h, &req.group_id)?;
    let keys = scope_keys(h, &req.scope, &committed)?;
    let b = bounds(h, &keys)?;
    let by_time = match req.strategy {
        ResetStrategy::Timestamp { timestamp_ms } => {
            let started = Instant::now();
            offsets_for_time(&h.consumer, &keys, timestamp_ms, h.timeout)
                .map_err(|e| h.kafka_err(started, e))?
        }
        _ => TimeLookup::new(),
    };
    let states: Vec<PartitionState> = keys
        .iter()
        .map(|k| PartitionState {
            topic: k.0.clone(),
            partition: k.1,
            current: committed.get(k).and_then(|c| c.clone().ok().flatten()),
            bounds: b
                .get(k)
                .cloned()
                .unwrap_or_else(|| Err("нет ответа брокера".to_string())),
        })
        .collect();
    Ok(plan_reset(&states, &req.strategy, &by_time))
}

/// Применяет ровно переданный план (тот, что пользователь видел в предпросмотре).
pub fn reset_offsets(h: &KafkaHandles, group: &str, targets: &[OffsetTarget]) -> AppResult<Vec<ResetResult>> {
    validate_targets(targets)?;
    check_resettable(&describe_group(h, group)?)?;
    let raw: Vec<(String, i32, i64)> = targets
        .iter()
        .map(|t| (t.topic.clone(), t.partition, t.offset))
        .collect();
    let started = Instant::now();
    let applied = match admin_ext::alter_consumer_group_offsets(&h.admin, h.timeout, group, &raw) {
        Ok(Ok(parts)) => parts,
        Ok(Err(item)) => {
            return Err(item_error(item.code, &item.message, "Не удалось сбросить оффсеты"));
        }
        Err(e) => return Err(h.kafka_err(started, e)),
    };
    let by_key: BTreeMap<Key, Option<String>> = applied
        .into_iter()
        .map(|p| ((p.topic, p.partition), p.error.map(|e| e.message)))
        .collect();
    Ok(targets
        .iter()
        .map(|t| ResetResult {
            topic: t.topic.clone(),
            partition: t.partition,
            offset: t.offset,
            error: by_key
                .get(&(t.topic.clone(), t.partition))
                .cloned()
                .unwrap_or_else(|| Some("брокер не подтвердил партицию".to_string())),
        })
        .collect())
}

/// Удаление группы. Состояние Empty проверяет вызывающий (`check_deletable`); если за это время
/// в группу кто-то вошёл, брокер ответит NonEmptyGroup → GROUP_NOT_EMPTY.
pub async fn delete_group(h: &Arc<KafkaHandles>, group: &str) -> AppResult<()> {
    let opts = AdminOptions::new()
        .request_timeout(Some(h.timeout))
        .operation_timeout(Some(h.timeout));
    let started = Instant::now();
    let results = match h.admin.delete_groups(&[group], &opts).await {
        Ok(r) => r,
        Err(e) => return Err(admin_error(h, started, e).await),
    };
    for r in results {
        r.map_err(|(name, code)| item_error(code, &name, "Не удалось удалить группу"))?;
    }
    Ok(())
}
```

- [ ] **Step 5: Тесты проходят**

Run:
```bash
KE_IT_BOOTSTRAP=localhost:9092 cargo test --manifest-path src-tauri/Cargo.toml --test it_groups --test it_admin_ext
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
```
Expected: PASS (3 + 4 теста), clippy чистый.

- [ ] **Step 6: Commit**

```bash
git add src-tauri/src/kafka src-tauri/tests
git commit -m "feat(groups): лаг, группы топика, предпросмотр и сброс оффсетов, удаление группы"
```

---

### Task 12: Команды для групп

**Files:**
- Create: `src-tauri/src/commands/groups.rs`
- Modify: `src-tauri/src/commands/mod.rs` (`pub mod groups;`)
- Modify: `src-tauri/src/lib.rs` (регистрация)

**Interfaces:**
- Consumes: `group_ops::*`, `groups::*`, `AppState::{guarded, with_handles}`, `with_pooled`, `ensure_name_confirmed`
- Produces — Tauri-команды:
  - `list_groups(connectionId) -> GroupList`
  - `describe_groups(connectionId, groupIds: string[]) -> GroupDescription[]`
  - `get_group_offsets(connectionId, groupId) -> GroupPartitionOffset[]`
  - `get_groups_lag(connectionId, groupIds: string[]) -> GroupLag[]`
  - `get_topic_groups(connectionId, topic) -> TopicGroups`
  - `preview_offset_reset(connectionId, request: ResetRequest) -> ResetPreviewRow[]` (не мутирующая)
  - `reset_offsets(connectionId, groupId, offsets: OffsetTarget[], confirm) -> ResetResult[]` (guard)
  - `delete_group(connectionId, groupId, confirm, typedName) -> ()` (guard + имя группы)
  - `reset_offsets_impl`, `delete_group_impl(st: &AppState, …)` — для тестов

- [ ] **Step 1: Тесты** — создать `src-tauri/src/commands/groups.rs` с тестами, добавить `pub mod groups;` в `commands/mod.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::secrets::MemoryStore;
    use crate::storage::connections::{Environment, sample_profile};

    fn state() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
        for (id, env, ro) in [("ro", Environment::Dev, true), ("prod", Environment::Prod, false), ("dev", Environment::Dev, false)] {
            let mut p = sample_profile(id);
            p.environment = env;
            p.read_only = ro;
            p.bootstrap_servers = "127.0.0.1:1".into();
            st.connections.upsert(p).unwrap();
        }
        (dir, st)
    }

    fn target() -> Vec<OffsetTarget> {
        vec![OffsetTarget { topic: "t".into(), partition: 0, offset: 0 }]
    }

    #[tokio::test]
    async fn group_mutations_are_guarded() {
        let (_d, st) = state();
        let g = || "g".to_string();
        let ro_reset = reset_offsets_impl(&st, "ro".into(), g(), target(), Some("conn-ro".into())).await;
        assert_eq!(ro_reset.unwrap_err().code, ErrorCode::ReadOnly);
        let ro_delete = delete_group_impl(&st, "ro".into(), g(), Some("conn-ro".into()), Some(g())).await;
        assert_eq!(ro_delete.unwrap_err().code, ErrorCode::ReadOnly);
        let prod_reset = reset_offsets_impl(&st, "prod".into(), g(), target(), None).await;
        assert_eq!(prod_reset.unwrap_err().code, ErrorCode::ConfirmationRequired);
        let prod_delete = delete_group_impl(&st, "prod".into(), g(), None, Some(g())).await;
        assert_eq!(prod_delete.unwrap_err().code, ErrorCode::ConfirmationRequired);
        let wrong_name = delete_group_impl(&st, "dev".into(), g(), None, Some("x".into())).await;
        assert_eq!(wrong_name.unwrap_err().code, ErrorCode::ConfirmationRequired);
        let empty_plan = reset_offsets_impl(&st, "dev".into(), g(), vec![], None).await;
        assert_eq!(empty_plan.unwrap_err().code, ErrorCode::InvalidInput);
    }
}
```

- [ ] **Step 2: Убедиться в падении**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib commands::groups`
Expected: FAIL — не найдены `reset_offsets_impl`, `delete_group_impl`.

- [ ] **Step 3: Реализация** — над тестами в `commands/groups.rs`:

```rust
use std::sync::Arc;

use tauri::State;

use super::{blocking, with_pooled};
use crate::error::AppResult;
use crate::guard::ensure_name_confirmed;
use crate::kafka::group_ops;
use crate::kafka::groups::{
    GroupDescription, GroupLag, GroupList, GroupPartitionOffset, OffsetTarget, ResetPreviewRow,
    ResetRequest, ResetResult, TopicGroups, check_deletable, validate_reset_request,
    validate_targets,
};
use crate::state::AppState;

/// Блокирующая операция на pooled-handles (см. `AppState::with_handles`).
async fn on_handles<T: Send + 'static>(
    st: &AppState,
    connection_id: String,
    op: impl FnOnce(&crate::kafka::pool::KafkaHandles) -> AppResult<T> + Send + 'static,
) -> AppResult<T> {
    let st = st.clone();
    blocking(move || st.with_handles(&connection_id, op)).await
}

#[tauri::command]
pub async fn list_groups(state: State<'_, AppState>, connection_id: String) -> AppResult<GroupList> {
    on_handles(state.inner(), connection_id, group_ops::list_groups).await
}

#[tauri::command]
pub async fn describe_groups(
    state: State<'_, AppState>,
    connection_id: String,
    group_ids: Vec<String>,
) -> AppResult<Vec<GroupDescription>> {
    on_handles(state.inner(), connection_id, move |h| group_ops::describe_groups(h, &group_ids)).await
}

#[tauri::command]
pub async fn get_group_offsets(
    state: State<'_, AppState>,
    connection_id: String,
    group_id: String,
) -> AppResult<Vec<GroupPartitionOffset>> {
    on_handles(state.inner(), connection_id, move |h| group_ops::group_offsets(h, &group_id)).await
}

#[tauri::command]
pub async fn get_groups_lag(
    state: State<'_, AppState>,
    connection_id: String,
    group_ids: Vec<String>,
) -> AppResult<Vec<GroupLag>> {
    on_handles(state.inner(), connection_id, move |h| group_ops::groups_lag(h, &group_ids)).await
}

#[tauri::command]
pub async fn get_topic_groups(
    state: State<'_, AppState>,
    connection_id: String,
    topic: String,
) -> AppResult<TopicGroups> {
    on_handles(state.inner(), connection_id, move |h| group_ops::topic_groups(h, &topic)).await
}

#[tauri::command]
pub async fn preview_offset_reset(
    state: State<'_, AppState>,
    connection_id: String,
    request: ResetRequest,
) -> AppResult<Vec<ResetPreviewRow>> {
    validate_reset_request(&request)?;
    on_handles(state.inner(), connection_id, move |h| group_ops::preview_reset(h, &request)).await
}

#[tauri::command]
pub async fn reset_offsets(
    state: State<'_, AppState>,
    connection_id: String,
    group_id: String,
    offsets: Vec<OffsetTarget>,
    confirm: Option<String>,
) -> AppResult<Vec<ResetResult>> {
    reset_offsets_impl(state.inner(), connection_id, group_id, offsets, confirm).await
}

pub async fn reset_offsets_impl(
    st: &AppState,
    connection_id: String,
    group_id: String,
    offsets: Vec<OffsetTarget>,
    confirm: Option<String>,
) -> AppResult<Vec<ResetResult>> {
    st.guarded(&connection_id, confirm.as_deref())?;
    validate_targets(&offsets)?;
    on_handles(st, connection_id, move |h| group_ops::reset_offsets(h, &group_id, &offsets)).await
}

#[tauri::command]
pub async fn delete_group(
    state: State<'_, AppState>,
    connection_id: String,
    group_id: String,
    confirm: Option<String>,
    typed_name: Option<String>,
) -> AppResult<()> {
    delete_group_impl(state.inner(), connection_id, group_id, confirm, typed_name).await
}

pub async fn delete_group_impl(
    st: &AppState,
    connection_id: String,
    group_id: String,
    confirm: Option<String>,
    typed_name: Option<String>,
) -> AppResult<()> {
    st.guarded(&connection_id, confirm.as_deref())?;
    ensure_name_confirmed(&group_id, typed_name.as_deref())?;
    let g = group_id.clone();
    let desc = on_handles(st, connection_id.clone(), move |h| group_ops::describe_group(h, &g)).await?;
    check_deletable(&desc)?;
    with_pooled(st, connection_id, |h: Arc<_>| async move {
        group_ops::delete_group(&h, &group_id).await
    })
    .await
}
```

- [ ] **Step 4: Регистрация** — в `lib.rs` после команд `topic_ops`:

```rust
            commands::groups::list_groups,
            commands::groups::describe_groups,
            commands::groups::get_group_offsets,
            commands::groups::get_groups_lag,
            commands::groups::get_topic_groups,
            commands::groups::preview_offset_reset,
            commands::groups::reset_offsets,
            commands::groups::delete_group,
```

- [ ] **Step 5: Тесты проходят**

Run: `cargo test --manifest-path src-tauri/Cargo.toml --lib && cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings`
Expected: PASS, clippy чистый.

- [ ] **Step 6: Commit**

```bash
git add src-tauri/src
git commit -m "feat(commands): команды consumer groups с защитой сброса и удаления"
```

---

### Task 13: API фронтенда, useConnection и ConfirmDialog

**Files:**
- Modify: `src/api/types.ts`, `src/api/client.ts`, `src/api/client.test.ts`
- Create: `src/lib/useConnection.ts`
- Create: `src/components/common/ConfirmDialog.tsx`, `src/components/common/ConfirmDialog.test.tsx`
- Modify: `src/styles.css`

**Interfaces:**
- Consumes: Tauri-команды Tasks 9 и 12
- Produces:
  - TS-типы (зеркало Rust DTO): `NewTopicSpec`, `ConfigChange`, `PurgedPartition`, `TopicDeleteResult`, `TopicPolicy`, `TopicPartitionRef`, `GroupSummary`, `GroupList`, `GroupMember`, `GroupDescription`, `GroupPartitionOffset`, `GroupLag`, `TopicGroupLag`, `TopicGroups`, `ResetStrategy`, `ResetScope`, `ResetRequest`, `ResetPreviewRow`, `OffsetTarget`, `ResetResult`, `Confirmation { confirm: string | null; typedName: string | null }`
  - `api.createTopic(connectionId, spec, confirm)`, `api.alterTopicConfigs(connectionId, topic, changes, confirm)`, `api.addPartitions(connectionId, topic, newTotal, confirm)`, `api.purgeTopic(connectionId, topic, partitions | null, c: Confirmation)`, `api.deleteTopics(connectionId, topics, c: Confirmation)`, `api.getTopicPolicies(connectionId, topics)`, `api.listGroups(connectionId)`, `api.describeGroups(connectionId, groupIds)`, `api.getGroupOffsets(connectionId, groupId)`, `api.getGroupsLag(connectionId, groupIds)`, `api.getTopicGroups(connectionId, topic)`, `api.previewOffsetReset(connectionId, request)`, `api.resetOffsets(connectionId, groupId, offsets, confirm)`, `api.deleteGroup(connectionId, groupId, c: Confirmation)`
  - `useConnection(connectionId: string): ConnectionProfile | undefined` (из кэша `["connections"]`)
  - `needsConfirmation(connection: ConnectionProfile, objectName?: string): boolean` — prod или нужен ввод имени
  - `<ConfirmDialog connection title confirmLabel objectName? objectLabel? danger? busy? onConfirm(c: Confirmation) onCancel>{описание}</ConfirmDialog>` — встроенный блок (не модальное окно); кнопка активна, когда введены имя подключения (prod) и имя объекта (если задано)

- [ ] **Step 1: Тесты клиента** — в `src/api/client.test.ts` внутри `describe("api client")` добавить:

```ts
  it("операции с топиками передают подтверждение отдельными полями", async () => {
    invoke.mockResolvedValue([]);
    await api.deleteTopics("c1", ["a", "b"], { confirm: "prod-1", typedName: "2" });
    expect(invoke).toHaveBeenCalledWith("delete_topics", {
      connectionId: "c1",
      topics: ["a", "b"],
      confirm: "prod-1",
      typedConfirmation: "2",
    });
    await api.purgeTopic("c1", "t", null, { confirm: null, typedName: "t" });
    expect(invoke).toHaveBeenLastCalledWith("purge_topic", {
      connectionId: "c1",
      topic: "t",
      partitions: null,
      confirm: null,
      typedName: "t",
    });
  });

  it("сброс оффсетов передаёт план и подтверждение", async () => {
    invoke.mockResolvedValue([]);
    const offsets = [{ topic: "t", partition: 0, offset: 5 }];
    await api.resetOffsets("c1", "g", offsets, null);
    expect(invoke).toHaveBeenCalledWith("reset_offsets", { connectionId: "c1", groupId: "g", offsets, confirm: null });
  });
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/api/client.test.ts`
Expected: FAIL — `api.deleteTopics is not a function`.

- [ ] **Step 3: Типы** — в конец `src/api/types.ts`:

```ts
export interface Confirmation {
  /** Имя подключения — для prod-подключений. */
  confirm: string | null;
  /** Введённое имя объекта (топика, группы) или число топиков при групповом удалении. */
  typedName: string | null;
}

export interface NewTopicSpec {
  name: string;
  partitions: number;
  /** -1 — значение брокера по умолчанию */
  replicationFactor: number;
  configs: Record<string, string>;
}

export type ConfigChange = { op: "set"; name: string; value: string } | { op: "delete"; name: string };

export interface PurgedPartition {
  partition: number;
  lowWatermark: number | null;
  error?: string;
}

export interface TopicDeleteResult {
  name: string;
  error?: AppError;
}

export interface TopicPolicy {
  name: string;
  cleanupPolicy: string | null;
}

export interface TopicPartitionRef {
  topic: string;
  partition: number;
}

export interface GroupSummary {
  groupId: string;
  state: string;
  isSimple: boolean;
}

export interface GroupList {
  groups: GroupSummary[];
  errors: string[];
}

export interface GroupMember {
  memberId: string;
  clientId: string;
  host: string;
  assignment: TopicPartitionRef[];
}

export interface GroupDescription {
  groupId: string;
  state: string;
  assignor: string;
  members: GroupMember[];
  error?: string;
}

export interface GroupPartitionOffset {
  topic: string;
  partition: number;
  committed: number | null;
  logEnd: number | null;
  lag: number | null;
  memberClientId: string | null;
  error?: string;
}

export interface GroupLag {
  groupId: string;
  totalLag: number | null;
  topics: string[];
  error?: string;
}

export interface TopicGroupLag {
  groupId: string;
  state: string;
  lag: number | null;
  partitions: number;
}

export interface TopicGroups {
  groups: TopicGroupLag[];
  incomplete: boolean;
}

export type ResetStrategy =
  | { type: "earliest" }
  | { type: "latest" }
  | { type: "timestamp"; timestampMs: number }
  | { type: "offset"; offset: number }
  | { type: "shift"; delta: number };

export type ResetScope =
  | { type: "all" }
  | { type: "topics"; topics: string[] }
  | { type: "partitions"; partitions: TopicPartitionRef[] };

export interface ResetRequest {
  groupId: string;
  strategy: ResetStrategy;
  scope: ResetScope;
}

export interface ResetPreviewRow {
  topic: string;
  partition: number;
  current: number | null;
  target: number | null;
  low: number | null;
  high: number | null;
  error?: string;
}

export interface OffsetTarget {
  topic: string;
  partition: number;
  offset: number;
}

export interface ResetResult {
  topic: string;
  partition: number;
  offset: number;
  error?: string;
}
```

- [ ] **Step 4: Клиент** — в `src/api/client.ts` добавить импорты новых типов и методы в объект `api` (после `getTopicConfigs`):

```ts
  createTopic: (connectionId: string, spec: NewTopicSpec, confirm: string | null) =>
    call<void>("create_topic", { connectionId, spec, confirm }),
  alterTopicConfigs: (connectionId: string, topic: string, changes: ConfigChange[], confirm: string | null) =>
    call<void>("alter_topic_configs", { connectionId, topic, changes, confirm }),
  addPartitions: (connectionId: string, topic: string, newTotal: number, confirm: string | null) =>
    call<void>("add_partitions", { connectionId, topic, newTotal, confirm }),
  purgeTopic: (connectionId: string, topic: string, partitions: number[] | null, c: Confirmation) =>
    call<PurgedPartition[]>("purge_topic", { connectionId, topic, partitions, confirm: c.confirm, typedName: c.typedName }),
  deleteTopics: (connectionId: string, topics: string[], c: Confirmation) =>
    call<TopicDeleteResult[]>("delete_topics", {
      connectionId,
      topics,
      confirm: c.confirm,
      typedConfirmation: c.typedName,
    }),
  getTopicPolicies: (connectionId: string, topics: string[]) =>
    call<TopicPolicy[]>("get_topic_policies", { connectionId, topics }),

  listGroups: (connectionId: string) => call<GroupList>("list_groups", { connectionId }),
  describeGroups: (connectionId: string, groupIds: string[]) =>
    call<GroupDescription[]>("describe_groups", { connectionId, groupIds }),
  getGroupOffsets: (connectionId: string, groupId: string) =>
    call<GroupPartitionOffset[]>("get_group_offsets", { connectionId, groupId }),
  getGroupsLag: (connectionId: string, groupIds: string[]) =>
    call<GroupLag[]>("get_groups_lag", { connectionId, groupIds }),
  getTopicGroups: (connectionId: string, topic: string) => call<TopicGroups>("get_topic_groups", { connectionId, topic }),
  previewOffsetReset: (connectionId: string, request: ResetRequest) =>
    call<ResetPreviewRow[]>("preview_offset_reset", { connectionId, request }),
  resetOffsets: (connectionId: string, groupId: string, offsets: OffsetTarget[], confirm: string | null) =>
    call<ResetResult[]>("reset_offsets", { connectionId, groupId, offsets, confirm }),
  deleteGroup: (connectionId: string, groupId: string, c: Confirmation) =>
    call<void>("delete_group", { connectionId, groupId, confirm: c.confirm, typedName: c.typedName }),
```

- [ ] **Step 5: Тесты клиента проходят**

Run: `pnpm vitest run src/api/client.test.ts`
Expected: PASS.

- [ ] **Step 6: `useConnection`** — создать `src/lib/useConnection.ts`:

```ts
import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import type { ConnectionProfile } from "../api/types";

/** Профиль подключения из общего кэша ["connections"]; undefined — пока грузится или удалён. */
export function useConnection(connectionId: string): ConnectionProfile | undefined {
  const { data } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  return data?.find((c) => c.id === connectionId);
}
```

- [ ] **Step 7: Тесты ConfirmDialog** — создать `src/components/common/ConfirmDialog.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { newProfile } from "../connections/profile";
import { ConfirmDialog, needsConfirmation } from "./ConfirmDialog";

const dev: ConnectionProfile = { ...newProfile(), id: "d", name: "dev-1", environment: "dev" };
const prod: ConnectionProfile = { ...newProfile(), id: "p", name: "prod-1", environment: "prod" };

describe("ConfirmDialog", () => {
  it("требует ввести имя объекта", async () => {
    const user = userEvent.setup();
    const onConfirm = vi.fn();
    render(
      <ConfirmDialog connection={dev} title="Удалить топик?" objectName="orders" objectLabel="имя топика" confirmLabel="Удалить" danger onConfirm={onConfirm} onCancel={() => {}} />,
    );
    const button = screen.getByRole("button", { name: "Удалить" });
    expect(button).toBeDisabled();
    expect(screen.queryByLabelText("Имя подключения")).not.toBeInTheDocument();
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "order");
    expect(button).toBeDisabled();
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "s");
    await user.click(button);
    expect(onConfirm).toHaveBeenCalledWith({ confirm: null, typedName: "orders" });
  });

  it("для prod требует имя подключения", async () => {
    const user = userEvent.setup();
    const onConfirm = vi.fn();
    render(<ConfirmDialog connection={prod} title="Создать топик?" confirmLabel="Создать" onConfirm={onConfirm} onCancel={() => {}} />);
    const button = screen.getByRole("button", { name: "Создать" });
    expect(button).toBeDisabled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(button);
    expect(onConfirm).toHaveBeenCalledWith({ confirm: "prod-1", typedName: null });
  });

  it("отмена", async () => {
    const user = userEvent.setup();
    const onCancel = vi.fn();
    render(<ConfirmDialog connection={prod} title="x" confirmLabel="Да" onConfirm={() => {}} onCancel={onCancel} />);
    await user.click(screen.getByRole("button", { name: "Отмена" }));
    expect(onCancel).toHaveBeenCalled();
  });

  it("needsConfirmation", () => {
    expect(needsConfirmation(dev)).toBe(false);
    expect(needsConfirmation(dev, "orders")).toBe(true);
    expect(needsConfirmation(prod)).toBe(true);
  });
});
```

- [ ] **Step 8: Убедиться в падении**

Run: `pnpm vitest run src/components/common/ConfirmDialog.test.tsx`
Expected: FAIL — модуль `./ConfirmDialog` не найден.

- [ ] **Step 9: Реализация** — создать `src/components/common/ConfirmDialog.tsx`:

```tsx
import { type ReactNode, useState } from "react";
import type { Confirmation, ConnectionProfile } from "../../api/types";

/** Нужен ли диалог: prod-подключение или операция с вводом имени объекта (удаление, purge). */
export function needsConfirmation(connection: ConnectionProfile, objectName?: string): boolean {
  return connection.environment === "prod" || objectName !== undefined;
}

interface Props {
  connection: ConnectionProfile;
  title: string;
  confirmLabel: string;
  /** Имя объекта, которое нужно ввести для подтверждения. */
  objectName?: string;
  /** Что именно вводится: «имя топика», «число топиков», «id группы». */
  objectLabel?: string;
  danger?: boolean;
  busy?: boolean;
  children?: ReactNode;
  onConfirm: (c: Confirmation) => void;
  onCancel: () => void;
}

/**
 * Подтверждение внутри страницы (нативный window.confirm в Tauri 2 не показывается).
 * Проверка дублируется в Rust: без верных значений команда вернёт CONFIRMATION_REQUIRED.
 */
export function ConfirmDialog({
  connection,
  title,
  confirmLabel,
  objectName,
  objectLabel = "имя",
  danger,
  busy,
  children,
  onConfirm,
  onCancel,
}: Props) {
  const [connName, setConnName] = useState("");
  const [typed, setTyped] = useState("");
  const isProd = connection.environment === "prod";
  const ok =
    (!isProd || connName.trim() === connection.name.trim()) &&
    (objectName === undefined || typed.trim() === objectName);

  return (
    <div className="confirm" role="group" aria-label={title}>
      <strong>{title}</strong>
      {children}
      {objectName !== undefined && (
        <label>
          Для подтверждения введите {objectLabel} <code>{objectName}</code>
          <input aria-label={`Подтверждение: ${objectLabel}`} value={typed} onChange={(e) => setTyped(e.target.value)} />
        </label>
      )}
      {isProd && (
        <label>
          Это prod-подключение. Введите его имя <code>{connection.name}</code>
          <input aria-label="Имя подключения" value={connName} onChange={(e) => setConnName(e.target.value)} />
        </label>
      )}
      <div className="toolbar">
        <button
          type="button"
          className={danger ? "danger" : "primary"}
          disabled={!ok || busy}
          onClick={() =>
            onConfirm({
              confirm: isProd ? connName.trim() : null,
              typedName: objectName !== undefined ? typed.trim() : null,
            })
          }
        >
          {confirmLabel}
        </button>
        <button type="button" onClick={onCancel}>
          Отмена
        </button>
      </div>
    </div>
  );
}
```

В `src/styles.css` добавить:
```css
.confirm { border: 1px solid var(--warn); border-radius: 6px; padding: 8px; display: flex; flex-direction: column; gap: 6px; background: color-mix(in srgb, var(--warn) 8%, transparent); }
.confirm label { display: flex; flex-direction: column; gap: 2px; }
.panel { border: 1px solid var(--border); border-radius: 6px; padding: 8px; display: flex; flex-direction: column; gap: 8px; }
```

- [ ] **Step 10: Тесты проходят**

Run: `pnpm vitest run src/components/common src/api && pnpm build`
Expected: PASS, `tsc` без ошибок.

- [ ] **Step 11: Commit**

```bash
git add src
git commit -m "feat(ui): API операций с топиками и группами, ConfirmDialog"
```

---

### Task 14: Создание топика, выбор и удаление в списке, колонка cleanup.policy

**Files:**
- Create: `src/components/topics/topicName.ts`, `src/components/topics/topicName.test.ts`
- Create: `src/components/topics/CreateTopicForm.tsx`, `src/components/topics/CreateTopicForm.test.tsx`
- Create: `src/components/topics/DeleteTopicsPanel.tsx`, `src/components/topics/DeleteTopicsPanel.test.tsx`
- Modify: `src/components/topics/TopicList.tsx`
- Modify: `src/styles.css` (`.topic-row`)

**Interfaces:**
- Consumes: `api.createTopic`, `api.deleteTopics`, `api.getTopicPolicies`, `api.getClusterOverview`, `useConnection`, `ConfirmDialog`, `needsConfirmation`, `parseProps` (`connections/profile.ts`), `visibleChunks`, `STATS_CHUNK`
- Produces:
  - `validateTopicName(name: string): string | null` (текст ошибки), `topicNameWarning(name: string): string | null` (одновременно `.` и `_`)
  - `TOPIC_PRESETS: { id: string; label: string; configs: Record<string, string> }[]`
  - `buildCreateCommand(bootstrap: string, spec: NewTopicSpec): string`
  - `<CreateTopicForm connection onCreated(name) onCancel />`
  - `<DeleteTopicsPanel connection topics: string[] onDone(deleted: string[]) onCancel />`

- [ ] **Step 1: Тесты чистых функций** — создать `src/components/topics/topicName.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { buildCreateCommand, TOPIC_PRESETS, topicNameWarning, validateTopicName } from "./topicName";

describe("topicName", () => {
  it("валидирует имя по правилам Kafka", () => {
    expect(validateTopicName("orders.v1_x-2")).toBeNull();
    expect(validateTopicName("")).toBe("Введите имя топика");
    expect(validateTopicName("..")).toBe("Имя топика не может быть «.» или «..»");
    expect(validateTopicName("a b")).toBe("Недопустимый символ « »: разрешены латинские буквы, цифры, «.», «_» и «-»");
    expect(validateTopicName("a".repeat(250))).toBe("Имя топика длиннее 249 символов");
  });

  it("предупреждает о коллизии метрик", () => {
    expect(topicNameWarning("a.b_c")).toContain("метрик");
    expect(topicNameWarning("a.b")).toBeNull();
  });

  it("строит команду kafka-topics", () => {
    const cmd = buildCreateCommand("localhost:9092", {
      name: "orders",
      partitions: 3,
      replicationFactor: -1,
      configs: { "cleanup.policy": "compact", "retention.ms": "1000" },
    });
    expect(cmd).toBe(
      "kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 3 " +
        "--config cleanup.policy=compact --config retention.ms=1000",
    );
    expect(buildCreateCommand("b:1", { name: "t", partitions: 1, replicationFactor: 2, configs: {} })).toContain(
      "--replication-factor 2",
    );
  });

  it("пресеты", () => {
    expect(TOPIC_PRESETS.map((p) => p.id)).toEqual(["delete", "compact", "compact-delete", "test"]);
    expect(TOPIC_PRESETS.find((p) => p.id === "test")?.configs).toEqual({ "retention.ms": "3600000" });
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/topics/topicName.test.ts`
Expected: FAIL — модуль не найден.

- [ ] **Step 3: Реализация** — создать `src/components/topics/topicName.ts`:

```ts
import type { NewTopicSpec } from "../../api/types";

const MAX_NAME = 249;

/** Те же правила, что в Rust (`kafka::admin::validate_topic_name`): ошибка — сразу у поля. */
export function validateTopicName(name: string): string | null {
  if (!name) return "Введите имя топика";
  if (name === "." || name === "..") return "Имя топика не может быть «.» или «..»";
  if (name.length > MAX_NAME) return `Имя топика длиннее ${MAX_NAME} символов`;
  const bad = [...name].find((c) => !/[A-Za-z0-9._-]/.test(c));
  if (bad !== undefined) return `Недопустимый символ «${bad}»: разрешены латинские буквы, цифры, «.», «_» и «-»`;
  return null;
}

export function topicNameWarning(name: string): string | null {
  return name.includes(".") && name.includes("_")
    ? "В имени есть и «.», и «_»: имена метрик таких топиков могут совпасть с другими"
    : null;
}

export const TOPIC_PRESETS: { id: string; label: string; configs: Record<string, string> }[] = [
  { id: "delete", label: "delete (по умолчанию)", configs: {} },
  { id: "compact", label: "compact", configs: { "cleanup.policy": "compact" } },
  { id: "compact-delete", label: "compact + delete", configs: { "cleanup.policy": "compact,delete" } },
  { id: "test", label: "тестовый (retention 1 ч)", configs: { "retention.ms": "3600000" } },
];

/** Эквивалентная команда — для справки и копирования. */
export function buildCreateCommand(bootstrap: string, spec: NewTopicSpec): string {
  const parts = [
    "kafka-topics.sh",
    `--bootstrap-server ${bootstrap}`,
    "--create",
    `--topic ${spec.name}`,
    `--partitions ${spec.partitions}`,
  ];
  if (spec.replicationFactor > 0) parts.push(`--replication-factor ${spec.replicationFactor}`);
  for (const [k, v] of Object.entries(spec.configs)) parts.push(`--config ${k}=${v}`);
  return parts.join(" ");
}
```

- [ ] **Step 4: Тесты проходят**

Run: `pnpm vitest run src/components/topics/topicName.test.ts`
Expected: PASS.

- [ ] **Step 5: Тесты форм** — создать `src/components/topics/CreateTopicForm.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      createTopic: vi.fn().mockResolvedValue(undefined),
      getClusterOverview: vi.fn().mockResolvedValue({ brokers: [{ id: 1, host: "h", port: 1 }] }),
    },
  };
});

import { api } from "../../api/client";
import { CreateTopicForm } from "./CreateTopicForm";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const prod: ConnectionProfile = { ...dev, environment: "prod", name: "prod-1" };

beforeEach(() => vi.clearAllMocks());

describe("CreateTopicForm", () => {
  it("валидирует имя, показывает команду и создаёт топик", async () => {
    const user = userEvent.setup();
    const onCreated = vi.fn();
    renderWithClient(<CreateTopicForm connection={dev} onCreated={onCreated} onCancel={() => {}} />);
    await user.type(screen.getByLabelText("Имя топика"), "bad name");
    expect(screen.getByText(/Недопустимый символ/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Создать" })).toBeDisabled();
    await user.clear(screen.getByLabelText("Имя топика"));
    await user.type(screen.getByLabelText("Имя топика"), "orders");
    await user.clear(screen.getByLabelText("Партиции"));
    await user.type(screen.getByLabelText("Партиции"), "3");
    await user.selectOptions(screen.getByLabelText("Пресет"), "compact");
    expect(screen.getByText(/--topic orders --partitions 3/)).toHaveTextContent("--config cleanup.policy=compact");
    await user.click(screen.getByRole("button", { name: "Создать" }));
    expect(api.createTopic).toHaveBeenCalledWith(
      "c",
      { name: "orders", partitions: 3, replicationFactor: 1, configs: { "cleanup.policy": "compact" } },
      null,
    );
    await vi.waitFor(() => expect(onCreated).toHaveBeenCalledWith("orders"));
  });

  it("в prod просит имя подключения перед созданием", async () => {
    const user = userEvent.setup();
    renderWithClient(<CreateTopicForm connection={prod} onCreated={() => {}} onCancel={() => {}} />);
    await user.type(screen.getByLabelText("Имя топика"), "t");
    await user.click(screen.getByRole("button", { name: "Создать" }));
    expect(api.createTopic).not.toHaveBeenCalled();
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Создать топик" }));
    expect(api.createTopic).toHaveBeenCalledWith("c", expect.objectContaining({ name: "t" }), "prod-1");
  });
});
```

Создать `src/components/topics/DeleteTopicsPanel.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      deleteTopics: vi.fn().mockResolvedValue([
        { name: "a" },
        { name: "b", error: { code: "NOT_AUTHORIZED", message: "Не удалось удалить топик", retryable: false } },
      ]),
    },
  };
});

import { api } from "../../api/client";
import { DeleteTopicsPanel } from "./DeleteTopicsPanel";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };

describe("DeleteTopicsPanel", () => {
  it("несколько топиков подтверждаются их числом, ошибки показываются по каждому", async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    renderWithClient(<DeleteTopicsPanel connection={dev} topics={["a", "b"]} onDone={onDone} onCancel={() => {}} />);
    await user.type(screen.getByLabelText("Подтверждение: число топиков"), "2");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(api.deleteTopics).toHaveBeenCalledWith("c", ["a", "b"], { confirm: null, typedName: "2" });
    expect(await screen.findByText(/b: Не удалось удалить топик/)).toBeInTheDocument();
    expect(onDone).toHaveBeenCalledWith(["a"]);
  });
});
```

- [ ] **Step 6: Убедиться в падении**

Run: `pnpm vitest run src/components/topics`
Expected: FAIL — модули `CreateTopicForm`, `DeleteTopicsPanel` не найдены.

- [ ] **Step 7: `CreateTopicForm`** — создать `src/components/topics/CreateTopicForm.tsx`:

```tsx
import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConnectionProfile, NewTopicSpec } from "../../api/types";
import { parseProps } from "../connections/profile";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { buildCreateCommand, TOPIC_PRESETS, topicNameWarning, validateTopicName } from "./topicName";

interface Props {
  connection: ConnectionProfile;
  onCreated: (name: string) => void;
  onCancel: () => void;
}

export function CreateTopicForm({ connection, onCreated, onCancel }: Props) {
  const cluster = useQuery({
    queryKey: ["cluster", connection.id],
    queryFn: () => api.getClusterOverview(connection.id),
  });
  const brokers = cluster.data?.brokers.length;
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState("1");
  const [rf, setRf] = useState<string | null>(null);
  const [preset, setPreset] = useState("delete");
  const [extra, setExtra] = useState("");
  const [confirming, setConfirming] = useState(false);

  // RF по умолчанию — min(3, число брокеров), пока пользователь не ввёл своё значение.
  const rfValue = rf ?? String(Math.min(3, brokers ?? 1));
  const nameError = validateTopicName(name);
  let configs: Record<string, string> = {};
  let extraError: string | null = null;
  try {
    configs = { ...TOPIC_PRESETS.find((p) => p.id === preset)?.configs, ...parseProps(extra) };
  } catch (e) {
    extraError = (e as Error).message;
  }
  const p = Number(partitions);
  const r = Number(rfValue);
  const numbersError =
    !Number.isInteger(p) || p < 1
      ? "Партиций должно быть не меньше 1"
      : !Number.isInteger(r) || (r < 1 && r !== -1)
        ? "Фактор репликации — целое ≥ 1 (или -1 — по умолчанию брокера)"
        : null;
  const spec: NewTopicSpec = { name, partitions: p, replicationFactor: r, configs };
  const valid = !nameError && !extraError && !numbersError;

  const create = useMutation({
    mutationFn: (confirm: string | null) => api.createTopic(connection.id, spec, confirm),
    onSuccess: () => onCreated(name),
  });

  const submit = () => {
    if (needsConfirmation(connection)) setConfirming(true);
    else create.mutate(null);
  };

  return (
    <div className="panel">
      <h3>Новый топик</h3>
      <div className="form">
        <label htmlFor="ct-name">Имя топика</label>
        <input id="ct-name" value={name} onChange={(e) => setName(e.target.value)} />
        {name && nameError && <p className="warn full">{nameError}</p>}
        {topicNameWarning(name) && <p className="muted full">{topicNameWarning(name)}</p>}
        <label htmlFor="ct-partitions">Партиции</label>
        <input id="ct-partitions" type="number" min={1} value={partitions} onChange={(e) => setPartitions(e.target.value)} />
        <label htmlFor="ct-rf">Фактор репликации</label>
        <input id="ct-rf" type="number" value={rfValue} onChange={(e) => setRf(e.target.value)} />
        {brokers !== undefined && <p className="muted full">Брокеров в кластере: {brokers}</p>}
        <label htmlFor="ct-preset">Пресет</label>
        <select id="ct-preset" value={preset} onChange={(e) => setPreset(e.target.value)}>
          {TOPIC_PRESETS.map((pr) => (
            <option key={pr.id} value={pr.id}>
              {pr.label}
            </option>
          ))}
        </select>
        <label htmlFor="ct-configs">Доп. конфиги (key=value)</label>
        <textarea id="ct-configs" rows={3} value={extra} onChange={(e) => setExtra(e.target.value)} />
      </div>
      {(extraError || numbersError) && <p className="warn">{extraError ?? numbersError}</p>}
      {valid && <pre className="muted">{buildCreateCommand(connection.bootstrapServers, spec)}</pre>}
      {create.error && <ErrorBanner error={create.error} />}
      {confirming ? (
        <ConfirmDialog
          connection={connection}
          title={`Создать топик «${name}»?`}
          confirmLabel="Создать топик"
          busy={create.isPending}
          onConfirm={(c) => create.mutate(c.confirm)}
          onCancel={() => setConfirming(false)}
        />
      ) : (
        <div className="toolbar">
          <button type="button" className="primary" disabled={!valid || create.isPending} onClick={submit}>
            Создать
          </button>
          <button type="button" onClick={onCancel}>
            Отмена
          </button>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 8: `DeleteTopicsPanel`** — создать `src/components/topics/DeleteTopicsPanel.tsx`:

```tsx
import { useMutation } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConnectionProfile, TopicDeleteResult } from "../../api/types";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";

interface Props {
  connection: ConnectionProfile;
  topics: string[];
  onDone: (deleted: string[]) => void;
  onCancel: () => void;
}

/** Удаление одного или нескольких топиков: один подтверждается именем, несколько — числом. */
export function DeleteTopicsPanel({ connection, topics, onDone, onCancel }: Props) {
  const single = topics.length === 1;
  const del = useMutation({
    mutationFn: (c: Parameters<typeof api.deleteTopics>[2]) => api.deleteTopics(connection.id, topics, c),
    onSuccess: (results: TopicDeleteResult[]) => onDone(results.filter((r) => !r.error).map((r) => r.name)),
  });
  const failed = del.data?.filter((r) => r.error) ?? [];

  return (
    <div className="panel">
      {failed.length > 0 && (
        <ul className="warn">
          {failed.map((r) => (
            <li key={r.name}>
              {r.name}: {r.error?.message} {r.error?.details && <span className="muted">({r.error.details})</span>}
            </li>
          ))}
        </ul>
      )}
      {del.error && <ErrorBanner error={del.error} />}
      {!del.data && (
        <ConfirmDialog
          connection={connection}
          title={single ? `Удалить топик «${topics[0]}»?` : `Удалить ${topics.length} топиков?`}
          objectName={single ? topics[0] : String(topics.length)}
          objectLabel={single ? "имя топика" : "число топиков"}
          confirmLabel="Удалить"
          danger
          busy={del.isPending}
          onConfirm={(c) => del.mutate(c)}
          onCancel={onCancel}
        >
          {!single && <p className="muted">{topics.join(", ")}</p>}
          <p className="muted">Данные удаляются безвозвратно.</p>
        </ConfirmDialog>
      )}
      {del.data && (
        <button type="button" onClick={onCancel}>
          Закрыть
        </button>
      )}
    </div>
  );
}
```

- [ ] **Step 9: Изменения `TopicList`** — в `src/components/topics/TopicList.tsx`:

Импорты дополнить:
```tsx
import { useConnection } from "../../lib/useConnection";
import { CreateTopicForm } from "./CreateTopicForm";
import { DeleteTopicsPanel } from "./DeleteTopicsPanel";
```

Состояние после `const open = useWorkspace(...)`:
```tsx
  const connection = useConnection(connectionId);
  const canMutate = connection !== undefined && !connection.readOnly;
  const [creating, setCreating] = useState(false);
  const [selecting, setSelecting] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [deleting, setDeleting] = useState(false);
  const toggle = (name: string) =>
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
```

После `statsResults`/`counts` добавить ленивую загрузку политик тем же чанкованием:
```tsx
  const policyResults = useQueries({
    queries: chunks.map((c) => {
      const slice = names.slice(c * STATS_CHUNK, (c + 1) * STATS_CHUNK);
      return {
        queryKey: ["topicPolicies", connectionId, slice],
        queryFn: () => api.getTopicPolicies(connectionId, slice),
        staleTime: 60_000,
      };
    }),
  });
  const policies = new Map<string, string | null>();
  policyResults.forEach((r, i) => {
    const slice = names.slice(chunks[i] * STATS_CHUNK, (chunks[i] + 1) * STATS_CHUNK);
    if (r.isError && !r.data) for (const n of slice) policies.set(n, null);
    for (const p of r.data ?? []) policies.set(p.name, p.cleanupPolicy);
  });
```

В `toolbar` после кнопки «Обновить» добавить:
```tsx
        {canMutate && (
          <>
            <button type="button" onClick={() => setCreating(true)} disabled={creating}>
              Создать топик
            </button>
            <button
              type="button"
              aria-pressed={selecting}
              onClick={() => {
                setSelecting((v) => !v);
                setSelected(new Set());
              }}
            >
              {selecting ? "Отменить выбор" : "Выбрать"}
            </button>
            {selecting && (
              <button type="button" className="danger" disabled={selected.size === 0} onClick={() => setDeleting(true)}>
                Удалить ({selected.size})
              </button>
            )}
          </>
        )}
```

Сразу после `</div>` тулбара:
```tsx
      {creating && connection && (
        <CreateTopicForm
          connection={connection}
          onCancel={() => setCreating(false)}
          onCreated={(name) => {
            setCreating(false);
            void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
            open({ kind: "topic", connectionId, topic: name });
          }}
        />
      )}
      {deleting && connection && (
        <DeleteTopicsPanel
          connection={connection}
          topics={[...selected].sort()}
          onCancel={() => setDeleting(false)}
          onDone={(deleted) => {
            setSelected((s) => new Set([...s].filter((n) => !deleted.includes(n))));
            void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
          }}
        />
      )}
```

Заголовок таблицы:
```tsx
        <div className="row header topic-row" style={{ height: ROW_HEIGHT }}>
          <span />
          <span>Имя</span>
          <span>Партиции</span>
          <span>RF</span>
          <span>Сообщений (≈)</span>
          <span>cleanup.policy</span>
          <span>Under-repl.</span>
        </div>
```

Строка (внутри `items.map`), первым и шестым элементами:
```tsx
                <span>
                  {selecting && (
                    <input
                      type="checkbox"
                      aria-label={`Выбрать ${t.name}`}
                      checked={selected.has(t.name)}
                      onClick={(e) => e.stopPropagation()}
                      onKeyDown={(e) => e.stopPropagation()}
                      onChange={() => toggle(t.name)}
                    />
                  )}
                </span>
```
```tsx
                <span>{policies.has(t.name) ? (policies.get(t.name) ?? "—") : "…"}</span>
```
(после ячейки «Сообщений», перед «Under-repl.»). В режиме выбора клик по строке переключает чекбокс вместо открытия топика:
```tsx
                {...activateProps(() =>
                  selecting ? toggle(t.name) : open({ kind: "topic", connectionId, topic: t.name }),
                )}
```

В `src/styles.css` заменить `.topic-row`:
```css
.topic-row { grid-template-columns: 28px minmax(200px, 1fr) 90px 60px 120px 130px 90px; }
```

- [ ] **Step 10: Тесты проходят**

Run: `pnpm vitest run src/components/topics && pnpm build`
Expected: PASS, `tsc` без ошибок.

- [ ] **Step 11: Commit**

```bash
git add src
git commit -m "feat(ui): создание топика, групповое удаление и колонка cleanup.policy"
```

---

### Task 15: Действия над топиком и редактор конфигов

**Files:**
- Create: `src/components/common/configDiff.ts`, `src/components/common/configDiff.test.ts`
- Create: `src/components/common/ConfigEditor.tsx`, `src/components/common/ConfigEditor.test.tsx`
- Create: `src/components/topics/TopicActions.tsx`, `src/components/topics/TopicActions.test.tsx`
- Modify: `src/components/topics/TopicDetails.tsx`, `src/components/topics/TopicDetails.test.tsx`

**Interfaces:**
- Consumes: `api.alterTopicConfigs`, `api.addPartitions`, `api.purgeTopic`, `api.deleteTopics`, `api.getTopicPolicies`, `ConfirmDialog`, `needsConfirmation`, `useConnection`, `DeleteTopicsPanel`, `tabId`
- Produces:
  - `type ConfigEdit = { kind: "set"; value: string } | { kind: "reset" }`
  - `diffConfigs(entries: ConfigEntry[], edits: Record<string, ConfigEdit>): { changes: ConfigChange[]; rows: { name: string; before: string | null; after: string | null }[] }` — `after: null` = «по умолчанию»; пустые правки (значение не изменилось, сброс уже default-параметра) отбрасываются
  - `isEditable(e: ConfigEntry): boolean` — не read-only и не sensitive
  - `<ConfigEditor connection topic entries onDone />`
  - `<TopicActions connection topic: TopicDetailsDto onDeleted />`

- [ ] **Step 1: Тесты diff** — создать `src/components/common/configDiff.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import type { ConfigEntry } from "../../api/types";
import { diffConfigs, isEditable } from "./configDiff";

const entry = (name: string, value: string | null, isDefault: boolean, extra: Partial<ConfigEntry> = {}): ConfigEntry => ({
  name,
  value,
  source: isDefault ? "Default" : "DynamicTopic",
  isDefault,
  isReadOnly: false,
  isSensitive: false,
  ...extra,
});

describe("diffConfigs", () => {
  const entries = [entry("retention.ms", "1000", false), entry("cleanup.policy", "delete", true), entry("segment.ms", "5", true)];

  it("SET для изменённых, DELETE для сброса, пустые правки отбрасываются", () => {
    const { changes, rows } = diffConfigs(entries, {
      "cleanup.policy": { kind: "set", value: "compact" },
      "retention.ms": { kind: "reset" },
      "segment.ms": { kind: "set", value: "5" },
    });
    expect(changes).toEqual([
      { op: "set", name: "cleanup.policy", value: "compact" },
      { op: "delete", name: "retention.ms" },
    ]);
    expect(rows).toEqual([
      { name: "cleanup.policy", before: "delete", after: "compact" },
      { name: "retention.ms", before: "1000", after: null },
    ]);
  });

  it("сброс default-параметра — не изменение", () => {
    expect(diffConfigs(entries, { "segment.ms": { kind: "reset" } }).changes).toEqual([]);
  });

  it("read-only и sensitive не редактируются", () => {
    expect(isEditable(entry("a", "1", true))).toBe(true);
    expect(isEditable(entry("a", "1", true, { isReadOnly: true }))).toBe(false);
    expect(isEditable(entry("a", null, true, { isSensitive: true }))).toBe(false);
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/common/configDiff.test.ts`
Expected: FAIL — модуль не найден.

- [ ] **Step 3: Реализация** — создать `src/components/common/configDiff.ts`:

```ts
import type { ConfigChange, ConfigEntry } from "../../api/types";

export type ConfigEdit = { kind: "set"; value: string } | { kind: "reset" };

export interface DiffRow {
  name: string;
  before: string | null;
  /** null — значение по умолчанию (DELETE) */
  after: string | null;
}

export function isEditable(e: ConfigEntry): boolean {
  return !e.isReadOnly && !e.isSensitive;
}

/** Правки → изменения для IncrementalAlterConfigs и строки diff «было → стало» (по имени параметра). */
export function diffConfigs(
  entries: ConfigEntry[],
  edits: Record<string, ConfigEdit>,
): { changes: ConfigChange[]; rows: DiffRow[] } {
  const byName = new Map(entries.map((e) => [e.name, e]));
  const changes: ConfigChange[] = [];
  const rows: DiffRow[] = [];
  for (const name of Object.keys(edits).sort()) {
    const e = byName.get(name);
    const edit = edits[name];
    if (!e || !isEditable(e)) continue;
    if (edit.kind === "reset") {
      if (e.isDefault) continue;
      changes.push({ op: "delete", name });
      rows.push({ name, before: e.value, after: null });
    } else {
      if (edit.value === (e.value ?? "")) continue;
      changes.push({ op: "set", name, value: edit.value });
      rows.push({ name, before: e.value, after: edit.value });
    }
  }
  return { changes, rows };
}
```

- [ ] **Step 4: Тест проходит**

Run: `pnpm vitest run src/components/common/configDiff.test.ts`
Expected: PASS.

- [ ] **Step 5: Тесты компонентов** — создать `src/components/common/ConfigEditor.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { ConfigEntry, ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { alterTopicConfigs: vi.fn().mockResolvedValue(undefined) } };
});

import { api } from "../../api/client";
import { ConfigEditor } from "./ConfigEditor";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const entries: ConfigEntry[] = [
  { name: "retention.ms", value: "1000", source: "DynamicTopic", isDefault: false, isReadOnly: false, isSensitive: false },
  { name: "segment.bytes", value: "1", source: "Default", isDefault: true, isReadOnly: true, isSensitive: false },
];

describe("ConfigEditor", () => {
  it("показывает diff и применяет изменения", async () => {
    const user = userEvent.setup();
    const onDone = vi.fn();
    renderWithClient(<ConfigEditor connection={dev} topic="t" entries={entries} onDone={onDone} />);
    expect(screen.getByLabelText("Значение segment.bytes")).toBeDisabled();
    const input = screen.getByLabelText("Значение retention.ms");
    await user.clear(input);
    await user.type(input, "2000");
    await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
    expect(screen.getByText("1000 → 2000")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Применить" }));
    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], null);
    await vi.waitFor(() => expect(onDone).toHaveBeenCalled());
  });

  it("сброс к default", async () => {
    const user = userEvent.setup();
    renderWithClient(<ConfigEditor connection={dev} topic="t" entries={entries} onDone={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Сбросить retention.ms" }));
    await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
    expect(screen.getByText("1000 → по умолчанию")).toBeInTheDocument();
  });
});
```

Создать `src/components/topics/TopicActions.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile, TopicDetails } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      addPartitions: vi.fn().mockResolvedValue(undefined),
      purgeTopic: vi.fn().mockResolvedValue([{ partition: 0, lowWatermark: 10 }, { partition: 1, lowWatermark: 4 }]),
      deleteTopics: vi.fn().mockResolvedValue([{ name: "orders" }]),
      getTopicPolicies: vi.fn().mockResolvedValue([{ name: "orders", cleanupPolicy: "compact" }]),
    },
  };
});

import { api } from "../../api/client";
import { TopicActions } from "./TopicActions";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const topic: TopicDetails = {
  name: "orders",
  internal: false,
  replicationFactor: 1,
  messageCount: 14,
  partitions: [
    { id: 0, leader: 1, replicas: [1], isr: [1], low: 0, high: 10, messageCount: 10, underReplicated: false },
    { id: 1, leader: 1, replicas: [1], isr: [1], low: 0, high: 4, messageCount: 4, underReplicated: false },
  ],
};

beforeEach(() => vi.clearAllMocks());

describe("TopicActions", () => {
  it("добавление партиций — только больше текущего", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Добавить партиции" }));
    const input = screen.getByLabelText("Новое число партиций");
    await user.clear(input);
    await user.type(input, "2");
    expect(screen.getByRole("button", { name: "Увеличить" })).toBeDisabled();
    await user.clear(input);
    await user.type(input, "4");
    await user.click(screen.getByRole("button", { name: "Увеличить" }));
    expect(api.addPartitions).toHaveBeenCalledWith("c", "orders", 4, null);
  });

  it("purge предупреждает о compact и требует имя топика", async () => {
    const user = userEvent.setup();
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Очистить" }));
    expect(await screen.findByText(/compact/)).toBeInTheDocument();
    await user.click(screen.getByLabelText("Партиция 1"));
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
    await user.click(screen.getByRole("button", { name: "Очистить партиции" }));
    expect(api.purgeTopic).toHaveBeenCalledWith("c", "orders", [0], { confirm: null, typedName: "orders" });
    expect(await screen.findByText("Партиция 0: новый low watermark 10")).toBeInTheDocument();
  });

  it("удаление сообщает наверх", async () => {
    const user = userEvent.setup();
    const onDeleted = vi.fn();
    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={onDeleted} />);
    await user.click(screen.getByRole("button", { name: "Удалить топик" }));
    await user.type(screen.getByLabelText("Подтверждение: имя топика"), "orders");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    await vi.waitFor(() => expect(onDeleted).toHaveBeenCalled());
  });
});
```

- [ ] **Step 6: Убедиться в падении**

Run: `pnpm vitest run src/components/common/ConfigEditor.test.tsx src/components/topics/TopicActions.test.tsx`
Expected: FAIL — модули не найдены.

- [ ] **Step 7: `ConfigEditor`** — создать `src/components/common/ConfigEditor.tsx`:

```tsx
import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConfigEntry, ConnectionProfile } from "../../api/types";
import { ConfirmDialog, needsConfirmation } from "./ConfirmDialog";
import { type ConfigEdit, diffConfigs, isEditable } from "./configDiff";
import { ErrorBanner } from "./ErrorBanner";

interface Props {
  connection: ConnectionProfile;
  topic: string;
  entries: ConfigEntry[];
  onDone: () => void;
}

export function ConfigEditor({ connection, topic, entries, onDone }: Props) {
  const [edits, setEdits] = useState<Record<string, ConfigEdit>>({});
  const [reviewing, setReviewing] = useState(false);
  const [query, setQuery] = useState("");
  const { changes, rows } = diffConfigs(entries, edits);
  const apply = useMutation({
    mutationFn: (confirm: string | null) => api.alterTopicConfigs(connection.id, topic, changes, confirm),
    onSuccess: () => {
      setEdits({});
      setReviewing(false);
      onDone();
    },
  });
  const q = query.trim().toLowerCase();
  const visible = entries.filter((e) => !q || e.name.toLowerCase().includes(q));
  const valueOf = (e: ConfigEntry) => {
    const edit = edits[e.name];
    if (!edit) return e.value ?? "";
    return edit.kind === "set" ? edit.value : "";
  };

  if (reviewing) {
    return (
      <div className="panel">
        <h3>Изменения конфигурации «{topic}»</h3>
        <ul>
          {rows.map((r) => (
            <li key={r.name}>
              <code>{r.name}</code>:{" "}
              <span>
                {r.before ?? "—"} → {r.after ?? "по умолчанию"}
              </span>
            </li>
          ))}
        </ul>
        {apply.error && <ErrorBanner error={apply.error} />}
        {needsConfirmation(connection) ? (
          <ConfirmDialog
            connection={connection}
            title="Применить изменения?"
            confirmLabel="Применить"
            busy={apply.isPending}
            onConfirm={(c) => apply.mutate(c.confirm)}
            onCancel={() => setReviewing(false)}
          />
        ) : (
          <div className="toolbar">
            <button type="button" className="primary" disabled={apply.isPending} onClick={() => apply.mutate(null)}>
              Применить
            </button>
            <button type="button" onClick={() => setReviewing(false)}>
              Назад
            </button>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="toolbar">
        <input aria-label="Поиск параметра" placeholder="Поиск параметра" value={query} onChange={(e) => setQuery(e.target.value)} />
        <button type="button" className="primary" disabled={changes.length === 0} onClick={() => setReviewing(true)}>
          Проверить изменения
        </button>
        <button type="button" disabled={Object.keys(edits).length === 0} onClick={() => setEdits({})}>
          Отменить правки
        </button>
        <span className="muted">изменений: {changes.length}</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Параметр</th>
            <th>Значение</th>
            <th>Источник</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {visible.map((e) => (
            <tr key={e.name}>
              <td style={{ fontWeight: e.isDefault ? 400 : 600 }}>{e.name}</td>
              <td>
                <input
                  aria-label={`Значение ${e.name}`}
                  disabled={!isEditable(e)}
                  value={e.isSensitive ? "••••••" : valueOf(e)}
                  placeholder={edits[e.name]?.kind === "reset" ? "по умолчанию" : undefined}
                  onChange={(ev) => setEdits((s) => ({ ...s, [e.name]: { kind: "set", value: ev.target.value } }))}
                />
              </td>
              <td className="muted">{e.source}</td>
              <td>
                {isEditable(e) && !e.isDefault && (
                  <button
                    type="button"
                    className="link"
                    aria-label={`Сбросить ${e.name}`}
                    onClick={() => setEdits((s) => ({ ...s, [e.name]: { kind: "reset" } }))}
                  >
                    к default
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

- [ ] **Step 8: `TopicActions`** — создать `src/components/topics/TopicActions.tsx`:

```tsx
import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConnectionProfile, TopicDetails } from "../../api/types";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { DeleteTopicsPanel } from "./DeleteTopicsPanel";

type Action = "partitions" | "purge" | "delete" | null;

interface Props {
  connection: ConnectionProfile;
  topic: TopicDetails;
  onDeleted: () => void;
}

export function TopicActions({ connection, topic, onDeleted }: Props) {
  const [action, setAction] = useState<Action>(null);
  return (
    <>
      <div className="toolbar">
        <button type="button" onClick={() => setAction("partitions")}>
          Добавить партиции
        </button>
        <button type="button" className="danger" onClick={() => setAction("purge")}>
          Очистить
        </button>
        <button type="button" className="danger" onClick={() => setAction("delete")}>
          Удалить топик
        </button>
      </div>
      {action === "partitions" && <AddPartitions connection={connection} topic={topic} onClose={() => setAction(null)} />}
      {action === "purge" && <Purge connection={connection} topic={topic} onClose={() => setAction(null)} />}
      {action === "delete" && (
        <DeleteTopicsPanel
          connection={connection}
          topics={[topic.name]}
          onCancel={() => setAction(null)}
          onDone={(deleted) => {
            if (deleted.includes(topic.name)) onDeleted();
          }}
        />
      )}
    </>
  );
}

function AddPartitions({ connection, topic, onClose }: { connection: ConnectionProfile; topic: TopicDetails; onClose: () => void }) {
  const current = topic.partitions.length;
  const [value, setValue] = useState(String(current + 1));
  const [confirming, setConfirming] = useState(false);
  const n = Number(value);
  const valid = Number.isInteger(n) && n > current;
  const run = useMutation({
    mutationFn: (confirm: string | null) => api.addPartitions(connection.id, topic.name, n, confirm),
    onSuccess: onClose,
  });
  return (
    <div className="panel">
      <label>
        Новое число партиций (сейчас {current})
        <input aria-label="Новое число партиций" type="number" min={current + 1} value={value} onChange={(e) => setValue(e.target.value)} />
      </label>
      <p className="muted">
        Уменьшить число партиций нельзя. Сообщения с одним ключом после увеличения могут попадать в другую партицию — порядок
        по ключу нарушится.
      </p>
      {run.error && <ErrorBanner error={run.error} />}
      {confirming ? (
        <ConfirmDialog
          connection={connection}
          title={`Увеличить число партиций «${topic.name}» до ${n}?`}
          confirmLabel="Подтвердить"
          busy={run.isPending}
          onConfirm={(c) => run.mutate(c.confirm)}
          onCancel={() => setConfirming(false)}
        />
      ) : (
        <div className="toolbar">
          <button
            type="button"
            className="primary"
            disabled={!valid || run.isPending}
            onClick={() => (needsConfirmation(connection) ? setConfirming(true) : run.mutate(null))}
          >
            Увеличить
          </button>
          <button type="button" onClick={onClose}>
            Отмена
          </button>
        </div>
      )}
    </div>
  );
}

function Purge({ connection, topic, onClose }: { connection: ConnectionProfile; topic: TopicDetails; onClose: () => void }) {
  const ids = topic.partitions.map((p) => p.id);
  const [selected, setSelected] = useState<Set<number>>(new Set(ids));
  const policy = useQuery({
    queryKey: ["topicPolicies", connection.id, [topic.name]],
    queryFn: () => api.getTopicPolicies(connection.id, [topic.name]),
  });
  const compact = policy.data?.[0]?.cleanupPolicy?.includes("compact") ?? false;
  const run = useMutation({
    mutationFn: (c: Parameters<typeof api.purgeTopic>[3]) =>
      api.purgeTopic(connection.id, topic.name, selected.size === ids.length ? null : [...selected].sort((a, b) => a - b), c),
  });
  const toggle = (id: number) =>
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  if (run.data) {
    return (
      <div className="panel">
        <ul>
          {run.data.map((p) => (
            <li key={p.partition} className={p.error ? "warn" : ""}>
              {p.error ? `Партиция ${p.partition}: ${p.error}` : `Партиция ${p.partition}: новый low watermark ${p.lowWatermark}`}
            </li>
          ))}
        </ul>
        <button type="button" onClick={onClose}>
          Закрыть
        </button>
      </div>
    );
  }

  return (
    <div className="panel">
      {compact && (
        <p className="warn">
          У топика cleanup.policy = {policy.data?.[0]?.cleanupPolicy}: purge удалит и последние значения ключей (compact).
        </p>
      )}
      <div className="toolbar">
        {ids.map((id) => (
          <label key={id}>
            <input type="checkbox" aria-label={`Партиция ${id}`} checked={selected.has(id)} onChange={() => toggle(id)} /> {id}
          </label>
        ))}
      </div>
      {run.error && <ErrorBanner error={run.error} />}
      <ConfirmDialog
        connection={connection}
        title={`Очистить ${selected.size} из ${ids.length} партиций «${topic.name}»?`}
        objectName={topic.name}
        objectLabel="имя топика"
        confirmLabel="Очистить партиции"
        danger
        busy={run.isPending || selected.size === 0}
        onConfirm={(c) => run.mutate(c)}
        onCancel={onClose}
      >
        <p className="muted">Сообщения до текущего конца выбранных партиций будут удалены безвозвратно.</p>
      </ConfirmDialog>
    </div>
  );
}
```

- [ ] **Step 9: Подключить в `TopicDetails`** — в `src/components/topics/TopicDetails.tsx`:

Импорты:
```tsx
import { useQueryClient } from "@tanstack/react-query";
import { useConnection } from "../../lib/useConnection";
import { tabId, useWorkspace } from "../../state/workspace";
import { ConfigEditor } from "../common/ConfigEditor";
import { TopicActions } from "./TopicActions";
```

В теле `TopicDetails` после `const q = useQuery(...)`:
```tsx
  const connection = useConnection(connectionId);
  const canMutate = connection !== undefined && !connection.readOnly;
  const queryClient = useQueryClient();
  const closeTab = useWorkspace((s) => s.close);
```

После блока `toolbar` (перед `subtabs`):
```tsx
      {canMutate && connection && !t.internal && (
        <TopicActions
          connection={connection}
          topic={t}
          onDeleted={() => {
            void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
            closeTab(tabId({ kind: "topic", connectionId, topic }));
          }}
        />
      )}
```

Вызов конфигов: `{sub === "configs" && <TopicConfigs connectionId={connectionId} topic={topic} />}` оставить, а `TopicConfigs` заменить:
```tsx
function TopicConfigs({ connectionId, topic }: { connectionId: string; topic: string }) {
  const connection = useConnection(connectionId);
  const [editing, setEditing] = useState(false);
  const q = useQuery({
    queryKey: ["topicConfigs", connectionId, topic],
    queryFn: () => api.getTopicConfigs(connectionId, topic),
  });
  if (q.isPending) return <p className="empty">Загрузка…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const canEdit = connection !== undefined && !connection.readOnly;
  return (
    <>
      {canEdit && (
        <div className="toolbar">
          <button type="button" aria-pressed={editing} onClick={() => setEditing((v) => !v)}>
            {editing ? "Закрыть редактор" : "Редактировать"}
          </button>
        </div>
      )}
      {editing && connection ? (
        <ConfigEditor
          connection={connection}
          topic={topic}
          entries={q.data}
          onDone={() => {
            setEditing(false);
            void q.refetch();
          }}
        />
      ) : (
        <ConfigTable entries={q.data} />
      )}
    </>
  );
}
```

В `src/components/topics/TopicDetails.test.tsx` в мок `api` добавить `listConnections: vi.fn().mockResolvedValue([])` (компонент теперь читает подключения; при пустом списке действия скрыты, существующие проверки не меняются).

- [ ] **Step 10: Тесты проходят**

Run: `pnpm vitest run src/components && pnpm build`
Expected: PASS, `tsc` без ошибок.

- [ ] **Step 11: Commit**

```bash
git add src
git commit -m "feat(ui): добавление партиций, purge, удаление топика и редактор конфигов с diff"
```

---

### Task 16: Вкладки групп и список consumer groups

**Files:**
- Modify: `src/state/workspace.ts`, `src/state/workspace.test.ts`
- Modify: `src/components/Layout.tsx` (заголовки вкладок), `src/components/TabContent.tsx`, `src/components/Sidebar.tsx`
- Create: `src/components/groups/groupFilter.ts`, `src/components/groups/groupFilter.test.ts`
- Create: `src/components/groups/GroupList.tsx`
- Create: `src/components/groups/GroupDetails.tsx` (каркас; наполняется в Task 17)
- Modify: `src/styles.css` (`.group-row`)

**Interfaces:**
- Consumes: `api.listGroups`, `api.describeGroups`, `api.getGroupsLag`, `visibleChunks` (`topics/topicFilter.ts`), `activateProps`, `formatCount`
- Produces:
  - `type ResetPrefill = { topic: string; partition: number; offset: number; nonce: number }`
  - `TabSpec` += `{ kind: "groups"; connectionId }` (id `groups:<conn>`) и `{ kind: "group"; connectionId; groupId; resetPrefill?: ResetPrefill }` (id `group:<conn>:<groupId>`)
  - `open(spec)` для уже открытой вкладки **заменяет её spec** (новый `resetPrefill` доходит до открытой вкладки группы)
  - `GROUP_CHUNK = 50`, `filterGroups(groups: GroupSummary[], query: string, state: string): GroupSummary[]` (`state === "all"` — без фильтра), `groupStates(groups): string[]` (уникальные, по алфавиту)
  - `<GroupList connectionId />`, `<GroupDetails connectionId groupId resetPrefill? />`

- [ ] **Step 1: Тесты** — в `src/state/workspace.test.ts` добавить:

```ts
  it("повторное open обновляет spec открытой вкладки", () => {
    const { open } = useWorkspace.getState();
    open({ kind: "group", connectionId: "c", groupId: "g" });
    open({ kind: "topics", connectionId: "c" });
    const prefill = { topic: "t", partition: 1, offset: 42, nonce: 1 };
    open({ kind: "group", connectionId: "c", groupId: "g", resetPrefill: prefill });
    const s = useWorkspace.getState();
    expect(s.tabs).toHaveLength(2);
    expect(s.activeId).toBe("group:c:g");
    expect(s.tabs[0]).toMatchObject({ resetPrefill: prefill });
    expect(tabId({ kind: "groups", connectionId: "c" })).toBe("groups:c");
  });
```

Создать `src/components/groups/groupFilter.test.ts`:
```ts
import { describe, expect, it } from "vitest";
import type { GroupSummary } from "../../api/types";
import { filterGroups, groupStates } from "./groupFilter";

const g = (groupId: string, state: string): GroupSummary => ({ groupId, state, isSimple: false });
const groups = [g("orders-svc", "Stable"), g("billing", "Empty"), g("orders-batch", "Empty")];

describe("groupFilter", () => {
  it("поиск по подстроке без учёта регистра и фильтр по состоянию", () => {
    expect(filterGroups(groups, "ORDERS", "all").map((x) => x.groupId)).toEqual(["orders-svc", "orders-batch"]);
    expect(filterGroups(groups, "orders", "Empty").map((x) => x.groupId)).toEqual(["orders-batch"]);
    expect(filterGroups(groups, "", "all")).toHaveLength(3);
  });

  it("список состояний", () => {
    expect(groupStates(groups)).toEqual(["Empty", "Stable"]);
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/state src/components/groups`
Expected: FAIL — нет kind `group`, модуль `groupFilter` не найден.

- [ ] **Step 3: workspace** — в `src/state/workspace.ts`:

```ts
export type ResetPrefill = { topic: string; partition: number; offset: number; nonce: number };

export type TabSpec =
  | { kind: "cluster"; connectionId: string }
  | { kind: "topics"; connectionId: string }
  | { kind: "topic"; connectionId: string; topic: string }
  | { kind: "groups"; connectionId: string }
  | { kind: "group"; connectionId: string; groupId: string; resetPrefill?: ResetPrefill }
  | { kind: "connection-form"; connectionId: string | null };
```
В `tabId` добавить ветку:
```ts
    case "group":
      return `group:${spec.connectionId}:${spec.groupId}`;
```
`open` заменить:
```ts
  open: (spec) =>
    set((s) => {
      const id = tabId(spec);
      const tab = { ...spec, id } as Tab;
      // Уже открытая вкладка получает новый spec (например, предзаполненный сброс оффсетов).
      if (s.tabs.some((t) => t.id === id)) return { tabs: s.tabs.map((t) => (t.id === id ? tab : t)), activeId: id };
      return { tabs: [...s.tabs, tab], activeId: id };
    }),
```

- [ ] **Step 4: Фильтр** — создать `src/components/groups/groupFilter.ts`:

```ts
import type { GroupSummary } from "../../api/types";

/** Детали (участники, лаг) грузятся лениво чанками по 50 групп из видимой области. */
export const GROUP_CHUNK = 50;

export function filterGroups(groups: GroupSummary[], query: string, state: string): GroupSummary[] {
  const q = query.trim().toLowerCase();
  return groups.filter((g) => (state === "all" || g.state === state) && (!q || g.groupId.toLowerCase().includes(q)));
}

export function groupStates(groups: GroupSummary[]): string[] {
  return [...new Set(groups.map((g) => g.state))].sort();
}
```

- [ ] **Step 5: Тесты проходят**

Run: `pnpm vitest run src/state src/components/groups/groupFilter.test.ts`
Expected: PASS.

- [ ] **Step 6: Каркас `GroupDetails`** — создать `src/components/groups/GroupDetails.tsx` (наполняется в Task 17):

```tsx
import type { ResetPrefill } from "../../state/workspace";

export function GroupDetails({ groupId }: { connectionId: string; groupId: string; resetPrefill?: ResetPrefill }) {
  return (
    <div className="page">
      <h2>{groupId}</h2>
    </div>
  );
}
```

- [ ] **Step 7: `GroupList`** — создать `src/components/groups/GroupList.tsx`:

```tsx
import { useMemo, useRef, useState } from "react";
import { useQueries, useQuery } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { api } from "../../api/client";
import type { GroupDescription, GroupLag } from "../../api/types";
import { activateProps } from "../../lib/activate";
import { formatCount } from "../../lib/format";
import { useDebouncedValue } from "../../lib/useDebouncedValue";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { visibleChunks } from "../topics/topicFilter";
import { filterGroups, GROUP_CHUNK, groupStates } from "./groupFilter";

const ROW_HEIGHT = 30;

export function GroupList({ connectionId }: { connectionId: string }) {
  const [query, setQuery] = useState("");
  const [state, setState] = useState("all");
  const debounced = useDebouncedValue(query, 250);
  const open = useWorkspace((s) => s.open);
  const q = useQuery({ queryKey: ["groups", connectionId], queryFn: () => api.listGroups(connectionId) });
  const all = q.data?.groups ?? [];
  const rows = useMemo(() => filterGroups(all, debounced, state), [all, debounced, state]);
  const ids = useMemo(() => rows.map((r) => r.groupId), [rows]);

  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });
  const items = virtualizer.getVirtualItems();
  const chunks = visibleChunks(items[0]?.index ?? 0, items[items.length - 1]?.index ?? -1, GROUP_CHUNK);
  const slices = chunks.map((c) => ids.slice(c * GROUP_CHUNK, (c + 1) * GROUP_CHUNK));
  const details = useQueries({
    queries: slices.map((slice) => ({
      queryKey: ["groupDescriptions", connectionId, slice],
      queryFn: () => api.describeGroups(connectionId, slice),
      staleTime: 15_000,
    })),
  });
  const lags = useQueries({
    queries: slices.map((slice) => ({
      queryKey: ["groupsLag", connectionId, slice],
      queryFn: () => api.getGroupsLag(connectionId, slice),
      staleTime: 15_000,
    })),
  });
  const descById = new Map<string, GroupDescription>();
  details.forEach((d) => d.data?.forEach((x) => descById.set(x.groupId, x)));
  const lagById = new Map<string, GroupLag | null>();
  lags.forEach((l, i) => {
    if (l.isError && !l.data) for (const id of slices[i]) lagById.set(id, null);
    l.data?.forEach((x) => lagById.set(x.groupId, x));
  });

  if (q.isPending) return <p className="empty">Загрузка групп…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>Consumer groups</h2>
        <input aria-label="Поиск группы" placeholder="Поиск группы" value={query} onChange={(e) => setQuery(e.target.value)} />
        <select aria-label="Состояние" value={state} onChange={(e) => setState(e.target.value)}>
          <option value="all">Все состояния</option>
          {groupStates(all).map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          Обновить
        </button>
        <span className="muted">
          {rows.length} из {all.length}
        </span>
      </div>
      {q.data.errors.length > 0 && (
        <p className="warn" title={q.data.errors.join("\n")}>
          Список может быть неполным: часть брокеров не ответила.
        </p>
      )}
      <div className="vlist" ref={parentRef}>
        <div className="row header group-row" style={{ height: ROW_HEIGHT }}>
          <span>Группа</span>
          <span>Состояние</span>
          <span>Участники</span>
          <span>Assignor</span>
          <span>Лаг</span>
          <span>Топики</span>
        </div>
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {items.map((item) => {
            const g = rows[item.index];
            const d = descById.get(g.groupId);
            const lag = lagById.get(g.groupId);
            return (
              <div
                key={g.groupId}
                className="row clickable group-row"
                style={{ top: item.start, height: ROW_HEIGHT }}
                {...activateProps(() => open({ kind: "group", connectionId, groupId: g.groupId }))}
              >
                <span title={g.groupId}>{g.groupId}</span>
                <span>{g.state}</span>
                <span title={d?.error}>{d ? (d.error ? "—" : d.members.length) : "…"}</span>
                <span>{d?.assignor || (d ? "—" : "…")}</span>
                <span title={lag?.error}>{lag === undefined ? "…" : formatCount(lag?.totalLag ?? null)}</span>
                <span title={lag?.topics.join(", ")}>{lag?.topics.join(", ") ?? ""}</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
```

В `src/styles.css`:
```css
.group-row { grid-template-columns: minmax(220px, 1fr) 150px 90px 110px 110px minmax(120px, 1fr); }
```

- [ ] **Step 8: Навигация** — `src/components/TabContent.tsx`: импорты `GroupList`, `GroupDetails` и ветки:
```tsx
    case "groups":
      return <GroupList connectionId={tab.connectionId} />;
    case "group":
      return <GroupDetails connectionId={tab.connectionId} groupId={tab.groupId} resetPrefill={tab.resetPrefill} />;
```

`src/components/Layout.tsx`, в `tabTitle`:
```tsx
    case "groups":
      return `${conn} · Группы`;
    case "group":
      return `${conn} · ${tab.groupId}`;
```

`src/components/Sidebar.tsx`, после пункта «Топики»:
```tsx
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "groups", connectionId: c.id })}>
                  Consumer groups
                </button>
              </li>
```

- [ ] **Step 9: Проверка**

Run: `pnpm vitest run && pnpm build`
Expected: PASS, `tsc` без ошибок.

- [ ] **Step 10: Commit**

```bash
git add src
git commit -m "feat(ui): вкладки consumer groups и список групп с ленивым лагом"
```

---

### Task 17: Детали группы и удаление

**Files:**
- Modify: `src/components/groups/GroupDetails.tsx`
- Create: `src/components/groups/groupLag.ts`, `src/components/groups/GroupDetails.test.tsx`

**Interfaces:**
- Consumes: `api.describeGroups`, `api.getGroupOffsets`, `api.deleteGroup`, `useConnection`, `ConfirmDialog`, `useWorkspace`, `tabId`, `formatCount`
- Produces:
  - `groupTotalLag(rows: GroupPartitionOffset[]): number | null` — как `total_lag` в Rust: `null`, если у какой-либо строки `error`; строки без коммита не входят в сумму
  - `<GroupDetails connectionId groupId resetPrefill? />` — состояние, таблица партиций, участники, автообновление, удаление; слот для `ResetOffsetsPanel` (Task 18) — кнопка «Сбросить оффсеты» и `panel === "reset"`

- [ ] **Step 1: Тесты** — создать `src/components/groups/GroupDetails.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile, GroupDescription, GroupPartitionOffset } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";
import { groupTotalLag } from "./groupLag";

const dev: ConnectionProfile = { ...newProfile(), id: "c", name: "dev-1", environment: "dev" };
const offsets: GroupPartitionOffset[] = [
  { topic: "orders", partition: 0, committed: 2, logEnd: 5, lag: 3, memberClientId: "svc-1" },
  { topic: "orders", partition: 1, committed: null, logEnd: 4, lag: null, memberClientId: "svc-1" },
];
const stable: GroupDescription = {
  groupId: "g",
  state: "Stable",
  assignor: "range",
  members: [{ memberId: "m1", clientId: "svc-1", host: "/10.0.0.1", assignment: [{ topic: "orders", partition: 0 }, { topic: "orders", partition: 1 }] }],
};

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn(),
      describeGroups: vi.fn(),
      getGroupOffsets: vi.fn(),
      deleteGroup: vi.fn().mockResolvedValue(undefined),
    },
  };
});

import { api } from "../../api/client";
import { GroupDetails } from "./GroupDetails";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.listConnections).mockResolvedValue([dev]);
  vi.mocked(api.getGroupOffsets).mockResolvedValue(offsets);
});

describe("groupTotalLag", () => {
  it("сумма по партициям с коммитом, ошибка — неизвестно", () => {
    expect(groupTotalLag(offsets)).toBe(3);
    expect(groupTotalLag([...offsets, { ...offsets[0], partition: 2, lag: null, logEnd: null, error: "нет лидера" }])).toBeNull();
  });
});

describe("GroupDetails", () => {
  it("показывает партиции, участников и запрещает удаление активной группы", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([stable]);
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    expect(await screen.findByText("нет коммита")).toBeInTheDocument();
    expect(screen.getByText("/10.0.0.1")).toBeInTheDocument();
    expect(screen.getByText("Суммарный лаг: 3")).toBeInTheDocument();
    expect(await screen.findByRole("button", { name: "Удалить группу" })).toBeDisabled();
  });

  it("удаляет пустую группу после ввода id и закрывает вкладку", async () => {
    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
    useWorkspace.setState({ tabs: [], activeId: null });
    useWorkspace.getState().open({ kind: "group", connectionId: "c", groupId: "g" });
    const user = userEvent.setup();
    renderWithClient(<GroupDetails connectionId="c" groupId="g" />);
    await user.click(await screen.findByRole("button", { name: "Удалить группу" }));
    await user.type(screen.getByLabelText("Подтверждение: id группы"), "g");
    await user.click(screen.getByRole("button", { name: "Удалить" }));
    expect(api.deleteGroup).toHaveBeenCalledWith("c", "g", { confirm: null, typedName: "g" });
    await vi.waitFor(() => expect(useWorkspace.getState().tabs).toHaveLength(0));
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/groups/GroupDetails.test.tsx`
Expected: FAIL — модуль `groupLag` не найден.

- [ ] **Step 3: `groupLag.ts`**:

```ts
import type { GroupPartitionOffset } from "../../api/types";

/** Как total_lag в Rust: null, если хоть одна партиция с ошибкой; партиции без коммита не суммируются. */
export function groupTotalLag(rows: GroupPartitionOffset[]): number | null {
  if (rows.some((r) => r.error)) return null;
  return rows.reduce((sum, r) => sum + (r.lag ?? 0), 0);
}
```

- [ ] **Step 4: `GroupDetails`** — заменить содержимое `src/components/groups/GroupDetails.tsx`:

```tsx
import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { Confirmation, GroupDescription, GroupPartitionOffset } from "../../api/types";
import { formatCount } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import { type ResetPrefill, tabId, useWorkspace } from "../../state/workspace";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { groupTotalLag } from "./groupLag";

const REFRESH_OPTIONS = [
  { label: "Автообновление: выкл.", value: 0 },
  { label: "каждые 5 с", value: 5_000 },
  { label: "каждые 10 с", value: 10_000 },
];

type Panel = "reset" | "delete" | null;

interface Props {
  connectionId: string;
  groupId: string;
  resetPrefill?: ResetPrefill;
}

export function GroupDetails({ connectionId, groupId, resetPrefill }: Props) {
  const connection = useConnection(connectionId);
  const canMutate = connection !== undefined && !connection.readOnly;
  const [refreshMs, setRefreshMs] = useState(0);
  const [panel, setPanel] = useState<Panel>(resetPrefill ? "reset" : null);
  const queryClient = useQueryClient();
  const closeTab = useWorkspace((s) => s.close);

  // Новый предзаполненный сброс («Сбросить группу сюда») открывает панель сброса.
  useEffect(() => {
    if (resetPrefill) setPanel("reset");
  }, [resetPrefill]);

  const desc = useQuery({
    queryKey: ["group", connectionId, groupId],
    queryFn: async (): Promise<GroupDescription> => (await api.describeGroups(connectionId, [groupId]))[0],
    refetchInterval: refreshMs || false,
  });
  const offsets = useQuery({
    queryKey: ["groupOffsets", connectionId, groupId],
    queryFn: () => api.getGroupOffsets(connectionId, groupId),
    refetchInterval: refreshMs || false,
  });
  const remove = useMutation({
    mutationFn: (c: Confirmation) => api.deleteGroup(connectionId, groupId, c),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["groups", connectionId] });
      closeTab(tabId({ kind: "group", connectionId, groupId }));
    },
  });
  const refresh = () => {
    void desc.refetch();
    void offsets.refetch();
  };
  const d = desc.data;
  const rows = offsets.data ?? [];

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{groupId}</h2>
        {d && (
          <span className="muted">
            {d.state} · участников: {d.members.length} · assignor: {d.assignor || "—"}
          </span>
        )}
        <select aria-label="Автообновление" value={refreshMs} onChange={(e) => setRefreshMs(Number(e.target.value))}>
          {REFRESH_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <button type="button" disabled={desc.isFetching || offsets.isFetching} onClick={refresh}>
          Обновить
        </button>
      </div>
      {canMutate && (
        <div className="toolbar">
          <button type="button" onClick={() => setPanel("reset")}>
            Сбросить оффсеты
          </button>
          <button
            type="button"
            className="danger"
            disabled={d?.state !== "Empty"}
            title={d?.state !== "Empty" ? "Удалить можно только группу без участников (Empty)" : undefined}
            onClick={() => setPanel("delete")}
          >
            Удалить группу
          </button>
        </div>
      )}
      {desc.error && <ErrorBanner error={desc.error} onRetry={refresh} />}
      {d?.error && <p className="warn">{d.error}</p>}
      {panel === "delete" && connection && (
        <>
          {remove.error && <ErrorBanner error={remove.error} />}
          <ConfirmDialog
            connection={connection}
            title={`Удалить группу «${groupId}»?`}
            objectName={groupId}
            objectLabel="id группы"
            confirmLabel="Удалить"
            danger
            busy={remove.isPending}
            onConfirm={(c) => remove.mutate(c)}
            onCancel={() => setPanel(null)}
          >
            <p className="muted">Будут удалены все закоммиченные оффсеты группы.</p>
          </ConfirmDialog>
        </>
      )}
      {/* Task 18: панель сброса оффсетов (panel === "reset") */}

      <h3>Партиции</h3>
      {offsets.error && <ErrorBanner error={offsets.error} onRetry={refresh} />}
      {offsets.data && <p className="muted">Суммарный лаг: {formatCount(groupTotalLag(rows))}</p>}
      <OffsetsTable rows={rows} />

      <h3>Участники</h3>
      {d && d.members.length === 0 && <p className="muted">Нет активных участников.</p>}
      {d && d.members.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Client ID</th>
              <th>Хост</th>
              <th>Member ID</th>
              <th>Назначение</th>
            </tr>
          </thead>
          <tbody>
            {d.members.map((m) => (
              <tr key={m.memberId}>
                <td>{m.clientId}</td>
                <td>{m.host}</td>
                <td className="muted">{m.memberId}</td>
                <td>{m.assignment.map((a) => `${a.topic}:${a.partition}`).join(", ")}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function OffsetsTable({ rows }: { rows: GroupPartitionOffset[] }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Топик</th>
          <th>Партиция</th>
          <th>Committed</th>
          <th>Log end</th>
          <th>Лаг</th>
          <th>Участник</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={`${r.topic}:${r.partition}`}>
            <td>{r.topic}</td>
            <td>{r.partition}</td>
            <td className={r.error ? "warn" : ""} title={r.error}>
              {r.committed !== null ? r.committed : r.error ? "—" : <span className="muted">нет коммита</span>}
            </td>
            <td>{r.logEnd ?? "—"}</td>
            <td>{formatCount(r.lag)}</td>
            <td>{r.memberClientId ?? "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 5: Тесты проходят**

Run: `pnpm vitest run src/components/groups && pnpm build`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/components/groups
git commit -m "feat(ui): детали consumer group, лаг по партициям и удаление группы"
```

---

### Task 18: Сброс оффсетов с обязательным предпросмотром

**Files:**
- Create: `src/components/groups/reset.ts`, `src/components/groups/reset.test.ts`
- Create: `src/components/groups/ResetOffsetsPanel.tsx`, `src/components/groups/ResetOffsetsPanel.test.tsx`
- Modify: `src/components/groups/GroupDetails.tsx` (подключить панель)

**Interfaces:**
- Consumes: `api.previewOffsetReset`, `api.resetOffsets`, `ConfirmDialog`, `needsConfirmation`, `ResetPrefill`
- Produces:
  - `interface ResetForm { strategy: ResetStrategy["type"]; value: string; scope: ResetScope["type"]; scopeText: string }`
  - `defaultResetForm(prefill?: ResetPrefill): ResetForm` — с prefill: стратегия `offset` = `prefill.offset`, область `partitions` = `topic:partition`
  - `buildResetRequest(groupId: string, form: ResetForm): ResetRequest` — бросает `Error` с текстом для пользователя
  - `targetsFromPreview(rows: ResetPreviewRow[]): OffsetTarget[]` — строки с `target !== null` и без ошибки
  - `<ResetOffsetsPanel connection groupId prefill? onApplied onClose />`

- [ ] **Step 1: Тесты** — создать `src/components/groups/reset.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import { buildResetRequest, defaultResetForm, targetsFromPreview } from "./reset";

describe("reset", () => {
  it("стратегии и области", () => {
    const base = defaultResetForm();
    expect(base).toEqual({ strategy: "earliest", value: "", scope: "all", scopeText: "" });
    expect(buildResetRequest("g", base)).toEqual({ groupId: "g", strategy: { type: "earliest" }, scope: { type: "all" } });
    expect(buildResetRequest("g", { ...base, strategy: "shift", value: "-10" }).strategy).toEqual({ type: "shift", delta: -10 });
    expect(buildResetRequest("g", { ...base, strategy: "offset", value: "42" }).strategy).toEqual({ type: "offset", offset: 42 });
    const ts = buildResetRequest("g", { ...base, strategy: "timestamp", value: "2026-10-15T12:00" }).strategy;
    expect(ts).toEqual({ type: "timestamp", timestampMs: new Date("2026-10-15T12:00").getTime() });
    expect(buildResetRequest("g", { ...base, scope: "topics", scopeText: "a, b\nc" }).scope).toEqual({ type: "topics", topics: ["a", "b", "c"] });
    expect(buildResetRequest("g", { ...base, scope: "partitions", scopeText: "orders.v1:0, x:12" }).scope).toEqual({
      type: "partitions",
      partitions: [
        { topic: "orders.v1", partition: 0 },
        { topic: "x", partition: 12 },
      ],
    });
  });

  it("ошибки ввода", () => {
    const base = defaultResetForm();
    expect(() => buildResetRequest("g", { ...base, strategy: "offset", value: "-1" })).toThrow("Оффсет — целое число ≥ 0");
    expect(() => buildResetRequest("g", { ...base, strategy: "shift", value: "0" })).toThrow("Сдвиг — ненулевое целое число");
    expect(() => buildResetRequest("g", { ...base, strategy: "timestamp", value: "" })).toThrow("Укажите дату и время");
    expect(() => buildResetRequest("g", { ...base, scope: "topics", scopeText: " " })).toThrow("Укажите хотя бы один топик");
    expect(() => buildResetRequest("g", { ...base, scope: "partitions", scopeText: "orders" })).toThrow("«orders»: ожидается топик:партиция");
  });

  it("предзаполнение из сообщения", () => {
    expect(defaultResetForm({ topic: "t", partition: 2, offset: 7, nonce: 1 })).toEqual({
      strategy: "offset",
      value: "7",
      scope: "partitions",
      scopeText: "t:2",
    });
  });

  it("применяются только строки без ошибок", () => {
    const rows = [
      { topic: "t", partition: 0, current: 1, target: 0, low: 0, high: 5 },
      { topic: "t", partition: 1, current: null, target: null, low: 0, high: 5, error: "Нет закоммиченного оффсета" },
    ];
    expect(targetsFromPreview(rows)).toEqual([{ topic: "t", partition: 0, offset: 0 }]);
  });
});
```

Создать `src/components/groups/ResetOffsetsPanel.test.tsx`:
```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../../api/types";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { previewOffsetReset: vi.fn(), resetOffsets: vi.fn() } };
});

import { api } from "../../api/client";
import { ResetOffsetsPanel } from "./ResetOffsetsPanel";

const prod: ConnectionProfile = { ...newProfile(), id: "c", name: "prod-1", environment: "prod" };

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.previewOffsetReset).mockResolvedValue([
    { topic: "t", partition: 0, current: 5, target: 0, low: 0, high: 9 },
    { topic: "t", partition: 1, current: null, target: null, low: 0, high: 3, error: "Нет закоммиченного оффсета — сдвиг невозможен" },
  ]);
  vi.mocked(api.resetOffsets).mockResolvedValue([{ topic: "t", partition: 0, offset: 0 }]);
});

describe("ResetOffsetsPanel", () => {
  it("применяет ровно показанный план после подтверждения prod", async () => {
    const user = userEvent.setup();
    const onApplied = vi.fn();
    renderWithClient(<ResetOffsetsPanel connection={prod} groupId="g" onApplied={onApplied} onClose={() => {}} />);
    expect(screen.queryByRole("button", { name: /Применить/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    expect(api.previewOffsetReset).toHaveBeenCalledWith("c", { groupId: "g", strategy: { type: "earliest" }, scope: { type: "all" } });
    expect(await screen.findByText("Нет закоммиченного оффсета — сдвиг невозможен")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Применить (1)" }));
    await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
    await user.click(screen.getByRole("button", { name: "Сбросить оффсеты" }));
    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], "prod-1");
    await vi.waitFor(() => expect(onApplied).toHaveBeenCalled());
  });

  it("изменение формы сбрасывает предпросмотр", async () => {
    const user = userEvent.setup();
    renderWithClient(<ResetOffsetsPanel connection={prod} groupId="g" onApplied={() => {}} onClose={() => {}} />);
    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
    await screen.findByRole("button", { name: "Применить (1)" });
    await user.selectOptions(screen.getByLabelText("Стратегия"), "latest");
    expect(screen.queryByRole("button", { name: /Применить/ })).not.toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/groups`
Expected: FAIL — модули `reset`, `ResetOffsetsPanel` не найдены.

- [ ] **Step 3: `reset.ts`**:

```ts
import type { OffsetTarget, ResetPreviewRow, ResetRequest, ResetScope, ResetStrategy } from "../../api/types";
import type { ResetPrefill } from "../../state/workspace";

export interface ResetForm {
  strategy: ResetStrategy["type"];
  /** offset / shift — число; timestamp — значение input datetime-local (локальное время). */
  value: string;
  scope: ResetScope["type"];
  /** topics: имена через запятую/пробел/перевод строки; partitions: «топик:партиция» через запятую. */
  scopeText: string;
}

export function defaultResetForm(prefill?: ResetPrefill): ResetForm {
  if (!prefill) return { strategy: "earliest", value: "", scope: "all", scopeText: "" };
  return { strategy: "offset", value: String(prefill.offset), scope: "partitions", scopeText: `${prefill.topic}:${prefill.partition}` };
}

const tokens = (text: string) => text.split(/[\s,]+/).filter(Boolean);

function parseStrategy(form: ResetForm): ResetStrategy {
  const n = Number(form.value);
  switch (form.strategy) {
    case "earliest":
    case "latest":
      return { type: form.strategy };
    case "offset":
      if (form.value.trim() === "" || !Number.isInteger(n) || n < 0) throw new Error("Оффсет — целое число ≥ 0");
      return { type: "offset", offset: n };
    case "shift":
      if (!Number.isInteger(n) || n === 0) throw new Error("Сдвиг — ненулевое целое число");
      return { type: "shift", delta: n };
    case "timestamp": {
      const ms = new Date(form.value).getTime();
      if (!form.value || Number.isNaN(ms)) throw new Error("Укажите дату и время");
      return { type: "timestamp", timestampMs: ms };
    }
  }
}

function parseScope(form: ResetForm): ResetScope {
  switch (form.scope) {
    case "all":
      return { type: "all" };
    case "topics": {
      const topics = tokens(form.scopeText);
      if (topics.length === 0) throw new Error("Укажите хотя бы один топик");
      return { type: "topics", topics };
    }
    case "partitions": {
      const partitions = tokens(form.scopeText).map((t) => {
        const i = t.lastIndexOf(":");
        const p = Number(t.slice(i + 1));
        if (i <= 0 || !Number.isInteger(p) || p < 0 || t.slice(i + 1) === "") throw new Error(`«${t}»: ожидается топик:партиция`);
        return { topic: t.slice(0, i), partition: p };
      });
      if (partitions.length === 0) throw new Error("Укажите хотя бы одну партицию");
      return { type: "partitions", partitions };
    }
  }
}

export function buildResetRequest(groupId: string, form: ResetForm): ResetRequest {
  return { groupId, strategy: parseStrategy(form), scope: parseScope(form) };
}

export function targetsFromPreview(rows: ResetPreviewRow[]): OffsetTarget[] {
  return rows
    .filter((r) => r.target !== null && !r.error)
    .map((r) => ({ topic: r.topic, partition: r.partition, offset: r.target as number }));
}
```

- [ ] **Step 4: `ResetOffsetsPanel.tsx`**:

```tsx
import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConnectionProfile, OffsetTarget, ResetPreviewRow } from "../../api/types";
import type { ResetPrefill } from "../../state/workspace";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { buildResetRequest, defaultResetForm, type ResetForm, targetsFromPreview } from "./reset";

const STRATEGIES: { id: ResetForm["strategy"]; label: string }[] = [
  { id: "earliest", label: "С начала (earliest)" },
  { id: "latest", label: "С конца (latest)" },
  { id: "timestamp", label: "По времени" },
  { id: "offset", label: "Конкретный оффсет" },
  { id: "shift", label: "Сдвиг на ±N" },
];
const SCOPES: { id: ResetForm["scope"]; label: string }[] = [
  { id: "all", label: "Вся группа" },
  { id: "topics", label: "Топики" },
  { id: "partitions", label: "Партиции" },
];

interface Props {
  connection: ConnectionProfile;
  groupId: string;
  prefill?: ResetPrefill;
  onApplied: () => void;
  onClose: () => void;
}

export function ResetOffsetsPanel({ connection, groupId, prefill, onApplied, onClose }: Props) {
  const [form, setForm] = useState<ResetForm>(() => defaultResetForm(prefill));
  const [formError, setFormError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  const preview = useMutation({
    mutationFn: () => api.previewOffsetReset(connection.id, buildResetRequest(groupId, form)),
  });
  const apply = useMutation({
    mutationFn: ({ targets, confirm }: { targets: OffsetTarget[]; confirm: string | null }) =>
      api.resetOffsets(connection.id, groupId, targets, confirm),
    onSuccess: (results) => {
      setConfirming(false);
      if (results.every((r) => !r.error)) onApplied();
    },
  });
  // Любое изменение формы делает предпросмотр недействительным: применяется только показанный план.
  const update = (patch: Partial<ResetForm>) => {
    setForm((f) => ({ ...f, ...patch }));
    preview.reset();
    apply.reset();
    setConfirming(false);
  };
  const runPreview = () => {
    try {
      buildResetRequest(groupId, form);
      setFormError(null);
      preview.mutate();
    } catch (e) {
      setFormError((e as Error).message);
    }
  };
  const rows: ResetPreviewRow[] = preview.data ?? [];
  const targets = targetsFromPreview(rows);
  const failed = apply.data?.filter((r) => r.error) ?? [];

  return (
    <div className="panel">
      <h3>Сброс оффсетов группы «{groupId}»</h3>
      <p className="muted">Сброс возможен только для группы без активных участников (Empty).</p>
      <div className="form">
        <label htmlFor="rs-strategy">Стратегия</label>
        <select id="rs-strategy" value={form.strategy} onChange={(e) => update({ strategy: e.target.value as ResetForm["strategy"], value: "" })}>
          {STRATEGIES.map((s) => (
            <option key={s.id} value={s.id}>
              {s.label}
            </option>
          ))}
        </select>
        {form.strategy === "timestamp" && (
          <>
            <label htmlFor="rs-value">Дата и время (локальное)</label>
            <input id="rs-value" type="datetime-local" step={1} value={form.value} onChange={(e) => update({ value: e.target.value })} />
          </>
        )}
        {(form.strategy === "offset" || form.strategy === "shift") && (
          <>
            <label htmlFor="rs-value">{form.strategy === "offset" ? "Оффсет" : "Сдвиг (±N)"}</label>
            <input id="rs-value" type="number" value={form.value} onChange={(e) => update({ value: e.target.value })} />
          </>
        )}
        <label htmlFor="rs-scope">Область</label>
        <select id="rs-scope" value={form.scope} onChange={(e) => update({ scope: e.target.value as ResetForm["scope"] })}>
          {SCOPES.map((s) => (
            <option key={s.id} value={s.id}>
              {s.label}
            </option>
          ))}
        </select>
        {form.scope !== "all" && (
          <>
            <label htmlFor="rs-scope-text">{form.scope === "topics" ? "Топики" : "Партиции (топик:партиция)"}</label>
            <textarea id="rs-scope-text" rows={2} value={form.scopeText} onChange={(e) => update({ scopeText: e.target.value })} />
          </>
        )}
      </div>
      {formError && <p className="warn">{formError}</p>}
      <div className="toolbar">
        <button type="button" className="primary" disabled={preview.isPending} onClick={runPreview}>
          Предпросмотр
        </button>
        <button type="button" onClick={onClose}>
          Закрыть
        </button>
      </div>
      {preview.error && <ErrorBanner error={preview.error} />}
      {preview.data && (
        <>
          <table>
            <thead>
              <tr>
                <th>Топик</th>
                <th>Партиция</th>
                <th>Текущий</th>
                <th>Новый</th>
                <th>Low / High</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={`${r.topic}:${r.partition}`}>
                  <td>{r.topic}</td>
                  <td>{r.partition}</td>
                  <td>{r.current ?? <span className="muted">нет коммита</span>}</td>
                  <td>{r.target ?? "—"}</td>
                  <td className="muted">
                    {r.low ?? "—"} / {r.high ?? "—"}
                  </td>
                  <td className="warn">{r.error}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {apply.error && <ErrorBanner error={apply.error} />}
          {failed.length > 0 && (
            <ul className="warn">
              {failed.map((r) => (
                <li key={`${r.topic}:${r.partition}`}>
                  {r.topic}:{r.partition} — {r.error}
                </li>
              ))}
            </ul>
          )}
          {confirming ? (
            <ConfirmDialog
              connection={connection}
              title={`Сбросить оффсеты ${targets.length} партиций группы «${groupId}»?`}
              confirmLabel="Сбросить оффсеты"
              busy={apply.isPending}
              onConfirm={(c) => apply.mutate({ targets, confirm: c.confirm })}
              onCancel={() => setConfirming(false)}
            />
          ) : (
            <button
              type="button"
              className="primary"
              disabled={targets.length === 0 || apply.isPending}
              onClick={() => (needsConfirmation(connection) ? setConfirming(true) : apply.mutate({ targets, confirm: null }))}
            >
              Применить ({targets.length})
            </button>
          )}
        </>
      )}
    </div>
  );
}
```

- [ ] **Step 5: Подключить в `GroupDetails`** — в `src/components/groups/GroupDetails.tsx` добавить импорт `import { ResetOffsetsPanel } from "./ResetOffsetsPanel";` и заменить комментарий `{/* Task 18: … */}` на:

```tsx
      {panel === "reset" && connection && (
        <ResetOffsetsPanel
          key={resetPrefill?.nonce ?? 0}
          connection={connection}
          groupId={groupId}
          prefill={resetPrefill}
          onApplied={refresh}
          onClose={() => setPanel(null)}
        />
      )}
```

- [ ] **Step 6: Тесты проходят**

Run: `pnpm vitest run src/components/groups && pnpm build`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/components/groups
git commit -m "feat(ui): сброс оффсетов с обязательным предпросмотром"
```

---

### Task 19: Группы топика и «Сбросить группу сюда»

**Files:**
- Create: `src/components/topics/TopicGroups.tsx`, `src/components/topics/TopicGroups.test.tsx`
- Modify: `src/components/topics/TopicDetails.tsx` (подвкладка «Consumer groups»)
- Modify: `src/components/browse/MessageDetails.tsx`, `src/components/browse/BrowsePanel.tsx`
- Create: `src/components/browse/MessageDetails.test.tsx`

**Interfaces:**
- Consumes: `api.getTopicGroups`, `api.listGroups`, `useConnection`, `useWorkspace.open`, `ResetPrefill`
- Produces:
  - `<TopicGroups connectionId topic />` — группы с коммитами в топик, лаг, переход во вкладку группы
  - `<MessageDetails message utc connectionId topic />` — добавлено действие «Сбросить группу сюда…» (скрыто для read-only): выбор группы → вкладка группы с `resetPrefill = { topic, partition, offset: message.offset, nonce }` (группа начнёт чтение с этого сообщения)

- [ ] **Step 1: Тесты** — создать `src/components/topics/TopicGroups.test.tsx`:

```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      getTopicGroups: vi.fn().mockResolvedValue({
        groups: [
          { groupId: "svc", state: "Stable", lag: 12, partitions: 3 },
          { groupId: "old", state: "Empty", lag: null, partitions: 1 },
        ],
        incomplete: true,
      }),
    },
  };
});

import { TopicGroups } from "./TopicGroups";

describe("TopicGroups", () => {
  it("показывает группы топика и открывает группу", async () => {
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    renderWithClient(<TopicGroups connectionId="c" topic="orders" />);
    expect(await screen.findByText("svc")).toBeInTheDocument();
    expect(screen.getByText("—")).toBeInTheDocument();
    expect(screen.getByText(/проверены не все группы/)).toBeInTheDocument();
    await user.click(screen.getByText("svc"));
    expect(useWorkspace.getState().activeId).toBe("group:c:svc");
  });
});
```

Создать `src/components/browse/MessageDetails.test.tsx`:
```tsx
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { BrowsedMessage } from "../../api/types";
import { useWorkspace } from "../../state/workspace";
import { renderWithClient } from "../../test/render";
import { newProfile } from "../connections/profile";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      listConnections: vi.fn().mockResolvedValue([{ ...newProfile(), id: "c", name: "dev", environment: "dev" }]),
      listGroups: vi.fn().mockResolvedValue({ groups: [{ groupId: "svc", state: "Empty", isSimple: false }], errors: [] }),
    },
  };
});

import { MessageDetails } from "./MessageDetails";

const msg: BrowsedMessage = { partition: 2, offset: 41, timestamp: 0, key: null, value: null, headers: [], size: 0 };

describe("MessageDetails: сбросить группу сюда", () => {
  it("открывает вкладку группы с предзаполненным сбросом", async () => {
    useWorkspace.setState({ tabs: [], activeId: null });
    const user = userEvent.setup();
    renderWithClient(<MessageDetails message={msg} utc={false} connectionId="c" topic="orders" />);
    await user.click(await screen.findByRole("button", { name: "Сбросить группу сюда…" }));
    await user.type(screen.getByLabelText("Группа"), "svc");
    await user.click(screen.getByRole("button", { name: "Перейти к сбросу" }));
    const tab = useWorkspace.getState().tabs[0];
    expect(tab).toMatchObject({ kind: "group", connectionId: "c", groupId: "svc", resetPrefill: { topic: "orders", partition: 2, offset: 41 } });
  });
});
```

- [ ] **Step 2: Убедиться в падении**

Run: `pnpm vitest run src/components/topics/TopicGroups.test.tsx src/components/browse/MessageDetails.test.tsx`
Expected: FAIL — нет `TopicGroups`; у `MessageDetails` нет кнопки.

- [ ] **Step 3: `TopicGroups.tsx`**:

```tsx
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { activateProps } from "../../lib/activate";
import { formatCount } from "../../lib/format";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";

export function TopicGroups({ connectionId, topic }: { connectionId: string; topic: string }) {
  const open = useWorkspace((s) => s.open);
  const q = useQuery({
    queryKey: ["topicGroups", connectionId, topic],
    queryFn: () => api.getTopicGroups(connectionId, topic),
  });
  if (q.isPending) return <p className="empty">Поиск групп с коммитами в топик…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  return (
    <div>
      <div className="toolbar">
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          Обновить
        </button>
        {q.data.incomplete && (
          <span className="warn">За время запроса проверены не все группы — список может быть неполным.</span>
        )}
      </div>
      {q.data.groups.length === 0 ? (
        <p className="empty">Нет групп с коммитами в этот топик.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Группа</th>
              <th>Состояние</th>
              <th>Партиций с коммитами</th>
              <th>Лаг</th>
            </tr>
          </thead>
          <tbody>
            {q.data.groups.map((g) => (
              <tr key={g.groupId} className="clickable" {...activateProps(() => open({ kind: "group", connectionId, groupId: g.groupId }))}>
                <td>{g.groupId}</td>
                <td>{g.state}</td>
                <td>{g.partitions}</td>
                <td>{formatCount(g.lag)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
```

- [ ] **Step 4: Подвкладка в `TopicDetails`** — в `src/components/topics/TopicDetails.tsx`:

```tsx
import { TopicGroups } from "./TopicGroups";

type SubTab = "messages" | "partitions" | "configs" | "groups";
const SUBTABS: { id: SubTab; label: string }[] = [
  { id: "messages", label: "Сообщения" },
  { id: "partitions", label: "Партиции" },
  { id: "configs", label: "Конфигурация" },
  { id: "groups", label: "Consumer groups" },
];
```
и после строки с `TopicConfigs`:
```tsx
      {sub === "groups" && <TopicGroups connectionId={connectionId} topic={topic} />}
```

- [ ] **Step 5: «Сбросить группу сюда»** — заменить содержимое `src/components/browse/MessageDetails.tsx` (функции `copy` и `DataBlock` не меняются, `MessageDetails` получает новые пропсы и действие):

```tsx
import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { BrowsedMessage, DecodedData } from "../../api/types";
import { formatBytes, formatTimestamp } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import { useWorkspace } from "../../state/workspace";
import { prettyText } from "./params";

function copy(text: string) {
  try {
    void navigator.clipboard.writeText(text).catch(() => {});
  } catch {
    /* буфер обмена недоступен */
  }
}

function DataBlock({ title, data }: { title: string; data: DecodedData | null }) {
  return (
    <section>
      <h4>
        {title} {data && <span className="muted">({data.format}, {formatBytes(data.size)})</span>}
        {data && (
          <button
            type="button"
            className="link"
            aria-label={`Копировать ${title === "Ключ" ? "ключ" : "значение"}`}
            onClick={() => copy(data.text)}
          >
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

interface Props {
  message: BrowsedMessage | null;
  utc: boolean;
  connectionId: string;
  topic: string;
}

export function MessageDetails({ message, utc, connectionId, topic }: Props) {
  const connection = useConnection(connectionId);
  if (!message) return <div className="details-pane muted">Выберите сообщение</div>;
  return (
    <div className="details-pane">
      <p className="muted">
        Партиция {message.partition} · offset {message.offset} · {formatTimestamp(message.timestamp, utc)}
        <button
          type="button"
          className="link"
          aria-label="Копировать сообщение как JSON"
          onClick={() => copy(JSON.stringify(message, null, 2))}
        >
          {" "}
          копировать всё как JSON
        </button>
      </p>
      {connection && !connection.readOnly && <ResetGroupHere connectionId={connectionId} topic={topic} message={message} />}
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

/** Выбор группы → вкладка группы с предзаполненным сбросом: группа начнёт чтение с этого сообщения. */
function ResetGroupHere({ connectionId, topic, message }: { connectionId: string; topic: string; message: BrowsedMessage }) {
  const [open, setOpen] = useState(false);
  const [group, setGroup] = useState("");
  const openTab = useWorkspace((s) => s.open);
  const groups = useQuery({
    queryKey: ["groups", connectionId],
    queryFn: () => api.listGroups(connectionId),
    enabled: open,
  });
  const listId = `groups-${connectionId}`;
  if (!open) {
    return (
      <button type="button" className="link" onClick={() => setOpen(true)}>
        Сбросить группу сюда…
      </button>
    );
  }
  return (
    <div className="toolbar">
      <input aria-label="Группа" list={listId} placeholder="group.id" value={group} onChange={(e) => setGroup(e.target.value)} />
      <datalist id={listId}>
        {groups.data?.groups.map((g) => (
          <option key={g.groupId} value={g.groupId} />
        ))}
      </datalist>
      <button
        type="button"
        disabled={!group.trim()}
        onClick={() => {
          openTab({
            kind: "group",
            connectionId,
            groupId: group.trim(),
            resetPrefill: { topic, partition: message.partition, offset: message.offset, nonce: Date.now() },
          });
          setOpen(false);
        }}
      >
        Перейти к сбросу
      </button>
      <button type="button" onClick={() => setOpen(false)}>
        Отмена
      </button>
    </div>
  );
}
```

В `src/components/browse/BrowsePanel.tsx` передать новые пропсы:
```tsx
        <MessageDetails message={selected} utc={utc} connectionId={connectionId} topic={topic} />
```

- [ ] **Step 6: Тесты проходят**

Run: `pnpm vitest run && pnpm build`
Expected: PASS (включая `TopicDetails.test.tsx` — в его моке `listConnections` уже есть с Task 15).

- [ ] **Step 7: Commit**

```bash
git add src
git commit -m "feat(ui): группы топика и действие «Сбросить группу сюда»"
```

---

### Task 20: README, полная проверка и ручной смоук M2a

**Files:**
- Modify: `README.md`
- Modify: `docs/superpowers/plans/2026-10-14-kafka-explorer-m1.md` (раздел «Отложено из M1» — отметить, что взято в M2a)

- [ ] **Step 1: README** — в `README.md` после раздела «Где хранятся данные» добавить:

````markdown
## Защита от изменений
- **Только чтение** (флаг профиля): все изменяющие операции (создание/изменение/очистка/удаление топиков, сброс и удаление групп) отклоняются в Rust с кодом `READ_ONLY`.
- **prod**: каждая изменяющая операция требует ввести имя подключения; удаление и очистка дополнительно — имя объекта.
- Сброс оффсетов и удаление группы — только для группы без участников (`Empty`).

Проверить сброс вручную:
```bash
docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group>
```
````

- [ ] **Step 2: Отметка в плане M1** — в `docs/superpowers/plans/2026-10-14-kafka-explorer-m1.md` в разделе «Отложено из M1» заменить строку про размер на диске на:
```markdown
- Колонка `cleanup.policy` в списке топиков — сделано в M2a. Размер топика на диске (DescribeLogDirs) — вне MVP: librdkafka не поддерживает DescribeLogDirs.
```
и добавить в конец раздела:
```markdown
Из бэклога в M2a взяты: атомарная запись файлов, блокировки без паники, пул без мьютекса на время создания клиента, секрет SASL при смене протокола, уборка в IT. Остальные пункты бэклога (кроме общего кода чтения — M2b) — M4.
```

- [ ] **Step 3: Полный прогон**

Run:
```bash
docker compose -f dev/docker-compose.yml up -d
pnpm test && pnpm build && \
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings && \
KE_IT_BOOTSTRAP=localhost:9092 KE_IT_SASL_BOOTSTRAP=localhost:9094 cargo test --manifest-path src-tauri/Cargo.toml --lib --tests
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -c '^it-'
```
Expected: всё зелёное; последняя команда печатает `0`.

- [ ] **Step 4: Ручной смоук в `pnpm tauri dev`**

Подготовка: подключение `local` (`localhost:9092`, окружение `dev`), топик `smoke` с 50 сообщениями из смоука M1 (если его нет — создать командами из плана M1, Task 21).

1. «Топики» → «Создать топик»: имя `m2a smoke` → ошибка у поля; имя `m2a-smoke`, 3 партиции, пресет `compact` → внизу команда `kafka-topics.sh … --config cleanup.policy=compact` → «Создать» → открылась вкладка топика; в списке колонка cleanup.policy = `compact`.
2. Вкладка `m2a-smoke` → «Конфигурация» → «Редактировать»: `retention.ms` = `3600000` → «Проверить изменения» → diff «… → 3600000» → «Применить»; после обновления значение выделено как изменённое. «к default» → применить → снова default, `cleanup.policy` остался `compact`.
3. «Добавить партиции»: 2 → кнопка неактивна; 5 → «Увеличить» → «Партиции» показывает 5.
4. Отправить 10 сообщений в `m2a-smoke` (`docker exec -i ke-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic m2a-smoke`), «Очистить» → предупреждение про compact → ввести имя → в результате новые low watermark; «Партиции»: сообщений 0.
5. Группа: `docker exec ke-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic smoke --group smoke-grp --from-beginning --max-messages 20` → в дереве «Consumer groups» → `smoke-grp` (Empty), лаг ≈ 30. Детали: партиции с committed/log end/лаг.
6. «Сбросить оффсеты» → «С начала» → «Предпросмотр» → таблица «текущий → новый» = 0 → «Применить (3)» → committed 0, лаг 50. Проверить: `docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group smoke-grp`.
7. Запустить в терминале консьюмер группы без `--max-messages` (группа станет Stable) → «Сбросить оффсеты» → «Предпросмотр» → «Применить» → баннер `GROUP_NOT_EMPTY` со списком участников; «Удалить группу» неактивна. Остановить консьюмер.
8. Топик `smoke` → «Сообщения» → «Читать» → выбрать сообщение offset 10 партиции 0 → «Сбросить группу сюда…» → `smoke-grp` → «Перейти к сбросу» → во вкладке группы открыта панель сброса: «Конкретный оффсет» 10, область `smoke:0`.
9. Подвкладка «Consumer groups» у `smoke` → `smoke-grp` в списке с лагом; клик открывает группу.
10. Подключение переключить в окружение `prod` (снять «Только чтение») → «Создать топик» → появляется поле «Имя подключения», без него кнопка неактивна. Включить «Только чтение» → кнопки изменений скрыты.
11. Удалить `m2a-smoke` (ввод имени), удалить `smoke-grp` (Empty, ввод id) → исчезли из списков.

Каждую найденную проблему фиксировать отдельным коммитом с тестом, который её воспроизводит.

- [ ] **Step 5: Commit**

```bash
git add README.md docs/superpowers/plans/2026-10-14-kafka-explorer-m1.md
git commit -m "docs: защита изменений в README, отложенное из M1 закрыто в M2a"
```

---

## Что сознательно не входит в M2a

- M2b (`docs/superpowers/plans/2026-10-15-kafka-explorer-m2b.md`): Producer (одиночная и пакетная отправка, шаблоны, история), consumer-сессии, «Отправить повторно»; бэклог «общий код чтения».
- Вне MVP: размер топика на диске (librdkafka не поддерживает DescribeLogDirs).
- M4: rack брокеров и режим KRaft/ZK, настраиваемый таймаут и шаги TLS/SASL в Test connection, IT для SSL, остальной бэклог M1 (UI-мелочи, a11y, парсер фильтров, `plan_latest`, дубль vite).
