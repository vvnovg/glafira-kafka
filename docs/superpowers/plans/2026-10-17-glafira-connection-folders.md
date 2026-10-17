# Папки подключений, экспорт и импорт — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Вложенные папки подключений в левой панели и перенос дерева между компьютерами файлом экспорта/импорта (без паролей).

**Architecture:** Папки хранятся в том же `connections.json`, что и подключения (одна атомарная запись), операции над деревом — методы `ConnectionStore` в новом модуле `storage/tree.rs`. Экспорт и импорт — чистые функции в `storage/exchange.rs` (файл формата `glafira-connections` → план импорта → применение одной записью), команды Tauri — в `commands/folders.rs`. Фронтенд рисует дерево из `list_folders` + `list_connections` (у профилей новые поля `folderId`, `order`), диалоги и меню — в `src/components/connections/tree/`.

**Tech Stack:** Rust (serde, uuid, Tauri 2 commands), React 19, TanStack Query, react-i18next, HTML5 drag-and-drop, Vitest, cargo test.

**Спецификация:** `docs/superpowers/specs/2026-10-17-glafira-connection-folders-design.md`.

## Global Constraints

- Папка: `{ id: UUID, name, parentId | null, order }`; у профиля `folderId | null`, `order`. Глубина не ограничена.
- Инварианты: ссылки на папки существуют; циклов нет; имя папки непустое (после trim), до 100 символов, уникально среди соседей без учёта регистра; удаляется только пустая папка.
- `connections.json`: `schema_version` остаётся `1`; неизвестные поля файла, профилей и папок сохраняются при записи (`#[serde(flatten)] extra`). Миграция: подключения в корне, `order` — индекс в текущем списке.
- Файл экспорта: `format = "glafira-connections"`, `version = 1` (мажор); неизвестные поля игнорируются; подключение с неизвестным значением или не прошедшее `validate_profile` — пропускается с причиной; ошибка только при `version > 1` или чужом `format`.
- Секреты не экспортируются никогда; `needsPassword { sasl, truststore, keystore, key }` — какие пароли были сохранены.
- Импорт: сопоставление по `id`; новые — под цель со структурой файла; совпавшие подключения при `updateExisting` получают настройки, но остаются на месте и сохраняют пароли; совпавшие папки не двигаются и не переименовываются; конфликт имени в той же папке — суффикс « (2)», « (3)»…; чужие подключения не трогаются; во Free `environment = prod` → `readOnly = true`; применение одной записью, при ошибке ничего не меняется.
- Папки, экспорт, импорт — во Free.
- Все тексты — ключи словарей во всех 8 языках (en эталон, ru смысл, de/fr/es/it/pl/pt-BR по `docs/i18n/glossary.md`); ошибки Rust — `app_err!` с ключами `errors.folders.*`, `errors.import.*`.
- В Tauri `window.confirm`/`prompt` не работают — подтверждения и ввод имени только внутри страницы.
- Проверки после каждой задачи: `cargo test --lib` (из `src-tauri`), `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, `pnpm test`, `pnpm i18n:check`, `pnpm build`.
- Коммиты — на русском в стиле репозитория, заканчиваются строкой атрибуции модели исполнителя. Push только с согласия пользователя.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `src-tauri/src/storage/connections.rs` | модель (`ConnectionFolder`, новые поля профиля, `extra`), файл, миграция, `upsert` с сохранением `extra`/`order` |
| `src-tauri/src/storage/tree.rs` | операции над деревом: создать/переименовать/удалить папку, переместить, пути папок |
| `src-tauri/src/storage/exchange.rs` | формат файла, экспорт, план импорта, применение |
| `src-tauri/src/commands/folders.rs` | команды Tauri папок, экспорта, импорта |
| `src/api/types.ts`, `src/api/client.ts` | типы и вызовы |
| `src/components/connections/tree/folderTree.ts` | чистые функции: построение дерева, фильтр, пути, допустимость перемещения |
| `src/components/connections/tree/ConnectionTree.tsx` | дерево в левой панели (папки, подключения, сворачивание, фильтр, меню, DnD) |
| `src/components/connections/tree/FolderMenu.tsx` | меню папки и заголовка, ввод имени внутри страницы |
| `src/components/connections/tree/ImportDialog.tsx` | диалог предпросмотра импорта |
| `src/components/Sidebar.tsx` | использует `ConnectionTree` |
| `src/components/connections/ConnectionForm.tsx` | поле «Папка» |
| `docs/smoke/folders.md` | смоук |

---

### Task 1: Модель папок и сохранение неизвестных полей

**Files:**
- Modify: `src-tauri/src/storage/connections.rs`, `src/api/types.ts`

**Interfaces:**
- Produces (Rust):
  ```rust
  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  #[serde(rename_all = "camelCase")]
  pub struct ConnectionFolder {
      pub id: String,
      pub name: String,
      #[serde(default)]
      pub parent_id: Option<String>,
      #[serde(default)]
      pub order: i64,
      #[serde(flatten)]
      pub extra: serde_json::Map<String, serde_json::Value>,
  }
  ```
  В `ConnectionProfile` добавить (в конец структуры):
  ```rust
      #[serde(default)]
      pub folder_id: Option<String>,
      #[serde(default)]
      pub order: i64,
      #[serde(flatten)]
      pub extra: serde_json::Map<String, serde_json::Value>,
  ```
  `ConnectionsFile` — `folders: Vec<ConnectionFolder>` (`#[serde(default)]`) и `#[serde(flatten)] extra`.
  `ConnectionStore` хранит `RwLock<StoreData>`, где `pub(crate) struct StoreData { pub connections: Vec<ConnectionProfile>, pub folders: Vec<ConnectionFolder>, pub extra: serde_json::Map<…> }`; методы `list()` (как раньше — профили), `list_folders() -> Vec<ConnectionFolder>`, `pub(crate) fn update<R>(&self, f: impl FnOnce(&mut StoreData) -> AppResult<R>) -> AppResult<R>` — клонирует данные, применяет `f`, проверяет инварианты (`check_tree`), пишет файл и только потом заменяет кэш.
- Produces (TS): `ConnectionProfile` получает `folderId?: string | null; order?: number`; новый тип `ConnectionFolder { id: string; name: string; parentId: string | null; order: number }`.

- [ ] **Step 1: Падающие тесты** (в `mod tests` файла):

```rust
#[test]
fn migration_keeps_connections_in_root_with_order() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("connections.json"),
        r#"{"schemaVersion":1,"connections":[
          {"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"}},
          {"id":"b","name":"B","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1","security":{"protocol":"PLAINTEXT"}}]}"#,
    ).unwrap();
    let store = ConnectionStore::open(dir.path()).unwrap();
    let list = store.list();
    assert_eq!(list.iter().map(|c| (c.id.as_str(), c.folder_id.clone(), c.order)).collect::<Vec<_>>(),
               vec![("a", None, 0), ("b", None, 1)]);
    assert!(store.list_folders().is_empty());
}

#[test]
fn unknown_fields_survive_write() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("connections.json"),
        r#"{"schemaVersion":1,"future":{"x":1},"folders":[{"id":"f","name":"F","parentId":null,"order":0,"icon":"star"}],
           "connections":[{"id":"a","name":"A","color":"#fff","environment":"dev","readOnly":false,"bootstrapServers":"h:1",
           "security":{"protocol":"PLAINTEXT"},"folderId":"f","order":0,"pinned":true}]}"#,
    ).unwrap();
    let store = ConnectionStore::open(dir.path()).unwrap();
    let mut p = store.get("a").unwrap();
    p.name = "A2".into();
    p.extra.clear(); // как профиль из формы фронтенда — без неизвестных полей
    store.upsert(p).unwrap();
    let raw: serde_json::Value = serde_json::from_slice(&std::fs::read(dir.path().join("connections.json")).unwrap()).unwrap();
    assert_eq!(raw["future"]["x"], 1);
    assert_eq!(raw["folders"][0]["icon"], "star");
    assert_eq!(raw["connections"][0]["pinned"], true);
    assert_eq!(raw["connections"][0]["name"], "A2");
    assert_eq!(raw["connections"][0]["folderId"], "f");
}

#[test]
fn new_connection_goes_to_end_of_its_folder() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConnectionStore::open(dir.path()).unwrap();
    store.upsert(sample_profile("a")).unwrap();
    store.upsert(sample_profile("b")).unwrap();
    assert_eq!(store.get("b").unwrap().order, 1);
}

#[test]
fn dangling_folder_reference_is_rejected_on_write() {
    let dir = tempfile::tempdir().unwrap();
    let store = ConnectionStore::open(dir.path()).unwrap();
    let mut p = sample_profile("a");
    p.folder_id = Some("missing".into());
    let e = store.upsert(p).unwrap_err();
    assert_eq!(e.key, "errors.folders.notFound");
}
```

`sample_profile` дополнить полями `folder_id: None, order: 0, extra: Default::default()`. Если в крейте нет `tempfile` в dev-зависимостях — посмотреть, как временные каталоги создают существующие тесты хранилищ, и использовать тот же способ.

- [ ] **Step 2: Убедиться, что тесты падают** — `cd src-tauri && cargo test --lib storage::connections` → ошибки компиляции (нет полей).

- [ ] **Step 3: Реализовать.**
  - Структуры выше; `open`: после чтения выставить `order = индекс` всем подключениям, если у всех `order == 0` и их больше одного (миграция старого файла); папки и висячие ссылки при чтении: `folder_id`/`parent_id` на несуществующую папку → `None` (корень), без ошибки.
  - `upsert(profile)`: если профиль с таким `id` есть — перенести из старого `extra`, если у входящего `extra` пуст; `order` входящего сохраняется как есть. Новый профиль: `order = max(order соседей в той же папке) + 1` (или 0). Через `update`.
  - `check_tree(&StoreData) -> AppResult<()>`: все `folder_id`/`parent_id` существуют (`errors.folders.notFound`), нет циклов (`errors.folders.cycle`), имена папок валидны и уникальны среди соседей (`errors.folders.nameEmpty`, `errors.folders.nameTooLong`, `errors.folders.nameTaken`). Тексты:

| Ключ | en | ru |
|---|---|---|
| `errors.folders.notFound` | Folder {{id}} not found | Папка {{id}} не найдена |
| `errors.folders.cycle` | A folder cannot be moved into itself or its subfolder | Папку нельзя переместить в неё саму или во вложенную папку |
| `errors.folders.nameEmpty` | Enter a folder name | Укажите имя папки |
| `errors.folders.nameTooLong` | The folder name is longer than 100 characters | Имя папки длиннее 100 символов |
| `errors.folders.nameTaken` | A folder named “{{name}}” already exists here | Папка «{{name}}» здесь уже есть |
| `errors.folders.notEmpty` | The folder is not empty: move or delete its connections and folders first | Папка не пуста: сначала перенесите или удалите её подключения и папки |

  Ключи — во всех 8 словарях (`errors.folders` в разделе `errors`), Pro-языки — по глоссарию; в глоссарий добавить строку `| folder | папка | Ordner | dossier | carpeta | cartella | folder | pasta |`. `errors.folders.notEmpty` понадобится в Task 2 — добавить сразу.
  - `delete(id)` и прочие существующие методы — через `update`.
  - TS-типы.

- [ ] **Step 4: Тесты и проверки** — `cargo test --lib`, clippy, fmt, `pnpm test`, `pnpm i18n:check`, `pnpm build` → PASS (существующие тесты фронтенда, создающие профили, при необходимости дополнить `folderId: null, order: 0` только если TypeScript требует — поля необязательные).

- [ ] **Step 5: Commit** — `feat(folders): модель папок подключений и сохранение неизвестных полей`.

### Task 2: Операции над деревом и команды папок

**Files:**
- Create: `src-tauri/src/storage/tree.rs`, `src-tauri/src/commands/folders.rs`
- Modify: `src-tauri/src/storage/mod.rs`, `src-tauri/src/commands/mod.rs`, `src-tauri/src/lib.rs` (регистрация), `src-tauri/src/commands/connections.rs` (новое подключение в папку), `src/api/client.ts`

**Interfaces:**
- Consumes: `ConnectionStore::update`, `StoreData`, `check_tree` (Task 1).
- Produces (Rust, `impl ConnectionStore` в `tree.rs`):
  - `create_folder(&self, name: &str, parent_id: Option<&str>) -> AppResult<ConnectionFolder>` — новый UUID, `name.trim()`, `order` в конце уровня;
  - `rename_folder(&self, id: &str, name: &str) -> AppResult<ConnectionFolder>`;
  - `delete_folder(&self, id: &str) -> AppResult<()>` — `errors.folders.notEmpty`, если есть дочерние папки или подключения;
  - `move_items(&self, items: &[TreeItemRef], parent_id: Option<&str>, before: Option<&TreeItemRef>) -> AppResult<()>` — переносит элементы в `parent_id` и ставит перед `before` (или в конец); пересчитывает `order` уровня `0..n` (папки и подключения нумеруются раздельно); циклы ловит `check_tree`;
  - `folder_path(&self, id: &str) -> Vec<String>` — имена от корня (для предпросмотра импорта и формы).
  - `#[derive(Deserialize, Clone)] #[serde(rename_all = "camelCase")] pub struct TreeItemRef { pub kind: TreeItemKind, pub id: String }`, `#[serde(rename_all = "lowercase")] pub enum TreeItemKind { Folder, Connection }`.
- Produces (команды): `list_folders() -> Vec<ConnectionFolder>`, `create_folder(name, parent_id)`, `rename_folder(id, name)`, `delete_folder(id)`, `move_items(items, parent_id, before)`.
- Produces (TS, `api`): `listFolders()`, `createFolder(name, parentId)`, `renameFolder(id, name)`, `deleteFolder(id)`, `moveItems(items: { kind: "folder" | "connection"; id: string }[], parentId: string | null, before: { kind; id } | null)`.

- [ ] **Step 1: Падающие тесты** в `tree.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::connections::{sample_profile, ConnectionStore};

    fn store() -> (tempfile::TempDir, ConnectionStore) {
        let d = tempfile::tempdir().unwrap();
        let s = ConnectionStore::open(d.path()).unwrap();
        (d, s)
    }

    #[test]
    fn create_rename_delete() {
        let (_d, s) = store();
        let a = s.create_folder("  ИФТ ", None).unwrap();
        assert_eq!(a.name, "ИФТ");
        let b = s.create_folder("kafka-1", Some(&a.id)).unwrap();
        assert_eq!(s.folder_path(&b.id), vec!["ИФТ", "kafka-1"]);
        assert_eq!(s.create_folder("ифт", None).unwrap_err().key, "errors.folders.nameTaken");
        assert_eq!(s.delete_folder(&a.id).unwrap_err().key, "errors.folders.notEmpty");
        s.rename_folder(&b.id, "kafka-2").unwrap();
        s.delete_folder(&b.id).unwrap();
        s.delete_folder(&a.id).unwrap();
        assert!(s.list_folders().is_empty());
    }

    #[test]
    fn move_into_descendant_is_a_cycle() {
        let (_d, s) = store();
        let a = s.create_folder("A", None).unwrap();
        let b = s.create_folder("B", Some(&a.id)).unwrap();
        let e = s.move_items(&[TreeItemRef { kind: TreeItemKind::Folder, id: a.id.clone() }], Some(&b.id), None).unwrap_err();
        assert_eq!(e.key, "errors.folders.cycle");
        assert_eq!(s.list_folders().iter().find(|f| f.id == a.id).unwrap().parent_id, None, "ничего не изменилось");
    }

    #[test]
    fn move_connections_and_reorder() {
        let (_d, s) = store();
        for id in ["a", "b", "c"] { s.upsert(sample_profile(id)).unwrap(); }
        let f = s.create_folder("F", None).unwrap();
        let conn = |id: &str| TreeItemRef { kind: TreeItemKind::Connection, id: id.into() };
        s.move_items(&[conn("c"), conn("a")], Some(&f.id), None).unwrap();
        let in_f: Vec<_> = { let mut v: Vec<_> = s.list().into_iter().filter(|p| p.folder_id.as_deref() == Some(&f.id)).collect(); v.sort_by_key(|p| p.order); v.into_iter().map(|p| p.id).collect() };
        assert_eq!(in_f, vec!["c", "a"]);
        s.move_items(&[conn("a")], Some(&f.id), Some(&conn("c"))).unwrap();
        let mut v: Vec<_> = s.list().into_iter().filter(|p| p.folder_id.as_deref() == Some(&f.id)).collect();
        v.sort_by_key(|p| p.order);
        assert_eq!(v.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(), vec!["a", "c"]);
        assert_eq!(s.get("b").unwrap().folder_id, None);
    }
}
```

- [ ] **Step 2: Убедиться, что тесты падают** — `cargo test --lib storage::tree` → нет модуля/методов.
- [ ] **Step 3: Реализовать** методы, команды (по образцу `commands/connections.rs`: `State<'_, AppState>`, `blocking` для записи), регистрацию в `lib.rs`, вызовы в `client.ts`. `save_connection`: если у нового профиля `folder_id` указан — проверка существования папки уже в `check_tree`.
- [ ] **Step 4: Проверки** (все из Global Constraints).
- [ ] **Step 5: Commit** — `feat(folders): операции над деревом папок и команды`.

### Task 3: Экспорт

**Files:**
- Create: `src-tauri/src/storage/exchange.rs`
- Modify: `src-tauri/src/storage/mod.rs`, `src-tauri/src/commands/folders.rs`, `src-tauri/src/lib.rs`, `src/api/client.ts`, `src/api/types.ts`, словари

**Interfaces:**
- Consumes: `ConnectionStore::{list, list_folders}`, `SecretStore`, `secret_key`, константы `SASL_PASSWORD`, `SSL_TRUSTSTORE_PASSWORD`, `SSL_KEYSTORE_PASSWORD`, `SSL_KEY_PASSWORD`.
- Produces:
  ```rust
  pub const EXCHANGE_FORMAT: &str = "glafira-connections";
  pub const EXCHANGE_VERSION: u32 = 1;

  #[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
  #[serde(rename_all = "camelCase")]
  pub struct NeedsPassword { pub sasl: bool, pub truststore: bool, pub keystore: bool, pub key: bool }

  #[derive(Serialize)]
  #[serde(rename_all = "camelCase")]
  pub struct ExchangeFileOut<'a> {
      pub format: &'a str, pub version: u32, pub exported_at: String, pub app_version: &'a str,
      pub folders: Vec<ConnectionFolder>,
      pub connections: Vec<ExportedConnection>,
  }
  #[derive(Serialize, Deserialize, Clone, Debug)]
  #[serde(rename_all = "camelCase")]
  pub struct ExportedConnection { #[serde(flatten)] pub profile: ConnectionProfile, #[serde(default)] pub needs_password: NeedsPassword }

  pub fn build_export(store: &ConnectionStore, secrets: &dyn SecretStore, root: Option<&str>, app_version: &str, now: &str)
      -> AppResult<ExchangeFileOut<'static>>; // поддерево root (сама папка получает parent_id = None) или всё дерево
  ```
  Профили экспортируются с очищенным `extra` (неизвестные локальные поля не выносятся). Команда `export_connections(path: String, folder_id: Option<String>) -> ExportSummary { connections: usize, folders: usize }` — пишет файл атомарно (`write_json`). TS: `exportConnections(path, folderId | null): Promise<{ connections: number; folders: number }>`.

- [ ] **Step 1: Падающие тесты** в `exchange.rs`: экспорт всего (все папки и подключения, `format`, `version`); экспорт поддерева (только потомки, корень с `parentId: null`, подключения вне поддерева отсутствуют); в JSON нет строк паролей (положить в тестовый `SecretStore` пароль `s3cr3t` и проверить `!json.contains("s3cr3t")`), `needsPassword.sasl == true` для подключения с сохранённым SASL-паролем; несуществующий `root` → `errors.folders.notFound`. Тестовый `SecretStore` — как в существующих тестах (`grep -rn "impl SecretStore for" src-tauri/src`).
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать**; `app_version` — `env!("CARGO_PKG_VERSION")`, `now` — RFC 3339 из `chrono`/`time` (как в `diagnostics.rs`).
- [ ] **Step 4: Проверки.**
- [ ] **Step 5: Commit** — `feat(folders): экспорт подключений без паролей`.

### Task 4: План и применение импорта

**Files:**
- Modify: `src-tauri/src/storage/exchange.rs`, `src-tauri/src/commands/folders.rs`, `src-tauri/src/lib.rs`, `src/api/client.ts`, `src/api/types.ts`, словари

**Interfaces:**
- Consumes: Task 1–3, `validate_profile`, `LicenseState::is_pro()` (или как лицензия проверяется в `save_connection_blocking` — `st.license`).
- Produces:
  ```rust
  pub fn parse_exchange(bytes: &[u8]) -> AppResult<ParsedExchange>;
  // ParsedExchange { folders: Vec<ConnectionFolder>, connections: Vec<(ConnectionProfile, NeedsPassword)>, skipped: Vec<Skipped> }
  // Читает serde_json::Value: format ≠ EXCHANGE_FORMAT → errors.import.notGlafiraFile; version > EXCHANGE_VERSION → errors.import.incompatibleVersion;
  // каждое подключение десериализуется отдельно: ошибка serde или validate_profile → Skipped { name, reason: LocalizedText }.

  pub struct ImportOptions<'a> { pub target: Option<&'a str>, pub update_existing: bool, pub is_pro: bool }
  pub fn plan_import(data: &StoreData, parsed: &ParsedExchange, opts: &ImportOptions, file_exists: &dyn Fn(&str) -> bool) -> AppResult<ImportPlan>;
  // ImportPlan { next: StoreData, preview: ImportPreview }

  #[derive(Serialize)] #[serde(rename_all = "camelCase")]
  pub struct ImportPreview { pub added: Vec<PreviewItem>, pub updated: Vec<PreviewItem>, pub unchanged: Vec<PreviewItem>,
                             pub skipped: Vec<Skipped>, pub warnings: Vec<LocalizedText>, pub needs_passwords: usize }
  #[derive(Serialize)] #[serde(rename_all = "camelCase")]
  pub struct PreviewItem { pub kind: TreeItemKind, pub id: String, pub name: String, pub path: Vec<String> }
  ```
  Команды: `preview_import(path, target_folder_id, update_existing) -> ImportPreview` (только чтение); `apply_import(path, target_folder_id, update_existing) -> ImportPreview` (пересчитывает план на свежих данных и пишет `next` через `ConnectionStore::update` одной записью). TS: `previewImport(path, targetFolderId | null, updateExisting)`, `applyImport(...)`, тип `ImportPreview`.

  Алгоритм `plan_import` (порядок важен):
  1. Проверить, что `target` существует (`errors.folders.notFound`).
  2. Папки файла в порядке «родитель раньше потомка» (топологически; папка с `parentId`, которого нет в файле, считается корневой в файле). Для каждой: `id` есть локально → «без изменений» (папка не двигается и не переименовывается); иначе — создать с тем же `id`, родитель = соответствующая папка (локальная с этим `id`, если родитель из файла уже был локально, иначе созданная) или `target` для корневых папок файла; имя занято среди соседей → суффикс « (2)», « (3)»…; «новое».
  3. Подключения: `id` есть локально → если настройки (все поля, кроме `id`, `folder_id`, `order`, `extra`) совпадают → «без изменений»; иначе при `update_existing` → обновить настройки, оставить `folder_id`, `order`, `extra` локальными → «обновится»; при `!update_existing` → «без изменений». Нет локально → создать в папке из файла (с учётом шага 2) или в `target`, `order` в конце, имя с суффиксом при конфликте среди соседей → «новое».
  4. `!is_pro` и `environment == Prod` у нового или обновлённого → `read_only = true`, одно предупреждение `warnings.import.prodReadOnly` с количеством.
  5. Для новых и обновлённых с путями к файлам сертификатов (`ca_location`, `certificate_path`, `key_path`), которых нет (`file_exists`), — одно предупреждение `warnings.import.missingFiles` с количеством подключений.
  6. `needs_passwords` — число новых подключений с любым флагом `needsPassword` (у обновлённых пароли получателя сохраняются).
  7. `check_tree(&next)` — на всякий случай; ошибка → вернуть её.

  Тексты (все 8 словарей; ключи `warnings.*` — новый раздел верхнего уровня, перед `hints`):

| Ключ | en | ru |
|---|---|---|
| `errors.import.notGlafiraFile` | This is not a Glafira connections file | Это не файл подключений Glafira |
| `errors.import.incompatibleVersion` | The file was created by an incompatible version of Glafira — update the app | Файл создан в несовместимой версии Glafira — обновите приложение |
| `errors.import.invalidJson` | The file is damaged or is not JSON: {{error}} | Файл повреждён или это не JSON: {{error}} |
| `errors.import.unsupportedConnection` | Not supported by this version of Glafira (update the app): {{error}} | Не поддерживается этой версией Glafira (обновите приложение): {{error}} |
| `warnings.import.prodReadOnly_one` / `_other` | {{count}} prod connection will be read-only: changing prod connections requires Pro / {{count}} prod connections will be read-only: changing prod connections requires Pro | {{count}} prod-подключение будет только для чтения: изменение prod-подключений доступно в Pro / … (формы ru: one, few, many, other) |
| `warnings.import.missingFiles_one` / `_other` | Certificate files of {{count}} connection are not found on this computer — check the paths / … of {{count}} connections … | Файлы сертификатов {{count}} подключения не найдены на этом компьютере — проверьте пути / … |

  Плюральные формы — по `Intl.PluralRules` каждого языка (ru: `_one`, `_few`, `_many`, `_other`; pl так же; fr, es, it, pt-BR: `_one`, `_many`, `_other`).

- [ ] **Step 1: Падающие тесты** (по одному на правило, на `StoreData` в памяти и `ParsedExchange`, собранном из JSON-строки):
  1. новые папка и подключение под `target`, структура файла сохранена;
  2. совпавшее подключение с изменённым `bootstrapServers`: `update_existing = true` → обновлено, `folder_id`/`order`/`extra` прежние; `false` → без изменений;
  3. совпавшая папка, переименованная в файле, — не переименована и не перемещена; новое подключение файла внутри неё попадает в неё;
  4. конфликт имени → « (2)», при занятом « (2)» → « (3)»;
  5. чужое локальное подключение не изменилось;
  6. `is_pro = false`, prod в файле → `read_only = true` и предупреждение; `is_pro = true` → `read_only` как в файле;
  7. отсутствующий файл сертификата → предупреждение `missingFiles`;
  8. `parse_exchange`: лишние поля верхнего уровня и в профиле — читается; подключение с `"security":{"protocol":"SASL_SSL","sasl":{"mechanism":"OAUTHBEARER",…}}` — в `skipped`, остальные в плане; `version: 2` → `errors.import.incompatibleVersion`; `format: "other"` → `errors.import.notGlafiraFile`; мусор → `errors.import.invalidJson`;
  9. атомарность: `apply_import` при ошибке записи (каталог только для чтения — по образцу существующих тестов `fs.rs`, если такие есть; иначе — `ConnectionStore::update`, где замыкание возвращает ошибку) — `connections.json` не изменился;
  10. экспорт → импорт в пустое хранилище (круг) даёт те же папки и подключения (без паролей).
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать** функции, команды, TS.
- [ ] **Step 4: Проверки.**
- [ ] **Step 5: Commit** — `feat(folders): предпросмотр и применение импорта`.

### Task 5: Дерево в левой панели

**Files:**
- Create: `src/components/connections/tree/folderTree.ts` (+ `.test.ts`), `src/components/connections/tree/ConnectionTree.tsx` (+ `.test.tsx`)
- Modify: `src/components/Sidebar.tsx`, `src/styles.css`, словари

**Interfaces:**
- Consumes: `api.listFolders`, `api.listConnections` (queryKey `["folders"]`, `["connections"]`).
- Produces (`folderTree.ts`):
  ```ts
  export type TreeNode =
    | { kind: "folder"; folder: ConnectionFolder; children: TreeNode[] }
    | { kind: "connection"; connection: ConnectionProfile };
  export function buildTree(folders: ConnectionFolder[], connections: ConnectionProfile[]): TreeNode[]; // папки первыми, внутри по order, затем по имени
  export function filterTree(nodes: TreeNode[], query: string): TreeNode[]; // регистр не важен; совпала папка — остаётся целиком; совпало подключение — остаются его предки
  export function folderPaths(folders: ConnectionFolder[]): { id: string; path: string[] }[]; // для выпадающих списков, отсортированы по пути
  export function canMoveFolder(folders: ConnectionFolder[], folderId: string, targetParentId: string | null): boolean; // не в себя и не в потомка
  ```
- Produces (`ConnectionTree`): компонент без пропсов; рендер узла подключения — текущая разметка `Sidebar` (точка цвета, имя, `EnvBadge`, вложенный список «Топики», «Группы»…), вынесенная в `ConnectionNode`. Папка: кнопка-переключатель ▸/▾ + имя; состояние свёрнутых папок — `localStorage` ключ `glafira.tree.collapsed` (массив id, чтение/запись в `try/catch`). Поле фильтра над деревом (`sidebar.filter` — placeholder и aria-label); при непустом фильтре все папки развёрнуты.

Тексты: `sidebar.filter` en «Filter connections» / ru «Фильтр подключений»; `sidebar.folderToggle` en «Expand or collapse {{name}}» / ru «Развернуть или свернуть {{name}}»; `sidebar.noMatches` en «Nothing found» / ru «Ничего не найдено» — во всех 8 словарях.

- [ ] **Step 1: Падающие тесты** `folderTree.test.ts`: порядок (папки первыми, по `order`); вложенность 3 уровня; висячий `folderId` → в корне; фильтр по подключению оставляет предков, по папке — всё содержимое; `canMoveFolder` запрещает себя и потомка. `ConnectionTree.test.tsx` (моки `api.listFolders`/`listConnections` как в существующих тестах `Sidebar`/`Layout`): папка с подключением отображается, клик по ▸ сворачивает и состояние переживает перерисовку (localStorage), фильтр скрывает несовпавшие и показывает «Ничего не найдено». Существующие тесты, которые ищут подключения в левой панели (`Layout.test.tsx`, `ProGating.test.tsx`), должны проходить без изменений, кроме добавления мока `listFolders` (возвращает `[]`).
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать**; стили: `.tree-folder` (жирнее, без точки цвета), отступ вложенности — как у текущего `.tree ul`.
- [ ] **Step 4: Проверки.**
- [ ] **Step 5: Commit** — `feat(folders): дерево папок в левой панели и фильтр`.

### Task 6: Действия с папками, перетаскивание, поле «Папка»

**Files:**
- Create: `src/components/connections/tree/FolderMenu.tsx`
- Modify: `src/components/connections/tree/ConnectionTree.tsx` (+ тест), `src/components/connections/ConnectionForm.tsx` (+ тест), `src/state/workspace.ts` (если нужно передать `folderId` в форму нового подключения), `src/styles.css`, словари

**Interfaces:**
- Consumes: `api.createFolder/renameFolder/deleteFolder/moveItems`, `folderPaths`, `canMoveFolder`.
- Поведение:
  - Заголовок панели: кнопки «+ подключение» (как сейчас), «+ папка» (`sidebar.newFolder`), «⋯» (меню: «Экспортировать всё…», «Импортировать…» — пункты появляются в Task 7; в этой задаче меню содержит только «Новая папка»).
  - У папки — кнопка «⋯» и правый клик (`onContextMenu` с `preventDefault`) открывают меню: «Новая папка», «Новое подключение здесь», «Переименовать», «Удалить» (+ экспорт/импорт в Task 7). Меню — абсолютный `div role="menu"` с `role="menuitem"`, закрывается Esc и кликом вне.
  - Новая папка и переименование — строка ввода прямо в дереве (`input` с автофокусом; Enter — сохранить, Esc — отмена; ошибка Rust — под полем через `tr(err)`).
  - Удаление — подтверждение внутри страницы (как `ConfirmDialog`, без ввода имени); ошибка «не пуста» — показать текст ошибки.
  - Перетаскивание: `draggable` у папок и подключений; цели — папка (положить внутрь) и узел (положить перед ним в его родителя); недопустимая цель (`canMoveFolder` = false) — без `preventDefault` в `onDragOver`, без подсветки; на drop — `api.moveItems`, затем `invalidateQueries(["folders"])` и `["connections"]`.
  - Форма подключения: поле «Папка» (`connections.form.folder`) — `select` с «(корень)» (`connections.form.folderRoot`) и путями из `folderPaths` через « / »; по умолчанию `folderId` профиля или папка, из которой открыли «Новое подключение здесь».
    `save_connection` для существующего подключения место в дереве не меняет (Task 1: `upsert` сохраняет хранимые `folder_id`/`order`). Поэтому при сохранении существующего подключения с изменённой папкой форма после успешного `saveConnection` вызывает `api.moveItems([{ kind: "connection", id }], newFolderId, null)`; для нового подключения `folderId` уходит в профиле.

Тексты (8 словарей): `sidebar.newFolder` «New folder» / «Новая папка»; `sidebar.menu` «More actions» / «Ещё действия»; `sidebar.folder.newConnection` «New connection here» / «Новое подключение здесь»; `sidebar.folder.rename` «Rename» / «Переименовать»; `sidebar.folder.delete` «Delete» / «Удалить»; `sidebar.folder.deleteConfirm` «Delete folder “{{name}}”?» / «Удалить папку «{{name}}»?»; `sidebar.folder.namePlaceholder` «Folder name» / «Имя папки»; `connections.form.folder` «Folder» / «Папка»; `connections.form.folderRoot` «(root)» / «(корень)».

- [ ] **Step 1: Падающие тесты**: создание папки через «+ папка» (ввод имени, Enter → `api.createFolder("ИФТ", null)`); переименование через меню; удаление с подтверждением; ошибка `errors.folders.nameTaken` показывается под полем; drop подключения на папку вызывает `api.moveItems([{kind:"connection",id}], folderId, null)` (через `fireEvent.dragStart/dragOver/drop` с `dataTransfer`-заглушкой); drop папки в её потомка не вызывает `moveItems`; форма подключения отправляет выбранный `folderId`; «Новое подключение здесь» открывает форму с предвыбранной папкой.
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать.**
- [ ] **Step 4: Проверки.**
- [ ] **Step 5: Commit** — `feat(folders): действия с папками, перетаскивание и поле «Папка»`.

### Task 7: Экспорт и импорт в интерфейсе

**Files:**
- Create: `src/components/connections/tree/ImportDialog.tsx` (+ `.test.tsx`)
- Modify: `src/components/connections/tree/FolderMenu.tsx`, `src/components/connections/tree/ConnectionTree.tsx`, `src/state/workspace.ts` (вкладка или модальное окно — по образцу существующих диалогов), словари

**Interfaces:**
- Consumes: `api.exportConnections`, `api.previewImport`, `api.applyImport`, `save`/`open` из `@tauri-apps/plugin-dialog` (фильтр `{ name: "Glafira", extensions: ["json"] }`, имя по умолчанию `glafira-connections-<YYYY-MM-DD>.glafira-connections.json` или `<имя папки>.glafira-connections.json`), `Hint`.
- Поведение:
  - Меню заголовка: «Экспортировать всё…», «Импортировать…»; меню папки: «Экспортировать папку…», «Импортировать сюда…».
  - Экспорт: диалог сохранения → `exportConnections` → сообщение в панели (`role="status"`) «Сохранено: {{connections}} подключений, {{folders}} папок» (плюралы не нужны — формат «подключений: N, папок: M»).
  - Импорт: диалог открытия → `ImportDialog` (модальный, внутри страницы): цель (`select`: «(корень)» и пути; при «Импортировать сюда» — эта папка) с `<Hint k="hints.import.target">`; флажок «Обновлять существующие» (включён) с `<Hint k="hints.import.updateExisting">`; при смене цели или флажка — повторный `previewImport`; сводка «Добавится: N · Обновится: M · Без изменений: K · Пропущено: S»; раскрываемые списки по группам с путями; предупреждения (`tr(w)`) и «Введите пароли для {{count}} подключений», если `needsPasswords > 0`; кнопки «Импортировать» (`applyImport`, затем `invalidateQueries`, закрыть, сообщение «Импортировано: добавлено N, обновлено M») и «Отмена». Ошибка разбора файла — `ErrorBanner` в диалоге.

Тексты (8 словарей): `sidebar.exportAll` «Export all…» / «Экспортировать всё…»; `sidebar.import` «Import…» / «Импортировать…»; `sidebar.folder.export` «Export folder…» / «Экспортировать папку…»; `sidebar.folder.importHere` «Import here…» / «Импортировать сюда…»; `exchange.exported` «Saved: connections {{connections}}, folders {{folders}}» / «Сохранено: подключений {{connections}}, папок {{folders}}»; `exchange.import.title` «Import connections» / «Импорт подключений»; `exchange.import.target` «Import into» / «Куда импортировать»; `exchange.import.updateExisting` «Update existing connections» / «Обновлять существующие подключения»; `exchange.import.summary` «Added: {{added}} · Updated: {{updated}} · Unchanged: {{unchanged}} · Skipped: {{skipped}}» / «Добавится: {{added}} · Обновится: {{updated}} · Без изменений: {{unchanged}} · Пропущено: {{skipped}}»; `exchange.import.groupAdded` «New» / «Новые»; `exchange.import.groupUpdated` «Will be updated» / «Обновятся»; `exchange.import.groupUnchanged` «Unchanged» / «Без изменений»; `exchange.import.groupSkipped` «Skipped» / «Пропущены»; `exchange.import.passwords` «Enter passwords for {{count}} connections after import» / «После импорта введите пароли для {{count}} подключений» (как «label: value» при необходимости плюралов — допускается `_one/_other` по правилам языка); `exchange.import.apply` «Import» / «Импортировать»; `exchange.imported` «Imported: added {{added}}, updated {{updated}}» / «Импортировано: добавлено {{added}}, обновлено {{updated}}»; `hints.import.target` «Where new folders and connections go. Existing ones stay where they are.» / «Куда попадут новые папки и подключения. Существующие останутся на своих местах.»; `hints.import.updateExisting` «Connections that already exist get the settings from the file; your passwords and their place in the tree are kept.» / «Уже существующие подключения получат настройки из файла; ваши пароли и их место в дереве сохранятся.»

- [ ] **Step 1: Падающие тесты** `ImportDialog.test.tsx` (моки api и plugin-dialog): сводка и группы из `previewImport`; смена цели вызывает `previewImport` с новым `targetFolderId`; снятие флажка — с `updateExisting=false`; предупреждения и строка о паролях; «Импортировать» вызывает `applyImport` с теми же аргументами и закрывает диалог; ошибка `errors.import.incompatibleVersion` показывается баннером. В тесте дерева: «Экспортировать папку…» вызывает `exportConnections(path, folderId)`; отмена диалога сохранения ничего не вызывает.
- [ ] **Step 2: Тесты падают.**
- [ ] **Step 3: Реализовать.**
- [ ] **Step 4: Проверки.**
- [ ] **Step 5: Commit** — `feat(folders): экспорт и импорт в интерфейсе`.

### Task 8: Документация и смоук

**Files:**
- Create: `docs/smoke/folders.md`
- Modify: `README.md` (раздел о подключениях — абзац о папках и переносе), `docs/superpowers/specs/2026-10-17-glafira-connection-folders-design.md` (§3: `list_folders` + поля профиля вместо `list_connection_tree`)

- [ ] **Step 1: `docs/smoke/folders.md`:**

```markdown
# Смоук: папки подключений, экспорт и импорт

`pnpm tauri dev`, Kafka: `docker compose -f dev/docker-compose.yml up -d`.

1. Старые подключения на месте, в корне, в прежнем порядке.
2. «+ папка» → «Проект А»; в ней «Новая папка» → «ИФТ», «ПТ»; переименовать «ПТ» → «ПСИ»; имя «ифт» рядом с «ИФТ» — ошибка.
3. Перетащить подключения в «ИФТ»; папку «ИФТ» — в корень и обратно; папку «Проект А» внутрь «ИФТ» — не даёт.
4. Удалить непустую папку — ошибка «Папка не пуста»; пустую — удаляется.
5. Свернуть «Проект А», перезапустить приложение — свёрнута. Фильтр «ифт» — видна ветка «Проект А / ИФТ».
6. Форма подключения: поле «Папка», «Новое подключение здесь» — папка предвыбрана.
7. «Экспортировать всё…» и «Экспортировать папку…» — файлы сохранены; открыть в редакторе — паролей нет, есть `needsPassword`.
8. Второй профиль данных (или другой компьютер): «Импортировать…» → предпросмотр «Добавится …», импорт, ввести пароли — подключения работают.
9. Изменить в исходном приложении адрес подключения, экспортировать, импортировать снова: «Обновится: 1»; место в дереве и пароль сохранились; снять «Обновлять существующие» — «Без изменений».
10. «Импортировать сюда…» на папке — новые попадают в неё.
11. Free (без ключа Pro): импорт файла с prod-подключением — предупреждение, подключение только для чтения.
12. Файл с добавленным вручную лишним полем — импортируется; `"version": 2` — «Файл создан в несовместимой версии Glafira».
13. Язык English и Deutsch — меню, диалог импорта и ошибки на выбранном языке.
```

- [ ] **Step 2: README и спека** — абзац о папках и экспорте/импорте; в спеке §3 заменить строку `list_connection_tree` на `list_folders() -> Vec<ConnectionFolder>` (профили из `list_connections` содержат `folderId` и `order`).
- [ ] **Step 3: Полный прогон** — все проверки из Global Constraints.
- [ ] **Step 4: Commit** — `docs(folders): смоук, README и уточнение спеки`.

---

## После плана

- Финальное ревью ветки (`superpowers:requesting-code-review`): инварианты дерева, атомарность, отсутствие секретов в экспорте, правила импорта §5, совместимость форматов, Pro-гейтинг prod, переводы.
- Ручной смоук пользователем по `docs/smoke/folders.md`.
