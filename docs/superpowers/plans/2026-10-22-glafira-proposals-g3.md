# G3 «Предложения LLM в формах» — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** LLM-ассистент через MCP предлагает изменение (конфиги топика, новый топик, партиции, сброс оффсетов), предложение показывается в Glafira рядом с объектом, а применяет его человек заполненной формой приложения; статус «применено» ставит Rust после успеха операции. Плюс хвосты финальной волны G2.

**Architecture:** Предложение — запись в `health/<connectionId>.json` рядом с замечаниями (`src-tauri/src/health/store.rs`); виды изменения, проверка против фактов кластера и сравнение применённого — чистый модуль `src-tauri/src/health/proposals.rs`; факты собирает `src-tauri/src/mcp/backend.rs` существующими функциями чтения. Крейт `glafira-mcp` получает модуль `proposals.rs` (`ChangeSpec`, `ProposalOp`, `ProposalOutput`), метод `Backend::proposals` и три инструмента (`server/proposal_tools.rs`). Команды интерфейса — `commands/health.rs`; четыре изменяющие команды принимают `proposal_id` и помечают предложение применённым после успеха. Фронтенд: раздел «Предложения LLM» на вкладке «Замечания», карточки в блоках топика и группы, поле `proposal` у вкладок `topics`/`topic`/`group` и заполнение четырёх форм.

**Tech Stack:** Rust (serde, tokio, rmcp 3.5.1, schemars 1), Tauri 2, React 19, TanStack Query 5 (`useIsMutating`), react-i18next, Vitest.

**Спецификация:** `docs/superpowers/specs/2026-10-22-glafira-proposals-g3-design.md`.

**Проверено до написания плана.** Весь код плана собран и прогнан в отдельном worktree (ветка `proto/g3`, по коммиту на задачу); повторное применение всех блоков плана в чистом worktree даёт тот же результат побайтно:
- приложение — clippy, fmt, 475 unit-тестов, интеграционные `it_*` с локальной Kafka (в том числе новый тест предложений в `it_health`);
- крейт — 28 unit-тестов, 36 тестов сервера, `manifest`, `gui_stdio`, clippy, fmt;
- фронтенд — 458 тестов в 69 файлах, `tsc`, сборка, `test:scripts` (71), `i18n:check` — словари 12 языков и главы руководства.

Код переносится как есть. Перед каждым блоком кода стоит HTML-комментарий с его назначением: `block: patch <имя>` — патч `git diff` против текущего состояния ветки (применять `git apply --recount`); `block: file <путь> part=impl|tests|whole` — новый файл (у Rust-файлов две части: реализация и модуль тестов, итоговый файл = реализация, пустая строка, модуль тестов); `block: spec /tmp/<имя>.json` — строки словарей для скрипта `add-keys.mjs`; `block: helper` — сам скрипт. Ограждения блоков — четыре обратные кавычки: внутри встречаются тройные. Блоки извлекайте программно (скрипт по комментариям), не перепечатывайте.

## Global Constraints

- Кластер меняют только люди через формы. Инструменты `propose_change`, `list_proposals`, `withdraw_proposal` пишут только в файл Glafira; крейт `glafira-mcp` по-прежнему не зависит от `rdkafka`, `tauri` и `kafka-explorer` (тест `tests/manifest.rs`). Удаление, очистка топиков, удаление групп и отправка сообщений через предложения не проходят.
- Виды изменения — ровно четыре: `alterTopicConfigs` (`changes [{ name, value | null }]`, 1–50 записей, значения ≤ 1024 символов), `createTopic` (`replicationFactor` по умолчанию `-1`, до 50 конфигов), `addPartitions` (`newTotal` больше текущего), `resetOffsets` (`strategy`/`scope` как у формы сброса, списки ≤ 100). JSON-форма одна у LLM, в файле и во фронтенде (`kind`, camelCase).
- Проверка предложения при создании — против живого кластера (спец. §5): внутренние топики отвергаются, неизвестный ключ конфига — `invalid_argument`, нет топика/группы — `not_found`.
- Отметка «применено» ставится только в Rust после успеха операции (`commands::health::mark_applied`): нет предложения, оно закрыто, другой объект — операция всё равно успешна, отметка пропускается; у `reset_offsets` — только если ни одна партиция не вернула ошибку; `appliedAsProposed` — сравнение с предложенным.
- Хранение: `proposals` в `health/<connectionId>.json`, срок 7 дней от создания при любом статусе, не больше 50 открытых (`limit_reached`), повтор того же автора по тому же виду и объекту обновляет запись; статусы `open | applied | rejected | withdrawn`; «Открыть снова» — только из `rejected`. Небезопасный id подключения (импорт) получает файл `h-<hex байтов id>.json`.
- Журнал MCP: у `propose_change` — `connectionId`, `kind`, `target`, `severity`, `relatedRuleId`; без заголовка, обоснования и значений.
- Всего инструментов 19 (тест-«сторожок»); ни инструкции, ни описания не содержат слова `Pro` целиком.
- Тексты для LLM — по-английски; строки интерфейса — ключами во всех 12 словарях (en — эталон ключей, ru — эталон смысла, остальные — по `docs/i18n/glossary.md`); новые ключи — скриптом `add-keys.mjs` из spec-файлов плана, `git diff --stat src/locales` показывает только добавления (кроме обновлённого `health.sourceLlm`). Подтверждения — только внутри страницы (`window.confirm` в Tauri 2 не работает).
- Правило `cluster.noController` срабатывает только при партициях без лидера (параметр `leaderless`); при лидерах у всех — `cluster.controllerUnknown` («предупреждение» в любом окружении). Всего 21 правило.
- Проверки после каждой задачи (что применимо): крейт — в `crates/glafira-mcp`: `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`; приложение — в `src-tauri`: `cargo test --lib`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`; фронтенд — `pnpm test`, `pnpm exec tsc --noEmit`, `pnpm i18n:check`, `pnpm build`. Интеграционные тесты — `KE_IT_BOOTSTRAP=localhost:9092` (dev-стенд `docker compose -f dev/docker-compose.yml up -d`, один брокер).
- Тесты Rust не используют Unix-пути вида `/nonexistent/…` (на Windows они не абсолютны).
- Отладочный бинарь — `src-tauri/target/debug/kafka-explorer`; `src-tauri/target` может быть символической ссылкой — не удалять и не заменять, без `cargo clean`.
- Коммиты — на русском в стиле репозитория, последняя строка — трейлер `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Файлы добавлять поимённо (в рабочем дереве есть чужие неотслеживаемые `dev/certs/out/*`). Push — только с согласия пользователя.

## Вспомогательный скрипт для словарей

Строки на 12 языках добавляются скриптом `add-keys.mjs` (во временный каталог, в репозиторий не коммитится). Spec-файл: `{ "path": "a.b", "after": "<ключ>", "values": { "<язык>": { … } } }` — объект `values[язык]` вставляется по пути `path`: если раздела нет — создаётся сразу после ключа `after` (или в конце), если есть — ключи добавляются в него (существующий ключ с тем же именем заменяется — так обновляется `health.sourceLlm`). Формат файла (2 пробела, порядок ключей) сохраняется.

<!-- block: helper /tmp/add-keys.mjs -->
````js
// node add-keys.mjs <localesDir> <spec.json>: spec = { path: "errors.health", after: "mcp", values: { en: {...}, ru: {...}, ... } }
// Вставляет объект values[lang] по пути (создаёт раздел после ключа `after`), сохраняя порядок ключей и формат (2 пробела).
import fs from "node:fs";
const [dir, specFile] = process.argv.slice(2);
const spec = JSON.parse(fs.readFileSync(specFile, "utf8"));
for (const [lang, add] of Object.entries(spec.values)) {
  const file = `${dir}/${lang}.json`;
  const root = JSON.parse(fs.readFileSync(file, "utf8"));
  const parts = spec.path.split(".");
  const leaf = parts.pop();
  let obj = root;
  for (const p of parts) obj = obj[p];
  if (obj[leaf] === undefined) {
    const out = {};
    let placed = false;
    for (const [k, v] of Object.entries(obj)) {
      out[k] = v;
      if (k === spec.after) { out[leaf] = add; placed = true; }
    }
    if (!placed) out[leaf] = add;
    for (const k of Object.keys(obj)) delete obj[k];
    Object.assign(obj, out);
  } else {
    Object.assign(obj[leaf], add);
  }
  fs.writeFileSync(file, JSON.stringify(root, null, 2) + "\n");
}
````

Запуск: `node /tmp/add-keys.mjs src/locales /tmp/<spec>.json`, затем `pnpm i18n:check`.

## Структура файлов

| Файл | Ответственность |
|---|---|
| `src-tauri/src/health/proposals.rs` (новый) | виды изменения `Change`, `Proposal`, статусы, факты `Facts`, чистая проверка `validate`, сравнение `applied_as_proposed` |
| `src-tauri/src/health/store.rs` | `proposals` в файле, `propose`, `set_proposal_status`, `mark_applied`, `withdraw_own`, удаление; hex-имя файла для импортированных id |
| `src-tauri/src/health/service.rs` | идущая проверка под другим окружением: дождаться и запустить свою |
| `src-tauri/src/health/rules/brokers.rs`, `rules/mod.rs`, `texts.rs` | `cluster.noController` только при партициях без лидера, новое `cluster.controllerUnknown` |
| `src-tauri/src/kafka/cluster.rs` | повтор `rd_kafka_controllerid` |
| `src-tauri/src/kafka/groups.rs`, `kafka/admin.rs` | `Serialize` у типов сброса, `PartialEq` у `NewTopicSpec` |
| `src-tauri/src/mcp/backend.rs` | `Backend::proposals`: факты из кластера, проверка, запись; единый код `internal` |
| `src-tauri/src/commands/health.rs` | `HealthView.proposals`, `mark_applied`, команды статуса и удаления предложений |
| `src-tauri/src/commands/topic_ops.rs`, `commands/groups.rs`, `src-tauri/src/lib.rs` | `proposal_id` (и `request`) у изменяющих команд, регистрация команд |
| `src-tauri/tests/it_health.rs` | интеграция: предложение сверяется с кластером, применяется командой, помечается |
| `crates/glafira-mcp/src/proposals.rs` (новый) | `ChangeSpec` (схема для LLM), `ProposalOp`, `ProposalOutput`, `ProposalView` |
| `crates/glafira-mcp/src/server/proposal_tools.rs` (новый) | `propose_change`, `list_proposals`, `withdraw_proposal` |
| `crates/glafira-mcp/src/server/mod.rs`, `model.rs`, `lib.rs` | роутер, `Backend::proposals`, `instructions` |
| `src/api/types.ts`, `src/api/client.ts` | типы и команды предложений, `proposalId` у изменяющих команд |
| `src/lib/useHealth.ts` | ключ мутации проверки, действия с предложениями |
| `src/components/health/findings.ts`, `proposals.ts` (новый) | предложения в значках, описание изменения, статус, вкладка формы |
| `src/components/health/{CheckBar,FindingsPanel,HealthBlock,ProposalItem,ProposalBanner}.tsx` | общее «Проверка…», пометка окружения, раздел и карточки предложений, строка над формой |
| `src/state/workspace.ts`, `src/components/TabContent.tsx` | `ProposalPrefill`, `ResetPrefill` из предложения, проброс во вкладки |
| `src/components/common/ConfigEditor.tsx`, `topics/{CreateTopicForm,TopicActions,TopicDetails,TopicList}.tsx`, `groups/{GroupDetails,ResetOffsetsPanel}.tsx`, `groups/reset.ts` | формы, открываемые заполненными, с `proposalId` |
| `docs/guide/*/health-check.md`, `docs/guide/*/llm-mcp.md`, `docs/i18n/glossary.md`, `README.md` | руководство на 12 языках, глоссарий, описание |
| `docs/smoke/g3.md` | чек-лист GUI-смоука |

---

### Task 1: Хвосты G2 в Rust: hex-имя файла, проверка другого окружения, правило `cluster.controllerUnknown`, повтор controller id, единый код `internal`

**Files:**
- Modify: `src-tauri/src/health/store.rs` (`file_stem`, `MAX_ID_BYTES`), `src-tauri/src/health/service.rs` (`running` с окружением, цикл в `run_source`), `src-tauri/src/health/rules/brokers.rs`, `src-tauri/src/health/rules/mod.rs` (таблица, 21 правило), `src-tauri/src/health/texts.rs`, `src-tauri/src/kafka/cluster.rs` (`native_cluster_info`), `src-tauri/src/mcp/backend.rs` (`health_to_backend`), `src/locales/*.json` (12 словарей: `health.rules.cluster.controllerUnknown.*`, `health.ruleNames.cluster.controllerUnknown`)
- Test: модули `tests` в тех же файлах

**Interfaces:**
- Consumes: `hex::encode` (зависимость уже есть), `crate::storage::connections::Environment`, фикстуры `health::rules::fixtures`, фейковые источники `Counting`/`Gated` из тестов `service.rs`.
- Produces: `health::store::{MAX_ID_BYTES = 100, file_stem(id) -> Option<String>}` (uuid — как есть, иначе `h-<hex>`; пустой и длиннее 100 байт — `None` → `errors.connections.notFound`); `HealthService.running: HashMap<String, (Environment, watch::Receiver<Outcome>)>`; `run_source` ждёт идущую проверку другого окружения и запускает свою; правило `cluster.controllerUnknown` (`W, W, W`) и `cluster.noController` с параметром `leaderless`; `texts::english` для обоих; `mcp::backend::{BACKGROUND_FAILED, FILE_ERROR_KEYS, FILE_ERROR_PREFIX, TASK_PANICKED_KEY}`, `health_to_backend` отдаёт `internal` для файловых ошибок и паники фоновой задачи; ключи словарей для нового правила (их проверяет тест `every_rule_has_interface_texts`).

Остатки финальной волны G2 (спец. G3 §9, пп. 3–5), всё в Rust. Импортированное подключение с id вроде `team/prod` раньше не имело файла `health/` и давало баннер «подключение не найдено» на обзоре — теперь имя файла строится из hex байтов id. Проверка, запущенная под одним окружением, не годится присоединившемуся с другим (строгость другая): он дожидается её и запускает свою. `cluster.noController` при всех живых лидерах — почти наверняка таймаут запроса controller id, а не кластер без контроллера: это теперь отдельное правило-предупреждение `cluster.controllerUnknown`, а сам запрос повторяется один раз. Паника фоновой задачи у операций G2/G3 отдаётся LLM кодом `internal` (строчными), а не `INTERNAL` с текстом про сертификаты.

- [ ] **Step 1: Тесты.**

Патч тестовых модулей (`store.rs`, `service.rs`, `rules/brokers.rs`, `rules/mod.rs`, `mcp/backend.rs`): hex-имя и удаление файла по импортированному id, длинный и пустой id; проверка dev ждёт prod и запускает свою (`dev_src.metadata_calls` 0 → 1, в файле — dev); `cluster.noController` только с партициями без лидера, иначе `cluster.controllerUnknown`; таблица из 21 правила; классификация ошибок по ключу.

<!-- block: patch task1-tests.diff -->
````diff
diff --git a/src-tauri/src/health/rules/brokers.rs b/src-tauri/src/health/rules/brokers.rs
index 017e768..c6d16a4 100644
--- a/src-tauri/src/health/rules/brokers.rs
+++ b/src-tauri/src/health/rules/brokers.rs
@@ -158,14 +180,41 @@ mod tests {
     }
 
     #[test]
-    fn no_controller_is_critical_everywhere() {
-        let mut s = snap(3, vec![]);
+    fn no_controller_with_leaderless_partitions_is_critical_everywhere() {
+        let mut s = snap(
+            3,
+            vec![topic("a", &[(-1, &[1, 2], &[]), (2, &[2, 3], &[2, 3])])],
+        );
         s.controller_id = None;
         for env in ENVS {
             let f = evaluate(&s, env);
-            assert_eq!(f[0].rule_id, "cluster.noController");
+            assert_eq!(f[0].rule_id, "cluster.noController", "{env:?}");
             assert_eq!(f[0].severity, Severity::Critical);
+            assert_eq!(f[0].params["leaderless"], 1);
+            assert!(
+                !f.iter().any(|x| x.rule_id == "cluster.controllerUnknown"),
+                "{env:?}"
+            );
+        }
+    }
+
+    #[test]
+    fn unknown_controller_with_leaders_everywhere_is_a_warning() {
+        let mut s = snap(3, vec![topic("a", &[(1, &[1, 2], &[1, 2])])]);
+        s.controller_id = None;
+        for env in ENVS {
+            let f = evaluate(&s, env);
+            assert_eq!(f[0].rule_id, "cluster.controllerUnknown", "{env:?}");
+            assert_eq!(f[0].severity, Severity::Warning);
+            assert!(!f.iter().any(|x| x.rule_id == "cluster.noController"));
         }
+        // Без топиков лидеров нет вовсе — тоже «неизвестен», а не «нет контроллера».
+        let mut empty = snap(3, vec![]);
+        empty.controller_id = None;
+        assert_eq!(
+            evaluate(&empty, Environment::Prod)[0].rule_id,
+            "cluster.controllerUnknown"
+        );
     }
 
     #[test]
diff --git a/src-tauri/src/health/rules/mod.rs b/src-tauri/src/health/rules/mod.rs
index 4fab81b..44e4753 100644
--- a/src-tauri/src/health/rules/mod.rs
+++ b/src-tauri/src/health/rules/mod.rs
@@ -171,7 +172,7 @@ mod tests {
         ids.sort();
         ids.dedup();
         assert_eq!(ids.len(), n);
-        assert_eq!(n, 20);
+        assert_eq!(n, 21);
     }
 
     #[test]
@@ -179,6 +180,7 @@ mod tests {
         // Строки таблицы §4: правило, prod, stage, прочие («К», «П», «И», «—»).
         let spec = [
             ("cluster.noController", "ККК"),
+            ("cluster.controllerUnknown", "ППП"),
             ("partition.offline", "ККК"),
             ("partition.underReplicated", "КПИ"),
             ("cluster.singleBroker", "КП—"),
diff --git a/src-tauri/src/health/service.rs b/src-tauri/src/health/service.rs
index d8b9b92..1c20f60 100644
--- a/src-tauri/src/health/service.rs
+++ b/src-tauri/src/health/service.rs
@@ -469,6 +478,70 @@ mod tests {
         assert!(svc.store.load("c1", now_ms()).unwrap().last_check.is_some());
     }
 
+    #[tokio::test]
+    async fn check_of_another_environment_waits_and_runs_its_own() {
+        let dir = tempfile::tempdir().unwrap();
+        let svc = Arc::new(HealthService::open(dir.path()));
+        // Проверка на prod стоит у ворот; тем временем окружение подключения сменили на dev.
+        let (entered, gate) = (
+            Arc::new(tokio::sync::Notify::new()),
+            Arc::new(tokio::sync::Notify::new()),
+        );
+        let gated = Arc::new(Gated {
+            inner: Counting::default(),
+            entered: entered.clone(),
+            gate: gate.clone(),
+        });
+        let prod = tokio::spawn({
+            let svc = svc.clone();
+            async move {
+                svc.run_source(
+                    "c1",
+                    svc.generation("c1"),
+                    Environment::Prod,
+                    Trigger::Ui,
+                    gated,
+                    LIMITS,
+                )
+                .await
+            }
+        });
+        entered.notified().await;
+        let dev_src = Arc::new(Counting {
+            lag: 1,
+            ..Default::default()
+        });
+        let dev = tokio::spawn({
+            let (svc, dev_src) = (svc.clone(), dev_src.clone());
+            async move {
+                svc.run_source(
+                    "c1",
+                    svc.generation("c1"),
+                    Environment::Dev,
+                    Trigger::Mcp { client: None },
+                    dev_src,
+                    LIMITS,
+                )
+                .await
+            }
+        });
+        // Пока prod не закончилась, dev ждёт и свой источник не трогает.
+        tokio::time::sleep(Duration::from_millis(100)).await;
+        assert_eq!(dev_src.metadata_calls.load(Ordering::SeqCst), 0);
+        gate.notify_one();
+        let prod = prod.await.unwrap().unwrap();
+        let dev = dev.await.unwrap().unwrap();
+        assert_ne!(prod.run_id, dev.run_id);
+        assert_eq!(
+            (prod.environment, dev.environment),
+            (Environment::Prod, Environment::Dev)
+        );
+        assert_eq!(dev_src.metadata_calls.load(Ordering::SeqCst), 1);
+        // В файле — последняя (dev) проверка.
+        let saved = svc.store.load("c1", now_ms()).unwrap().last_check.unwrap();
+        assert_eq!(saved.run_id, dev.run_id);
+    }
+
     #[tokio::test]
     async fn generation_read_before_deletion_is_not_saved() {
         let dir = tempfile::tempdir().unwrap();
diff --git a/src-tauri/src/health/store.rs b/src-tauri/src/health/store.rs
index c5213d7..466f5da 100644
--- a/src-tauri/src/health/store.rs
+++ b/src-tauri/src/health/store.rs
@@ -670,15 +680,39 @@ mod tests {
     }
 
     #[test]
-    fn unsafe_connection_id_is_not_a_path() {
-        let (_d, s) = store();
-        for id in ["../x", "a/b", "", "a b"] {
+    fn imported_id_gets_a_hex_file_name_and_is_never_a_path() {
+        let (d, s) = store();
+        // Импортированный id с «/», «..» и пробелом: файл внутри health/, по hex байтов id.
+        for (id, stem) in [
+            ("../x", "h-2e2e2f78"),
+            ("a/b", "h-612f62"),
+            ("a b", "h-612062"),
+        ] {
+            s.set_ignored(id, "k", true, T0).unwrap();
+            assert_eq!(file_stem(id).as_deref(), Some(stem), "{id}");
+            let file = d.path().join("health").join(format!("{stem}.json"));
+            assert!(file.exists(), "{id}");
+            assert!(s.load(id, T0).unwrap().is_ignored("k"), "{id}");
+            s.delete_connection(id).unwrap();
+            assert!(!file.exists(), "{id}");
+        }
+        assert!(!d.path().join("x.json").exists());
+        assert!(!d.path().join("health").join("b.json").exists());
+        // Uuid приложения — без изменений.
+        assert_eq!(file_stem("c1").as_deref(), Some("c1"));
+        // Пустой и слишком длинный id файла не имеют.
+        let long = "й".repeat(MAX_ID_BYTES / 2 + 1);
+        for id in ["", long.as_str()] {
             assert_eq!(
                 s.load(id, T0).unwrap_err().code,
                 ErrorCode::NotFound,
-                "{id}"
+                "{id:?}"
             );
         }
+        assert_eq!(
+            file_stem(&"a".repeat(MAX_ID_BYTES)).as_deref(),
+            Some("a".repeat(MAX_ID_BYTES).as_str())
+        );
     }
 
     #[test]
diff --git a/src-tauri/src/mcp/backend.rs b/src-tauri/src/mcp/backend.rs
index c9c6742..5c11c48 100644
--- a/src-tauri/src/mcp/backend.rs
+++ b/src-tauri/src/mcp/backend.rs
@@ -1042,6 +1052,52 @@ mod tests {
         }
     }
 
+    #[test]
+    fn health_errors_are_classified_by_key() {
+        // Файловые ошибки и паника фоновой задачи — `internal` строчными с общим текстом;
+        // остальное — как в G1.
+        let err = |key: &str| {
+            let mut e = app_err!(
+                ErrorCode::Internal,
+                "errors.io",
+                "Failed to write {{path}}",
+                path = "/secret-path/health/open.json"
+            );
+            e.key = key.to_string();
+            e
+        };
+        for key in [
+            "errors.io",
+            "errors.json",
+            "errors.storage.writeFailed",
+            "errors.storage.newerSchema",
+        ] {
+            let e = health_to_backend(err(key));
+            assert_eq!(
+                (e.code.as_str(), e.message.as_str()),
+                ("internal", HEALTH_FILE_FAILED),
+                "{key}"
+            );
+        }
+        let e = health_to_backend(err(TASK_PANICKED_KEY));
+        assert_eq!(
+            (e.code.as_str(), e.message.as_str()),
+            ("internal", BACKGROUND_FAILED)
+        );
+        let e = health_to_backend(crate::health::store::connection_not_found("x"));
+        assert_eq!(e.code, "NOT_FOUND");
+        let e = health_to_backend(app_err!(
+            ErrorCode::Internal,
+            "errors.kafka.emptyResponse",
+            "Empty {{request}} response",
+            request = "DescribeConfigs"
+        ));
+        assert_eq!(
+            (e.code.as_str(), e.message.as_str()),
+            ("INTERNAL", "Empty DescribeConfigs response")
+        );
+    }
+
     #[test]
     fn target_name_is_stored_as_it_was_checked() {
         let target = |kind, name: &str| {
````

- [ ] **Step 2: Увидеть падение.**

Run: `cd src-tauri && cargo test --lib -- health:: mcp::backend::` — Expected: ошибка компиляции (не найдены `file_stem`, `MAX_ID_BYTES`, `BACKGROUND_FAILED`, `TASK_PANICKED_KEY`).

- [ ] **Step 3: Реализация.**

Патч реализации (включая тексты LLM в `texts.rs` и повтор `controllerid` в `kafka/cluster.rs`):

<!-- block: patch task1-src.diff -->
````diff
diff --git a/src-tauri/src/health/rules/brokers.rs b/src-tauri/src/health/rules/brokers.rs
index 017e768..c6d16a4 100644
--- a/src-tauri/src/health/rules/brokers.rs
+++ b/src-tauri/src/health/rules/brokers.rs
@@ -18,8 +18,30 @@ const SKEW_MIN_AVERAGE: usize = 10;
 const SKEW_DENOMINATOR: usize = 5;
 
 pub(super) fn check(s: &ClusterSnapshot, out: &mut Findings) {
+    // Контроллер неизвестен: при партициях без лидера кластер действительно без контроллера;
+    // если лидеры у всех — скорее всего, запрос id не успел ответить (предупреждение).
     if s.controller_id.is_none() {
-        out.emit("cluster.noController", Target::cluster(), None, json!({}));
+        let leaderless = s
+            .topics
+            .iter()
+            .flat_map(|t| &t.partitions)
+            .filter(|p| p.offline())
+            .count();
+        if leaderless > 0 {
+            out.emit(
+                "cluster.noController",
+                Target::cluster(),
+                None,
+                json!({ "leaderless": leaderless }),
+            );
+        } else {
+            out.emit(
+                "cluster.controllerUnknown",
+                Target::cluster(),
+                None,
+                json!({}),
+            );
+        }
     }
     if s.brokers.len() == 1 {
         out.emit("cluster.singleBroker", Target::cluster(), None, json!({}));
diff --git a/src-tauri/src/health/rules/mod.rs b/src-tauri/src/health/rules/mod.rs
index 4fab81b..44e4753 100644
--- a/src-tauri/src/health/rules/mod.rs
+++ b/src-tauri/src/health/rules/mod.rs
@@ -39,6 +39,7 @@ const fn rule(
 /// Таблица §4 спецификации: prod, stage, остальные окружения.
 pub const RULES: &[RuleDef] = &[
     rule("cluster.noController", C, C, C),
+    rule("cluster.controllerUnknown", W, W, W),
     rule("partition.offline", C, C, C),
     rule("partition.underReplicated", C, W, I),
     rule("cluster.singleBroker", C, W, N),
diff --git a/src-tauri/src/health/service.rs b/src-tauri/src/health/service.rs
index d8b9b92..1c20f60 100644
--- a/src-tauri/src/health/service.rs
+++ b/src-tauri/src/health/service.rs
@@ -29,7 +29,8 @@ pub fn now_ms() -> i64 {
 
 pub struct HealthService {
     pub store: HealthStore,
-    running: Mutex<HashMap<String, watch::Receiver<Outcome>>>,
+    /// Идущие проверки: окружение, под которым считается строгость, и канал результата.
+    running: Mutex<HashMap<String, (Environment, watch::Receiver<Outcome>)>>,
     /// Поколение данных подключения: растёт при удалении (`forget`), нет записи — 0. Идущая проверка
     /// сохраняет результат, только если поколение с её начала не изменилось.
     generations: Mutex<HashMap<String, u64>>,
@@ -143,32 +144,40 @@ impl HealthService {
         src: Arc<dyn Source>,
         limits: Limits,
     ) -> AppResult<Arc<CheckRun>> {
-        let rx = {
-            let mut running = self.running.lock_safe();
-            match running.get(connection_id) {
-                Some(rx) => rx.clone(),
-                None => {
-                    let (tx, rx) = watch::channel(None);
-                    running.insert(connection_id.to_string(), rx.clone());
-                    let this = self.clone();
-                    let id = connection_id.to_string();
-                    // Проверка доводится до конца, даже если вызвавший перестал ждать.
-                    tauri::async_runtime::spawn(async move {
-                        let r = this
-                            .check(&id, generation, env, trigger, src, limits)
-                            .await
-                            .map(Arc::new);
-                        this.running.lock_safe().remove(&id);
-                        if r.is_ok() {
-                            this.changed(&id);
-                        }
-                        let _ = tx.send(Some(r));
-                    });
-                    rx
+        loop {
+            let (rx, same_env) = {
+                let mut running = self.running.lock_safe();
+                match running.get(connection_id) {
+                    Some((running_env, rx)) => (rx.clone(), *running_env == env),
+                    None => {
+                        let (tx, rx) = watch::channel(None);
+                        running.insert(connection_id.to_string(), (env, rx.clone()));
+                        let this = self.clone();
+                        let id = connection_id.to_string();
+                        let (trigger, src) = (trigger.clone(), src.clone());
+                        // Проверка доводится до конца, даже если вызвавший перестал ждать.
+                        tauri::async_runtime::spawn(async move {
+                            let r = this
+                                .check(&id, generation, env, trigger, src, limits)
+                                .await
+                                .map(Arc::new);
+                            this.running.lock_safe().remove(&id);
+                            if r.is_ok() {
+                                this.changed(&id);
+                            }
+                            let _ = tx.send(Some(r));
+                        });
+                        (rx, true)
+                    }
                 }
+            };
+            if same_env {
+                return wait(rx).await;
             }
-        };
-        wait(rx).await
+            // Идущая проверка считается под другим окружением (его сменили в форме подключения):
+            // её строгость не годится — дождаться её и запустить свою.
+            let _ = wait(rx).await;
+        }
     }
 
     async fn check(
diff --git a/src-tauri/src/health/store.rs b/src-tauri/src/health/store.rs
index c5213d7..466f5da 100644
--- a/src-tauri/src/health/store.rs
+++ b/src-tauri/src/health/store.rs
@@ -181,13 +181,25 @@ fn same_title(a: &str, b: &str) -> bool {
     a.trim().to_lowercase() == b.trim().to_lowercase()
 }
 
-/// Идентификатор подключения в имени файла: только то, что создаёт приложение (uuid).
-fn safe_id(id: &str) -> bool {
-    !id.is_empty()
-        && id.len() <= 64
-        && id
-            .chars()
-            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
+/// Самый длинный id подключения, у которого есть файл: hex-имя (`h-` + 2 символа на байт + `.json`)
+/// должно укладываться в 255 байт имени файла.
+pub const MAX_ID_BYTES: usize = 100;
+
+/// Имя файла по id подключения. Id, который создаёт приложение (uuid), — как есть; любой другой
+/// (импортированный файл подключений может принести что угодно) — `h-<hex байтов id>`: такое имя
+/// безопасно для файловой системы и однозначно. Пустой и слишком длинный id файла не имеют.
+pub(crate) fn file_stem(id: &str) -> Option<String> {
+    if id.is_empty() || id.len() > MAX_ID_BYTES {
+        return None;
+    }
+    let plain = id
+        .chars()
+        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
+    Some(if plain {
+        id.to_string()
+    } else {
+        format!("h-{}", hex::encode(id))
+    })
 }
 
 /// Подключения нет (или его id не годится для имени файла).
@@ -214,10 +226,8 @@ impl HealthStore {
     }
 
     fn path(&self, connection_id: &str) -> AppResult<PathBuf> {
-        if !safe_id(connection_id) {
-            return Err(connection_not_found(connection_id));
-        }
-        Ok(self.dir.join(format!("{connection_id}.json")))
+        let stem = file_stem(connection_id).ok_or_else(|| connection_not_found(connection_id))?;
+        Ok(self.dir.join(format!("{stem}.json")))
     }
 
     /// Каталог `health/` — только для владельца (файлы — 0600, см. `atomic_write`).
diff --git a/src-tauri/src/health/texts.rs b/src-tauri/src/health/texts.rs
index 58781a4..62a81dd 100644
--- a/src-tauri/src/health/texts.rs
+++ b/src-tauri/src/health/texts.rs
@@ -53,7 +53,14 @@ pub fn english(f: &Finding) -> (String, String) {
     match f.rule_id.as_str() {
         "cluster.noController" => (
             "No active controller".into(),
-            "The cluster reports no controller: metadata changes, leader elections and topic operations will not work until a controller is elected.".into(),
+            format!(
+                "The cluster reports no controller and {} partition(s) have no leader: metadata changes, leader elections and topic operations will not work until a controller is elected.",
+                num(f, "leaderless")
+            ),
+        ),
+        "cluster.controllerUnknown" => (
+            "Controller unknown".into(),
+            "Glafira could not learn the controller id in time, but every partition has a leader, so the cluster is probably healthy; repeat the check. If the controller stays unknown, check the controller quorum (KRaft) or ZooKeeper.".into(),
         ),
         "partition.offline" => (
             format!("Topic {name} has offline partitions"),
diff --git a/src-tauri/src/kafka/cluster.rs b/src-tauri/src/kafka/cluster.rs
index 1e61d4b..f9f3213 100644
--- a/src-tauri/src/kafka/cluster.rs
+++ b/src-tauri/src/kafka/cluster.rs
@@ -88,7 +88,12 @@ fn native_cluster_info(h: &KafkaHandles) -> (Option<String>, Option<i32>) {
             rd::rd_kafka_mem_free(rk, ptr as *mut c_void);
             Some(s)
         };
-        let controller = rd::rd_kafka_controllerid(rk, ms);
+        // Id контроллера приходит с метаданными; `-1` — брокер его не сообщил или ответ не
+        // успел за таймаут. Один повтор отличает второе от первого.
+        let mut controller = rd::rd_kafka_controllerid(rk, ms);
+        if controller < 0 {
+            controller = rd::rd_kafka_controllerid(rk, ms);
+        }
         (cluster_id, (controller >= 0).then_some(controller))
     }
 }
diff --git a/src-tauri/src/mcp/backend.rs b/src-tauri/src/mcp/backend.rs
index c9c6742..5c11c48 100644
--- a/src-tauri/src/mcp/backend.rs
+++ b/src-tauri/src/mcp/backend.rs
@@ -56,14 +56,24 @@ fn to_backend(e: AppError) -> BackendError {
 
 /// Текст для LLM при сбое файла `health/<id>.json` (спец. G2 §9): путь к файлу не раскрывается.
 const HEALTH_FILE_FAILED: &str = "Glafira could not read or save its data for this connection";
+/// Текст для LLM, когда фоновая задача Glafira не завершилась (паника blocking-потока).
+const BACKGROUND_FAILED: &str = "A background task in Glafira failed; retry the call";
 
-/// Ошибки операций G2: сбой файла Glafira (диск, права, версия файла — ключи `errors.io`,
-/// `errors.json`, `errors.storage.*`) — `internal` с общим текстом, а не «настройки или сертификаты
-/// подключения». Различаем по ключу, а не по коду: проверка (`latest_or_run`) возвращает и ошибки
-/// Kafka и настроек подключения — они, как и «не найдено», остаются как в G1.
+/// Ключи ошибок файла Glafira: диск, разбор JSON, атомарная запись и версия файла.
+const FILE_ERROR_KEYS: [&str; 2] = ["errors.io", "errors.json"];
+const FILE_ERROR_PREFIX: &str = "errors.storage.";
+const TASK_PANICKED_KEY: &str = "errors.app.taskPanicked";
+
+/// Ошибки операций G2 и G3: сбой файла Glafira (`errors.io`, `errors.json`, `errors.storage.*`) и
+/// паника фоновой задачи (`errors.app.taskPanicked`) — код `internal` (строчными, как у других
+/// ошибок шлюза) с общим текстом, а не «настройки или сертификаты подключения». Различаем по
+/// ключу, а не по коду: проверка (`latest_or_run`) возвращает и ошибки Kafka и настроек
+/// подключения — они, как и «не найдено», остаются как в G1.
 fn health_to_backend(e: AppError) -> BackendError {
-    if e.key == "errors.io" || e.key == "errors.json" || e.key.starts_with("errors.storage.") {
+    if FILE_ERROR_KEYS.contains(&e.key.as_str()) || e.key.starts_with(FILE_ERROR_PREFIX) {
         BackendError::new("internal", HEALTH_FILE_FAILED)
+    } else if e.key == TASK_PANICKED_KEY {
+        BackendError::new("internal", BACKGROUND_FAILED)
     } else {
         to_backend(e)
     }
````

Строки нового правила на 12 языках — два spec-файла:

<!-- block: spec /tmp/g3-task1-rules.json -->
````json
{ "path": "health.rules", "after": "source", "values": {
 "en": { "cluster.controllerUnknown": { "title": "Controller unknown", "details": "Glafira could not learn the controller id in time, but every partition has a leader, so the cluster is probably healthy.", "fix": "Repeat the check. If the controller stays unknown, check the controller quorum (KRaft) or ZooKeeper." } },
 "ru": { "cluster.controllerUnknown": { "title": "Контроллер неизвестен", "details": "Glafira не успела узнать id контроллера, но у всех партиций есть лидер — скорее всего, кластер в порядке.", "fix": "Повторите проверку. Если контроллер остаётся неизвестным, проверьте кворум контроллеров (KRaft) или ZooKeeper." } },
 "de": { "cluster.controllerUnknown": { "title": "Controller unbekannt", "details": "Glafira konnte die Controller-ID nicht rechtzeitig ermitteln, aber jede Partition hat einen Leader – der Cluster ist wahrscheinlich in Ordnung.", "fix": "Wiederholen Sie die Prüfung. Bleibt der Controller unbekannt, prüfen Sie das Controller-Quorum (KRaft) oder ZooKeeper." } },
 "fr": { "cluster.controllerUnknown": { "title": "Contrôleur inconnu", "details": "Glafira n’a pas pu obtenir l’identifiant du contrôleur à temps, mais chaque partition a un leader : le cluster est probablement sain.", "fix": "Relancez la vérification. Si le contrôleur reste inconnu, vérifiez le quorum de contrôleurs (KRaft) ou ZooKeeper." } },
 "es": { "cluster.controllerUnknown": { "title": "Controlador desconocido", "details": "Glafira no pudo obtener a tiempo el id del controlador, pero todas las particiones tienen leader: probablemente el clúster está bien.", "fix": "Repite la comprobación. Si el controlador sigue desconocido, revisa el quórum de controladores (KRaft) o ZooKeeper." } },
 "it": { "cluster.controllerUnknown": { "title": "Controller sconosciuto", "details": "Glafira non è riuscita a ottenere in tempo l’id del controller, ma ogni partizione ha un leader: probabilmente il cluster è sano.", "fix": "Ripeti il controllo. Se il controller resta sconosciuto, controlla il quorum dei controller (KRaft) o ZooKeeper." } },
 "pl": { "cluster.controllerUnknown": { "title": "Kontroler nieznany", "details": "Glafira nie zdążyła ustalić id kontrolera, ale każda partycja ma leadera — klaster jest prawdopodobnie sprawny.", "fix": "Powtórz sprawdzenie. Jeśli kontroler pozostaje nieznany, sprawdź kworum kontrolerów (KRaft) lub ZooKeeper." } },
 "pt-BR": { "cluster.controllerUnknown": { "title": "Controlador desconhecido", "details": "O Glafira não conseguiu obter a tempo o id do controlador, mas todas as partições têm leader: o cluster provavelmente está saudável.", "fix": "Repita a verificação. Se o controlador continuar desconhecido, verifique o quórum de controladores (KRaft) ou o ZooKeeper." } },
 "zh-CN": { "cluster.controllerUnknown": { "title": "控制器未知", "details": "Glafira 未能及时获取控制器 ID，但所有分区都有 Leader，集群很可能是正常的。", "fix": "重新运行检查。如果控制器仍然未知，请检查控制器仲裁（KRaft）或 ZooKeeper。" } },
 "zh-TW": { "cluster.controllerUnknown": { "title": "控制器未知", "details": "Glafira 未能及時取得控制器 ID，但所有分區都有 Leader，叢集很可能是正常的。", "fix": "請重新執行檢查。若控制器仍然未知，請檢查控制器仲裁（KRaft）或 ZooKeeper。" } },
 "ja": { "cluster.controllerUnknown": { "title": "コントローラーが不明です", "details": "Glafiraは時間内にコントローラーIDを取得できませんでしたが、すべてのパーティションにリーダーがあるため、クラスターはおそらく正常です。", "fix": "チェックをやり直してください。コントローラーが不明のままなら、コントローラーのクォーラム（KRaft）またはZooKeeperを確認してください。" } },
 "ko": { "cluster.controllerUnknown": { "title": "컨트롤러를 알 수 없습니다", "details": "Glafira가 컨트롤러 ID를 제때 확인하지 못했지만 모든 파티션에 리더가 있으므로 클러스터는 정상일 가능성이 큽니다.", "fix": "점검을 다시 실행하세요. 컨트롤러가 계속 알 수 없는 상태이면 컨트롤러 쿼럼(KRaft) 또는 ZooKeeper를 확인하세요." } }
}}
````

Run: `node /tmp/add-keys.mjs src/locales /tmp/g3-task1-rules.json`

<!-- block: spec /tmp/g3-task1-names.json -->
````json
{ "path": "health.ruleNames", "after": "status", "values": {
 "en": { "cluster.controllerUnknown": "Controller unknown" },
 "ru": { "cluster.controllerUnknown": "Контроллер неизвестен" },
 "de": { "cluster.controllerUnknown": "Controller unbekannt" },
 "fr": { "cluster.controllerUnknown": "Contrôleur inconnu" },
 "es": { "cluster.controllerUnknown": "Controlador desconocido" },
 "it": { "cluster.controllerUnknown": "Controller sconosciuto" },
 "pl": { "cluster.controllerUnknown": "Kontroler nieznany" },
 "pt-BR": { "cluster.controllerUnknown": "Controlador desconhecido" },
 "zh-CN": { "cluster.controllerUnknown": "控制器未知" },
 "zh-TW": { "cluster.controllerUnknown": "控制器未知" },
 "ja": { "cluster.controllerUnknown": "コントローラー不明" },
 "ko": { "cluster.controllerUnknown": "컨트롤러 알 수 없음" }
}}
````

Run: `node /tmp/add-keys.mjs src/locales /tmp/g3-task1-names.json`

- [ ] **Step 4: Тесты проходят.**

Run: `cd src-tauri && cargo test --lib -- health:: mcp::backend::` — Expected: `test result: ok. 81 passed`. Затем `cargo test --lib` (455 passed), `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`, из корня `pnpm i18n:check` — без замечаний.

- [ ] **Step 5: Commit.**

````bash
git add src-tauri/src/health/store.rs src-tauri/src/health/service.rs src-tauri/src/health/rules/brokers.rs src-tauri/src/health/rules/mod.rs src-tauri/src/health/texts.rs src-tauri/src/kafka/cluster.rs src-tauri/src/mcp/backend.rs src/locales/*.json
git commit -m "fix(health): хвосты G2 — hex-имя файла для импортированных id, проверка другого окружения ждёт и запускается заново, правило cluster.controllerUnknown, повтор controllerid, единый код internal

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 2: Хвосты G2 во фронтенде: общее «Проверка…» и пометка проверки под другим окружением; формулировки спеки G2

**Files:**
- Modify: `src/lib/useHealth.ts` (`healthCheckKey`, `mutationKey`), `src/components/health/CheckBar.tsx`, `src/components/health/CheckBar.test.tsx`, `src/components/health/FindingsPanel.test.tsx` (мок `listConnections`), `docs/superpowers/specs/2026-10-19-glafira-health-annotations-g2-design.md` (§2, §3, §4, §8), `src/locales/*.json` (`health.checkedAs`)

**Interfaces:**
- Consumes: `useIsMutating` из TanStack Query 5, `useConnection(connectionId)` (кэш `["connections"]`), `ENV_LABELS` из `src/lib/env.ts`.
- Produces: `healthCheckKey(connectionId) = ["healthCheck", connectionId]`; `useHealthActions().run` с `mutationKey`; `CheckBar` показывает «Проверка…» по `useIsMutating` (общая для обзора и вкладки) и « · проверено как PROD» (`health.checkedAs`), если `lastCheck.environment` отличается от окружения подключения.

Спец. G3 §9, пп. 1–2 (половина интерфейса) и п. 6. `CheckBar` стоит и на обзоре кластера, и на вкладке «Замечания»: раньше «Проверка…» видел только тот, где нажали кнопку. Пометка окружения — к задаче 1: сервис уже не переиспользует проверку другого окружения, но прошлая проверка в файле могла быть сделана под другим.

- [ ] **Step 1: Тесты.**

Патч тестов `CheckBar.test.tsx` (две полосы делят одну мутацию; пометка при расхождении окружений и её отсутствие без него) и мок `listConnections` в `FindingsPanel.test.tsx` (внутри вкладки теперь есть `useConnection`):

<!-- block: patch task2-tests.diff -->
````diff
diff --git a/src/components/health/CheckBar.test.tsx b/src/components/health/CheckBar.test.tsx
index 6aabfeb..f061345 100644
--- a/src/components/health/CheckBar.test.tsx
+++ b/src/components/health/CheckBar.test.tsx
@@ -1,13 +1,14 @@
-import { screen } from "@testing-library/react";
+import { screen, waitFor } from "@testing-library/react";
 import userEvent from "@testing-library/user-event";
 import { beforeEach, describe, expect, it, vi } from "vitest";
 import type { Finding, HealthView } from "../../api/types";
 import { useWorkspace } from "../../state/workspace";
 import { renderWithClient } from "../../test/render";
+import { newProfile } from "../connections/profile";
 
 vi.mock("../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../api/client")>();
-  return { ...orig, api: { getHealth: vi.fn(), runHealthCheck: vi.fn() } };
+  return { ...orig, api: { getHealth: vi.fn(), runHealthCheck: vi.fn(), listConnections: vi.fn() } };
 });
 
 import { api } from "../../api/client";
@@ -32,9 +33,49 @@ const problem: Finding = { key: "k1", ruleId: "topic.replicationFactorOne", seve
 
 beforeEach(() => {
   vi.clearAllMocks();
+  vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "dev-1", environment: "prod" }]);
   useWorkspace.setState({ tabs: [], activeId: null });
 });
 
+describe("CheckBar: общее состояние и окружение проверки", () => {
+  it("«Проверка…» видна во всех CheckBar подключения, пока идёт одна проверка", async () => {
+    vi.mocked(api.getHealth).mockResolvedValue(checked([]));
+    let finish: (v: HealthView) => void = () => {};
+    vi.mocked(api.runHealthCheck).mockReturnValue(new Promise<HealthView>((resolve) => (finish = resolve)));
+    const user = userEvent.setup();
+    renderWithClient(
+      <>
+        <CheckBar connectionId="c1" allLink />
+        <CheckBar connectionId="c1" />
+      </>,
+    );
+    const buttons = await screen.findAllByRole("button", { name: "Проверить кластер" });
+    expect(buttons).toHaveLength(2);
+    await user.click(buttons[0]);
+    expect(await screen.findAllByRole("button", { name: "Проверка…" })).toHaveLength(2);
+    expect(screen.getAllByRole("button", { name: "Проверка…" }).every((b) => b.hasAttribute("disabled"))).toBe(true);
+    finish(checked([problem]));
+    expect(await screen.findAllByRole("button", { name: "Проверить кластер" })).toHaveLength(2);
+    expect(api.runHealthCheck).toHaveBeenCalledTimes(1);
+  });
+
+  it("помечает проверку, сделанную под другим окружением, чем у подключения сейчас", async () => {
+    // Подключение теперь dev, а последняя проверка считалась как prod.
+    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "dev-1", environment: "dev" }]);
+    vi.mocked(api.getHealth).mockResolvedValue(checked([problem]));
+    renderWithClient(<CheckBar connectionId="c1" />);
+    expect(await screen.findByText(/проверено как PROD/)).toBeInTheDocument();
+  });
+
+  it("без расхождения окружений пометки нет", async () => {
+    vi.mocked(api.getHealth).mockResolvedValue(checked([problem]));
+    renderWithClient(<CheckBar connectionId="c1" />);
+    await screen.findByText(/Последняя проверка/);
+    await waitFor(() => expect(api.listConnections).toHaveBeenCalled());
+    expect(screen.queryByText(/проверено как/)).not.toBeInTheDocument();
+  });
+});
+
 describe("CheckBar: ссылка «Все замечания»", () => {
   it("с allLink открывает вкладку «Замечания»", async () => {
     vi.mocked(api.getHealth).mockResolvedValue(checked([problem]));
diff --git a/src/components/health/FindingsPanel.test.tsx b/src/components/health/FindingsPanel.test.tsx
index bb3e498..63a530f 100644
--- a/src/components/health/FindingsPanel.test.tsx
+++ b/src/components/health/FindingsPanel.test.tsx
@@ -9,6 +9,7 @@ vi.mock("../../api/client", async (importOriginal) => {
   return {
     ...orig,
     api: {
+      listConnections: vi.fn().mockResolvedValue([]),
       getHealth: vi.fn(),
       runHealthCheck: vi.fn(),
       setFindingIgnored: vi.fn(),
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/health/CheckBar.test.tsx` — Expected: FAIL — найдена только одна кнопка «Проверка…», нет текста «проверено как PROD».

- [ ] **Step 3: Реализация.**

<!-- block: patch task2-src.diff -->
````diff
diff --git a/docs/superpowers/specs/2026-10-19-glafira-health-annotations-g2-design.md b/docs/superpowers/specs/2026-10-19-glafira-health-annotations-g2-design.md
index 2a67ee4..51b4951 100644
--- a/docs/superpowers/specs/2026-10-19-glafira-health-annotations-g2-design.md
+++ b/docs/superpowers/specs/2026-10-19-glafira-health-annotations-g2-design.md
@@ -40,7 +40,7 @@ G2 — эта спека; G3 — предложения LLM, которые от
 
 - `src-tauri/src/health/mod.rs` — `Checker`: запуск проверки, ожидание уже идущей, дедлайн, событие.
 - `health/snapshot.rs` — `ClusterSnapshot` (только данные); сборка — `health/collect.rs` (трейт `Source`, лимиты и дедлайн)
-  и `health/kafka_source.rs` (через существующие `kafka::cluster`, `kafka::configs`, `kafka::group_ops`). Для конфигов топиков добавляется пакетный `kafka::configs::describe_configs_many`
+  и `health/kafka_source.rs` (через существующие `kafka::cluster`, `kafka::configs`, `kafka::group_ops`). Для конфигов топиков добавляется пакетный `kafka::configs::describe_topic_configs`
   (до 100 ресурсов за запрос); сейчас есть только вызов на один ресурс.
 - `health/rules/{brokers,topics,broker_configs,groups}.rs` — правила над `&ClusterSnapshot`; находки собирает
   `Findings::emit`, строгость берётся из таблицы по окружению. `health/rules/mod.rs` — таблица `RULES`, `rule_ids()`,
@@ -72,7 +72,8 @@ G2 — эта спека; G3 — предложения LLM, которые от
 - объект, данные которого не прочитаны (ошибка `DescribeConfigs` у топика, ошибка описания группы), считается
   непроверенным: правила по нему не срабатывают, находка `check.partial` называет число и причину. Ложных находок по
   непрочитанным данным нет;
-- чувствительные значения (`isSensitive`, маска по имени из G1 `rules::mask`) в снимок не попадают.
+- чувствительные значения в снимок не попадают: `kafka_source.rs` читает только ключи из белых списков
+  (`TOPIC_CONFIG_KEYS`, `DRIFT_KEYS`) и отбрасывает значение с `isSensitive`.
 
 ## 4. Каталог правил
 
@@ -82,7 +83,8 @@ G2 — эта спека; G3 — предложения LLM, которые от
 
 | `ruleId` | Объект | Условие | prod | stage | прочие |
 |---|---|---|---|---|---|
-| `cluster.noController` | cluster | контроллер неизвестен | К | К | К |
+| `cluster.noController` | cluster | контроллер неизвестен и есть партиции без лидера; в параметрах — их число (`leaderless`) | К | К | К |
+| `cluster.controllerUnknown` | cluster | контроллер неизвестен, но у всех партиций есть лидер — скорее всего, запрос id не успел за таймаут (G3 §9) | П | П | П |
 | `partition.offline` | topic | есть партиции без лидера; в параметрах — номера (до 20) | К | К | К |
 | `partition.underReplicated` | topic | есть партиции с ISR меньше реплик (кроме offline — у них своя находка); номера (до 20) | К | П | И |
 | `cluster.singleBroker` | cluster | ровно один брокер | К | П | — |
@@ -219,7 +221,7 @@ status, closedAtMs? }`:
 
 ## 8. Локализация и документация
 
-- Все строки — ключами во всех 12 словарях: интерфейс вкладки и блоков (раздел `health` после `mcp`), тексты 20 правил
+- Все строки — ключами во всех 12 словарях: интерфейс вкладки и блоков (раздел `health` после `mcp`), тексты 21 правила
   (`health.rules.<ruleId>.title|details|fix`), короткие имена правил для групп и фильтров (`health.ruleNames`),
   ошибки (`errors.health.*`), подсказки (`hints.health.*`).
 - Глоссарий (`docs/i18n/glossary.md`): «находка» (finding), «замечание» (annotation), «проверка кластера» (cluster check),
diff --git a/src/components/health/CheckBar.tsx b/src/components/health/CheckBar.tsx
index 8dd4562..2805e49 100644
--- a/src/components/health/CheckBar.tsx
+++ b/src/components/health/CheckBar.tsx
@@ -1,7 +1,10 @@
+import { useIsMutating } from "@tanstack/react-query";
 import { useTranslation } from "react-i18next";
 import type { CheckRun } from "../../api/types";
 import { currentLocale } from "../../i18n";
-import { useHealth, useHealthActions } from "../../lib/useHealth";
+import { ENV_LABELS } from "../../lib/env";
+import { useConnection } from "../../lib/useConnection";
+import { healthCheckKey, useHealth, useHealthActions } from "../../lib/useHealth";
 import { useWorkspace } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { Hint } from "../common/Hint";
@@ -28,20 +31,26 @@ function useWho() {
 /**
  * Кнопка «Проверить кластер» и итог последней проверки: время, кто, число находок по строгости.
  * `allLink` — ссылка «Все замечания» на вкладку замечаний (на обзоре кластера; на самой вкладке не нужна).
+ * «Проверка…» общая для всех CheckBar подключения (обзор и вкладка «Замечания»): идущую мутацию
+ * видно по её ключу, а не по локальному `isPending`. Проверка, сделанная под другим окружением,
+ * чем у подключения сейчас, помечается: её строгость считалась иначе.
  */
 export function CheckBar({ connectionId, allLink = false }: { connectionId: string; allLink?: boolean }) {
   const { t } = useTranslation();
   const open = useWorkspace((s) => s.open);
   const health = useHealth(connectionId);
+  const connection = useConnection(connectionId);
   const { run } = useHealthActions(connectionId);
+  const checking = useIsMutating({ mutationKey: healthCheckKey(connectionId) }) > 0;
   const who = useWho();
   const last = health.data?.lastCheck ?? null;
   const visible = findingsOf(health.data, false);
+  const otherEnv = last !== null && connection !== undefined && last.environment !== connection.environment;
   return (
     <div className="check-bar">
       <div className="check-bar-row">
-        <button type="button" disabled={run.isPending} onClick={() => run.mutate()}>
-          {run.isPending ? t("health.checking") : t("health.check")}
+        <button type="button" disabled={checking} onClick={() => run.mutate()}>
+          {checking ? t("health.checking") : t("health.check")}
         </button>
         <Hint k="hints.health.check" label={t("health.check")} />
         {last ? (
@@ -64,6 +73,7 @@ export function CheckBar({ connectionId, allLink = false }: { connectionId: stri
               </>
             )}
             {last.incomplete && <span className="warn"> · {t("health.partial")}</span>}
+            {otherEnv && <span className="warn"> · {t("health.checkedAs", { env: ENV_LABELS[last.environment] })}</span>}
           </span>
         ) : (
           !health.isPending && <span className="muted">{t("health.never")}</span>
diff --git a/src/lib/useHealth.ts b/src/lib/useHealth.ts
index 0013e5c..1566a20 100644
--- a/src/lib/useHealth.ts
+++ b/src/lib/useHealth.ts
@@ -7,6 +7,8 @@ import type { AnnotationStatus, HealthView } from "../api/types";
 /** Событие из Rust (`crate::health::CHANGED_EVENT`): изменилась проверка или замечания подключения. */
 export const HEALTH_EVENT = "health://changed";
 export const healthKey = (connectionId: string) => ["health", connectionId] as const;
+/** Ключ мутации «Проверить кластер»: по нему все CheckBar подключения видят идущую проверку (`useIsMutating`). */
+export const healthCheckKey = (connectionId: string) => ["healthCheck", connectionId] as const;
 
 /**
  * Данные свежи минуту: значок стоит в каждой строке списка и появляется при прокрутке заново, а `get_health` —
@@ -43,7 +45,7 @@ export function useHealthActions(connectionId: string) {
   const qc = useQueryClient();
   const onSuccess = (v: HealthView) => qc.setQueryData(healthKey(connectionId), v);
   return {
-    run: useMutation({ mutationFn: () => api.runHealthCheck(connectionId), onSuccess }),
+    run: useMutation({ mutationKey: healthCheckKey(connectionId), mutationFn: () => api.runHealthCheck(connectionId), onSuccess }),
     ignore: useMutation({
       mutationFn: (p: { key: string; ignored: boolean }) => api.setFindingIgnored(connectionId, p.key, p.ignored),
       onSuccess,
````

Строка пометки на 12 языках:

<!-- block: spec /tmp/g3-task2-checkedas.json -->
````json
{ "path": "health", "after": "mcp", "values": {
 "en": { "checkedAs": "checked as {{env}}" },
 "ru": { "checkedAs": "проверено как {{env}}" },
 "de": { "checkedAs": "geprüft als {{env}}" },
 "fr": { "checkedAs": "vérifié comme {{env}}" },
 "es": { "checkedAs": "comprobado como {{env}}" },
 "it": { "checkedAs": "controllato come {{env}}" },
 "pl": { "checkedAs": "sprawdzono jako {{env}}" },
 "pt-BR": { "checkedAs": "verificado como {{env}}" },
 "zh-CN": { "checkedAs": "按 {{env}} 检查" },
 "zh-TW": { "checkedAs": "以 {{env}} 檢查" },
 "ja": { "checkedAs": "{{env}}として確認" },
 "ko": { "checkedAs": "점검 환경: {{env}}" }
}}
````

Run: `node /tmp/add-keys.mjs src/locales /tmp/g3-task2-checkedas.json`

- [ ] **Step 4: Тесты проходят.**

Run: `pnpm vitest run src/components/health` — Expected: `Tests 27 passed`. Затем `pnpm exec tsc --noEmit`, `pnpm i18n:check` — без замечаний.

- [ ] **Step 5: Commit.**

````bash
git add src/lib/useHealth.ts src/components/health/CheckBar.tsx src/components/health/CheckBar.test.tsx src/components/health/FindingsPanel.test.tsx docs/superpowers/specs/2026-10-19-glafira-health-annotations-g2-design.md src/locales/*.json
git commit -m "fix(health): CheckBar — общее «Проверка…» по ключу мутации, пометка проверки под другим окружением; формулировки спеки G2

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 3: `health::proposals`: виды изменения, проверка против фактов, сравнение применённого; предложения в хранилище

**Files:**
- Create: `src-tauri/src/health/proposals.rs`
- Modify: `src-tauri/src/health/mod.rs` (`pub mod proposals;`), `src-tauri/src/health/store.rs` (`proposals`, методы), `src-tauri/src/kafka/groups.rs` (`Serialize` у `ResetStrategy`, `ResetScope`, `ResetRequest`), `src-tauri/src/kafka/admin.rs` (`PartialEq` у `NewTopicSpec`), `src/locales/*.json` (`errors.health.proposalNotFound`, `errors.health.proposalClosed`)
- Test: модули `tests` в `proposals.rs` и `store.rs`

**Interfaces:**
- Consumes: `kafka::admin::{ConfigChange, NewTopicSpec, validate_new_topic}`, `kafka::groups::{ResetRequest, ResetScope, ResetStrategy, validate_reset_request}`, `kafka::metadata::is_internal`, `health::model::{Severity, Target}`, `store::{sanitize, Author, MAX_TITLE_CHARS, MAX_BODY_CHARS}`.
- Produces: `health::proposals::{MAX_CONFIG_ENTRIES = 50, MAX_CONFIG_VALUE_CHARS = 1024, MAX_SCOPE_ITEMS = 100, MAX_RATIONALE_CHARS = 2000, MAX_REJECT_REASON_CHARS = 300, MAX_RELATED_KEY_CHARS = 200, MAX_OPEN_PROPOSALS = 50, ConfigValueChange { name, value: Option<String> }, Change::{AlterTopicConfigs { topic, changes } | CreateTopic { name, partitions, replication_factor (default -1), configs } | AddPartitions { topic, new_total } | ResetOffsets { group_id, strategy, scope }} (serde `kind`, camelCase) с `kind_name()`, `target()`, `trimmed()`; ProposalStatus::{Open | Applied | Rejected | Withdrawn}.as_str(); Proposal { id, severity, title, rationale, target, change, related_rule_id, related_finding_key, client, session_id, created_at_ms, updated_at_ms, status, closed_at_ms, applied_as_proposed, reject_reason }; Facts { brokers, topics: Vec<TopicFact { name, partitions, internal }>, config_keys: Option<BTreeSet<String>>, groups: Option<BTreeSet<String>> }; ProposalError::{Invalid(String) | NotFound(String)}; validate(&Change, &Facts); Applied::{AlterTopicConfigs { topic, changes: Vec<ConfigChange> } | CreateTopic(NewTopicSpec) | AddPartitions { topic, new_total } | ResetOffsets { group_id, request: Option<ResetRequest> }}.matches(&Change); applied_as_proposed(&Change, &Applied)}`; `health::store::{PROPOSAL_TTL_MS, HealthFile.proposals, NewProposal { severity, title, rationale, change, related_rule_id, related_finding_key, author }, Proposed::{Created(id) | Updated(id) | LimitReached}, Withdrawn::{Withdrawn | NotFound | NotOwner | Closed(ProposalStatus)}, MarkApplied::{Applied { as_proposed } | Skipped(&'static str)}, HealthStore::{propose, set_proposal_status(id, pid, status, reason, now), mark_applied(id, pid, &Applied, now), withdraw_own(id, pid, &Author, now), delete_proposal, delete_all_proposals}}`.

Спец. G3 §3, §5, §6 (хранение). Модуль `proposals.rs` не ходит в Kafka: факты ему приносит бэкенд (задача 5), а он только сверяет и сравнивает. `Facts.config_keys`/`groups` — `None`, когда факты для этого вида не собирались (имена тогда не проверяются). Хранилище: повтор того же автора по тому же виду и объекту обновляет открытое предложение; лимит 50 открытых; срок 7 дней при любом статусе; `mark_applied` никогда не ломает уже прошедшую операцию — у него есть только `Skipped(почему)`. Ключи ошибок `errors.health.proposalNotFound` и `errors.health.proposalClosed` вызываются из Rust, поэтому попадают в словари в этой же задаче.

- [ ] **Step 1: Тесты.**

Модуль тестов нового файла `src-tauri/src/health/proposals.rs` (JSON-форма `change`, вид/объект/обрезка, проверки каждого вида, `matches`, `applied_as_proposed`):

<!-- block: file src-tauri/src/health/proposals.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::groups::TopicPartitionRef;

    fn facts() -> Facts {
        Facts {
            brokers: 3,
            topics: vec![
                TopicFact {
                    name: "orders".into(),
                    partitions: 3,
                    internal: false,
                },
                TopicFact {
                    name: "__consumer_offsets".into(),
                    partitions: 50,
                    internal: true,
                },
            ],
            config_keys: Some(
                ["min.insync.replicas", "retention.ms", "cleanup.policy"]
                    .into_iter()
                    .map(String::from)
                    .collect(),
            ),
            groups: Some(["billing".to_string()].into_iter().collect()),
        }
    }

    fn set(name: &str, value: Option<&str>) -> ConfigValueChange {
        ConfigValueChange {
            name: name.into(),
            value: value.map(String::from),
        }
    }

    fn alter(topic: &str, changes: Vec<ConfigValueChange>) -> Change {
        Change::AlterTopicConfigs {
            topic: topic.into(),
            changes,
        }
    }

    fn create(name: &str, rf: i32) -> Change {
        Change::CreateTopic {
            name: name.into(),
            partitions: 6,
            replication_factor: rf,
            configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
        }
    }

    fn reset_change(group: &str, scope: ResetScope) -> Change {
        Change::ResetOffsets {
            group_id: group.into(),
            strategy: ResetStrategy::Earliest,
            scope,
        }
    }

    fn invalid(r: Result<(), ProposalError>) -> String {
        match r {
            Err(ProposalError::Invalid(m)) => m,
            other => panic!("expected invalid_argument, got {other:?}"),
        }
    }

    fn not_found(r: Result<(), ProposalError>) -> String {
        match r {
            Err(ProposalError::NotFound(m)) => m,
            other => panic!("expected not_found, got {other:?}"),
        }
    }

    #[test]
    fn change_json_uses_kind_tag_and_camel_case() {
        let c = alter(
            "orders",
            vec![
                set("min.insync.replicas", Some("2")),
                set("retention.ms", None),
            ],
        );
        let v = serde_json::to_value(&c).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "kind": "alterTopicConfigs",
                "topic": "orders",
                "changes": [
                    { "name": "min.insync.replicas", "value": "2" },
                    { "name": "retention.ms", "value": null }
                ]
            })
        );
        let back: Change = serde_json::from_value(v).unwrap();
        assert_eq!(back, c);
        // createTopic без replicationFactor и configs — значение брокера и пусто.
        let c: Change = serde_json::from_value(serde_json::json!({
            "kind": "createTopic", "name": "dlq", "partitions": 1
        }))
        .unwrap();
        assert_eq!(
            c,
            Change::CreateTopic {
                name: "dlq".into(),
                partitions: 1,
                replication_factor: -1,
                configs: BTreeMap::new()
            }
        );
        let r = reset_change(
            "billing",
            ResetScope::Partitions {
                partitions: vec![TopicPartitionRef {
                    topic: "orders".into(),
                    partition: 1,
                }],
            },
        );
        assert_eq!(
            serde_json::to_value(&r).unwrap(),
            serde_json::json!({
                "kind": "resetOffsets", "groupId": "billing",
                "strategy": { "type": "earliest" },
                "scope": { "type": "partitions", "partitions": [{ "topic": "orders", "partition": 1 }] }
            })
        );
    }

    #[test]
    fn kind_target_and_trim() {
        let c = alter(" orders ", vec![]).trimmed();
        assert_eq!(c.kind_name(), "alterTopicConfigs");
        assert_eq!(c.target(), Target::topic("orders"));
        assert_eq!(create("dlq", 3).target(), Target::topic("dlq"));
        assert_eq!(
            reset_change(" billing", ResetScope::All).trimmed().target(),
            Target::group("billing")
        );
        assert_eq!(
            Change::AddPartitions {
                topic: "orders".into(),
                new_total: 6
            }
            .kind_name(),
            "addPartitions"
        );
    }

    #[test]
    fn alter_configs_is_checked_against_topic_and_known_keys() {
        let f = facts();
        assert_eq!(
            validate(
                &alter(
                    "orders",
                    vec![
                        set("min.insync.replicas", Some("2")),
                        set("retention.ms", None)
                    ]
                ),
                &f
            ),
            Ok(())
        );
        assert!(
            not_found(validate(
                &alter("missing", vec![set("retention.ms", None)]),
                &f
            ))
            .contains("missing")
        );
        assert!(
            invalid(validate(
                &alter("__consumer_offsets", vec![set("retention.ms", None)]),
                &f
            ))
            .contains("internal")
        );
        assert!(invalid(validate(&alter("orders", vec![]), &f)).contains("1 to 50"));
        let many: Vec<_> = (0..51).map(|i| set(&format!("k{i}"), Some("1"))).collect();
        assert!(invalid(validate(&alter("orders", many), &f)).contains("1 to 50"));
        assert!(
            invalid(validate(&alter("orders", vec![set("nope", Some("1"))]), &f))
                .contains("unknown topic config nope")
        );
        assert!(
            invalid(validate(
                &alter(
                    "orders",
                    vec![set("retention.ms", Some("1")), set("retention.ms", None)]
                ),
                &f
            ))
            .contains("more than once")
        );
        assert!(
            invalid(validate(
                &alter("orders", vec![set("retention.ms", Some("  "))]),
                &f
            ))
            .contains("must not be empty")
        );
        assert!(
            invalid(validate(
                &alter("orders", vec![set("retention.ms", Some(&"x".repeat(1025)))]),
                &f
            ))
            .contains("1024")
        );
        assert!(
            invalid(validate(&alter("orders", vec![set(" ", Some("1"))]), &f))
                .contains("empty name")
        );
        // Без списка ключей (факты не собраны) имена не проверяются.
        let mut no_keys = facts();
        no_keys.config_keys = None;
        assert_eq!(
            validate(&alter("orders", vec![set("nope", Some("1"))]), &no_keys),
            Ok(())
        );
    }

    #[test]
    fn create_topic_is_checked_against_name_rules_brokers_and_existing_topics() {
        let f = facts();
        assert_eq!(validate(&create("orders-dlq", 3), &f), Ok(()));
        assert_eq!(validate(&create("orders-dlq", -1), &f), Ok(()));
        assert!(invalid(validate(&create("orders", 3), &f)).contains("already exists"));
        assert!(invalid(validate(&create("bad name", 3), &f)).contains("Invalid character"));
        assert!(
            invalid(validate(&create("orders-dlq", 4), &f))
                .contains("above the number of brokers (3)")
        );
        assert!(invalid(validate(&create("orders-dlq", 0), &f)).contains("at least 1"));
        assert!(invalid(validate(&create("__internal", 1), &f)).contains("internal"));
        let mut many = create("dlq", 1);
        if let Change::CreateTopic { configs, .. } = &mut many {
            *configs = (0..51)
                .map(|i| (format!("k{i}"), "1".to_string()))
                .collect();
        }
        assert!(invalid(validate(&many, &f)).contains("at most 50"));
        let mut empty_value = create("dlq", 1);
        if let Change::CreateTopic { configs, .. } = &mut empty_value {
            configs.insert("retention.ms".into(), " ".into());
        }
        assert!(invalid(validate(&empty_value, &f)).contains("must not be empty"));
    }

    #[test]
    fn add_partitions_needs_an_existing_topic_and_a_greater_total() {
        let f = facts();
        let add = |topic: &str, n: i32| Change::AddPartitions {
            topic: topic.into(),
            new_total: n,
        };
        assert_eq!(validate(&add("orders", 4), &f), Ok(()));
        assert!(invalid(validate(&add("orders", 3), &f)).contains("has 3 partitions"));
        assert!(invalid(validate(&add("orders", 0), &f)).contains("has 3 partitions"));
        assert!(not_found(validate(&add("missing", 4), &f)).contains("missing"));
        assert!(invalid(validate(&add("__consumer_offsets", 60), &f)).contains("internal"));
    }

    #[test]
    fn reset_offsets_is_checked_against_groups_topics_and_partitions() {
        let f = facts();
        assert_eq!(
            validate(&reset_change("billing", ResetScope::All), &f),
            Ok(())
        );
        assert!(
            not_found(validate(&reset_change("nope", ResetScope::All), &f))
                .contains("consumer group nope")
        );
        assert!(
            not_found(validate(
                &reset_change(
                    "billing",
                    ResetScope::Topics {
                        topics: vec!["missing".into()]
                    }
                ),
                &f
            ))
            .contains("topic missing")
        );
        assert!(
            invalid(validate(
                &reset_change("billing", ResetScope::Topics { topics: vec![] }),
                &f
            ))
            .contains("No topics")
        );
        let part = |p: i32| ResetScope::Partitions {
            partitions: vec![TopicPartitionRef {
                topic: "orders".into(),
                partition: p,
            }],
        };
        assert_eq!(validate(&reset_change("billing", part(2)), &f), Ok(()));
        assert!(
            invalid(validate(&reset_change("billing", part(3)), &f))
                .contains("partition 3 does not exist")
        );
        assert!(
            invalid(validate(&reset_change("billing", part(-1)), &f)).contains("does not exist")
        );
        let too_many = ResetScope::Topics {
            topics: (0..101).map(|_| "orders".to_string()).collect(),
        };
        assert!(invalid(validate(&reset_change("billing", too_many), &f)).contains("at most 100"));
        let negative = Change::ResetOffsets {
            group_id: "billing".into(),
            strategy: ResetStrategy::Offset { offset: -1 },
            scope: ResetScope::All,
        };
        assert!(invalid(validate(&negative, &f)).contains("negative"));
        let zero = Change::ResetOffsets {
            group_id: "billing".into(),
            strategy: ResetStrategy::Shift { delta: 0 },
            scope: ResetScope::All,
        };
        assert!(invalid(validate(&zero, &f)).contains("must not be 0"));
        // Пустая группа не обязана быть пустой: форма сама не даст применить.
        let mut no_groups = facts();
        no_groups.groups = None;
        assert_eq!(
            validate(&reset_change("anything", ResetScope::All), &no_groups),
            Ok(())
        );
    }

    #[test]
    fn applied_matches_only_the_same_kind_and_object() {
        let c = alter("orders", vec![set("retention.ms", Some("1"))]);
        let applied = Applied::AlterTopicConfigs {
            topic: "orders".into(),
            changes: vec![],
        };
        assert!(applied.matches(&c));
        assert!(!applied.matches(&alter("other", vec![])));
        assert!(!applied.matches(&Change::AddPartitions {
            topic: "orders".into(),
            new_total: 4
        }));
        assert!(
            Applied::CreateTopic(NewTopicSpec {
                name: "x".into(),
                partitions: 1,
                replication_factor: 1,
                configs: BTreeMap::new()
            })
            .matches(&create("dlq", 1))
        );
        let reset = |group: &str| Applied::ResetOffsets {
            group_id: group.into(),
            request: None,
        };
        assert!(reset("billing").matches(&reset_change("billing", ResetScope::All)));
        // Другая группа того же вида — не тот объект.
        assert!(!reset("other").matches(&reset_change("billing", ResetScope::All)));
    }

    #[test]
    fn applied_as_proposed_compares_sets_fields_and_requests() {
        let c = alter(
            "orders",
            vec![
                set("min.insync.replicas", Some("2")),
                set("retention.ms", None),
            ],
        );
        let same = Applied::AlterTopicConfigs {
            topic: "orders".into(),
            changes: vec![
                ConfigChange::Delete {
                    name: "retention.ms".into(),
                },
                ConfigChange::Set {
                    name: "min.insync.replicas".into(),
                    value: "2".into(),
                },
            ],
        };
        assert!(applied_as_proposed(&c, &same));
        let edited = Applied::AlterTopicConfigs {
            topic: "orders".into(),
            changes: vec![ConfigChange::Set {
                name: "min.insync.replicas".into(),
                value: "3".into(),
            }],
        };
        assert!(!applied_as_proposed(&c, &edited));

        let spec = |rf: i32| {
            Applied::CreateTopic(NewTopicSpec {
                name: "dlq".into(),
                partitions: 6,
                replication_factor: rf,
                configs: BTreeMap::from([("cleanup.policy".to_string(), "compact".to_string())]),
            })
        };
        assert!(applied_as_proposed(&create("dlq", -1), &spec(-1)));
        // Форма подставила число брокеров вместо «-1» — это правка.
        assert!(!applied_as_proposed(&create("dlq", -1), &spec(3)));

        let add = Change::AddPartitions {
            topic: "orders".into(),
            new_total: 6,
        };
        assert!(applied_as_proposed(
            &add,
            &Applied::AddPartitions {
                topic: "orders".into(),
                new_total: 6
            }
        ));
        assert!(!applied_as_proposed(
            &add,
            &Applied::AddPartitions {
                topic: "orders".into(),
                new_total: 8
            }
        ));

        let r = reset_change("billing", ResetScope::All);
        let request = |strategy| ResetRequest {
            group_id: "billing".into(),
            strategy,
            scope: ResetScope::All,
        };
        assert!(applied_as_proposed(
            &r,
            &Applied::ResetOffsets {
                group_id: "billing".into(),
                request: Some(request(ResetStrategy::Earliest))
            }
        ));
        assert!(!applied_as_proposed(
            &r,
            &Applied::ResetOffsets {
                group_id: "billing".into(),
                request: Some(request(ResetStrategy::Latest))
            }
        ));
        assert!(!applied_as_proposed(
            &r,
            &Applied::ResetOffsets {
                group_id: "billing".into(),
                request: None
            }
        ));
        // Запрос по другой группе — не «как предложено».
        assert!(!applied_as_proposed(
            &r,
            &Applied::ResetOffsets {
                group_id: "other".into(),
                request: Some(ResetRequest {
                    group_id: "other".into(),
                    strategy: ResetStrategy::Earliest,
                    scope: ResetScope::All,
                }),
            }
        ));
        // Разные виды — никогда.
        assert!(!applied_as_proposed(&r, &same));
    }
}
````

Патч тестов `store.rs` (обновление вместо дубля, лимит 50, срок, переходы статусов, отметка «применено» с правками, отзыв только своего, удаление):

<!-- block: patch task3-tests.diff -->
````diff
diff --git a/src-tauri/src/health/store.rs b/src-tauri/src/health/store.rs
index 466f5da..45071ba 100644
--- a/src-tauri/src/health/store.rs
+++ b/src-tauri/src/health/store.rs
@@ -437,6 +688,8 @@ impl HealthStore {
 mod tests {
     use super::*;
     use crate::health::model::TargetKind;
+    use crate::health::proposals::ConfigValueChange;
+    use crate::kafka::admin::ConfigChange;
 
     const ID: &str = "c1";
     const T0: i64 = 1_800_000_000_000;
@@ -742,6 +995,357 @@ mod tests {
         assert_eq!(mode(&d.path().join("health/c1.json")), 0o600);
     }
 
+    fn alter(topic: &str, value: &str) -> Change {
+        Change::AlterTopicConfigs {
+            topic: topic.into(),
+            changes: vec![ConfigValueChange {
+                name: "min.insync.replicas".into(),
+                value: Some(value.into()),
+            }],
+        }
+    }
+
+    fn proposal(change: Change, who: Author) -> NewProposal {
+        NewProposal {
+            severity: Severity::Warning,
+            title: "Raise min.insync.replicas".into(),
+            rationale: "acks=all\nneeds it".into(),
+            change,
+            related_rule_id: Some("topic.minIsrOne".into()),
+            related_finding_key: None,
+            author: who,
+        }
+    }
+
+    fn proposed_id(p: Proposed) -> String {
+        match p {
+            Proposed::Created(id) => id,
+            other => panic!("{other:?}"),
+        }
+    }
+
+    fn applied(topic: &str, value: &str) -> Applied {
+        Applied::AlterTopicConfigs {
+            topic: topic.into(),
+            changes: vec![ConfigChange::Set {
+                name: "min.insync.replicas".into(),
+                value: value.into(),
+            }],
+        }
+    }
+
+    #[test]
+    fn proposal_of_same_author_kind_and_object_is_updated() {
+        let (d, s) = store();
+        let claude = || author(Some("claude-ai"), "s1");
+        let id = proposed_id(
+            s.propose(ID, proposal(alter(" orders ", "2"), claude()), T0)
+                .unwrap(),
+        );
+        let mut again = proposal(alter("orders", "3"), author(Some("claude-ai"), "s9"));
+        again.title = "  New title ".into();
+        again.severity = Severity::Critical;
+        assert_eq!(
+            s.propose(ID, again, T0 + 5).unwrap(),
+            Proposed::Updated(id.clone())
+        );
+        // Другой вид по тому же топику и другой автор — новые записи.
+        proposed_id(
+            s.propose(
+                ID,
+                proposal(
+                    Change::AddPartitions {
+                        topic: "orders".into(),
+                        new_total: 6,
+                    },
+                    claude(),
+                ),
+                T0,
+            )
+            .unwrap(),
+        );
+        proposed_id(
+            s.propose(
+                ID,
+                proposal(alter("orders", "2"), author(Some("cursor"), "s2")),
+                T0,
+            )
+            .unwrap(),
+        );
+        let f = s.load(ID, T0 + 5).unwrap();
+        assert_eq!(f.proposals.len(), 3);
+        let p = f.proposals.iter().find(|p| p.id == id).unwrap();
+        assert_eq!(p.title, "New title");
+        assert_eq!(p.severity, Severity::Critical);
+        assert_eq!(p.updated_at_ms, T0 + 5);
+        assert_eq!(p.target, Target::topic("orders"));
+        assert_eq!(p.change, alter("orders", "3"));
+        assert_eq!(p.status, ProposalStatus::Open);
+        assert_eq!(p.client.as_deref(), Some("claude-ai"));
+        assert_eq!(p.session_id, None);
+        let raw: serde_json::Value =
+            serde_json::from_slice(&std::fs::read(d.path().join("health/c1.json")).unwrap())
+                .unwrap();
+        assert_eq!(raw["proposals"][0]["change"]["kind"], "alterTopicConfigs");
+        assert_eq!(raw["proposals"][0]["status"], "open");
+        assert!(raw["proposals"][0].get("appliedAsProposed").is_none());
+    }
+
+    #[test]
+    fn open_proposals_are_limited_to_fifty() {
+        let (d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(alter("t0", "2"), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        let mut f = s.load(ID, T0).unwrap();
+        let first = f.proposals[0].clone();
+        f.proposals = (0..MAX_OPEN_PROPOSALS)
+            .map(|i| Proposal {
+                id: if i == 0 { id.clone() } else { format!("p{i}") },
+                target: Target::topic(&format!("t{i}")),
+                change: alter(&format!("t{i}"), "2"),
+                ..first.clone()
+            })
+            .collect();
+        write_json(&d.path().join("health/c1.json"), &f).unwrap();
+        assert_eq!(
+            s.propose(
+                ID,
+                proposal(alter("one-more", "2"), author(Some("c"), "s")),
+                T0
+            )
+            .unwrap(),
+            Proposed::LimitReached
+        );
+        // Обновление существующего лимит не трогает.
+        assert_eq!(
+            s.propose(ID, proposal(alter("t5", "3"), author(Some("c"), "s")), T0)
+                .unwrap(),
+            Proposed::Updated("p5".into())
+        );
+        // Закрытые не считаются.
+        s.set_proposal_status(ID, &id, ProposalStatus::Rejected, None, T0)
+            .unwrap();
+        proposed_id(
+            s.propose(
+                ID,
+                proposal(alter("one-more", "2"), author(Some("c"), "s")),
+                T0,
+            )
+            .unwrap(),
+        );
+    }
+
+    #[test]
+    fn proposals_expire_after_seven_days_whatever_the_status() {
+        let (_d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(alter("t", "2"), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        s.mark_applied(ID, &id, &applied("t", "2"), T0 + 1).unwrap();
+        assert_eq!(
+            s.load(ID, T0 + PROPOSAL_TTL_MS - 1)
+                .unwrap()
+                .proposals
+                .len(),
+            1
+        );
+        assert!(
+            s.load(ID, T0 + PROPOSAL_TTL_MS)
+                .unwrap()
+                .proposals
+                .is_empty()
+        );
+    }
+
+    #[test]
+    fn status_transitions_reject_reopen_and_refuse_closed() {
+        let (_d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(alter("t", "2"), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        s.set_proposal_status(
+            ID,
+            &id,
+            ProposalStatus::Rejected,
+            Some("  too risky\u{7} ".into()),
+            T0 + 1,
+        )
+        .unwrap();
+        let p = &s.load(ID, T0).unwrap().proposals[0];
+        assert_eq!(
+            (p.status, p.closed_at_ms),
+            (ProposalStatus::Rejected, Some(T0 + 1))
+        );
+        assert_eq!(p.reject_reason.as_deref(), Some("too risky"));
+        // Повторное отклонение отклонённого — ошибка; «Открыть снова» — обратно в open.
+        let e = s
+            .set_proposal_status(ID, &id, ProposalStatus::Rejected, None, T0 + 2)
+            .unwrap_err();
+        assert_eq!(e.key, "errors.health.proposalClosed");
+        s.set_proposal_status(ID, &id, ProposalStatus::Open, None, T0 + 3)
+            .unwrap();
+        let p = &s.load(ID, T0).unwrap().proposals[0];
+        assert_eq!(
+            (p.status, p.closed_at_ms, p.reject_reason.clone()),
+            (ProposalStatus::Open, None, None)
+        );
+        assert_eq!(p.updated_at_ms, T0 + 3);
+        // Пустая причина не хранится.
+        s.set_proposal_status(
+            ID,
+            &id,
+            ProposalStatus::Rejected,
+            Some("   ".into()),
+            T0 + 4,
+        )
+        .unwrap();
+        assert_eq!(s.load(ID, T0).unwrap().proposals[0].reject_reason, None);
+        // Применённое снова не открыть и не отклонить.
+        let id2 = proposed_id(
+            s.propose(ID, proposal(alter("u", "2"), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        s.mark_applied(ID, &id2, &applied("u", "2"), T0).unwrap();
+        for to in [ProposalStatus::Open, ProposalStatus::Rejected] {
+            let e = s.set_proposal_status(ID, &id2, to, None, T0).unwrap_err();
+            assert_eq!(
+                (e.code, e.key.as_str()),
+                (ErrorCode::InvalidInput, "errors.health.proposalClosed")
+            );
+            assert_eq!(e.params["status"], "applied");
+        }
+        let e = s
+            .set_proposal_status(ID, "nope", ProposalStatus::Rejected, None, T0)
+            .unwrap_err();
+        assert_eq!(e.key, "errors.health.proposalNotFound");
+    }
+
+    #[test]
+    fn mark_applied_records_edits_and_skips_foreign_or_closed() {
+        let (_d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(alter("t", "2"), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        // Другой объект того же вида — пропуск, предложение открыто.
+        assert_eq!(
+            s.mark_applied(ID, &id, &applied("other", "2"), T0).unwrap(),
+            MarkApplied::Skipped("proposal is about another change")
+        );
+        assert_eq!(
+            s.mark_applied(ID, "nope", &applied("t", "2"), T0).unwrap(),
+            MarkApplied::Skipped("proposal not found")
+        );
+        assert_eq!(
+            s.load(ID, T0).unwrap().proposals[0].status,
+            ProposalStatus::Open
+        );
+        // С правками: значение другое.
+        assert_eq!(
+            s.mark_applied(ID, &id, &applied("t", "3"), T0 + 1).unwrap(),
+            MarkApplied::Applied { as_proposed: false }
+        );
+        let p = &s.load(ID, T0).unwrap().proposals[0];
+        assert_eq!(
+            (p.status, p.closed_at_ms, p.applied_as_proposed),
+            (ProposalStatus::Applied, Some(T0 + 1), Some(false))
+        );
+        // Уже закрытое — пропуск.
+        assert_eq!(
+            s.mark_applied(ID, &id, &applied("t", "2"), T0 + 2).unwrap(),
+            MarkApplied::Skipped("proposal is not open")
+        );
+        let id2 = proposed_id(
+            s.propose(ID, proposal(alter("u", "2"), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        assert_eq!(
+            s.mark_applied(ID, &id2, &applied("u", "2"), T0).unwrap(),
+            MarkApplied::Applied { as_proposed: true }
+        );
+    }
+
+    #[test]
+    fn llm_withdraws_only_its_own_open_proposals() {
+        let (_d, s) = store();
+        let named = proposed_id(
+            s.propose(
+                ID,
+                proposal(alter("a", "2"), author(Some("claude-ai"), "s1")),
+                T0,
+            )
+            .unwrap(),
+        );
+        let unnamed = proposed_id(
+            s.propose(ID, proposal(alter("b", "2"), author(None, "s2")), T0)
+                .unwrap(),
+        );
+        assert_eq!(
+            s.withdraw_own(ID, &named, &author(Some("cursor"), "s1"), T0)
+                .unwrap(),
+            Withdrawn::NotOwner
+        );
+        assert_eq!(
+            s.withdraw_own(ID, &unnamed, &author(None, "s3"), T0)
+                .unwrap(),
+            Withdrawn::NotOwner
+        );
+        assert_eq!(
+            s.withdraw_own(ID, &unnamed, &author(None, "s2"), T0)
+                .unwrap(),
+            Withdrawn::Withdrawn
+        );
+        assert_eq!(
+            s.withdraw_own(ID, &unnamed, &author(None, "s2"), T0)
+                .unwrap(),
+            Withdrawn::Closed(ProposalStatus::Withdrawn)
+        );
+        assert_eq!(
+            s.withdraw_own(ID, "nope", &author(Some("claude-ai"), "s1"), T0)
+                .unwrap(),
+            Withdrawn::NotFound
+        );
+        s.mark_applied(ID, &named, &applied("a", "2"), T0).unwrap();
+        assert_eq!(
+            s.withdraw_own(ID, &named, &author(Some("claude-ai"), "s9"), T0)
+                .unwrap(),
+            Withdrawn::Closed(ProposalStatus::Applied)
+        );
+        // Отозванное остаётся в файле (закрытые видны человеку) до срока.
+        let f = s.load(ID, T0).unwrap();
+        assert_eq!(f.proposals.len(), 2);
+        assert_eq!(f.proposals[1].status, ProposalStatus::Withdrawn);
+    }
+
+    #[test]
+    fn proposals_are_deleted_one_by_one_and_all_at_once() {
+        let (_d, s) = store();
+        let id = proposed_id(
+            s.propose(ID, proposal(alter("a", "2"), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        proposed_id(
+            s.propose(ID, proposal(alter("b", "2"), author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        s.delete_proposal(ID, &id, T0).unwrap();
+        let e = s.delete_proposal(ID, &id, T0).unwrap_err();
+        assert_eq!(e.key, "errors.health.proposalNotFound");
+        assert_eq!(s.delete_all_proposals(ID, T0).unwrap(), 1);
+        assert!(s.load(ID, T0).unwrap().proposals.is_empty());
+        // Замечания при этом не трогаются.
+        created(
+            s.annotate(ID, note("n", author(Some("c"), "s")), T0)
+                .unwrap(),
+        );
+        assert_eq!(s.delete_all_proposals(ID, T0).unwrap(), 0);
+        assert_eq!(s.load(ID, T0).unwrap().annotations.len(), 1);
+    }
+
     #[test]
     fn delete_connection_removes_file() {
         let (d, s) = store();
````

- [ ] **Step 2: Подключить модуль и увидеть падение.**

В `src-tauri/src/health/mod.rs` добавьте `pub mod proposals;` (входит в патч реализации ниже; можно применить его первым хунком). Run: `cd src-tauri && cargo test --lib -- health::proposals health::store` — Expected: ошибка компиляции (нет `Change`, `validate`, `propose`, …).

- [ ] **Step 3: Реализация.**

Реализация `src-tauri/src/health/proposals.rs` (до модуля тестов):

<!-- block: file src-tauri/src/health/proposals.rs part=impl -->
````rust
//! Предложения LLM (спец. G3 §3, §5): виды изменения, проверка против фактов кластера и
//! сравнение применённого с предложенным. Модуль не ходит в Kafka и ничего в нём не меняет:
//! применяет изменение только человек через формы и их команды.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::model::{Severity, Target};
use crate::kafka::admin::{ConfigChange, NewTopicSpec, validate_new_topic};
use crate::kafka::groups::{ResetRequest, ResetScope, ResetStrategy, validate_reset_request};
use crate::kafka::metadata::is_internal;

/// Записей в `alterTopicConfigs` и ключей конфигов в `createTopic`.
pub const MAX_CONFIG_ENTRIES: usize = 50;
/// Длина значения конфига.
pub const MAX_CONFIG_VALUE_CHARS: usize = 1024;
/// Топиков или партиций в области сброса оффсетов.
pub const MAX_SCOPE_ITEMS: usize = 100;
pub const MAX_RATIONALE_CHARS: usize = 2000;
pub const MAX_REJECT_REASON_CHARS: usize = 300;
pub const MAX_RELATED_KEY_CHARS: usize = 200;
/// Открытых предложений на подключение.
pub const MAX_OPEN_PROPOSALS: usize = 50;

/// Одна запись изменения конфига топика: `value: None` — вернуть значение по умолчанию.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigValueChange {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
}

fn default_replication_factor() -> i32 {
    -1
}

/// Что предлагает LLM. Одна и та же JSON-форма у модели, в файле и во фронтенде.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Change {
    AlterTopicConfigs {
        topic: String,
        changes: Vec<ConfigValueChange>,
    },
    CreateTopic {
        name: String,
        partitions: i32,
        /// `-1` — значение брокера по умолчанию.
        #[serde(default = "default_replication_factor")]
        replication_factor: i32,
        #[serde(default)]
        configs: BTreeMap<String, String>,
    },
    AddPartitions {
        topic: String,
        new_total: i32,
    },
    ResetOffsets {
        group_id: String,
        strategy: ResetStrategy,
        scope: ResetScope,
    },
}

impl Change {
    /// Имя вида изменения — как `kind` в JSON (журнал, дубли).
    pub fn kind_name(&self) -> &'static str {
        match self {
            Change::AlterTopicConfigs { .. } => "alterTopicConfigs",
            Change::CreateTopic { .. } => "createTopic",
            Change::AddPartitions { .. } => "addPartitions",
            Change::ResetOffsets { .. } => "resetOffsets",
        }
    }

    /// Объект предложения: топик (у `createTopic` — будущий) или группа.
    pub fn target(&self) -> Target {
        match self {
            Change::AlterTopicConfigs { topic, .. } | Change::AddPartitions { topic, .. } => {
                Target::topic(topic)
            }
            Change::CreateTopic { name, .. } => Target::topic(name),
            Change::ResetOffsets { group_id, .. } => Target::group(group_id),
        }
    }

    /// Имена по краям обрезаются (как у `annotate` сверка идёт по обрезанному имени).
    pub fn trimmed(mut self) -> Self {
        match &mut self {
            Change::AlterTopicConfigs { topic, .. } | Change::AddPartitions { topic, .. } => {
                *topic = topic.trim().to_string();
            }
            Change::CreateTopic { name, .. } => *name = name.trim().to_string(),
            Change::ResetOffsets { group_id, .. } => *group_id = group_id.trim().to_string(),
        }
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProposalStatus {
    Open,
    /// Человек применил формой (ставит только Rust после успеха операции).
    Applied,
    /// Человек отклонил; «Открыть снова» возвращает в `Open`.
    Rejected,
    /// LLM отозвала своё.
    Withdrawn,
}

impl ProposalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ProposalStatus::Open => "open",
            ProposalStatus::Applied => "applied",
            ProposalStatus::Rejected => "rejected",
            ProposalStatus::Withdrawn => "withdrawn",
        }
    }
}

/// Предложение в файле `health/<connectionId>.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Proposal {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub target: Target,
    pub change: Change,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub related_finding_key: Option<String>,
    /// Имя клиента из диалога подтверждения; `None` — безымянный.
    #[serde(default)]
    pub client: Option<String>,
    /// Сессия безымянного клиента: отозвать можно только из неё.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub status: ProposalStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
    /// Только у `applied`: человек применил без правок (`true`) или с правками.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_as_proposed: Option<bool>,
    /// Только у `rejected`: необязательный комментарий человека.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

/// Факты о кластере для проверки предложения. Собирает их `mcp/backend.rs` существующими
/// функциями чтения; здесь — только сверка.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Facts {
    pub brokers: usize,
    pub topics: Vec<TopicFact>,
    /// Ключи конфигов топика из `DescribeConfigs` — только для `alterTopicConfigs`.
    pub config_keys: Option<BTreeSet<String>>,
    /// Группы кластера — только для `resetOffsets`.
    pub groups: Option<BTreeSet<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFact {
    pub name: String,
    pub partitions: usize,
    pub internal: bool,
}

impl Facts {
    fn topic(&self, name: &str) -> Option<&TopicFact> {
        self.topics.iter().find(|t| t.name == name)
    }
}

/// Почему предложение отвергнуто: `invalid_argument` или `not_found` для LLM.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalError {
    Invalid(String),
    NotFound(String),
}

fn no_topic(name: &str) -> ProposalError {
    ProposalError::NotFound(format!("There is no topic {name} in this cluster"))
}

fn internal_topic(name: &str) -> ProposalError {
    ProposalError::Invalid(format!(
        "{name} is an internal topic: internal topics cannot be changed through Glafira"
    ))
}

fn check_config_value(name: &str, value: &str) -> Result<(), ProposalError> {
    if value.trim().is_empty() {
        return Err(ProposalError::Invalid(format!(
            "value of {name} must not be empty; use null to reset it to the default"
        )));
    }
    if value.chars().count() > MAX_CONFIG_VALUE_CHARS {
        return Err(ProposalError::Invalid(format!(
            "value of {name} is longer than {MAX_CONFIG_VALUE_CHARS} characters"
        )));
    }
    Ok(())
}

/// Проверка предложения против фактов кластера (спец. G3 §5). Значения конфигов Kafka
/// проверяет сама при применении — здесь только форма и объекты.
pub fn validate(change: &Change, facts: &Facts) -> Result<(), ProposalError> {
    match change {
        Change::AlterTopicConfigs { topic, changes } => {
            let t = facts.topic(topic).ok_or_else(|| no_topic(topic))?;
            if t.internal {
                return Err(internal_topic(topic));
            }
            if changes.is_empty() || changes.len() > MAX_CONFIG_ENTRIES {
                return Err(ProposalError::Invalid(format!(
                    "changes must have 1 to {MAX_CONFIG_ENTRIES} entries, got {}",
                    changes.len()
                )));
            }
            let mut seen = BTreeSet::new();
            for c in changes {
                if c.name.trim().is_empty() {
                    return Err(ProposalError::Invalid(
                        "a config change has an empty name".into(),
                    ));
                }
                if !seen.insert(c.name.as_str()) {
                    return Err(ProposalError::Invalid(format!(
                        "config {} is listed more than once",
                        c.name
                    )));
                }
                if let Some(keys) = &facts.config_keys
                    && !keys.contains(&c.name)
                {
                    return Err(ProposalError::Invalid(format!(
                        "unknown topic config {}: use names from get_topic_configs",
                        c.name
                    )));
                }
                if let Some(v) = &c.value {
                    check_config_value(&c.name, v)?;
                }
            }
            Ok(())
        }
        Change::CreateTopic {
            name,
            partitions,
            replication_factor,
            configs,
        } => {
            let spec = NewTopicSpec {
                name: name.clone(),
                partitions: *partitions,
                replication_factor: *replication_factor,
                configs: configs.clone(),
            };
            validate_new_topic(&spec).map_err(|e| ProposalError::Invalid(e.message))?;
            if is_internal(name) {
                return Err(internal_topic(name));
            }
            if facts.topic(name).is_some() {
                return Err(ProposalError::Invalid(format!(
                    "topic {name} already exists; propose alterTopicConfigs or addPartitions instead"
                )));
            }
            if *replication_factor > 0 && *replication_factor as usize > facts.brokers {
                return Err(ProposalError::Invalid(format!(
                    "replicationFactor {replication_factor} is above the number of brokers ({})",
                    facts.brokers
                )));
            }
            if configs.len() > MAX_CONFIG_ENTRIES {
                return Err(ProposalError::Invalid(format!(
                    "configs must have at most {MAX_CONFIG_ENTRIES} entries, got {}",
                    configs.len()
                )));
            }
            for (k, v) in configs {
                check_config_value(k, v)?;
            }
            Ok(())
        }
        Change::AddPartitions { topic, new_total } => {
            let t = facts.topic(topic).ok_or_else(|| no_topic(topic))?;
            if t.internal {
                return Err(internal_topic(topic));
            }
            if *new_total <= 0 || *new_total as usize <= t.partitions {
                return Err(ProposalError::Invalid(format!(
                    "topic {topic} has {} partitions; newTotal must be greater, got {new_total}",
                    t.partitions
                )));
            }
            Ok(())
        }
        Change::ResetOffsets {
            group_id,
            strategy,
            scope,
        } => {
            if let Some(groups) = &facts.groups
                && !groups.contains(group_id)
            {
                return Err(ProposalError::NotFound(format!(
                    "There is no consumer group {group_id} in this cluster"
                )));
            }
            let request = ResetRequest {
                group_id: group_id.clone(),
                strategy: strategy.clone(),
                scope: scope.clone(),
            };
            validate_reset_request(&request).map_err(|e| ProposalError::Invalid(e.message))?;
            if let ResetStrategy::Shift { delta: 0 } = strategy {
                return Err(ProposalError::Invalid("shift delta must not be 0".into()));
            }
            match scope {
                ResetScope::All => {}
                ResetScope::Topics { topics } => {
                    if topics.len() > MAX_SCOPE_ITEMS {
                        return Err(ProposalError::Invalid(format!(
                            "scope.topics must have at most {MAX_SCOPE_ITEMS} entries"
                        )));
                    }
                    for t in topics {
                        facts.topic(t).ok_or_else(|| no_topic(t))?;
                    }
                }
                ResetScope::Partitions { partitions } => {
                    if partitions.len() > MAX_SCOPE_ITEMS {
                        return Err(ProposalError::Invalid(format!(
                            "scope.partitions must have at most {MAX_SCOPE_ITEMS} entries"
                        )));
                    }
                    for p in partitions {
                        let t = facts.topic(&p.topic).ok_or_else(|| no_topic(&p.topic))?;
                        if p.partition < 0 || p.partition as usize >= t.partitions {
                            return Err(ProposalError::Invalid(format!(
                                "topic {} has {} partitions; partition {} does not exist",
                                p.topic, t.partitions, p.partition
                            )));
                        }
                    }
                }
            }
            Ok(())
        }
    }
}

/// Что человек применил формой — для сравнения с предложением (спец. G3 §6).
#[derive(Debug, Clone, PartialEq)]
pub enum Applied {
    AlterTopicConfigs {
        topic: String,
        changes: Vec<ConfigChange>,
    },
    CreateTopic(NewTopicSpec),
    AddPartitions {
        topic: String,
        new_total: i32,
    },
    /// `request` — стратегия и область из предпросмотра формы; `None` — форма их не передала:
    /// считается «с правками».
    ResetOffsets {
        group_id: String,
        request: Option<ResetRequest>,
    },
}

impl Applied {
    /// Вид и объект применённого: предложение другого вида или объекта не отмечается.
    pub fn matches(&self, change: &Change) -> bool {
        match (self, change) {
            (
                Applied::AlterTopicConfigs { topic: a, .. },
                Change::AlterTopicConfigs { topic: b, .. },
            )
            | (Applied::AddPartitions { topic: a, .. }, Change::AddPartitions { topic: b, .. }) => {
                a == b
            }
            (Applied::CreateTopic(_), Change::CreateTopic { .. }) => true,
            (
                Applied::ResetOffsets { group_id: a, .. },
                Change::ResetOffsets { group_id: b, .. },
            ) => a == b,
            _ => false,
        }
    }
}

fn config_set(changes: &[ConfigChange]) -> BTreeSet<(String, Option<String>)> {
    changes
        .iter()
        .map(|c| match c {
            ConfigChange::Set { name, value } => (name.clone(), Some(value.clone())),
            ConfigChange::Delete { name } => (name.clone(), None),
        })
        .collect()
}

/// Применено ровно то, что предлагалось: конфиги — множеством пар без учёта порядка, топик —
/// всеми полями, партиции — числом, сброс — стратегией и областью.
pub fn applied_as_proposed(change: &Change, applied: &Applied) -> bool {
    match (change, applied) {
        (
            Change::AlterTopicConfigs { changes, .. },
            Applied::AlterTopicConfigs {
                changes: applied, ..
            },
        ) => {
            let proposed: BTreeSet<(String, Option<String>)> = changes
                .iter()
                .map(|c| (c.name.clone(), c.value.clone()))
                .collect();
            proposed == config_set(applied)
        }
        (
            Change::CreateTopic {
                name,
                partitions,
                replication_factor,
                configs,
            },
            Applied::CreateTopic(spec),
        ) => {
            &spec.name == name
                && spec.partitions == *partitions
                && spec.replication_factor == *replication_factor
                && &spec.configs == configs
        }
        (Change::AddPartitions { new_total, .. }, Applied::AddPartitions { new_total: n, .. }) => {
            n == new_total
        }
        (
            Change::ResetOffsets {
                group_id,
                strategy,
                scope,
            },
            Applied::ResetOffsets {
                request: Some(r), ..
            },
        ) => &r.group_id == group_id && &r.strategy == strategy && &r.scope == scope,
        _ => false,
    }
}
````

Патч `mod.rs`, `store.rs`, `kafka/groups.rs`, `kafka/admin.rs`:

<!-- block: patch task3-src.diff -->
````diff
diff --git a/src-tauri/src/health/mod.rs b/src-tauri/src/health/mod.rs
index 467941f..55fb4bc 100644
--- a/src-tauri/src/health/mod.rs
+++ b/src-tauri/src/health/mod.rs
@@ -7,6 +7,7 @@ pub const CHANGED_EVENT: &str = "health://changed";
 pub mod collect;
 pub mod kafka_source;
 pub mod model;
+pub mod proposals;
 pub mod rules;
 pub mod service;
 pub mod snapshot;
diff --git a/src-tauri/src/health/store.rs b/src-tauri/src/health/store.rs
index 466f5da..45071ba 100644
--- a/src-tauri/src/health/store.rs
+++ b/src-tauri/src/health/store.rs
@@ -1,5 +1,6 @@
-//! Файл `health/<connectionId>.json` (спец. G2 §5): последняя проверка, скрытые находки и
-//! замечания LLM. Срок замечания — 7 дней с создания, открытых — не больше 200 на подключение.
+//! Файл `health/<connectionId>.json` (спец. G2 §5, G3 §3): последняя проверка, скрытые находки,
+//! замечания и предложения LLM. Срок замечания и предложения — 7 дней с создания; открытых —
+//! не больше 200 замечаний и 50 предложений на подключение.
 
 use std::collections::BTreeMap;
 use std::path::{Path, PathBuf};
@@ -8,6 +9,10 @@ use std::sync::Mutex;
 use serde::{Deserialize, Serialize};
 
 use super::model::{Finding, Severity, Target};
+use super::proposals::{
+    Applied, Change, MAX_OPEN_PROPOSALS, MAX_REJECT_REASON_CHARS, Proposal, ProposalStatus,
+    applied_as_proposed,
+};
 use crate::error::{AppError, AppResult, ErrorCode};
 use crate::storage::connections::Environment;
 use crate::storage::fs::{read_json_or_quarantine, write_json};
@@ -16,6 +21,8 @@ use crate::sync::MutexExt;
 pub const SCHEMA_VERSION: u32 = 1;
 pub const DIR_NAME: &str = "health";
 pub const ANNOTATION_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;
+/// Срок предложения — тот же, что у замечания.
+pub const PROPOSAL_TTL_MS: i64 = ANNOTATION_TTL_MS;
 pub const MAX_OPEN_ANNOTATIONS: usize = 200;
 pub const MAX_TITLE_CHARS: usize = 120;
 pub const MAX_BODY_CHARS: usize = 2000;
@@ -107,6 +114,9 @@ pub struct HealthFile {
     pub ignored: Vec<Ignored>,
     #[serde(default)]
     pub annotations: Vec<Annotation>,
+    /// Предложения LLM (G3): любой статус до истечения срока.
+    #[serde(default)]
+    pub proposals: Vec<Proposal>,
 }
 
 impl HealthFile {
@@ -117,6 +127,8 @@ impl HealthFile {
     fn purge(&mut self, now_ms: i64) {
         self.annotations
             .retain(|a| now_ms - a.created_at_ms < ANNOTATION_TTL_MS);
+        self.proposals
+            .retain(|p| now_ms - p.created_at_ms < PROPOSAL_TTL_MS);
     }
 }
 
@@ -129,9 +141,18 @@ pub struct Author {
 
 impl Author {
     fn owns(&self, a: &Annotation) -> bool {
-        match (&self.client, &a.client) {
+        self.owns_record(&a.client, a.session_id.as_deref())
+    }
+
+    fn owns_proposal(&self, p: &Proposal) -> bool {
+        self.owns_record(&p.client, p.session_id.as_deref())
+    }
+
+    /// Именованный клиент владеет записями с тем же именем; безымянный — только своей сессии.
+    fn owns_record(&self, client: &Option<String>, session_id: Option<&str>) -> bool {
+        match (&self.client, client) {
             (Some(mine), Some(theirs)) => mine == theirs,
-            (None, None) => a.session_id.as_deref() == Some(self.session_id.as_str()),
+            (None, None) => session_id == Some(self.session_id.as_str()),
             _ => false,
         }
     }
@@ -162,6 +183,45 @@ pub enum Removed {
     NotOwner,
 }
 
+/// Новое предложение LLM (спец. G3 §3).
+#[derive(Debug, Clone)]
+pub struct NewProposal {
+    pub severity: Severity,
+    pub title: String,
+    pub rationale: String,
+    pub change: Change,
+    pub related_rule_id: Option<String>,
+    pub related_finding_key: Option<String>,
+    pub author: Author,
+}
+
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub enum Proposed {
+    Created(String),
+    /// Открытое предложение того же автора, вида и объекта обновлено.
+    Updated(String),
+    LimitReached,
+}
+
+#[derive(Debug, Clone, Copy, PartialEq, Eq)]
+pub enum Withdrawn {
+    Withdrawn,
+    NotFound,
+    NotOwner,
+    /// Уже не открытое: применено, отклонено или отозвано раньше.
+    Closed(ProposalStatus),
+}
+
+/// Итог отметки «применено» в команде изменения (спец. G3 §6).
+#[derive(Debug, Clone, PartialEq, Eq)]
+pub enum MarkApplied {
+    Applied {
+        as_proposed: bool,
+    },
+    /// Предложения нет, оно закрыто или относится к другому объекту: операция прошла, отметки нет.
+    Skipped(&'static str),
+}
+
 /// Текст замечания: без управляющих символов (кроме перевода строки), не длиннее `max` символов.
 pub fn sanitize(text: &str, max: usize) -> String {
     let clean: String = text
@@ -422,6 +482,197 @@ impl HealthStore {
         })
     }
 
+    // ---- Предложения LLM (G3) ----
+
+    fn proposal_not_found() -> crate::error::AppError {
+        app_err!(
+            ErrorCode::NotFound,
+            "errors.health.proposalNotFound",
+            "The proposal was deleted or has expired"
+        )
+    }
+
+    /// `propose_change` из MCP: повтор того же автора по тому же виду и объекту среди открытых
+    /// обновляет запись; открытых — не больше `MAX_OPEN_PROPOSALS`.
+    pub fn propose(
+        &self,
+        connection_id: &str,
+        new: NewProposal,
+        now_ms: i64,
+    ) -> AppResult<Proposed> {
+        let title = sanitize(&new.title, MAX_TITLE_CHARS);
+        let rationale = sanitize(&new.rationale, MAX_BODY_CHARS);
+        let change = new.change.trimmed();
+        let target = change.target();
+        self.update(connection_id, now_ms, |f| {
+            if let Some(p) = f.proposals.iter_mut().find(|p| {
+                p.status == ProposalStatus::Open
+                    && new.author.owns_proposal(p)
+                    && p.change.kind_name() == change.kind_name()
+                    && p.target == target
+            }) {
+                p.severity = new.severity;
+                p.title = title;
+                p.rationale = rationale;
+                p.change = change;
+                p.related_rule_id = new.related_rule_id;
+                p.related_finding_key = new.related_finding_key;
+                p.updated_at_ms = now_ms;
+                return Proposed::Updated(p.id.clone());
+            }
+            let open = f
+                .proposals
+                .iter()
+                .filter(|p| p.status == ProposalStatus::Open)
+                .count();
+            if open >= MAX_OPEN_PROPOSALS {
+                return Proposed::LimitReached;
+            }
+            let id = uuid::Uuid::new_v4().to_string();
+            let session_id = new
+                .author
+                .client
+                .is_none()
+                .then(|| new.author.session_id.clone());
+            f.proposals.push(Proposal {
+                id: id.clone(),
+                severity: new.severity,
+                title,
+                rationale,
+                target,
+                change,
+                related_rule_id: new.related_rule_id,
+                related_finding_key: new.related_finding_key,
+                client: new.author.client,
+                session_id,
+                created_at_ms: now_ms,
+                updated_at_ms: now_ms,
+                status: ProposalStatus::Open,
+                closed_at_ms: None,
+                applied_as_proposed: None,
+                reject_reason: None,
+            });
+            Proposed::Created(id)
+        })
+    }
+
+    /// Человек: «Отклонить» (из открытого, с необязательной причиной) или «Открыть снова»
+    /// (из отклонённого). Применённое и отозванное не меняются.
+    pub fn set_proposal_status(
+        &self,
+        connection_id: &str,
+        proposal_id: &str,
+        status: ProposalStatus,
+        reason: Option<String>,
+        now_ms: i64,
+    ) -> AppResult<()> {
+        let reason = reason
+            .map(|r| sanitize(&r, MAX_REJECT_REASON_CHARS))
+            .filter(|r| !r.is_empty());
+        self.update(connection_id, now_ms, |f| {
+            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
+                return Err(Self::proposal_not_found());
+            };
+            match (p.status, status) {
+                (ProposalStatus::Open, ProposalStatus::Rejected) => {
+                    p.status = ProposalStatus::Rejected;
+                    p.closed_at_ms = Some(now_ms);
+                    p.reject_reason = reason;
+                }
+                (ProposalStatus::Rejected, ProposalStatus::Open) => {
+                    p.status = ProposalStatus::Open;
+                    p.closed_at_ms = None;
+                    p.reject_reason = None;
+                    p.updated_at_ms = now_ms;
+                }
+                (from, to) => {
+                    return Err(app_err!(
+                        ErrorCode::InvalidInput,
+                        "errors.health.proposalClosed",
+                        "The proposal is {{status}} and cannot be changed to {{to}}",
+                        status = from.as_str(),
+                        to = to.as_str()
+                    ));
+                }
+            }
+            Ok(())
+        })?
+    }
+
+    /// Команда изменения после успеха в Kafka: открытое предложение того же вида и объекта
+    /// становится `applied`; иначе отметка пропускается (операция уже прошла).
+    pub fn mark_applied(
+        &self,
+        connection_id: &str,
+        proposal_id: &str,
+        applied: &Applied,
+        now_ms: i64,
+    ) -> AppResult<MarkApplied> {
+        self.update(connection_id, now_ms, |f| {
+            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
+                return MarkApplied::Skipped("proposal not found");
+            };
+            if p.status != ProposalStatus::Open {
+                return MarkApplied::Skipped("proposal is not open");
+            }
+            if !applied.matches(&p.change) {
+                return MarkApplied::Skipped("proposal is about another change");
+            }
+            let as_proposed = applied_as_proposed(&p.change, applied);
+            p.status = ProposalStatus::Applied;
+            p.closed_at_ms = Some(now_ms);
+            p.applied_as_proposed = Some(as_proposed);
+            MarkApplied::Applied { as_proposed }
+        })
+    }
+
+    /// `withdraw_proposal` из MCP: только своё и только открытое.
+    pub fn withdraw_own(
+        &self,
+        connection_id: &str,
+        proposal_id: &str,
+        author: &Author,
+        now_ms: i64,
+    ) -> AppResult<Withdrawn> {
+        self.update(connection_id, now_ms, |f| {
+            let Some(p) = f.proposals.iter_mut().find(|p| p.id == proposal_id) else {
+                return Withdrawn::NotFound;
+            };
+            if !author.owns_proposal(p) {
+                return Withdrawn::NotOwner;
+            }
+            if p.status != ProposalStatus::Open {
+                return Withdrawn::Closed(p.status);
+            }
+            p.status = ProposalStatus::Withdrawn;
+            p.closed_at_ms = Some(now_ms);
+            Withdrawn::Withdrawn
+        })
+    }
+
+    pub fn delete_proposal(
+        &self,
+        connection_id: &str,
+        proposal_id: &str,
+        now_ms: i64,
+    ) -> AppResult<()> {
+        let found = self.update(connection_id, now_ms, |f| {
+            let n = f.proposals.len();
+            f.proposals.retain(|p| p.id != proposal_id);
+            f.proposals.len() < n
+        })?;
+        found.then_some(()).ok_or_else(Self::proposal_not_found)
+    }
+
+    /// «Удалить все предложения LLM» у подключения.
+    pub fn delete_all_proposals(&self, connection_id: &str, now_ms: i64) -> AppResult<usize> {
+        self.update(connection_id, now_ms, |f| {
+            let n = f.proposals.len();
+            f.proposals.clear();
+            n
+        })
+    }
+
     /// Удаление вместе с подключением.
     pub fn delete_connection(&self, connection_id: &str) -> AppResult<()> {
         let _g = self.lock.lock_safe();
diff --git a/src-tauri/src/kafka/admin.rs b/src-tauri/src/kafka/admin.rs
index be31925..6e49290 100644
--- a/src-tauri/src/kafka/admin.rs
+++ b/src-tauri/src/kafka/admin.rs
@@ -54,7 +54,7 @@ pub fn validate_topic_name(name: &str) -> AppResult<()> {
     Ok(())
 }
 
-#[derive(Debug, Clone, Deserialize)]
+#[derive(Debug, Clone, PartialEq, Deserialize)]
 #[serde(rename_all = "camelCase")]
 pub struct NewTopicSpec {
     pub name: String,
diff --git a/src-tauri/src/kafka/groups.rs b/src-tauri/src/kafka/groups.rs
index b2095cb..12a32ab 100644
--- a/src-tauri/src/kafka/groups.rs
+++ b/src-tauri/src/kafka/groups.rs
@@ -155,7 +155,7 @@ pub struct TopicGroups {
     pub incomplete: bool,
 }
 
-#[derive(Debug, Clone, PartialEq, Deserialize)]
+#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
 #[serde(
     tag = "type",
     rename_all = "camelCase",
@@ -169,7 +169,7 @@ pub enum ResetStrategy {
     Shift { delta: i64 },
 }
 
-#[derive(Debug, Clone, PartialEq, Deserialize)]
+#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
 #[serde(
     tag = "type",
     rename_all = "camelCase",
@@ -186,7 +186,7 @@ pub enum ResetScope {
     },
 }
 
-#[derive(Debug, Clone, PartialEq, Deserialize)]
+#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
 #[serde(rename_all = "camelCase")]
 pub struct ResetRequest {
     pub group_id: String,
````

Ключи ошибок на 12 языках:

<!-- block: spec /tmp/g3-task3-errors.json -->
````json
{
 "path": "errors.health",
 "after": "mcp",
 "values": {
  "en": {
   "proposalNotFound": "The proposal was deleted or has expired",
   "proposalClosed": "The proposal is {{status}} and cannot be changed to {{to}}"
  },
  "ru": {
   "proposalNotFound": "Предложение удалено или его срок истёк",
   "proposalClosed": "Предложение в статусе {{status}}, изменить его на {{to}} нельзя"
  },
  "de": {
   "proposalNotFound": "Der Vorschlag wurde gelöscht oder ist abgelaufen",
   "proposalClosed": "Der Vorschlag ist {{status}} und kann nicht auf {{to}} geändert werden"
  },
  "fr": {
   "proposalNotFound": "La proposition a été supprimée ou a expiré",
   "proposalClosed": "La proposition est {{status}} et ne peut pas passer à {{to}}"
  },
  "es": {
   "proposalNotFound": "La propuesta se eliminó o ha caducado",
   "proposalClosed": "La propuesta está {{status}} y no se puede cambiar a {{to}}"
  },
  "it": {
   "proposalNotFound": "La proposta è stata eliminata o è scaduta",
   "proposalClosed": "La proposta è {{status}} e non può passare a {{to}}"
  },
  "pl": {
   "proposalNotFound": "Propozycja została usunięta lub wygasła",
   "proposalClosed": "Propozycja ma stan {{status}} i nie można zmienić go na {{to}}"
  },
  "pt-BR": {
   "proposalNotFound": "A proposta foi excluída ou expirou",
   "proposalClosed": "A proposta está {{status}} e não pode mudar para {{to}}"
  },
  "zh-CN": {
   "proposalNotFound": "建议已删除或已过期",
   "proposalClosed": "建议处于 {{status}} 状态，无法改为 {{to}}"
  },
  "zh-TW": {
   "proposalNotFound": "建議已刪除或已過期",
   "proposalClosed": "建議處於 {{status}} 狀態，無法改為 {{to}}"
  },
  "ja": {
   "proposalNotFound": "提案は削除されたか期限切れです",
   "proposalClosed": "提案は{{status}}のため{{to}}に変更できません"
  },
  "ko": {
   "proposalNotFound": "제안이 삭제되었거나 만료되었습니다",
   "proposalClosed": "제안 상태: {{status}}. 다음 상태로 변경할 수 없음: {{to}}"
  }
 }
}
````

Run: `node /tmp/add-keys.mjs src/locales /tmp/g3-task3-errors.json`

- [ ] **Step 4: Тесты проходят.**

Run: `cd src-tauri && cargo test --lib -- health::proposals` — Expected: `8 passed`; `cargo test --lib -- health::store` — `20 passed`; `cargo test --lib` — `470 passed`; clippy и fmt — чисто; из корня `pnpm i18n:check` — без замечаний (ключи Rust найдены в словарях).

- [ ] **Step 5: Commit.**

````bash
git add src-tauri/src/health/mod.rs src-tauri/src/health/proposals.rs src-tauri/src/health/store.rs src-tauri/src/kafka/groups.rs src-tauri/src/kafka/admin.rs src/locales/*.json
git commit -m "feat(health): предложения LLM — виды изменения, проверка против фактов кластера, сравнение применённого, хранилище

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 4: Крейт `glafira-mcp`: `ChangeSpec`, `ProposalOp`/`ProposalOutput`, `Backend::proposals`, инструменты `propose_change`, `list_proposals`, `withdraw_proposal`

**Files:**
- Create: `crates/glafira-mcp/src/proposals.rs`, `crates/glafira-mcp/src/server/proposal_tools.rs`
- Modify: `crates/glafira-mcp/src/lib.rs` (`pub mod proposals;`), `crates/glafira-mcp/src/model.rs` (метод трейта), `crates/glafira-mcp/src/server/mod.rs` (роутер, `proposals()`, `INSTRUCTIONS`), `crates/glafira-mcp/tests/server.rs` (фейковый `Backend`, сторожок 19, новые тесты)
- Test: модули `tests` в новых файлах, `tests/server.rs`

**Interfaces:**
- Consumes: `health::{Severity, StatusFilter, Target, TargetKind}`, `model::{SessionView, BackendError, BoxFuture}`, `rules::fit`, обвязка `GatewayServer::{call, identify, slots}`.
- Produces: `glafira_mcp::proposals::{MAX_RELATED_KEY_CHARS = 200, ConfigChangeSpec { name, value: Option<String> }, PartitionRef { topic, partition }, ResetStrategySpec::{Earliest | Latest | Timestamp { timestamp_ms } | Offset { offset } | Shift { delta }}, ResetScopeSpec::{All | Topics { topics } | Partitions { partitions }}, ChangeSpec::{AlterTopicConfigs | CreateTopic { replication_factor: Option<i32>, configs: Option<BTreeMap> } | AddPartitions | ResetOffsets} с kind_name() и target_name(), NewProposal, ProposalOp::{Propose { connection_id, session, proposal } | List { connection_id, status } | Withdraw { connection_id, session, proposal_id }}.connection_id(), ProposalView { …, change: ChangeSpec, status: String, closed_at_ms, applied_as_proposed, reject_reason }, ProposalOutput::{Proposed { proposal_id, created, read_only } | Proposals(Vec<ProposalView>) | Withdrawn}}`; `Backend::proposals(&self, op) -> BoxFuture<Result<ProposalOutput, BackendError>>` (обязательный метод: без реализации в приложении (задача 5) `src-tauri` не соберётся — задачи 4 и 5 идут подряд); инструменты с подсказками `readOnlyHint: false`, `idempotentHint: true` у `propose_change`/`withdraw_proposal`; `INSTRUCTIONS` без слов «read-only gateway»: шлюз не меняет кластер, предложения применяет человек; сторожок — 19 инструментов; проверка «нет Pro» — по целому слову.

Спец. G3 §4. Схема `ChangeSpec` уходит LLM через `schemars` (поля camelCase через `rename_all` на вариантах); JSON совпадает с `health::proposals::Change` приложения, поэтому бэкенд конвертирует их через `serde_json` (задача 5). В журнал у `propose_change` идут только `connectionId`, `kind`, `target`, `severity`, `relatedRuleId`. `list_proposals` без страниц: при обрезке до 100 КБ остаются самые свежие (`newest_first`), как у `list_annotations`.

- [ ] **Step 1: Тесты.**

Модули тестов новых файлов (JSON-форма и схема `ChangeSpec`; сортировка `newest_first`):

<!-- block: file crates/glafira-mcp/src/proposals.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn change_json_matches_the_stored_form() {
        let c: ChangeSpec = serde_json::from_value(json!({
            "kind": "alterTopicConfigs", "topic": "orders",
            "changes": [{ "name": "min.insync.replicas", "value": "2" }, { "name": "retention.ms" }]
        }))
        .unwrap();
        assert_eq!(
            (c.kind_name(), c.target_name()),
            ("alterTopicConfigs", "orders")
        );
        assert_eq!(
            serde_json::to_value(&c).unwrap()["changes"][1],
            json!({ "name": "retention.ms", "value": null })
        );
        let c: ChangeSpec = serde_json::from_value(
            json!({ "kind": "createTopic", "name": "dlq", "partitions": 3 }),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&c).unwrap(),
            json!({ "kind": "createTopic", "name": "dlq", "partitions": 3 })
        );
        let c: ChangeSpec = serde_json::from_value(json!({
            "kind": "resetOffsets", "groupId": "billing",
            "strategy": { "type": "timestamp", "timestampMs": 5 },
            "scope": { "type": "partitions", "partitions": [{ "topic": "orders", "partition": 0 }] }
        }))
        .unwrap();
        assert_eq!(c.target_name(), "billing");
        assert_eq!(
            serde_json::to_value(&c).unwrap()["strategy"],
            json!({ "type": "timestamp", "timestampMs": 5 })
        );
        let c: ChangeSpec = serde_json::from_value(
            json!({ "kind": "addPartitions", "topic": "orders", "newTotal": 6 }),
        )
        .unwrap();
        assert_eq!(serde_json::to_value(&c).unwrap()["newTotal"], 6);
        assert!(
            serde_json::from_value::<ChangeSpec>(json!({ "kind": "deleteTopic", "topic": "x" }))
                .is_err()
        );
    }

    #[test]
    fn schema_uses_camel_case_field_names() {
        let schema = serde_json::to_value(schemars::schema_for!(ChangeSpec)).unwrap();
        let text = schema.to_string();
        for name in ["newTotal", "groupId", "replicationFactor", "timestampMs"] {
            assert!(text.contains(name), "{name}: {text}");
        }
        assert!(!text.contains("new_total"), "{text}");
    }
}
````

<!-- block: file crates/glafira-mcp/src/server/proposal_tools.rs part=tests -->
````rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{Target, TargetKind};

    fn view(id: &str, created: i64, updated: i64) -> ProposalView {
        ProposalView {
            id: id.into(),
            severity: Severity::Info,
            title: "t".into(),
            rationale: "r".into(),
            target: Target {
                kind: TargetKind::Topic,
                name: Some("orders".into()),
            },
            change: ChangeSpec::AddPartitions {
                topic: "orders".into(),
                new_total: 6,
            },
            related_rule_id: None,
            related_finding_key: None,
            client: None,
            status: "open".into(),
            created_at_ms: created,
            updated_at_ms: updated,
            closed_at_ms: None,
            applied_as_proposed: None,
            reject_reason: None,
        }
    }

    #[test]
    fn proposals_are_sorted_newest_first_with_stable_ties() {
        let mut v = vec![
            view("b", 1, 5),
            view("a", 1, 5),
            view("old", 0, 1),
            view("later-created", 3, 5),
            view("fresh", 0, 9),
        ];
        newest_first(&mut v);
        let ids: Vec<_> = v.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, ["fresh", "later-created", "a", "b", "old"]);
    }
}
````

Патч `tests/server.rs`: фейковый `Backend` получает `proposals` (сценарии `missing` → `not_found`, `too many` → `limit_reached`, `again` → `created: false`, `foreign`/`closed` у отзыва, `many_proposals`, `read_only`), сторожок 19 инструментов, проверка «Pro» по целому слову, тесты валидации и журнала, списка и отзыва, усечения:

<!-- block: patch task4-tests.diff -->
````diff
diff --git a/crates/glafira-mcp/tests/server.rs b/crates/glafira-mcp/tests/server.rs
index a5a91a9..73dcf6d 100644
--- a/crates/glafira-mcp/tests/server.rs
+++ b/crates/glafira-mcp/tests/server.rs
@@ -8,6 +8,7 @@ use std::time::Duration;
 
 use glafira_mcp::health::*;
 use glafira_mcp::model::*;
+use glafira_mcp::proposals::*;
 use glafira_mcp::{
     AuditLog, Backend, BackendError, BoxFuture, GatewayOptions, McpServerHandle, endpoint,
     handshake,
@@ -42,6 +43,12 @@ struct Fake {
     big_findings: AtomicBool,
     /// Сколько длинных замечаний отдаёт `list_annotations` (0 — одно короткое).
     many_notes: AtomicUsize,
+    /// Операции предложений, которые дошли до Backend.
+    proposal_ops: Mutex<Vec<ProposalOp>>,
+    /// Сколько длинных предложений отдаёт `list_proposals` (0 — одно короткое).
+    many_proposals: AtomicUsize,
+    /// Подключение только для чтения: `propose_change` отвечает `readOnly: true`.
+    read_only: AtomicBool,
 }
 
 /// Считает одновременные операции чтения на любом выходе из `read`.
@@ -84,6 +91,9 @@ impl Fake {
             check_delay: Mutex::new(Duration::ZERO),
             big_findings: AtomicBool::new(false),
             many_notes: AtomicUsize::new(0),
+            proposal_ops: Mutex::new(Vec::new()),
+            many_proposals: AtomicUsize::new(0),
+            read_only: AtomicBool::new(false),
         })
     }
 }
@@ -171,6 +181,32 @@ fn long_note(i: i64) -> AnnotationView {
     }
 }
 
+fn long_proposal(i: i64) -> ProposalView {
+    ProposalView {
+        id: format!("p{i}"),
+        severity: Severity::Warning,
+        title: format!("Proposal {i}"),
+        rationale: "x".repeat(2000),
+        target: Target {
+            kind: TargetKind::Topic,
+            name: Some("orders".into()),
+        },
+        change: ChangeSpec::AddPartitions {
+            topic: "orders".into(),
+            new_total: 6,
+        },
+        related_rule_id: None,
+        related_finding_key: None,
+        client: Some("claude-ai".into()),
+        status: "open".into(),
+        created_at_ms: i,
+        updated_at_ms: i,
+        closed_at_ms: None,
+        applied_as_proposed: None,
+        reject_reason: None,
+    }
+}
+
 fn finding(
     rule: &str,
     severity: Severity,
@@ -460,6 +496,60 @@ impl Backend for Fake {
             }
         })
     }
+
+    fn proposals(&self, op: ProposalOp) -> BoxFuture<'_, Result<ProposalOutput, BackendError>> {
+        Box::pin(async move {
+            self.proposal_ops.lock().unwrap().push(op.clone());
+            if op.connection_id() != "c1" {
+                return Err(BackendError::not_allowed());
+            }
+            match op {
+                ProposalOp::Propose { proposal, .. } => {
+                    if proposal.change.target_name() == "missing" {
+                        return Err(BackendError::not_found(
+                            "There is no topic missing in this cluster",
+                        ));
+                    }
+                    if proposal.title == "too many" {
+                        return Err(BackendError::new(
+                            "limit_reached",
+                            "At most 50 open proposals",
+                        ));
+                    }
+                    Ok(ProposalOutput::Proposed {
+                        proposal_id: "p1".into(),
+                        created: proposal.title != "again",
+                        read_only: self.read_only.load(Ordering::SeqCst),
+                    })
+                }
+                ProposalOp::List { .. } => {
+                    let many = self.many_proposals.load(Ordering::SeqCst);
+                    if many > 0 {
+                        // От старых к новым: без сортировки в ответ попали бы самые старые.
+                        return Ok(ProposalOutput::Proposals(
+                            (0..many).map(|i| long_proposal(i as i64)).collect(),
+                        ));
+                    }
+                    let mut applied = long_proposal(1);
+                    applied.rationale = "r".into();
+                    applied.status = "applied".into();
+                    applied.closed_at_ms = Some(2);
+                    applied.applied_as_proposed = Some(false);
+                    Ok(ProposalOutput::Proposals(vec![applied]))
+                }
+                ProposalOp::Withdraw { proposal_id, .. } => match proposal_id.as_str() {
+                    "foreign" => Err(BackendError::new(
+                        "not_allowed",
+                        "Only proposals created by this client can be withdrawn",
+                    )),
+                    "closed" => Err(BackendError::invalid_argument(
+                        "The proposal is already applied",
+                    )),
+                    _ => Ok(ProposalOutput::Withdrawn),
+                },
+            }
+        })
+    }
 }
 
 struct Env {
@@ -548,11 +638,17 @@ async fn exposes_exactly_the_tools() {
     let c = client(&e.ep).await;
     let info = c.peer_info().unwrap();
     let instructions = info.instructions.as_deref().unwrap();
-    assert!(instructions.contains("read-only gateway"));
+    assert!(instructions.contains("propose_change"));
+    assert!(instructions.contains("cannot be proposed"));
     assert!(instructions.contains("start with health_check"));
-    // С 0.3.0 платного тарифа нет: ни инструкции, ни описания инструментов не упоминают Pro.
-    assert!(!glafira_mcp::server::INSTRUCTIONS.contains("Pro"));
-    assert!(!instructions.contains("Pro"));
+    // С 0.3.0 платного тарифа нет: ни инструкции, ни описания инструментов не упоминают Pro
+    // (слово целиком: «proposal» и «appliedAsProposed» — не тариф).
+    let mentions_pro = |s: &str| {
+        s.split(|c: char| !c.is_ascii_alphanumeric())
+            .any(|w| w == "Pro")
+    };
+    assert!(!mentions_pro(glafira_mcp::server::INSTRUCTIONS));
+    assert!(!mentions_pro(instructions));
     let mut tools = c.list_all_tools().await.unwrap();
     tools.sort_by(|a, b| a.name.cmp(&b.name));
     let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
@@ -572,15 +668,21 @@ async fn exposes_exactly_the_tools() {
             "list_annotations",
             "list_connections",
             "list_consumer_groups",
+            "list_proposals",
             "list_topics",
             "observe_consumer_group",
+            "propose_change",
             "remove_annotation",
+            "withdraw_proposal",
         ]
     );
     for t in &tools {
         let a = t.annotations.as_ref().unwrap();
-        // Замечания пишутся в Glafira (не в кластер): только они не read-only.
-        let writes = matches!(t.name.as_ref(), "annotate" | "remove_annotation");
+        // Замечания и предложения пишутся в Glafira (не в кластер): только они не read-only.
+        let writes = matches!(
+            t.name.as_ref(),
+            "annotate" | "remove_annotation" | "propose_change" | "withdraw_proposal"
+        );
         assert_eq!(
             (a.read_only_hint, a.destructive_hint),
             (Some(!writes), Some(false)),
@@ -596,7 +698,7 @@ async fn exposes_exactly_the_tools() {
             t.name
         );
         assert!(
-            !t.description.as_deref().unwrap_or_default().contains("Pro"),
+            !mentions_pro(t.description.as_deref().unwrap_or_default()),
             "{}",
             t.name
         );
@@ -1477,3 +1579,187 @@ async fn short_annotation_list_is_not_marked_truncated() {
     assert_eq!(v["items"][0]["id"], "n2");
     assert_eq!(v["items"][2]["id"], "n0");
 }
+
+fn proposal(topic: &str, title: &str) -> Value {
+    json!({
+        "connectionId": "c1",
+        "severity": "warning",
+        "title": title,
+        "rationale": "acks=all producers need two in-sync replicas",
+        "change": {
+            "kind": "alterTopicConfigs",
+            "topic": topic,
+            "changes": [{ "name": "min.insync.replicas", "value": "2" }]
+        },
+        "relatedRuleId": "topic.minIsrOne",
+        "relatedFindingKey": "topic.minIsrOne:topic:orders",
+    })
+}
+
+#[tokio::test]
+async fn propose_change_validates_and_keeps_text_out_of_the_log() {
+    let fake = Fake::new(SessionAccess::Approved);
+    let e = env(fake.clone()).await;
+    let c = client(&e.ep).await;
+    let mut empty_rationale = proposal("orders", "t");
+    empty_rationale["rationale"] = json!("  ");
+    let mut long_key = proposal("orders", "t");
+    long_key["relatedFindingKey"] = json!("k".repeat(201));
+    for (args, code) in [
+        (proposal("orders", "  "), "invalid_argument"),
+        (empty_rationale, "invalid_argument"),
+        (long_key, "invalid_argument"),
+        (proposal("missing", "t"), "not_found"),
+        (proposal("orders", "too many"), "limit_reached"),
+    ] {
+        let msg = err(&call(&c, "propose_change", args).await);
+        assert!(msg.starts_with(code), "{msg}");
+    }
+    // Неизвестный вид изменения не проходит схему параметров.
+    let mut unknown = proposal("orders", "t");
+    unknown["change"] = json!({ "kind": "deleteTopic", "topic": "orders" });
+    let msg = err(&call(&c, "propose_change", unknown).await);
+    assert!(!msg.is_empty());
+    assert!(fake.proposal_ops.lock().unwrap().iter().all(|op| !matches!(
+        op,
+        ProposalOp::Propose { proposal: np, .. } if np.title == "t" && np.change.target_name() == "orders"
+    )));
+
+    let v = ok(call(
+        &c,
+        "propose_change",
+        proposal("orders", "Raise min.insync.replicas"),
+    )
+    .await);
+    assert_eq!(
+        v,
+        json!({ "proposalId": "p1", "created": true, "readOnly": false })
+    );
+    let ops = fake.proposal_ops.lock().unwrap().clone();
+    let Some(ProposalOp::Propose {
+        proposal: np,
+        session,
+        ..
+    }) = ops.last()
+    else {
+        panic!("{ops:?}")
+    };
+    assert_eq!(np.title, "Raise min.insync.replicas");
+    assert_eq!(np.related_rule_id.as_deref(), Some("topic.minIsrOne"));
+    assert_eq!(
+        np.related_finding_key.as_deref(),
+        Some("topic.minIsrOne:topic:orders")
+    );
+    assert_eq!(
+        np.change,
+        ChangeSpec::AlterTopicConfigs {
+            topic: "orders".into(),
+            changes: vec![ConfigChangeSpec {
+                name: "min.insync.replicas".into(),
+                value: Some("2".into())
+            }]
+        }
+    );
+    assert!(!session.id.is_empty());
+    let line = e.audit.recent(1)[0].to_string();
+    assert!(
+        line.contains("\"propose_change\"")
+            && line.contains("alterTopicConfigs")
+            && line.contains("\"orders\""),
+        "{line}"
+    );
+    assert!(
+        !line.contains("Raise min") && !line.contains("acks=all") && !line.contains("\"2\""),
+        "{line}"
+    );
+    // Повтор того же предложения — created: false, как отвечает Backend.
+    let v = ok(call(&c, "propose_change", proposal("orders", "again")).await);
+    assert_eq!(v["created"], false);
+}
+
+#[tokio::test]
+async fn read_only_connection_is_reported_to_the_llm() {
+    let fake = Fake::new(SessionAccess::Approved);
+    fake.read_only.store(true, Ordering::SeqCst);
+    let e = env(fake).await;
+    let c = client(&e.ep).await;
+    let v = ok(call(&c, "propose_change", proposal("orders", "t")).await);
+    assert_eq!(v["readOnly"], true);
+}
+
+#[tokio::test]
+async fn proposals_are_listed_with_decisions_and_withdrawn() {
+    let fake = Fake::new(SessionAccess::Approved);
+    let e = env(fake.clone()).await;
+    let c = client(&e.ep).await;
+    let v = ok(call(
+        &c,
+        "list_proposals",
+        json!({ "connectionId": "c1", "status": "closed" }),
+    )
+    .await);
+    let item = &v["items"][0];
+    assert_eq!(
+        (
+            item["id"].as_str(),
+            item["status"].as_str(),
+            item["appliedAsProposed"].as_bool()
+        ),
+        (Some("p1"), Some("applied"), Some(false))
+    );
+    assert_eq!(item["change"]["kind"], "addPartitions");
+    assert_eq!(item["change"]["newTotal"], 6);
+    assert!(item.get("rejectReason").is_none());
+    assert!(matches!(
+        fake.proposal_ops.lock().unwrap()[0],
+        ProposalOp::List {
+            status: StatusFilter::Closed,
+            ..
+        }
+    ));
+    let v = ok(call(
+        &c,
+        "withdraw_proposal",
+        json!({ "connectionId": "c1", "proposalId": "p1" }),
+    )
+    .await);
+    assert_eq!(v, json!({ "withdrawn": true }));
+    for (id, code) in [("foreign", "not_allowed"), ("closed", "invalid_argument")] {
+        let msg = err(&call(
+            &c,
+            "withdraw_proposal",
+            json!({ "connectionId": "c1", "proposalId": id }),
+        )
+        .await);
+        assert!(msg.starts_with(code), "{msg}");
+    }
+    // Чужое подключение — как у всех инструментов.
+    let msg = err(&call(&c, "list_proposals", json!({ "connectionId": "c2" })).await);
+    assert!(msg.starts_with("not_allowed"), "{msg}");
+}
+
+#[tokio::test]
+async fn long_proposal_list_is_cut_to_the_newest() {
+    let fake = Fake::new(SessionAccess::Approved);
+    fake.many_proposals.store(80, Ordering::SeqCst);
+    let e = env(fake).await;
+    let c = client(&e.ep).await;
+    let v = ok(call(&c, "list_proposals", json!({ "connectionId": "c1" })).await);
+    assert!(serde_json::to_vec(&v).unwrap().len() <= 100 * 1024);
+    assert_eq!(v["truncated"], true);
+    let hint = v["hint"].as_str().unwrap();
+    assert!(hint.contains("100 KB") && hint.contains("most recently updated"));
+    let ids: Vec<&str> = v["items"]
+        .as_array()
+        .unwrap()
+        .iter()
+        .map(|i| i["id"].as_str().unwrap())
+        .collect();
+    assert!(!ids.is_empty() && ids.len() < 80, "{}", ids.len());
+    let expected: Vec<String> = (0..80)
+        .rev()
+        .take(ids.len())
+        .map(|i| format!("p{i}"))
+        .collect();
+    assert_eq!(ids, expected);
+}
````

- [ ] **Step 2: Увидеть падение.**

Run: `cd crates/glafira-mcp && cargo test` — Expected: ошибка компиляции (нет модуля `proposals`, метода `proposals` у `Backend`).

- [ ] **Step 3: Реализация.**

Новый `crates/glafira-mcp/src/proposals.rs` (до модуля тестов):

<!-- block: file crates/glafira-mcp/src/proposals.rs part=impl -->
````rust
//! Предложения LLM (спец. G3 §3–§4): виды изменения (их схема уходит LLM), операции
//! [`ProposalOp`] и ответы. Ни одна операция не меняет кластер: предложение пишется в Glafira,
//! а применяет его человек заполненной формой приложения.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::health::{Severity, StatusFilter, Target};
use crate::model::SessionView;

/// Длина `relatedFindingKey`.
pub const MAX_RELATED_KEY_CHARS: usize = 200;

/// Одно изменение конфига топика.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigChangeSpec {
    /// Topic config name, e.g. min.insync.replicas (use names from get_topic_configs).
    pub name: String,
    /// New value as a string; null resets the config to the broker default.
    #[serde(default)]
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct PartitionRef {
    pub topic: String,
    pub partition: i32,
}

/// Куда сбросить оффсеты (`type`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ResetStrategySpec {
    /// Beginning of each partition.
    Earliest,
    /// End of each partition.
    Latest,
    /// First offset at or after this Unix time in milliseconds.
    #[serde(rename_all = "camelCase")]
    Timestamp { timestamp_ms: i64 },
    /// This exact offset on every selected partition.
    Offset { offset: i64 },
    /// Move by delta from the committed offset; negative moves back.
    Shift { delta: i64 },
}

/// Какие партиции сбрасывать (`type`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ResetScopeSpec {
    /// Every partition the group has committed offsets for.
    All,
    /// All committed partitions of these topics.
    Topics { topics: Vec<String> },
    /// These partitions only.
    Partitions { partitions: Vec<PartitionRef> },
}

/// Что предлагает LLM (`kind`). JSON-форма та же, что хранит Glafira и читает интерфейс.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ChangeSpec {
    /// Set or reset topic configs; the user applies them in the config editor with a before/after review.
    AlterTopicConfigs {
        topic: String,
        /// 1 to 50 entries, unique names.
        changes: Vec<ConfigChangeSpec>,
    },
    /// Create a topic; the user applies it in the create-topic form.
    #[serde(rename_all = "camelCase")]
    CreateTopic {
        name: String,
        partitions: i32,
        /// Omit or -1 for the broker default (default.replication.factor).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        replication_factor: Option<i32>,
        /// Topic configs to set at creation, e.g. {"cleanup.policy": "compact"}; up to 50.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        configs: Option<BTreeMap<String, String>>,
    },
    /// Increase the number of partitions (it can never decrease).
    #[serde(rename_all = "camelCase")]
    AddPartitions { topic: String, new_total: i32 },
    /// Reset committed offsets of a consumer group (the group must be empty when the user applies it); the user previews the plan first.
    #[serde(rename_all = "camelCase")]
    ResetOffsets {
        group_id: String,
        strategy: ResetStrategySpec,
        scope: ResetScopeSpec,
    },
}

impl ChangeSpec {
    /// Имя вида — как `kind` в JSON (журнал).
    pub fn kind_name(&self) -> &'static str {
        match self {
            ChangeSpec::AlterTopicConfigs { .. } => "alterTopicConfigs",
            ChangeSpec::CreateTopic { .. } => "createTopic",
            ChangeSpec::AddPartitions { .. } => "addPartitions",
            ChangeSpec::ResetOffsets { .. } => "resetOffsets",
        }
    }

    /// Имя объекта: топик (у `createTopic` — будущий) или группа (журнал).
    pub fn target_name(&self) -> &str {
        match self {
            ChangeSpec::AlterTopicConfigs { topic, .. }
            | ChangeSpec::AddPartitions { topic, .. } => topic,
            ChangeSpec::CreateTopic { name, .. } => name,
            ChangeSpec::ResetOffsets { group_id, .. } => group_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewProposal {
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub change: ChangeSpec,
    pub related_rule_id: Option<String>,
    pub related_finding_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposalOp {
    Propose {
        connection_id: String,
        session: SessionView,
        proposal: NewProposal,
    },
    List {
        connection_id: String,
        status: StatusFilter,
    },
    Withdraw {
        connection_id: String,
        session: SessionView,
        proposal_id: String,
    },
}

impl ProposalOp {
    pub fn connection_id(&self) -> &str {
        match self {
            ProposalOp::Propose { connection_id, .. }
            | ProposalOp::List { connection_id, .. }
            | ProposalOp::Withdraw { connection_id, .. } => connection_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProposalView {
    pub id: String,
    pub severity: Severity,
    pub title: String,
    pub rationale: String,
    pub target: Target,
    pub change: ChangeSpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_rule_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_finding_key: Option<String>,
    /// Клиент, оставивший предложение; `None` — безымянный.
    pub client: Option<String>,
    /// open, applied (человек применил), rejected (человек отклонил), withdrawn (LLM отозвала).
    pub status: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
    /// Только у applied: применено без правок (`true`) или с правками.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub applied_as_proposed: Option<bool>,
    /// Только у rejected: комментарий человека, если он его оставил.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProposalOutput {
    Proposed {
        proposal_id: String,
        created: bool,
        /// Подключение только для чтения: применить сможет человек, сняв флажок.
        read_only: bool,
    },
    Proposals(Vec<ProposalView>),
    Withdrawn,
}
````

Новый `crates/glafira-mcp/src/server/proposal_tools.rs` (до модуля тестов):

<!-- block: file crates/glafira-mcp/src/server/proposal_tools.rs part=impl -->
````rust
//! Инструменты предложений (спец. G3 §4). Кластер они не меняют: предложение пишется в Glafira
//! и показывается человеку рядом с объектом; применяет его человек формой.

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, tool, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::GatewayServer;
use crate::health::{Severity, StatusFilter};
use crate::model::BackendError;
use crate::proposals::*;
use crate::rules;

/// У `list_proposals` нет страниц: при обрезке остаются самые свежие, остальное — сужением по status.
const PROPOSALS_TRUNCATED_HINT: &str = "The response was truncated to 100 KB: only the most recently updated proposals are shown; narrow with status";

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ProposeChangeParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// info, warning or critical: how urgent the change is.
    pub severity: Severity,
    /// Short summary for the user, up to 120 characters; plain text.
    pub title: String,
    /// Why this change and what it does, up to 2000 characters; plain text, no markup or links.
    pub rationale: String,
    /// The change itself; kind selects the form the user will see.
    pub change: ChangeSpec,
    /// ruleId of the health_check finding this change fixes, if any.
    #[serde(default)]
    pub related_rule_id: Option<String>,
    /// key of the health_check finding this change fixes, if any.
    #[serde(default)]
    pub related_finding_key: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListProposalsParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// open (default), closed (applied, rejected or withdrawn) or all.
    #[serde(default)]
    pub status: Option<StatusFilter>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WithdrawProposalParams {
    /// Connection id from list_connections.
    pub connection_id: String,
    /// Proposal id from propose_change or list_proposals.
    pub proposal_id: String,
}

#[derive(Serialize)]
struct ProposalsBody {
    items: Vec<ProposalView>,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'static str>,
}

fn proposals_of(b: &mut ProposalsBody) -> &mut Vec<ProposalView> {
    &mut b.items
}

/// Сначала недавно обновлённые; при равенстве — созданные позже, затем по id.
fn newest_first(items: &mut [ProposalView]) {
    items.sort_by(|a, b| {
        b.updated_at_ms
            .cmp(&a.updated_at_ms)
            .then(b.created_at_ms.cmp(&a.created_at_ms))
            .then_with(|| a.id.cmp(&b.id))
    });
}

fn to_value<T: Serialize>(v: &T) -> Result<Value, BackendError> {
    serde_json::to_value(v).map_err(|e| BackendError::new("internal", e.to_string()))
}

#[tool_router(router = proposal_router, vis = "pub(crate)")]
impl GatewayServer {
    #[tool(
        description = "Propose a change for the user to apply in Glafira. Does not change the cluster: the proposal appears next to the object with your title and rationale, and the user opens it as a pre-filled form, may edit it and confirms (or rejects it). Kinds: alterTopicConfigs (set or reset topic configs), createTopic, addPartitions, resetOffsets (the group must be empty when applied). Deleting or purging cannot be proposed — describe it in the chat instead. One problem — one proposal; a proposal with the same kind and object as your open one replaces it. The proposal is checked against the cluster (unknown topic or group, unknown config name, partitions not increasing). Then check the user's decision with list_proposals. readOnly: true in the response means the connection is read-only in Glafira and the user must lift that first. A proposal expires after 7 days; at most 50 open per connection.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn propose_change(
        &self,
        Parameters(p): Parameters<ProposeChangeParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        // В журнал — вид и объект, без заголовка, обоснования и значений изменения.
        let args = json!({
            "connectionId": p.connection_id,
            "kind": p.change.kind_name(),
            "target": p.change.target_name(),
            "severity": p.severity,
            "relatedRuleId": p.related_rule_id,
        });
        Ok(self
            .call(
                &ctx,
                "propose_change",
                args,
                Some(&p.connection_id),
                async {
                    if p.title.trim().is_empty() {
                        return Err(BackendError::invalid_argument("title must not be empty"));
                    }
                    if p.rationale.trim().is_empty() {
                        return Err(BackendError::invalid_argument(
                            "rationale must not be empty: explain the change to the user",
                        ));
                    }
                    if p.related_finding_key
                        .as_deref()
                        .is_some_and(|k| k.chars().count() > MAX_RELATED_KEY_CHARS)
                    {
                        return Err(BackendError::invalid_argument(format!(
                            "relatedFindingKey is longer than {MAX_RELATED_KEY_CHARS} characters"
                        )));
                    }
                    let op = ProposalOp::Propose {
                        connection_id: p.connection_id.clone(),
                        session: self.identify(None),
                        proposal: NewProposal {
                            severity: p.severity,
                            title: p.title.clone(),
                            rationale: p.rationale.clone(),
                            change: p.change.clone(),
                            related_rule_id: p.related_rule_id.clone(),
                            related_finding_key: p.related_finding_key.clone(),
                        },
                    };
                    match self.proposals(op, true).await? {
                        ProposalOutput::Proposed {
                            proposal_id,
                            created,
                            read_only,
                        } => to_value(&json!({
                            "proposalId": proposal_id,
                            "created": created,
                            "readOnly": read_only,
                        })),
                        _ => Err(BackendError::unexpected()),
                    }
                },
            )
            .await)
    }

    #[tool(
        description = "List the change proposals left in Glafira for a connection by any client, with the user's decision: open, applied (appliedAsProposed tells whether the user applied it unchanged), rejected (with rejectReason when the user gave one) or withdrawn. Check it after proposing instead of asking the user in the chat.",
        annotations(
            read_only_hint = true,
            destructive_hint = false,
            open_world_hint = false
        )
    )]
    async fn list_proposals(
        &self,
        Parameters(p): Parameters<ListProposalsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .call(
                &ctx,
                "list_proposals",
                args,
                Some(&p.connection_id),
                async {
                    let op = ProposalOp::List {
                        connection_id: p.connection_id.clone(),
                        status: p.status.unwrap_or_default(),
                    };
                    match self.proposals(op, false).await? {
                        ProposalOutput::Proposals(mut items) => {
                            newest_first(&mut items);
                            let mut body = ProposalsBody {
                                items,
                                truncated: false,
                                hint: None,
                            };
                            if rules::fit(&mut body, proposals_of) {
                                body.truncated = true;
                                body.hint = Some(PROPOSALS_TRUNCATED_HINT);
                            }
                            to_value(&body)
                        }
                        _ => Err(BackendError::unexpected()),
                    }
                },
            )
            .await)
    }

    #[tool(
        description = "Withdraw one of your own open proposals from Glafira. A proposal of another client, or one the user already applied or rejected, cannot be withdrawn.",
        annotations(
            read_only_hint = false,
            destructive_hint = false,
            idempotent_hint = true,
            open_world_hint = false
        )
    )]
    async fn withdraw_proposal(
        &self,
        Parameters(p): Parameters<WithdrawProposalParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let args = to_value(&p).unwrap_or(Value::Null);
        Ok(self
            .call(
                &ctx,
                "withdraw_proposal",
                args,
                Some(&p.connection_id),
                async {
                    let op = ProposalOp::Withdraw {
                        connection_id: p.connection_id.clone(),
                        session: self.identify(None),
                        proposal_id: p.proposal_id.clone(),
                    };
                    match self.proposals(op, false).await? {
                        ProposalOutput::Withdrawn => to_value(&json!({ "withdrawn": true })),
                        _ => Err(BackendError::unexpected()),
                    }
                },
            )
            .await)
    }
}
````

Патч `lib.rs`, `model.rs`, `server/mod.rs`:

<!-- block: patch task4-src.diff -->
````diff
diff --git a/crates/glafira-mcp/src/lib.rs b/crates/glafira-mcp/src/lib.rs
index 2ed245e..d65fe60 100644
--- a/crates/glafira-mcp/src/lib.rs
+++ b/crates/glafira-mcp/src/lib.rs
@@ -2,7 +2,8 @@
 //!
 //! Крейт ничего не знает о Kafka: данные он получает через трейт [`Backend`], который
 //! реализует приложение, и умеет только читать — изменяющих операций в [`ReadOp`] нет.
-//! Замечания LLM (G2, [`health`]) пишутся в Glafira, а не в кластер.
+//! Замечания LLM (G2, [`health`]) и предложения изменений (G3, [`proposals`]) пишутся в Glafira,
+//! а не в кластер: предложение применяет человек формой приложения.
 
 pub mod audit;
 pub mod endpoint;
@@ -11,6 +12,7 @@ pub mod health;
 pub mod limits;
 pub mod model;
 pub mod observe;
+pub mod proposals;
 pub mod rules;
 pub mod server;
 
diff --git a/crates/glafira-mcp/src/model.rs b/crates/glafira-mcp/src/model.rs
index 469d844..7a5b005 100644
--- a/crates/glafira-mcp/src/model.rs
+++ b/crates/glafira-mcp/src/model.rs
@@ -163,6 +163,12 @@ pub trait Backend: Send + Sync + 'static {
         &self,
         op: crate::health::HealthOp,
     ) -> BoxFuture<'_, Result<crate::health::HealthOutput, BackendError>>;
+    /// Предложения изменений (спец. G3 §4): пишутся только в файл Glafira, кластер не меняется.
+    /// Реализация сверяет предложение с кластером и проверяет `llmAccess` подключения.
+    fn proposals(
+        &self,
+        op: crate::proposals::ProposalOp,
+    ) -> BoxFuture<'_, Result<crate::proposals::ProposalOutput, BackendError>>;
 }
 
 #[derive(Debug, Clone, PartialEq, Serialize)]
diff --git a/crates/glafira-mcp/src/server/mod.rs b/crates/glafira-mcp/src/server/mod.rs
index 19f11fa..2e0c0aa 100644
--- a/crates/glafira-mcp/src/server/mod.rs
+++ b/crates/glafira-mcp/src/server/mod.rs
@@ -25,17 +25,24 @@ use crate::endpoint::{BoxStream, Endpoint, Listener};
 use crate::health::{HealthOp, HealthOutput};
 use crate::limits::{CALLS_PER_MINUTE, GLOBAL_CONCURRENCY, RateLimiter, SESSION_CONCURRENCY};
 use crate::model::*;
+use crate::proposals::{ProposalOp, ProposalOutput};
 use crate::rules;
 
 mod health_tools;
+mod proposal_tools;
 mod tools;
 
 pub const INSTRUCTIONS: &str = "\
-Glafira is a read-only gateway to Apache Kafka clusters that the user explicitly allowed. \
-It can only read metadata: brokers, topics, partitions, configs and consumer groups. Nothing \
-in the clusters can be changed through it; when a change is needed, describe what to change and \
-where, and the user will do it in the Glafira window. Call list_connections first to get \
-connection ids.
+Glafira is a gateway to Apache Kafka clusters that the user explicitly allowed. It reads \
+metadata: brokers, topics, partitions, configs and consumer groups. Nothing in the clusters can \
+be changed through it: when a change is needed, propose it with propose_change (topic configs, a \
+new topic, more partitions, an offset reset) — the user sees the proposal next to the object in \
+Glafira and applies it themselves in a pre-filled form, or rejects it. Do not wait for the \
+decision in the same call: check it later with list_proposals (applied, rejected with a reason, \
+withdrawn). If propose_change answers readOnly: true, ask the user to lift read-only on the \
+connection in Glafira instead of looking for another way. Deleting or purging topics and groups \
+and sending messages cannot be proposed — describe them in the chat. Call list_connections first \
+to get connection ids.
 
 To review a cluster, start with health_check: it runs Glafira's deterministic rules. Take the \
 facts from it, then explain them, set priorities and find connections between them. Findings \
@@ -334,7 +341,7 @@ impl GatewayServer {
             shared,
             session_id,
             local: Arc::new(Semaphore::new(SESSION_CONCURRENCY)),
-            tool_router: Self::tool_router() + Self::health_router(),
+            tool_router: Self::tool_router() + Self::health_router() + Self::proposal_router(),
         }
     }
 
@@ -453,6 +460,17 @@ impl GatewayServer {
         self.shared.backend.health(op).await
     }
 
+    /// Операция предложений. `kafka` — она сверяет предложение с кластером (`propose_change`):
+    /// тогда занимает слоты параллельности, как чтение.
+    async fn proposals(&self, op: ProposalOp, kafka: bool) -> Result<ProposalOutput, BackendError> {
+        let _slots = if kafka {
+            Some(self.slots().await?)
+        } else {
+            None
+        };
+        self.shared.backend.proposals(op).await
+    }
+
     /// Общая обвязка инструмента: право, лимит, журнал, ответ или ошибка для LLM.
     async fn call<F>(
         &self,
````

- [ ] **Step 4: Тесты проходят.**

Run: `cd crates/glafira-mcp && cargo test` — Expected: unit `28 passed`, `tests/server.rs` `36 passed`, `manifest` и `gui_stdio` по 1; `cargo clippy --all-targets -- -D warnings` и `cargo fmt --check` — чисто. (`src-tauri` до задачи 5 не собирается: у `AppBackend` нет `proposals`.)

- [ ] **Step 5: Commit.**

````bash
git add crates/glafira-mcp/src/proposals.rs crates/glafira-mcp/src/server/proposal_tools.rs crates/glafira-mcp/src/lib.rs crates/glafira-mcp/src/model.rs crates/glafira-mcp/src/server/mod.rs crates/glafira-mcp/tests/server.rs
git commit -m "feat(mcp): инструменты propose_change, list_proposals, withdraw_proposal в крейте glafira-mcp; Backend::proposals; instructions о предложениях

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 5: `Backend::proposals` в приложении: факты из кластера, проверка, запись в файл Glafira, список и отзыв

**Files:**
- Modify: `src-tauri/src/mcp/backend.rs`
- Test: модуль `tests` там же

**Interfaces:**
- Consumes: `glafira_mcp::proposals as mp`, `health::proposals::{Change, Facts, TopicFact, validate, ProposalError, ProposalStatus, Proposal, MAX_OPEN_PROPOSALS, MAX_RELATED_KEY_CHARS}`, `health::store::{NewProposal, Proposed, Withdrawn, MAX_BODY_CHARS, sanitize}`, `kafka::cluster::cluster_metadata`, `commands::pooled_configs`, `group_ops::list_groups`, `HealthService::{generation, write, changed}`.
- Produces: `AppBackend::facts(connection_id, &Change) -> Facts` (метаданные всегда; ключи конфигов — для `alterTopicConfigs` по существующему не-внутреннему топику; группы — для `resetOffsets`); `proposals_impl`: порядок проверок — `relatedRuleId` из каталога, непустые заголовок и обоснование, `to_change` (serde-конвертация, иначе `invalid_argument`), `trimmed()`, факты, `validate` (`Invalid` → `invalid_argument`, `NotFound` → `not_found`), `read_only` из профиля, запись через `write` с поколением, `limit_reached` при 50 открытых; `List` читает файл на blocking-потоке; `Withdraw` → `not_found` / `not_allowed` / `invalid_argument("The proposal is already <status>")`; `fn to_change(&ChangeSpec) -> Result<Change, BackendError>`, `fn proposal_view(&Proposal) -> Result<ProposalView, BackendError>`.

Спец. G3 §5–§6. Порядок проверок намеренно такой: дешёвые отказы (`relatedRuleId`, пустые тексты) — до запроса к кластеру, поэтому тест `proposal_is_validated_before_the_cluster_is_read` видит `invalid_argument` без сети, а корректное предложение на недоступном `127.0.0.1:1` — `CONNECTION_FAILED`/`TIMEOUT`. Сверка объектов с кластером целиком проверяется интеграционным тестом (задача 6).

- [ ] **Step 1: Тесты.**

Патч модуля тестов `backend.rs` (`proposals_need_llm_access`, `proposal_is_validated_before_the_cluster_is_read`, `proposals_are_listed_and_withdrawn_by_their_author`, операции предложений в `health_file_errors_are_internal_without_the_path`):

<!-- block: patch task5-tests.diff -->
````diff
diff --git a/src-tauri/src/mcp/backend.rs b/src-tauri/src/mcp/backend.rs
index 5c11c48..bb23864 100644
--- a/src-tauri/src/mcp/backend.rs
+++ b/src-tauri/src/mcp/backend.rs
@@ -1050,6 +1270,27 @@ mod tests {
                 "{name}"
             );
         }
+        // Операции предложений, не ходящие в Kafka, — так же.
+        let ops = [
+            mp::ProposalOp::List {
+                connection_id: "open".into(),
+                status: mh::StatusFilter::All,
+            },
+            mp::ProposalOp::Withdraw {
+                connection_id: "open".into(),
+                session: claude.clone(),
+                proposal_id: "p1".into(),
+            },
+        ];
+        for op in ops {
+            let name = format!("{op:?}");
+            let e = b.proposals(op).await.unwrap_err();
+            assert_eq!(
+                (e.code.as_str(), e.message.as_str()),
+                ("internal", HEALTH_FILE_FAILED),
+                "{name}"
+            );
+        }
     }
 
     #[test]
@@ -1130,6 +1371,178 @@ mod tests {
         );
     }
 
+    fn alter_spec(topic: &str) -> mp::ChangeSpec {
+        mp::ChangeSpec::AlterTopicConfigs {
+            topic: topic.into(),
+            changes: vec![mp::ConfigChangeSpec {
+                name: "min.insync.replicas".into(),
+                value: Some("2".into()),
+            }],
+        }
+    }
+
+    fn new_proposal(title: &str, related: Option<&str>) -> mp::NewProposal {
+        mp::NewProposal {
+            severity: mh::Severity::Warning,
+            title: title.into(),
+            rationale: "acks=all needs two in-sync replicas".into(),
+            change: alter_spec("orders"),
+            related_rule_id: related.map(String::from),
+            related_finding_key: Some(" topic.minIsrOne:topic:orders ".into()),
+        }
+    }
+
+    fn propose<'a>(
+        b: &'a AppBackend,
+        session: &SessionView,
+        title: &str,
+        related: Option<&str>,
+    ) -> BoxFuture<'a, Result<mp::ProposalOutput, BackendError>> {
+        b.proposals(mp::ProposalOp::Propose {
+            connection_id: "open".into(),
+            session: session.clone(),
+            proposal: new_proposal(title, related),
+        })
+    }
+
+    #[tokio::test]
+    async fn proposals_need_llm_access() {
+        let (_d, b) = backend();
+        for id in ["closed", "missing"] {
+            let e = b
+                .proposals(mp::ProposalOp::List {
+                    connection_id: id.into(),
+                    status: mh::StatusFilter::All,
+                })
+                .await
+                .unwrap_err();
+            assert_eq!(e.code, "not_allowed", "{id}");
+        }
+    }
+
+    #[tokio::test]
+    async fn proposal_is_validated_before_the_cluster_is_read() {
+        let (d, b) = backend();
+        let claude = view("s1", Some("claude-ai"));
+        let _ = b.approvals.access(&claude, 0);
+        let e = propose(&b, &claude, "t", Some("no.such.rule"))
+            .await
+            .unwrap_err();
+        assert_eq!(e.code, "invalid_argument", "{e:?}");
+        let e = propose(&b, &claude, "\u{1}", None).await.unwrap_err();
+        assert_eq!(
+            (e.code.as_str(), e.message.as_str()),
+            ("invalid_argument", "title must not be empty")
+        );
+        assert!(!d.path().join("health").join("open.json").exists());
+        // Дальше предложение сверяется с кластером: 127.0.0.1:1 не отвечает.
+        let e = propose(
+            &b,
+            &claude,
+            "Raise min.insync.replicas",
+            Some("topic.minIsrOne"),
+        )
+        .await
+        .unwrap_err();
+        assert!(
+            ["CONNECTION_FAILED", "TIMEOUT"].contains(&e.code.as_str()),
+            "{e:?}"
+        );
+        assert!(
+            b.state
+                .health
+                .store
+                .load("open", now_ms())
+                .unwrap()
+                .proposals
+                .is_empty()
+        );
+    }
+
+    #[tokio::test]
+    async fn proposals_are_listed_and_withdrawn_by_their_author() {
+        let (_d, b) = backend();
+        let claude = view("s1", Some("claude-ai"));
+        let _ = b.approvals.access(&claude, 0);
+        // Предложение записано в файл (сверка с кластером проверяется интеграционным тестом).
+        let Proposed::Created(id) = b
+            .state
+            .health
+            .store
+            .propose(
+                "open",
+                NewProposal {
+                    severity: Severity::Critical,
+                    title: "Raise min.insync.replicas".into(),
+                    rationale: "r".into(),
+                    change: to_change(&alter_spec("orders")).unwrap(),
+                    related_rule_id: Some("topic.minIsrOne".into()),
+                    related_finding_key: None,
+                    author: b.author(&claude),
+                },
+                now_ms(),
+            )
+            .unwrap()
+        else {
+            panic!()
+        };
+        let list = |status| {
+            b.proposals(mp::ProposalOp::List {
+                connection_id: "open".into(),
+                status,
+            })
+        };
+        let mp::ProposalOutput::Proposals(items) = list(mh::StatusFilter::Open).await.unwrap()
+        else {
+            panic!()
+        };
+        assert_eq!(items.len(), 1);
+        let p = &items[0];
+        assert_eq!(
+            (p.id.as_str(), p.status.as_str(), p.client.as_deref()),
+            (id.as_str(), "open", Some("claude-ai"))
+        );
+        assert_eq!(p.severity, mh::Severity::Critical);
+        assert_eq!(p.change, alter_spec("orders"));
+        assert_eq!(p.target.name.as_deref(), Some("orders"));
+        assert_eq!(p.related_rule_id.as_deref(), Some("topic.minIsrOne"));
+        let mp::ProposalOutput::Proposals(closed) = list(mh::StatusFilter::Closed).await.unwrap()
+        else {
+            panic!()
+        };
+        assert!(closed.is_empty());
+
+        let other = view("s2", Some("cursor"));
+        let _ = b.approvals.access(&other, 0);
+        let withdraw = |s: &SessionView, pid: &str| {
+            b.proposals(mp::ProposalOp::Withdraw {
+                connection_id: "open".into(),
+                session: s.clone(),
+                proposal_id: pid.into(),
+            })
+        };
+        assert_eq!(withdraw(&other, &id).await.unwrap_err().code, "not_allowed");
+        assert_eq!(
+            withdraw(&claude, "nope").await.unwrap_err().code,
+            "not_found"
+        );
+        assert_eq!(
+            withdraw(&claude, &id).await.unwrap(),
+            mp::ProposalOutput::Withdrawn
+        );
+        let e = withdraw(&claude, &id).await.unwrap_err();
+        assert_eq!(
+            (e.code.as_str(), e.message.as_str()),
+            ("invalid_argument", "The proposal is already withdrawn")
+        );
+        let mp::ProposalOutput::Proposals(closed) = list(mh::StatusFilter::Closed).await.unwrap()
+        else {
+            panic!()
+        };
+        assert_eq!(closed[0].status, "withdrawn");
+        assert!(closed[0].closed_at_ms.is_some());
+    }
+
     #[tokio::test]
     async fn check_reuses_the_last_run_and_marks_ignored() {
         let (_d, b) = backend();
````

- [ ] **Step 2: Увидеть падение.**

Run: `cd src-tauri && cargo test --lib -- mcp::backend` — Expected: ошибка компиляции (`AppBackend` не реализует `proposals`, нет `to_change`).

- [ ] **Step 3: Реализация.**

<!-- block: patch task5-src.diff -->
````diff
diff --git a/src-tauri/src/mcp/backend.rs b/src-tauri/src/mcp/backend.rs
index 5c11c48..bb23864 100644
--- a/src-tauri/src/mcp/backend.rs
+++ b/src-tauri/src/mcp/backend.rs
@@ -1,11 +1,13 @@
 //! Реализация `glafira_mcp::Backend` (спец. G1 §2): существующие функции чтения и проверка
 //! `llmAccess` подключения при каждой операции. Функций, меняющих кластер, здесь нет и быть не
-//! должно; замечания LLM пишутся только в файл Glafira.
+//! должно; замечания и предложения LLM пишутся только в файл Glafira — предложение применяет
+//! человек формой (спец. G3 §6).
 
 use std::sync::Arc;
 
 use glafira_mcp::health as mh;
 use glafira_mcp::model as m;
+use glafira_mcp::proposals as mp;
 use glafira_mcp::{
     Backend, BackendError, BoxFuture, ReadOp, ReadOutput, SessionAccess, SessionView,
 };
@@ -15,10 +17,14 @@ use super::approvals::Approvals;
 use crate::commands::{blocking, pooled_configs};
 use crate::error::{AppError, AppResult, ErrorCode};
 use crate::health::model::{Finding, Severity, Target, TargetKind};
+use crate::health::proposals::{
+    Change, Facts, MAX_OPEN_PROPOSALS, MAX_RELATED_KEY_CHARS, Proposal, ProposalError,
+    ProposalStatus, TopicFact, validate,
+};
 use crate::health::service::now_ms;
 use crate::health::store::{
-    Annotated, AnnotationStatus, Author, CheckRun, MAX_TITLE_CHARS, NewAnnotation, Removed,
-    Trigger, sanitize,
+    Annotated, AnnotationStatus, Author, CheckRun, MAX_BODY_CHARS, MAX_TITLE_CHARS, NewAnnotation,
+    NewProposal, Proposed, Removed, Trigger, Withdrawn, sanitize,
 };
 use crate::health::{rules, texts};
 use crate::kafka::pool::KafkaHandles;
@@ -500,6 +506,213 @@ impl AppBackend {
     }
 }
 
+impl AppBackend {
+    /// Факты для проверки предложения (спец. G3 §5): метаданные всегда, ключи конфигов топика —
+    /// для `alterTopicConfigs` по существующему топику, список групп — для `resetOffsets`.
+    async fn facts(&self, connection_id: &str, change: &Change) -> Result<Facts, BackendError> {
+        let md = self
+            .on(connection_id.into(), cluster::cluster_metadata)
+            .await?;
+        let mut facts = Facts {
+            brokers: md.brokers.len(),
+            topics: md
+                .topics
+                .iter()
+                .map(|t| TopicFact {
+                    name: t.name.clone(),
+                    partitions: t.partitions.len(),
+                    internal: t.internal(),
+                })
+                .collect(),
+            config_keys: None,
+            groups: None,
+        };
+        match change {
+            Change::AlterTopicConfigs { topic, .. }
+                if facts.topics.iter().any(|t| &t.name == topic && !t.internal) =>
+            {
+                let entries = pooled_configs(
+                    &self.state,
+                    connection_id.into(),
+                    ResourceSpecifier::Topic(topic),
+                )
+                .await
+                .map_err(to_backend)?;
+                facts.config_keys = Some(entries.into_iter().map(|e| e.name).collect());
+            }
+            Change::ResetOffsets { .. } => {
+                let l = self
+                    .on(connection_id.into(), group_ops::list_groups)
+                    .await?;
+                facts.groups = Some(l.groups.into_iter().map(|g| g.group_id).collect());
+            }
+            _ => {}
+        }
+        Ok(facts)
+    }
+
+    async fn proposals_impl(&self, op: mp::ProposalOp) -> Result<mp::ProposalOutput, BackendError> {
+        // Поколение — до проверки доступа: запись в подключение, удалённое за это время, отклоняется.
+        let generation = self.state.health.generation(op.connection_id());
+        self.allowed(op.connection_id())?;
+        let st = self.state.clone();
+        match op {
+            mp::ProposalOp::Propose {
+                connection_id,
+                session,
+                proposal,
+            } => {
+                if let Some(r) = &proposal.related_rule_id
+                    && !rules::rule_ids().any(|id| id == r)
+                {
+                    return Err(BackendError::invalid_argument(format!(
+                        "relatedRuleId {r} is not a health_check rule"
+                    )));
+                }
+                if sanitize(&proposal.title, MAX_TITLE_CHARS).is_empty() {
+                    return Err(BackendError::invalid_argument("title must not be empty"));
+                }
+                if sanitize(&proposal.rationale, MAX_BODY_CHARS).is_empty() {
+                    return Err(BackendError::invalid_argument(
+                        "rationale must not be empty",
+                    ));
+                }
+                let change = to_change(&proposal.change)?.trimmed();
+                let facts = self.facts(&connection_id, &change).await?;
+                validate(&change, &facts).map_err(|e| match e {
+                    ProposalError::Invalid(m) => BackendError::invalid_argument(m),
+                    ProposalError::NotFound(m) => BackendError::not_found(m),
+                })?;
+                let read_only = st
+                    .connections
+                    .get(&connection_id)
+                    .map(|p| p.read_only)
+                    .unwrap_or(false);
+                let new = NewProposal {
+                    severity: to_severity(proposal.severity),
+                    title: proposal.title,
+                    rationale: proposal.rationale,
+                    change,
+                    related_rule_id: proposal.related_rule_id,
+                    related_finding_key: proposal
+                        .related_finding_key
+                        .map(|k| sanitize(&k, MAX_RELATED_KEY_CHARS))
+                        .filter(|k| !k.is_empty()),
+                    author: self.author(&session),
+                };
+                let (st2, id) = (st.clone(), connection_id.clone());
+                let r = blocking(move || {
+                    st2.health
+                        .write(&id, generation, |s| s.propose(&id, new, now_ms()))
+                })
+                .await
+                .map_err(health_to_backend)?;
+                let (proposal_id, created) = match r {
+                    Proposed::Created(id) => (id, true),
+                    Proposed::Updated(id) => (id, false),
+                    Proposed::LimitReached => {
+                        return Err(BackendError::new(
+                            "limit_reached",
+                            format!(
+                                "This connection already has {MAX_OPEN_PROPOSALS} open proposals; ask the user to apply, reject or delete some in Glafira"
+                            ),
+                        ));
+                    }
+                };
+                st.health.changed(&connection_id);
+                Ok(mp::ProposalOutput::Proposed {
+                    proposal_id,
+                    created,
+                    read_only,
+                })
+            }
+            mp::ProposalOp::List {
+                connection_id,
+                status,
+            } => {
+                let f = blocking(move || st.health.store.load(&connection_id, now_ms()))
+                    .await
+                    .map_err(health_to_backend)?;
+                let items = f
+                    .proposals
+                    .iter()
+                    .filter(|p| match status {
+                        mh::StatusFilter::Open => p.status == ProposalStatus::Open,
+                        mh::StatusFilter::Closed => p.status != ProposalStatus::Open,
+                        mh::StatusFilter::All => true,
+                    })
+                    .map(proposal_view)
+                    .collect::<Result<Vec<_>, _>>()?;
+                Ok(mp::ProposalOutput::Proposals(items))
+            }
+            mp::ProposalOp::Withdraw {
+                connection_id,
+                session,
+                proposal_id,
+            } => {
+                let author = self.author(&session);
+                let (st2, id) = (st.clone(), connection_id.clone());
+                let r = blocking(move || {
+                    st2.health.write(&id, generation, |s| {
+                        s.withdraw_own(&id, &proposal_id, &author, now_ms())
+                    })
+                })
+                .await
+                .map_err(health_to_backend)?;
+                match r {
+                    Withdrawn::Withdrawn => {
+                        st.health.changed(&connection_id);
+                        Ok(mp::ProposalOutput::Withdrawn)
+                    }
+                    Withdrawn::NotFound => Err(BackendError::not_found(
+                        "The proposal was deleted or has expired",
+                    )),
+                    Withdrawn::NotOwner => Err(BackendError::new(
+                        "not_allowed",
+                        "Only proposals created by this client can be withdrawn",
+                    )),
+                    Withdrawn::Closed(status) => Err(BackendError::invalid_argument(format!(
+                        "The proposal is already {}",
+                        status.as_str()
+                    ))),
+                }
+            }
+        }
+    }
+}
+
+/// Вид изменения из крейта в вид приложения: одна и та же JSON-форма (спец. G3 §3).
+fn to_change(spec: &mp::ChangeSpec) -> Result<Change, BackendError> {
+    serde_json::to_value(spec)
+        .ok()
+        .and_then(|v| serde_json::from_value(v).ok())
+        .ok_or_else(|| BackendError::invalid_argument("change has an unexpected shape"))
+}
+
+fn proposal_view(p: &Proposal) -> Result<mp::ProposalView, BackendError> {
+    let change = serde_json::to_value(&p.change)
+        .ok()
+        .and_then(|v| serde_json::from_value(v).ok())
+        .ok_or_else(|| BackendError::new("internal", "Unexpected stored proposal shape"))?;
+    Ok(mp::ProposalView {
+        id: p.id.clone(),
+        severity: from_severity(p.severity),
+        title: p.title.clone(),
+        rationale: p.rationale.clone(),
+        target: from_target(&p.target),
+        change,
+        related_rule_id: p.related_rule_id.clone(),
+        related_finding_key: p.related_finding_key.clone(),
+        client: p.client.clone(),
+        status: p.status.as_str().into(),
+        created_at_ms: p.created_at_ms,
+        updated_at_ms: p.updated_at_ms,
+        closed_at_ms: p.closed_at_ms,
+        applied_as_proposed: p.applied_as_proposed,
+        reject_reason: p.reject_reason.clone(),
+    })
+}
+
 fn kind_name(k: mh::TargetKind) -> &'static str {
     match k {
         mh::TargetKind::Cluster => "cluster",
@@ -740,6 +953,13 @@ impl Backend for AppBackend {
     fn health(&self, op: mh::HealthOp) -> BoxFuture<'_, Result<mh::HealthOutput, BackendError>> {
         Box::pin(self.health_impl(op))
     }
+
+    fn proposals(
+        &self,
+        op: mp::ProposalOp,
+    ) -> BoxFuture<'_, Result<mp::ProposalOutput, BackendError>> {
+        Box::pin(self.proposals_impl(op))
+    }
 }
 
 #[cfg(test)]
````

- [ ] **Step 4: Тесты проходят.**

Run: `cd src-tauri && cargo test --lib -- mcp::backend` — Expected: `18 passed`; `cargo test --lib` — `470 passed`; clippy и fmt — чисто.

- [ ] **Step 5: Commit.**

````bash
git add src-tauri/src/mcp/backend.rs
git commit -m "feat(mcp): Backend::proposals в приложении — факты из кластера, проверка, запись в файл Glafira, список и отзыв

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 6: Команды предложений, `proposal_id` в изменяющих командах с отметкой «применено», интеграционный тест

**Files:**
- Modify: `src-tauri/src/commands/health.rs`, `src-tauri/src/commands/topic_ops.rs`, `src-tauri/src/commands/groups.rs`, `src-tauri/src/lib.rs` (регистрация), `src-tauri/tests/it_health.rs`
- Test: модули `tests` в `commands/health.rs`, `topic_ops.rs`, `groups.rs`; `tests/it_health.rs`

**Interfaces:**
- Consumes: `health::proposals::{Applied, Proposal, ProposalStatus}`, `health::store::MarkApplied`, `HealthService::{generation, write, changed}`, `commands::blocking`.
- Produces: `commands::health::{HealthView.proposals: Vec<Proposal>, mark_applied(st, connection_id, proposal_id: Option<String>, applied: Applied) (pub(crate), async, ничего не возвращает: пропуск и ошибка файла — в лог), set_proposal_status(connection_id, proposal_id, status: ProposalStatus, reason: Option<String>), delete_proposal, delete_all_proposals}`; `topic_ops::{create_topic, alter_topic_configs, add_partitions}` и их `_impl` — последний параметр `proposal_id: Option<String>`; `groups::reset_offsets(_impl)` — `proposal_id: Option<String>, request: Option<ResetRequest>`, отметка только если все партиции без ошибки; команды зарегистрированы в `lib.rs`; интеграционный тест `proposal_is_checked_against_the_cluster_and_marked_applied`.

Спец. G3 §6. Tauri преобразует `proposalId`/`request` из camelCase в параметры `proposal_id`/`request`. Отметка идёт после успеха операции в Kafka и никогда её не отменяет. Интеграционный тест проходит путь целиком против dev-стенда: предложение по топику с `min.insync.replicas=2` (RF 1) → `alter_topic_configs_impl` с `proposal_id` → `applied`, `applied_as_proposed: true`; создание топика по предложению с `-1`; отказы по внутреннему топику, неизвестному ключу, уменьшению партиций, несуществующему топику; `readOnly` у read-only подключения.

- [ ] **Step 1: Тесты.**

Патч тестов (`commands/health.rs`: статусы, удаление, `mark_applied` с правками/пропусками; обновлённые вызовы в `topic_ops.rs` и `groups.rs`; интеграционный тест в `tests/it_health.rs`):

<!-- block: patch task6-tests.diff -->
````diff
diff --git a/src-tauri/src/commands/groups.rs b/src-tauri/src/commands/groups.rs
index 1ed7569..011983a 100644
--- a/src-tauri/src/commands/groups.rs
+++ b/src-tauri/src/commands/groups.rs
@@ -186,13 +214,22 @@ mod tests {
     async fn group_mutations_are_guarded() {
         let (_d, st) = state();
         let g = || "g".to_string();
-        let ro_reset =
-            reset_offsets_impl(&st, "ro".into(), g(), target(), Some("conn-ro".into())).await;
+        let ro_reset = reset_offsets_impl(
+            &st,
+            "ro".into(),
+            g(),
+            target(),
+            Some("conn-ro".into()),
+            None,
+            None,
+        )
+        .await;
         assert_eq!(ro_reset.unwrap_err().code, ErrorCode::ReadOnly);
         let ro_delete =
             delete_group_impl(&st, "ro".into(), g(), Some("conn-ro".into()), Some(g())).await;
         assert_eq!(ro_delete.unwrap_err().code, ErrorCode::ReadOnly);
-        let prod_reset = reset_offsets_impl(&st, "prod".into(), g(), target(), None).await;
+        let prod_reset =
+            reset_offsets_impl(&st, "prod".into(), g(), target(), None, None, None).await;
         assert_eq!(
             prod_reset.unwrap_err().code,
             ErrorCode::ConfirmationRequired
@@ -207,7 +244,7 @@ mod tests {
             wrong_name.unwrap_err().code,
             ErrorCode::ConfirmationRequired
         );
-        let empty_plan = reset_offsets_impl(&st, "dev".into(), g(), vec![], None).await;
+        let empty_plan = reset_offsets_impl(&st, "dev".into(), g(), vec![], None, None, None).await;
         assert_eq!(empty_plan.unwrap_err().code, ErrorCode::InvalidInput);
     }
 }
diff --git a/src-tauri/src/commands/health.rs b/src-tauri/src/commands/health.rs
index 1560998..563849a 100644
--- a/src-tauri/src/commands/health.rs
+++ b/src-tauri/src/commands/health.rs
@@ -127,7 +208,8 @@ mod tests {
 
     use super::*;
     use crate::health::model::{Severity, Target};
-    use crate::health::store::{Annotated, Author, NewAnnotation};
+    use crate::health::proposals::Change;
+    use crate::health::store::{Annotated, Author, NewAnnotation, NewProposal, Proposed};
     use crate::secrets::MemoryStore;
     use crate::storage::connections::sample_profile;
 
@@ -206,6 +288,150 @@ mod tests {
         assert!(!file.exists(), "запись удалённого подключения создала файл");
     }
 
+    fn proposal(st: &AppState, topic: &str) -> String {
+        match st
+            .health
+            .store
+            .propose(
+                "c1",
+                NewProposal {
+                    severity: Severity::Warning,
+                    title: "More partitions".into(),
+                    rationale: "r".into(),
+                    change: Change::AddPartitions {
+                        topic: topic.into(),
+                        new_total: 6,
+                    },
+                    related_rule_id: None,
+                    related_finding_key: None,
+                    author: Author {
+                        client: Some("claude-ai".into()),
+                        session_id: "s".into(),
+                    },
+                },
+                now_ms(),
+            )
+            .unwrap()
+        {
+            Proposed::Created(id) => id,
+            other => panic!("{other:?}"),
+        }
+    }
+
+    #[tokio::test]
+    async fn proposal_commands_change_status_and_delete() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let n = Arc::new(AtomicUsize::new(0));
+        let n2 = n.clone();
+        st.health.set_notify(Arc::new(move |_| {
+            n2.fetch_add(1, Ordering::SeqCst);
+        }));
+        let id = proposal(&st, "orders");
+        let v = change(&st, "c1".into(), {
+            let id = id.clone();
+            move |s, c| {
+                s.set_proposal_status(
+                    c,
+                    &id,
+                    ProposalStatus::Rejected,
+                    Some("later".into()),
+                    now_ms(),
+                )
+            }
+        })
+        .await
+        .unwrap();
+        assert_eq!(v.proposals[0].status, ProposalStatus::Rejected);
+        assert_eq!(v.proposals[0].reject_reason.as_deref(), Some("later"));
+        let v = change(&st, "c1".into(), {
+            let id = id.clone();
+            move |s, c| s.set_proposal_status(c, &id, ProposalStatus::Open, None, now_ms())
+        })
+        .await
+        .unwrap();
+        assert_eq!(v.proposals[0].status, ProposalStatus::Open);
+        let v = change(&st, "c1".into(), move |s, c| {
+            s.delete_proposal(c, &id, now_ms())
+        })
+        .await
+        .unwrap();
+        assert!(v.proposals.is_empty());
+        proposal(&st, "a");
+        proposal(&st, "b");
+        let v = change(&st, "c1".into(), |s, c| {
+            s.delete_all_proposals(c, now_ms()).map(|_| ())
+        })
+        .await
+        .unwrap();
+        assert!(v.proposals.is_empty());
+        assert_eq!(n.load(Ordering::SeqCst), 4);
+    }
+
+    #[tokio::test]
+    async fn mark_applied_closes_the_proposal_and_notifies() {
+        let dir = tempfile::tempdir().unwrap();
+        let st = state(dir.path());
+        let n = Arc::new(AtomicUsize::new(0));
+        let n2 = n.clone();
+        st.health.set_notify(Arc::new(move |_| {
+            n2.fetch_add(1, Ordering::SeqCst);
+        }));
+        let id = proposal(&st, "orders");
+        // Без id предложения — ничего не происходит.
+        mark_applied(
+            &st,
+            "c1",
+            None,
+            Applied::AddPartitions {
+                topic: "orders".into(),
+                new_total: 6,
+            },
+        )
+        .await;
+        assert_eq!(n.load(Ordering::SeqCst), 0);
+        // С правками (8 вместо 6).
+        mark_applied(
+            &st,
+            "c1",
+            Some(id.clone()),
+            Applied::AddPartitions {
+                topic: "orders".into(),
+                new_total: 8,
+            },
+        )
+        .await;
+        let p = &view(&st, "c1").unwrap().proposals[0];
+        assert_eq!(
+            (p.status, p.applied_as_proposed),
+            (ProposalStatus::Applied, Some(false))
+        );
+        assert_eq!(n.load(Ordering::SeqCst), 1);
+        // Неизвестное предложение и удалённое подключение — не ошибка (операция уже прошла).
+        mark_applied(
+            &st,
+            "c1",
+            Some("nope".into()),
+            Applied::AddPartitions {
+                topic: "orders".into(),
+                new_total: 8,
+            },
+        )
+        .await;
+        st.delete_connection_full("c1").unwrap();
+        mark_applied(
+            &st,
+            "c1",
+            Some(id),
+            Applied::AddPartitions {
+                topic: "orders".into(),
+                new_total: 8,
+            },
+        )
+        .await;
+        assert!(!dir.path().join("health").join("c1.json").exists());
+    }
+
     #[tokio::test]
     async fn unknown_connection_is_not_found() {
         let dir = tempfile::tempdir().unwrap();
diff --git a/src-tauri/src/commands/topic_ops.rs b/src-tauri/src/commands/topic_ops.rs
index 263913f..843c90a 100644
--- a/src-tauri/src/commands/topic_ops.rs
+++ b/src-tauri/src/commands/topic_ops.rs
@@ -253,7 +289,7 @@ mod tests {
     async fn all_mutations(st: &AppState, id: &str, confirm: Option<String>) -> Vec<ErrorCode> {
         let s = |s: &str| Some(s.to_string());
         vec![
-            create_topic_impl(st, id.into(), spec(), confirm.clone())
+            create_topic_impl(st, id.into(), spec(), confirm.clone(), None)
                 .await
                 .unwrap_err()
                 .code,
@@ -263,11 +299,12 @@ mod tests {
                 "t".into(),
                 vec![ConfigChange::Delete { name: "x".into() }],
                 confirm.clone(),
+                None,
             )
             .await
             .unwrap_err()
             .code,
-            add_partitions_impl(st, id.into(), "t".into(), 5, confirm.clone())
+            add_partitions_impl(st, id.into(), "t".into(), 5, confirm.clone(), None)
                 .await
                 .unwrap_err()
                 .code,
@@ -378,11 +415,11 @@ mod tests {
         let (_d, st) = state();
         let mut bad = spec();
         bad.name = "bad name".into();
-        let e = create_topic_impl(&st, "dev".into(), bad, None)
+        let e = create_topic_impl(&st, "dev".into(), bad, None, None)
             .await
             .unwrap_err();
         assert_eq!(e.code, ErrorCode::InvalidInput);
-        let e = alter_topic_configs_impl(&st, "dev".into(), "t".into(), vec![], None)
+        let e = alter_topic_configs_impl(&st, "dev".into(), "t".into(), vec![], None, None)
             .await
             .unwrap_err();
         assert_eq!(e.code, ErrorCode::InvalidInput);
diff --git a/src-tauri/tests/it_health.rs b/src-tauri/tests/it_health.rs
index 95cc983..650c113 100644
--- a/src-tauri/tests/it_health.rs
+++ b/src-tauri/tests/it_health.rs
@@ -6,11 +6,21 @@ mod common;
 use std::sync::Arc;
 use std::time::Duration;
 
+use glafira_mcp::health::Severity;
+use glafira_mcp::proposals::{
+    ChangeSpec, ConfigChangeSpec, NewProposal, ProposalOp, ProposalOutput,
+};
+use glafira_mcp::{Backend, SessionView};
+use kafka_explorer_lib::commands::topic_ops::{alter_topic_configs_impl, create_topic_impl};
+use kafka_explorer_lib::health::proposals::ProposalStatus;
 use kafka_explorer_lib::health::service::now_ms;
 use kafka_explorer_lib::health::store::Trigger;
+use kafka_explorer_lib::kafka::admin::{ConfigChange, NewTopicSpec};
+use kafka_explorer_lib::mcp::approvals::Approvals;
+use kafka_explorer_lib::mcp::backend::AppBackend;
 use kafka_explorer_lib::secrets::MemoryStore;
 use kafka_explorer_lib::state::AppState;
-use kafka_explorer_lib::storage::connections::Environment;
+use kafka_explorer_lib::storage::connections::{Environment, LlmAccess};
 use rdkafka::admin::{AdminOptions, NewTopic, TopicReplication};
 
 #[tokio::test(flavor = "multi_thread")]
@@ -85,3 +95,185 @@ async fn check_finds_problems_on_prod() {
     assert_eq!(saved.run_id, run.run_id);
     assert_eq!(saved.group_lag.get(&*group), Some(&3));
 }
+
+/// Предложение LLM (спец. G3 §5–§6): сверка с кластером, применение командой с `proposal_id`
+/// и отметка «применено».
+#[tokio::test(flavor = "multi_thread")]
+async fn proposal_is_checked_against_the_cluster_and_marked_applied() {
+    let Some(bs) = common::bootstrap() else {
+        return;
+    };
+    let strict = common::unique_topic("it-proposal-isr");
+    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
+    for r in common::admin(&bs)
+        .create_topics(
+            &[NewTopic::new(&strict, 1, TopicReplication::Fixed(1))
+                .set("min.insync.replicas", "2")],
+            &opts,
+        )
+        .await
+        .unwrap()
+    {
+        r.unwrap();
+    }
+    let strict = common::TempTopic::adopt(&bs, strict);
+    common::wait_for_topic_sync(&bs, &strict, 1);
+
+    let dir = tempfile::tempdir().unwrap();
+    let st = AppState::new(dir.path(), Arc::new(MemoryStore::default())).unwrap();
+    let mut p = common::plaintext_profile(&bs);
+    p.environment = Environment::Dev;
+    p.llm_access = LlmAccess::Metadata;
+    st.connections.upsert(p.clone()).unwrap();
+    let backend = AppBackend::new(
+        st.clone(),
+        Arc::new(Approvals::new(st.settings.clone())),
+        Arc::new(|_| {}),
+    );
+    let claude = SessionView {
+        id: "s1".into(),
+        client: Some(glafira_mcp::ClientIdent {
+            name: "claude-ai".into(),
+            version: "1".into(),
+        }),
+    };
+    let propose = |change: ChangeSpec| {
+        backend.proposals(ProposalOp::Propose {
+            connection_id: "it".into(),
+            session: claude.clone(),
+            proposal: NewProposal {
+                severity: Severity::Warning,
+                title: "Fix min.insync.replicas".into(),
+                rationale: "RF is 1, so min.insync.replicas = 2 blocks acks=all writes".into(),
+                change,
+                related_rule_id: Some("topic.minIsrUnreachable".into()),
+                related_finding_key: None,
+            },
+        })
+    };
+    let alter = |topic: &str, name: &str, value: &str| ChangeSpec::AlterTopicConfigs {
+        topic: topic.into(),
+        changes: vec![ConfigChangeSpec {
+            name: name.into(),
+            value: Some(value.into()),
+        }],
+    };
+
+    // Сверка с кластером: внутренний топик, неизвестный ключ, партиций не больше.
+    let e = propose(alter("__consumer_offsets", "min.insync.replicas", "1"))
+        .await
+        .unwrap_err();
+    assert_eq!(e.code, "invalid_argument");
+    assert!(e.message.contains("internal"), "{e:?}");
+    let e = propose(alter(&strict, "no.such.config", "1"))
+        .await
+        .unwrap_err();
+    assert!(e.message.contains("unknown topic config"), "{e:?}");
+    let e = propose(ChangeSpec::AddPartitions {
+        topic: strict.to_string(),
+        new_total: 1,
+    })
+    .await
+    .unwrap_err();
+    assert!(e.message.contains("has 1 partitions"), "{e:?}");
+    let e = propose(alter("no-such-topic", "min.insync.replicas", "1"))
+        .await
+        .unwrap_err();
+    assert_eq!(e.code, "not_found");
+
+    // Корректное предложение записано; подключение не read-only.
+    let ProposalOutput::Proposed {
+        proposal_id,
+        created,
+        read_only,
+    } = propose(alter(&strict, "min.insync.replicas", "1"))
+        .await
+        .unwrap()
+    else {
+        panic!()
+    };
+    assert!(created && !read_only);
+
+    // Человек применяет формой: команда с proposal_id меняет конфиг и помечает предложение.
+    alter_topic_configs_impl(
+        &st,
+        "it".into(),
+        strict.to_string(),
+        vec![ConfigChange::Set {
+            name: "min.insync.replicas".into(),
+            value: "1".into(),
+        }],
+        None,
+        Some(proposal_id.clone()),
+    )
+    .await
+    .unwrap();
+    let h = st.handles("it").unwrap();
+    let (value, _) = common::topic_config_eventually(&h, &strict, "min.insync.replicas", |v| {
+        v.0.as_deref() == Some("1")
+    })
+    .await;
+    assert_eq!(value.as_deref(), Some("1"));
+    let f = st.health.store.load("it", now_ms()).unwrap();
+    let saved = f.proposals.iter().find(|p| p.id == proposal_id).unwrap();
+    assert_eq!(saved.status, ProposalStatus::Applied);
+    assert_eq!(saved.applied_as_proposed, Some(true));
+
+    // Создание топика по предложению: значение брокера для фактора репликации (-1) — как в форме.
+    let name = common::unique_topic("it-proposal-new");
+    let ProposalOutput::Proposed { proposal_id, .. } = propose(ChangeSpec::CreateTopic {
+        name: name.clone(),
+        partitions: 2,
+        replication_factor: None,
+        configs: Some([("cleanup.policy".to_string(), "compact".to_string())].into()),
+    })
+    .await
+    .unwrap() else {
+        panic!()
+    };
+    let created_topic = common::TempTopic::adopt(&bs, name.clone());
+    create_topic_impl(
+        &st,
+        "it".into(),
+        NewTopicSpec {
+            name: name.clone(),
+            partitions: 2,
+            replication_factor: -1,
+            configs: [("cleanup.policy".to_string(), "compact".to_string())].into(),
+        },
+        None,
+        Some(proposal_id.clone()),
+    )
+    .await
+    .unwrap();
+    common::wait_for_topic_sync(&bs, &created_topic, 2);
+    let f = st.health.store.load("it", now_ms()).unwrap();
+    let saved = f.proposals.iter().find(|p| p.id == proposal_id).unwrap();
+    assert_eq!(
+        (saved.status, saved.applied_as_proposed),
+        (ProposalStatus::Applied, Some(true))
+    );
+    // Теперь топик есть: такое же предложение отвергается.
+    let e = propose(ChangeSpec::CreateTopic {
+        name: name.clone(),
+        partitions: 2,
+        replication_factor: None,
+        configs: None,
+    })
+    .await
+    .unwrap_err();
+    assert!(e.message.contains("already exists"), "{e:?}");
+
+    // Read-only подключение: предложение принимается, модели сообщается readOnly.
+    p.read_only = true;
+    st.connections.upsert(p).unwrap();
+    let ProposalOutput::Proposed { read_only, .. } = propose(ChangeSpec::AddPartitions {
+        topic: strict.to_string(),
+        new_total: 2,
+    })
+    .await
+    .unwrap() else {
+        panic!()
+    };
+    assert!(read_only);
+}
````

- [ ] **Step 2: Увидеть падение.**

Run: `cd src-tauri && cargo test --lib -- commands::` — Expected: ошибка компиляции (у `*_impl` меньше аргументов, нет `mark_applied`, `ProposalStatus`).

- [ ] **Step 3: Реализация.**

<!-- block: patch task6-src.diff -->
````diff
diff --git a/src-tauri/src/commands/groups.rs b/src-tauri/src/commands/groups.rs
index 1ed7569..011983a 100644
--- a/src-tauri/src/commands/groups.rs
+++ b/src-tauri/src/commands/groups.rs
@@ -1,8 +1,10 @@
 use tauri::State;
 
+use super::health::mark_applied;
 use super::{blocking, with_pooled};
 use crate::error::AppResult;
 use crate::guard::ensure_name_confirmed;
+use crate::health::proposals::Applied;
 use crate::kafka::group_ops;
 use crate::kafka::groups::{
     GroupDescription, GroupLag, GroupList, GroupPartitionOffset, OffsetTarget, ResetPreviewRow,
@@ -90,6 +92,9 @@ pub async fn preview_offset_reset(
     .await
 }
 
+/// `proposal_id` и `request` — форма открыта из предложения LLM: `request` (стратегия и область,
+/// показанные в предпросмотре) сравнивается с предложением; отметка «применено» ставится, только
+/// если ни одна партиция не вернула ошибку (спец. G3 §6).
 #[tauri::command]
 pub async fn reset_offsets(
     state: State<'_, AppState>,
@@ -97,8 +102,19 @@ pub async fn reset_offsets(
     group_id: String,
     offsets: Vec<OffsetTarget>,
     confirm: Option<String>,
+    proposal_id: Option<String>,
+    request: Option<ResetRequest>,
 ) -> AppResult<Vec<ResetResult>> {
-    reset_offsets_impl(state.inner(), connection_id, group_id, offsets, confirm).await
+    reset_offsets_impl(
+        state.inner(),
+        connection_id,
+        group_id,
+        offsets,
+        confirm,
+        proposal_id,
+        request,
+    )
+    .await
 }
 
 pub async fn reset_offsets_impl(
@@ -107,13 +123,25 @@ pub async fn reset_offsets_impl(
     group_id: String,
     offsets: Vec<OffsetTarget>,
     confirm: Option<String>,
+    proposal_id: Option<String>,
+    request: Option<ResetRequest>,
 ) -> AppResult<Vec<ResetResult>> {
     st.guarded(&connection_id, confirm.as_deref())?;
     validate_targets(&offsets)?;
+    let applied = Applied::ResetOffsets {
+        group_id: group_id.clone(),
+        request,
+    };
-    on_handles(st, connection_id, move |h| {
+    let results = on_handles(st, connection_id.clone(), move |h| {
         group_ops::reset_offsets(h, &group_id, &offsets)
     })
-    .await
+    .await?;
+    if results.iter().all(|r| r.error.is_none()) {
+        mark_applied(st, &connection_id, proposal_id, applied).await;
+    }
+    Ok(results)
 }
 
 #[tauri::command]
diff --git a/src-tauri/src/commands/health.rs b/src-tauri/src/commands/health.rs
index 1560998..563849a 100644
--- a/src-tauri/src/commands/health.rs
+++ b/src-tauri/src/commands/health.rs
@@ -1,4 +1,4 @@
-//! Команды проверки кластера и вкладки «Замечания» (спец. G2 §7). Каждое изменение шлёт
+//! Команды проверки кластера и вкладки «Замечания» (спец. G2 §7, G3 §6). Каждое изменение шлёт
 //! событие `health://changed`: значки и блоки других вкладок перечитывают данные.
 
 use serde::Serialize;
@@ -6,17 +6,22 @@ use tauri::State;
 
 use super::blocking;
 use crate::error::AppResult;
+use crate::health::proposals::{Applied, Proposal, ProposalStatus};
 use crate::health::service::now_ms;
-use crate::health::store::{Annotation, AnnotationStatus, CheckRun, HealthStore, Trigger};
+use crate::health::store::{
+    Annotation, AnnotationStatus, CheckRun, HealthStore, MarkApplied, Trigger,
+};
 use crate::state::AppState;
 
-/// Состояние подключения для интерфейса: последняя проверка, скрытые находки, замечания LLM.
+/// Состояние подключения для интерфейса: последняя проверка, скрытые находки, замечания и
+/// предложения LLM.
 #[derive(Debug, Clone, Serialize)]
 #[serde(rename_all = "camelCase")]
 pub struct HealthView {
     pub last_check: Option<CheckRun>,
     pub ignored: Vec<String>,
     pub annotations: Vec<Annotation>,
+    pub proposals: Vec<Proposal>,
 }
 
 fn view(st: &AppState, connection_id: &str) -> AppResult<HealthView> {
@@ -26,9 +31,47 @@ fn view(st: &AppState, connection_id: &str) -> AppResult<HealthView> {
         last_check: f.last_check,
         ignored: f.ignored.into_iter().map(|i| i.key).collect(),
         annotations: f.annotations,
+        proposals: f.proposals,
     })
 }
 
+/// Отметка «применено» после успеха операции в Kafka (спец. G3 §6). Операция уже прошла, поэтому
+/// ничто здесь её не отменяет: предложения нет, оно закрыто или про другой объект — отметка
+/// пропускается; ошибка файла — только в лог. Без `proposal_id` ничего не делает.
+pub(crate) async fn mark_applied(
+    st: &AppState,
+    connection_id: &str,
+    proposal_id: Option<String>,
+    applied: Applied,
+) {
+    let Some(pid) = proposal_id else {
+        return;
+    };
+    let (st, id, pid2) = (st.clone(), connection_id.to_string(), pid.clone());
+    let r = blocking(move || {
+        let generation = st.health.generation(&id);
+        st.profile(&id)?;
+        let r = st.health.write(&id, generation, |s| {
+            s.mark_applied(&id, &pid2, &applied, now_ms())
+        })?;
+        st.health.changed(&id);
+        Ok(r)
+    })
+    .await;
+    match r {
+        Ok(MarkApplied::Applied { .. }) => {}
+        Ok(MarkApplied::Skipped(why)) => {
+            tracing::warn!(
+                "proposal {pid} of connection {connection_id} not marked applied: {why}"
+            );
+        }
+        Err(e) => tracing::warn!(
+            "proposal {pid} of connection {connection_id} not marked applied: {}",
+            e.message
+        ),
+    }
+}
+
 /// Изменение файла подключения на blocking-потоке, затем событие и свежее состояние. Запись идёт
 /// через `HealthService::write`: поколение читается до проверки подключения, и подключение,
 /// удалённое в промежутке, файла не получит.
@@ -120,6 +163,44 @@ pub async fn delete_all_annotations(
     .await
 }
 
+/// «Отклонить» (`rejected`, причина необязательна) или «Открыть снова» (`open`).
+#[tauri::command]
+pub async fn set_proposal_status(
+    state: State<'_, AppState>,
+    connection_id: String,
+    proposal_id: String,
+    status: ProposalStatus,
+    reason: Option<String>,
+) -> AppResult<HealthView> {
+    change(&state, connection_id, move |s, id| {
+        s.set_proposal_status(id, &proposal_id, status, reason, now_ms())
+    })
+    .await
+}
+
+#[tauri::command]
+pub async fn delete_proposal(
+    state: State<'_, AppState>,
+    connection_id: String,
+    proposal_id: String,
+) -> AppResult<HealthView> {
+    change(&state, connection_id, move |s, id| {
+        s.delete_proposal(id, &proposal_id, now_ms())
+    })
+    .await
+}
+
+#[tauri::command]
+pub async fn delete_all_proposals(
+    state: State<'_, AppState>,
+    connection_id: String,
+) -> AppResult<HealthView> {
+    change(&state, connection_id, |s, id| {
+        s.delete_all_proposals(id, now_ms()).map(|_| ())
+    })
+    .await
+}
+
 #[cfg(test)]
 mod tests {
     use std::sync::Arc;
diff --git a/src-tauri/src/commands/topic_ops.rs b/src-tauri/src/commands/topic_ops.rs
index 263913f..843c90a 100644
--- a/src-tauri/src/commands/topic_ops.rs
+++ b/src-tauri/src/commands/topic_ops.rs
@@ -1,15 +1,18 @@
 use tauri::State;
 
+use super::health::mark_applied;
 use super::{blocking, with_pooled};
 use crate::error::{AppResult, ErrorCode};
 use crate::guard::ensure_name_confirmed;
+use crate::health::proposals::Applied;
 use crate::kafka::admin::{self, ConfigChange, NewTopicSpec, PurgedPartition, TopicDeleteResult};
 use crate::kafka::configs::{TopicPolicy, describe_topic_policies};
 use crate::kafka::metadata::is_internal;
 use crate::state::AppState;
 
 // Каждая мутирующая команда первым делом вызывает `AppState::guarded` (read-only / prod),
-// затем проверяет ввод и только потом обращается к сети.
+// затем проверяет ввод и только потом обращается к сети. `proposal_id` — форма открыта из
+// предложения LLM: после успеха оно помечается применённым (спец. G3 §6).
 
 #[tauri::command]
 pub async fn create_topic(
@@ -17,8 +20,9 @@ pub async fn create_topic(
     connection_id: String,
     spec: NewTopicSpec,
     confirm: Option<String>,
+    proposal_id: Option<String>,
 ) -> AppResult<()> {
-    create_topic_impl(state.inner(), connection_id, spec, confirm).await
+    create_topic_impl(state.inner(), connection_id, spec, confirm, proposal_id).await
 }
 
 pub async fn create_topic_impl(
@@ -26,13 +30,17 @@ pub async fn create_topic_impl(
     connection_id: String,
     spec: NewTopicSpec,
     confirm: Option<String>,
+    proposal_id: Option<String>,
 ) -> AppResult<()> {
     st.guarded(&connection_id, confirm.as_deref())?;
     admin::validate_new_topic(&spec)?;
-    with_pooled(st, connection_id, |h| async move {
+    let applied = Applied::CreateTopic(spec.clone());
+    with_pooled(st, connection_id.clone(), |h| async move {
         admin::create_topic(&h, &spec).await
     })
-    .await
+    .await?;
+    mark_applied(st, &connection_id, proposal_id, applied).await;
+    Ok(())
 }
 
 #[tauri::command]
@@ -42,8 +50,17 @@ pub async fn alter_topic_configs(
     topic: String,
     changes: Vec<ConfigChange>,
     confirm: Option<String>,
+    proposal_id: Option<String>,
 ) -> AppResult<()> {
-    alter_topic_configs_impl(state.inner(), connection_id, topic, changes, confirm).await
+    alter_topic_configs_impl(
+        state.inner(),
+        connection_id,
+        topic,
+        changes,
+        confirm,
+        proposal_id,
+    )
+    .await
 }
 
 pub async fn alter_topic_configs_impl(
@@ -52,16 +69,19 @@ pub async fn alter_topic_configs_impl(
     topic: String,
     changes: Vec<ConfigChange>,
     confirm: Option<String>,
+    proposal_id: Option<String>,
 ) -> AppResult<()> {
     st.guarded(&connection_id, confirm.as_deref())?;
     admin::validate_config_changes(&changes)?;
-    let st = st.clone();
-    blocking(move || {
-        st.with_handles(&connection_id, |h| {
-            admin::alter_topic_configs(h, &topic, &changes)
-        })
-    })
-    .await
+    let applied = Applied::AlterTopicConfigs {
+        topic: topic.clone(),
+        changes: changes.clone(),
+    };
+    let (st2, id) = (st.clone(), connection_id.clone());
+    blocking(move || st2.with_handles(&id, |h| admin::alter_topic_configs(h, &topic, &changes)))
+        .await?;
+    mark_applied(st, &connection_id, proposal_id, applied).await;
+    Ok(())
 }
 
 #[tauri::command]
@@ -71,8 +91,17 @@ pub async fn add_partitions(
     topic: String,
     new_total: i32,
     confirm: Option<String>,
+    proposal_id: Option<String>,
 ) -> AppResult<()> {
-    add_partitions_impl(state.inner(), connection_id, topic, new_total, confirm).await
+    add_partitions_impl(
+        state.inner(),
+        connection_id,
+        topic,
+        new_total,
+        confirm,
+        proposal_id,
+    )
+    .await
 }
 
 pub async fn add_partitions_impl(
@@ -81,14 +110,21 @@ pub async fn add_partitions_impl(
     topic: String,
     new_total: i32,
     confirm: Option<String>,
+    proposal_id: Option<String>,
 ) -> AppResult<()> {
     st.guarded(&connection_id, confirm.as_deref())?;
     let current = partition_ids(st, &connection_id, &topic).await?.len();
     admin::validate_partition_increase(current, new_total)?;
-    with_pooled(st, connection_id, |h| async move {
+    let applied = Applied::AddPartitions {
+        topic: topic.clone(),
+        new_total,
+    };
+    with_pooled(st, connection_id.clone(), |h| async move {
         admin::add_partitions(&h, &topic, new_total as usize).await
     })
-    .await
+    .await?;
+    mark_applied(st, &connection_id, proposal_id, applied).await;
+    Ok(())
 }
 
 #[tauri::command]
diff --git a/src-tauri/src/lib.rs b/src-tauri/src/lib.rs
index 90cd6c2..4856403 100644
--- a/src-tauri/src/lib.rs
+++ b/src-tauri/src/lib.rs
@@ -171,6 +171,9 @@ pub fn run() {
             commands::health::set_annotation_status,
             commands::health::delete_annotation,
             commands::health::delete_all_annotations,
+            commands::health::set_proposal_status,
+            commands::health::delete_proposal,
+            commands::health::delete_all_proposals,
             commands::produce::produce,
             commands::produce::produce_batch,
             commands::produce::list_history,
````

- [ ] **Step 4: Тесты проходят.**

Run: `cd src-tauri && cargo test --lib -- commands::` — Expected: `47 passed`; `cargo test --lib` — `475 passed`; `KE_IT_BOOTSTRAP=localhost:9092 cargo test --test it_health` — `2 passed` (нужен dev-стенд); clippy и fmt — чисто.

- [ ] **Step 5: Commit.**

````bash
git add src-tauri/src/commands/health.rs src-tauri/src/commands/topic_ops.rs src-tauri/src/commands/groups.rs src-tauri/src/lib.rs src-tauri/tests/it_health.rs
git commit -m "feat(health): команды предложений, proposal_id в изменяющих командах с отметкой «применено», интеграционный тест

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 7: Фронтенд: типы и API предложений, действия `useHealth`, предложения в значках, описание изменения, строки на 12 языках

**Files:**
- Create: `src/components/health/proposals.ts`, `src/components/health/proposals.test.ts`
- Modify: `src/api/types.ts`, `src/api/client.ts`, `src/lib/useHealth.ts`, `src/components/health/findings.ts`, `src/components/health/findings.test.ts`, фикстуры `CheckBar.test.tsx`, `FindingsPanel.test.tsx`, `HealthBlock.test.tsx` (поле `proposals`), `src/locales/*.json` (`health.proposals.*`, обновлённый `health.sourceLlm`)

**Interfaces:**
- Consumes: `formatAgo` из `CheckBar.tsx`, `ResetStrategy`/`ResetScope` из `types.ts`, ключи `groups.reset.*`, `common.config.defaultValue`.
- Produces: типы `ProposalStatus`, `ConfigValueChange`, `ProposalChange`, `Proposal`, `HealthView.proposals`; `api.{setProposalStatus(connectionId, proposalId, status: "rejected" | "open", reason: string | null), deleteProposal, deleteAllProposals}`; `api.{createTopic, alterTopicConfigs, addPartitions}` с необязательным последним `proposalId: string | null = null`; `api.resetOffsets(connectionId, groupId, offsets, confirm, proposal: { id, request } | null = null)` → `proposalId`/`request` в IPC; `useHealthActions().{setProposalStatus({ id, status, reason? }), removeProposal, removeAllProposals}`; `findings.ts`: `openProposals(view)`, `itemsFor(...).proposals`, `BadgeItem` с `kind: "proposal"`, подсказка значка с заголовками предложений; `proposals.ts`: `strategyText`, `scopeText`, `changeSummary(change, t, lang)`, `proposalStatusText(p, t, nowMs, lang)`.

Спец. G3 §7 (текст карточки) и §8 (API). Описание изменения строится из `change` словами формы сброса (`groups.reset.*`) и новыми ключами `health.proposals.change.*`; `common.config.defaultValue` — для `null`. Старые тесты форм и клиента в этой задаче не трогаются: новые параметры API необязательны. Фильтр «Источник» → «LLM» теперь означает «замечания и предложения» — ключ `health.sourceLlm` обновляется тем же spec-файлом.

- [ ] **Step 1: Тесты.**

Патч тестов `findings.test.ts` (предложения в значке и подсказке) и фикстур, новый `proposals.test.ts` (описания изменений, статусы):

<!-- block: patch task7-tests.diff -->
````diff
diff --git a/src/components/health/CheckBar.test.tsx b/src/components/health/CheckBar.test.tsx
index f061345..6dbdf5a 100644
--- a/src/components/health/CheckBar.test.tsx
+++ b/src/components/health/CheckBar.test.tsx
@@ -27,6 +27,7 @@ const checked = (findings: Finding[]): HealthView => ({
   },
   ignored: [],
   annotations: [],
+  proposals: [],
 });
 
 const problem: Finding = { key: "k1", ruleId: "topic.replicationFactorOne", severity: "critical", target: { kind: "topic", name: "orders" }, params: {} };
@@ -99,7 +100,7 @@ describe("CheckBar: ссылка «Все замечания»", () => {
   });
 
   it("пока проверок не было, ссылки нет", async () => {
-    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] });
+    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] });
     renderWithClient(<CheckBar connectionId="c1" allLink />);
     await screen.findByText("Кластер ещё не проверялся.");
     expect(screen.queryByRole("button", { name: "Все замечания" })).not.toBeInTheDocument();
diff --git a/src/components/health/FindingsPanel.test.tsx b/src/components/health/FindingsPanel.test.tsx
index 63a530f..a3f9aa4 100644
--- a/src/components/health/FindingsPanel.test.tsx
+++ b/src/components/health/FindingsPanel.test.tsx
@@ -64,6 +64,7 @@ function view(p: Partial<HealthView> = {}): HealthView {
     },
     ignored: [],
     annotations: [],
+    proposals: [],
     ...p,
   };
 }
@@ -72,7 +73,7 @@ beforeEach(() => vi.clearAllMocks());
 
 describe("FindingsPanel", () => {
   it("запускает проверку и группирует находки по правилу", async () => {
-    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [] });
+    vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] });
     vi.mocked(api.runHealthCheck).mockResolvedValue(view());
     const user = userEvent.setup();
     renderWithClient(<FindingsPanel connectionId="c1" />);
diff --git a/src/components/health/HealthBlock.test.tsx b/src/components/health/HealthBlock.test.tsx
index d77e3e0..2541c52 100644
--- a/src/components/health/HealthBlock.test.tsx
+++ b/src/components/health/HealthBlock.test.tsx
@@ -52,6 +52,7 @@ const view: HealthView = {
       status: "open",
     },
   ],
+  proposals: [],
 };
 
 const isTopic = (name: string) => (x: { kind: string; name?: string }) => x.kind === "topic" && x.name === name;
diff --git a/src/components/health/findings.test.ts b/src/components/health/findings.test.ts
index 76de8c4..df29ff4 100644
--- a/src/components/health/findings.test.ts
+++ b/src/components/health/findings.test.ts
@@ -1,5 +1,5 @@
 import { describe, expect, it } from "vitest";
-import type { Annotation, Finding, HealthView } from "../../api/types";
+import type { Annotation, Finding, HealthView, Proposal } from "../../api/types";
 import { i18n } from "../../i18n";
 import { BADGE_TITLES_MAX, badgeFor, badgeTooltip, findingsOf, findingTexts, itemsFor, sameTarget } from "./findings";
 
@@ -37,6 +37,7 @@ function view(findings: Finding[], annotations: Annotation[] = [], ignored: stri
     },
     ignored,
     annotations,
+    proposals: [],
   };
 }
 
@@ -104,6 +105,29 @@ describe("selection and badges", () => {
     expect(badgeFor(view([info]), () => true)).toBeNull();
     expect(sameTarget({ kind: "cluster" }, { kind: "cluster", name: "x" })).toBe(true);
   });
+
+  it("counts open proposals like notes and names them in the tooltip", () => {
+    const proposal = (name: string, status: Proposal["status"], severity: Proposal["severity"] = "warning"): Proposal => ({
+      id: name + status,
+      severity,
+      title: `  Fix\n${name}  `,
+      rationale: "r",
+      target: { kind: "topic", name },
+      change: { kind: "addPartitions", topic: name, newTotal: 6 },
+      client: "claude-ai",
+      createdAtMs: 1,
+      updatedAtMs: 1,
+      status,
+    });
+    const v = { ...view([a], [note("warning", "a")]), proposals: [proposal("a", "open"), proposal("a", "applied"), proposal("a", "open", "info"), proposal("b", "open", "critical")] };
+    const isA = (x: Finding["target"]) => sameTarget(x, { kind: "topic", name: "a" });
+    expect(itemsFor(v, isA).proposals).toHaveLength(2);
+    const b = badgeFor(v, isA)!;
+    expect(b).toMatchObject({ count: 3, severity: "critical" });
+    expect(b.items.map((i) => i.kind)).toEqual(["finding", "note", "proposal"]);
+    expect(badgeTooltip(b, t, "ru").split("\n")[3]).toBe("Fix a");
+    expect(badgeFor(v, () => true)).toMatchObject({ count: 4 });
+  });
 });
 
 describe("badge tooltip", () => {
@@ -119,7 +143,7 @@ describe("badge tooltip", () => {
     const b = badgeFor(v, isTopic);
     expect(b).toMatchObject({ count: 4, severity: "critical" });
     // Внутри строгости: сначала находки, затем замечания; порядок как в данных.
-    expect(b?.items.map((i) => (i.kind === "finding" ? i.finding.target.name : i.note.title))).toEqual(["c1", "Critical note", "w1", "Warning note"]);
+    expect(b?.items.map((i) => (i.kind === "finding" ? i.finding.target.name : i.kind === "note" ? i.note.title : i.proposal.title))).toEqual(["c1", "Critical note", "w1", "Warning note"]);
     expect(badgeTooltip(b!, t, "ru")).toBe(
       ["Требуют внимания: 4", "Фактор репликации 1 у c1", "Critical note", "Фактор репликации 1 у w1", "Warning note"].join("\n"),
     );
diff --git a/src/components/health/proposals.test.ts b/src/components/health/proposals.test.ts
new file mode 100644
index 0000000..7043116
--- /dev/null
+++ b/src/components/health/proposals.test.ts
@@ -0,0 +1,77 @@
+import { describe, expect, it } from "vitest";
+import type { Proposal, ProposalChange } from "../../api/types";
+import { i18n } from "../../i18n";
+import { changeSummary, proposalStatusText, scopeText, strategyText } from "./proposals";
+
+const t = i18n.getFixedT("ru");
+
+function proposal(p: Partial<Proposal> = {}): Proposal {
+  return {
+    id: "p1",
+    severity: "warning",
+    title: "Raise min.insync.replicas",
+    rationale: "r",
+    target: { kind: "topic", name: "orders" },
+    change: { kind: "addPartitions", topic: "orders", newTotal: 6 },
+    client: "claude-ai",
+    createdAtMs: 1,
+    updatedAtMs: 1,
+    status: "open",
+    ...p,
+  };
+}
+
+describe("changeSummary", () => {
+  it("lists config changes with the default marker", () => {
+    const c: ProposalChange = {
+      kind: "alterTopicConfigs",
+      topic: "orders",
+      changes: [
+        { name: "min.insync.replicas", value: "2" },
+        { name: "retention.ms", value: null },
+      ],
+    };
+    expect(changeSummary(c, t, "ru")).toBe("Конфиги топика: min.insync.replicas → 2; retention.ms → по умолчанию");
+  });
+
+  it("describes a new topic with the broker default and configs", () => {
+    const c: ProposalChange = { kind: "createTopic", name: "dlq", partitions: 6, replicationFactor: -1, configs: { "cleanup.policy": "compact" } };
+    expect(changeSummary(c, t, "ru")).toBe("Новый топик — партиций: 6, фактор репликации: по умолчанию брокера, конфиги: cleanup.policy=compact");
+    expect(changeSummary({ ...c, replicationFactor: 3, configs: {} }, t, "ru")).toBe("Новый топик — партиций: 6, фактор репликации: 3");
+  });
+
+  it("describes partitions and offset resets with the words of the reset form", () => {
+    expect(changeSummary({ kind: "addPartitions", topic: "orders", newTotal: 6 }, t, "ru")).toBe("Партиций: → 6");
+    expect(changeSummary({ kind: "resetOffsets", groupId: "billing", strategy: { type: "earliest" }, scope: { type: "all" } }, t, "ru")).toBe(
+      "Оффсеты группы: С начала (earliest), Вся группа",
+    );
+    expect(
+      changeSummary(
+        {
+          kind: "resetOffsets",
+          groupId: "billing",
+          strategy: { type: "offset", offset: 10 },
+          scope: { type: "partitions", partitions: [{ topic: "orders", partition: 0 }, { topic: "orders", partition: 1 }] },
+        },
+        t,
+        "ru",
+      ),
+    ).toBe("Оффсеты группы: Конкретный оффсет 10, Партиции: orders:0, orders:1");
+    expect(strategyText({ type: "shift", delta: -5 }, t, "ru")).toBe("Сдвиг на ±N -5");
+    expect(strategyText({ type: "shift", delta: 5 }, t, "ru")).toBe("Сдвиг на ±N +5");
+    expect(strategyText({ type: "timestamp", timestampMs: Date.UTC(2026, 9, 22, 7, 5, 9) }, t, "ru")).toMatch(/^По времени \d/);
+    expect(scopeText({ type: "topics", topics: ["a", "b"] }, t)).toBe("Топики: a, b");
+  });
+});
+
+describe("proposalStatusText", () => {
+  it("is empty for open and tells how a closed one ended", () => {
+    const now = 10 * 60_000;
+    expect(proposalStatusText(proposal(), t, now, "ru")).toBeNull();
+    expect(proposalStatusText(proposal({ status: "applied", closedAtMs: now - 5 * 60_000, appliedAsProposed: true }), t, now, "ru")).toBe("применено 5 минут назад");
+    expect(proposalStatusText(proposal({ status: "applied", closedAtMs: now - 5 * 60_000, appliedAsProposed: false }), t, now, "ru")).toBe("применено с правками 5 минут назад");
+    expect(proposalStatusText(proposal({ status: "rejected" }), t, now, "ru")).toBe("отклонено");
+    expect(proposalStatusText(proposal({ status: "rejected", rejectReason: "позже" }), t, now, "ru")).toBe("отклонено: позже");
+    expect(proposalStatusText(proposal({ status: "withdrawn" }), t, now, "ru")).toBe("отозвано");
+  });
+});
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/health/findings.test.ts src/components/health/proposals.test.ts` — Expected: FAIL — `./proposals` не найден, `itemsFor(...).proposals` undefined.

- [ ] **Step 3: Реализация.**

Новый `src/components/health/proposals.ts`:

<!-- block: file src/components/health/proposals.ts part=whole -->
````ts
import type { TFunction } from "i18next";
import type { Proposal, ProposalChange, ResetScope, ResetStrategy } from "../../api/types";
import { formatAgo } from "./CheckBar";

/** Стратегия сброса словами формы сброса: «С начала (earliest)», «По времени 22.10.2026, 10:00:00», «Сдвиг на ±N −5». */
export function strategyText(s: ResetStrategy, t: TFunction, lang: string): string {
  switch (s.type) {
    case "earliest":
      return t("groups.reset.strategyEarliest");
    case "latest":
      return t("groups.reset.strategyLatest");
    case "timestamp":
      return `${t("groups.reset.strategyTimestamp")} ${new Intl.DateTimeFormat(lang, { dateStyle: "short", timeStyle: "medium" }).format(new Date(s.timestampMs))}`;
    case "offset":
      return `${t("groups.reset.strategyOffset")} ${s.offset}`;
    case "shift":
      return `${t("groups.reset.strategyShift")} ${s.delta > 0 ? "+" : ""}${s.delta}`;
  }
}

/** Область сброса словами формы: «Вся группа», «Топики: a, b», «Партиции: t:0, t:1». */
export function scopeText(s: ResetScope, t: TFunction): string {
  switch (s.type) {
    case "all":
      return t("groups.reset.scopeAll");
    case "topics":
      return `${t("groups.reset.scopeTopics")}: ${s.topics.join(", ")}`;
    case "partitions":
      return `${t("groups.reset.scopePartitions")}: ${s.partitions.map((p) => `${p.topic}:${p.partition}`).join(", ")}`;
  }
}

/** Описание изменения для карточки предложения (спец. G3 §7): что именно меняется, на языке интерфейса. */
export function changeSummary(change: ProposalChange, t: TFunction, lang: string): string {
  switch (change.kind) {
    case "alterTopicConfigs":
      return `${t("health.proposals.change.alterTopicConfigs")} ${change.changes
        .map((c) => `${c.name} → ${c.value ?? t("common.config.defaultValue")}`)
        .join("; ")}`;
    case "createTopic": {
      const rf = change.replicationFactor > 0 ? String(change.replicationFactor) : t("health.proposals.change.brokerDefault");
      const head = t("health.proposals.change.createTopic", { partitions: change.partitions, rf });
      const configs = Object.entries(change.configs).map(([k, v]) => `${k}=${v}`);
      return configs.length === 0 ? head : `${head}, ${t("health.proposals.change.configs", { configs: configs.join(", ") })}`;
    }
    case "addPartitions":
      return t("health.proposals.change.addPartitions", { newTotal: change.newTotal });
    case "resetOffsets":
      return t("health.proposals.change.resetOffsets", { strategy: strategyText(change.strategy, t, lang), scope: scopeText(change.scope, t) });
  }
}

/** Статус закрытого предложения: «применено 5 минут назад», «применено с правками …», «отклонено: …», «отозвано». */
export function proposalStatusText(p: Proposal, t: TFunction, nowMs: number, lang: string): string | null {
  switch (p.status) {
    case "open":
      return null;
    case "applied": {
      const when = formatAgo(p.closedAtMs ?? p.updatedAtMs, nowMs, lang);
      return p.appliedAsProposed === false ? t("health.proposals.status.appliedEdited", { when }) : t("health.proposals.status.applied", { when });
    }
    case "rejected":
      return p.rejectReason ? t("health.proposals.status.rejectedWithReason", { reason: p.rejectReason }) : t("health.proposals.status.rejected");
    case "withdrawn":
      return t("health.proposals.status.withdrawn");
  }
}
````

Патч типов, клиента, хука и `findings.ts`:

<!-- block: patch task7-src.diff -->
````diff
diff --git a/src/api/client.ts b/src/api/client.ts
index 1f8418e..0a3f8be 100644
--- a/src/api/client.ts
+++ b/src/api/client.ts
@@ -97,6 +97,11 @@ export const api = {
   deleteAnnotation: (connectionId: string, annotationId: string) =>
     call<HealthView>("delete_annotation", { connectionId, annotationId }),
   deleteAllAnnotations: (connectionId: string) => call<HealthView>("delete_all_annotations", { connectionId }),
+  setProposalStatus: (connectionId: string, proposalId: string, status: "rejected" | "open", reason: string | null) =>
+    call<HealthView>("set_proposal_status", { connectionId, proposalId, status, reason }),
+  deleteProposal: (connectionId: string, proposalId: string) =>
+    call<HealthView>("delete_proposal", { connectionId, proposalId }),
+  deleteAllProposals: (connectionId: string) => call<HealthView>("delete_all_proposals", { connectionId }),
   listConnections: () => call<ConnectionProfile[]>("list_connections"),
   saveConnection: (profile: ConnectionProfile, saslPassword?: string, tlsPasswords?: TlsPasswordsInput) =>
     call<ConnectionProfile>("save_connection", {
@@ -132,12 +137,13 @@ export const api = {
   getTopicConfigs: (connectionId: string, topic: string) =>
     call<ConfigEntry[]>("get_topic_configs", { connectionId, topic }),
 
-  createTopic: (connectionId: string, spec: NewTopicSpec, confirm: string | null) =>
-    call<void>("create_topic", { connectionId, spec, confirm }),
-  alterTopicConfigs: (connectionId: string, topic: string, changes: ConfigChange[], confirm: string | null) =>
-    call<void>("alter_topic_configs", { connectionId, topic, changes, confirm }),
-  addPartitions: (connectionId: string, topic: string, newTotal: number, confirm: string | null) =>
-    call<void>("add_partitions", { connectionId, topic, newTotal, confirm }),
+  // `proposalId` — форма открыта из предложения LLM: Rust отметит его применённым после успеха.
+  createTopic: (connectionId: string, spec: NewTopicSpec, confirm: string | null, proposalId: string | null = null) =>
+    call<void>("create_topic", { connectionId, spec, confirm, proposalId }),
+  alterTopicConfigs: (connectionId: string, topic: string, changes: ConfigChange[], confirm: string | null, proposalId: string | null = null) =>
+    call<void>("alter_topic_configs", { connectionId, topic, changes, confirm, proposalId }),
+  addPartitions: (connectionId: string, topic: string, newTotal: number, confirm: string | null, proposalId: string | null = null) =>
+    call<void>("add_partitions", { connectionId, topic, newTotal, confirm, proposalId }),
   purgeTopic: (connectionId: string, topic: string, partitions: number[] | null, c: Confirmation) =>
     call<PurgedPartition[]>("purge_topic", { connectionId, topic, partitions, confirm: c.confirm, typedName: c.typedName }),
   deleteTopics: (connectionId: string, topics: string[], c: Confirmation) =>
@@ -160,8 +166,22 @@ export const api = {
   getTopicGroups: (connectionId: string, topic: string) => call<TopicGroups>("get_topic_groups", { connectionId, topic }),
   previewOffsetReset: (connectionId: string, request: ResetRequest) =>
     call<ResetPreviewRow[]>("preview_offset_reset", { connectionId, request }),
-  resetOffsets: (connectionId: string, groupId: string, offsets: OffsetTarget[], confirm: string | null) =>
-    call<ResetResult[]>("reset_offsets", { connectionId, groupId, offsets, confirm }),
+  // `proposal` — сброс открыт из предложения LLM: его id и запрос предпросмотра для сравнения.
+  resetOffsets: (
+    connectionId: string,
+    groupId: string,
+    offsets: OffsetTarget[],
+    confirm: string | null,
+    proposal: { id: string; request: ResetRequest } | null = null,
+  ) =>
+    call<ResetResult[]>("reset_offsets", {
+      connectionId,
+      groupId,
+      offsets,
+      confirm,
+      proposalId: proposal?.id ?? null,
+      request: proposal?.request ?? null,
+    }),
   deleteGroup: (connectionId: string, groupId: string, c: Confirmation) =>
     call<void>("delete_group", { connectionId, groupId, confirm: c.confirm, typedName: c.typedName }),
 
diff --git a/src/api/types.ts b/src/api/types.ts
index cb4efe8..0aee0d3 100644
--- a/src/api/types.ts
+++ b/src/api/types.ts
@@ -678,9 +678,46 @@ export interface Annotation {
   closedAtMs?: number;
 }
 
-/** Состояние подключения: последняя проверка, скрытые находки (ключи), замечания LLM. */
+export type ProposalStatus = "open" | "applied" | "rejected" | "withdrawn";
+
+/** Одно изменение конфига топика; `value: null` — вернуть значение по умолчанию. */
+export interface ConfigValueChange {
+  name: string;
+  value: string | null;
+}
+
+/** Что предлагает LLM (G3); та же форма, что у модели и в файле Glafira. */
+export type ProposalChange =
+  | { kind: "alterTopicConfigs"; topic: string; changes: ConfigValueChange[] }
+  | { kind: "createTopic"; name: string; partitions: number; replicationFactor: number; configs: Record<string, string> }
+  | { kind: "addPartitions"; topic: string; newTotal: number }
+  | { kind: "resetOffsets"; groupId: string; strategy: ResetStrategy; scope: ResetScope };
+
+/** Предложение LLM: изменение, которое человек применяет формой; статус «применено» ставит Rust. */
+export interface Proposal {
+  id: string;
+  severity: Severity;
+  title: string;
+  rationale: string;
+  target: HealthTarget;
+  change: ProposalChange;
+  relatedRuleId?: string;
+  relatedFindingKey?: string;
+  client: string | null;
+  createdAtMs: number;
+  updatedAtMs: number;
+  status: ProposalStatus;
+  closedAtMs?: number;
+  /** Только у applied: применено без правок (true) или с правками. */
+  appliedAsProposed?: boolean;
+  /** Только у rejected: комментарий человека. */
+  rejectReason?: string;
+}
+
+/** Состояние подключения: последняя проверка, скрытые находки (ключи), замечания и предложения LLM. */
 export interface HealthView {
   lastCheck: CheckRun | null;
   ignored: string[];
   annotations: Annotation[];
+  proposals: Proposal[];
 }
diff --git a/src/components/health/findings.ts b/src/components/health/findings.ts
index 2140dcd..048d9b9 100644
--- a/src/components/health/findings.ts
+++ b/src/components/health/findings.ts
@@ -1,5 +1,5 @@
 import type { TFunction } from "i18next";
-import type { Annotation, Finding, HealthTarget, HealthView, Severity } from "../../api/types";
+import type { Annotation, Finding, HealthTarget, HealthView, Proposal, Severity } from "../../api/types";
 
 export const SEVERITY_RANK: Record<Severity, number> = { info: 0, warning: 1, critical: 2 };
 export const SEVERITIES: Severity[] = ["critical", "warning", "info"];
@@ -61,35 +61,42 @@ export function openNotes(view: HealthView | undefined): Annotation[] {
   return (view?.annotations ?? []).filter((a) => a.status === "open");
 }
 
-/** Что относится к объекту: видимые находки и открытые замечания. */
+export function openProposals(view: HealthView | undefined): Proposal[] {
+  return (view?.proposals ?? []).filter((p) => p.status === "open");
+}
+
+/** Что относится к объекту: видимые находки, открытые замечания и открытые предложения. */
 export function itemsFor(view: HealthView | undefined, match: (t: HealthTarget) => boolean) {
   return {
     findings: findingsOf(view, false).filter((f) => match(f.target)),
     notes: openNotes(view).filter((a) => match(a.target)),
+    proposals: openProposals(view).filter((p) => match(p.target)),
   };
 }
 
-/** Что посчитал значок: находка или открытое замечание уровня «критично» или «предупреждение». */
+/** Что посчитал значок: находка, открытое замечание или открытое предложение уровня «критично» или «предупреждение». */
 export type BadgeItem =
   | { kind: "finding"; severity: Severity; finding: Finding }
-  | { kind: "note"; severity: Severity; note: Annotation };
+  | { kind: "note"; severity: Severity; note: Annotation }
+  | { kind: "proposal"; severity: Severity; proposal: Proposal };
 
 export interface Badge {
   count: number;
   severity: Severity;
-  /** Самые строгие первыми; внутри строгости — сначала находки, затем замечания, порядок данных сохраняется. */
+  /** Самые строгие первыми; внутри строгости — сначала находки, затем замечания, затем предложения; порядок данных сохраняется. */
   items: BadgeItem[];
 }
 
 /** Сколько заголовков показывает подсказка значка; остальное сворачивается в «…». */
 export const BADGE_TITLES_MAX = 10;
 
-/** Значок: число видимых находок и открытых замечаний уровня «критично» и «предупреждение». */
+/** Значок: число видимых находок, открытых замечаний и предложений уровня «критично» и «предупреждение». */
 export function badgeFor(view: HealthView | undefined, match: (t: HealthTarget) => boolean): Badge | null {
-  const { findings, notes } = itemsFor(view, match);
+  const { findings, notes, proposals } = itemsFor(view, match);
   const items: BadgeItem[] = [
     ...findings.map((finding): BadgeItem => ({ kind: "finding", severity: finding.severity, finding })),
     ...notes.map((note): BadgeItem => ({ kind: "note", severity: note.severity, note })),
+    ...proposals.map((proposal): BadgeItem => ({ kind: "proposal", severity: proposal.severity, proposal })),
   ]
     .filter((i) => i.severity !== "info")
     // Сортировка стабильна: порядок внутри одной строгости остаётся прежним.
@@ -98,15 +105,17 @@ export function badgeFor(view: HealthView | undefined, match: (t: HealthTarget)
   return { count: items.length, severity: items[0].severity, items };
 }
 
+const oneLine = (s: string) => s.replace(/\s+/g, " ").trim();
+
 /**
  * Подсказка значка: первая строка — «Требуют внимания: N», далее заголовки учтённого (не больше 10, затем «…»).
  * Строки через перевод строки: нативная подсказка `title` его показывает. Пробелы и переводы строк
- * в заголовке замечания LLM схлопываются: одна строка подсказки — один заголовок.
+ * в заголовке замечания или предложения LLM схлопываются: одна строка подсказки — один заголовок.
  */
 export function badgeTooltip(badge: Badge, t: TFunction, lang: string): string {
   const titles = badge.items
     .slice(0, BADGE_TITLES_MAX)
-    .map((i) => (i.kind === "finding" ? findingTexts(i.finding, t, lang).title : i.note.title.replace(/\s+/g, " ").trim()));
+    .map((i) => (i.kind === "finding" ? findingTexts(i.finding, t, lang).title : i.kind === "note" ? oneLine(i.note.title) : oneLine(i.proposal.title)));
   if (badge.items.length > BADGE_TITLES_MAX) titles.push("…");
   return [t("health.badgeTitle", { count: badge.count }), ...titles].join("\n");
 }
diff --git a/src/lib/useHealth.ts b/src/lib/useHealth.ts
index 1566a20..d838c65 100644
--- a/src/lib/useHealth.ts
+++ b/src/lib/useHealth.ts
@@ -17,7 +17,7 @@ export const healthCheckKey = (connectionId: string) => ["healthCheck", connecti
  */
 const HEALTH_STALE_MS = 60_000;
 
-/** Последняя проверка, скрытые находки и замечания LLM подключения. */
+/** Последняя проверка, скрытые находки, замечания и предложения LLM подключения. */
 export function useHealth(connectionId: string) {
   return useQuery({ queryKey: healthKey(connectionId), queryFn: () => api.getHealth(connectionId), staleTime: HEALTH_STALE_MS });
 }
@@ -56,5 +56,13 @@ export function useHealthActions(connectionId: string) {
     }),
     remove: useMutation({ mutationFn: (id: string) => api.deleteAnnotation(connectionId, id), onSuccess }),
     removeAll: useMutation({ mutationFn: () => api.deleteAllAnnotations(connectionId), onSuccess }),
+    // Предложения LLM: отклонить (с необязательной причиной) или открыть снова, удалить одно или все.
+    setProposalStatus: useMutation({
+      mutationFn: (p: { id: string; status: "rejected" | "open"; reason?: string }) =>
+        api.setProposalStatus(connectionId, p.id, p.status, p.reason ?? null),
+      onSuccess,
+    }),
+    removeProposal: useMutation({ mutationFn: (id: string) => api.deleteProposal(connectionId, id), onSuccess }),
+    removeAllProposals: useMutation({ mutationFn: () => api.deleteAllProposals(connectionId), onSuccess }),
   };
 }
````

Строки раздела `health` на 12 языках (новый объект `proposals` и обновлённый `sourceLlm`):

<!-- block: spec /tmp/g3-task7-health.json -->
````json
{
 "path": "health",
 "after": "mcp",
 "values": {
  "en": {
   "sourceLlm": "LLM notes and proposals",
   "proposals": {
    "title": "LLM proposals",
    "intro": "LLM assistants connected over MCP can propose changes here. Nothing is applied until you open a proposal in a form and confirm it.",
    "none": "No LLM proposals.",
    "by": "LLM proposal · {{client}}",
    "openForm": "Open in form",
    "readOnly": "The connection is read-only",
    "reject": "Reject",
    "reasonPlaceholder": "Reason (optional)",
    "rejectConfirm": "Reject proposal",
    "deleteAll": "Delete all LLM proposals",
    "deleteAllConfirm": "Delete all LLM proposals of this connection?",
    "applying": "Applying LLM proposal: {{title}}",
    "status": {
     "applied": "applied {{when}}",
     "appliedEdited": "applied with edits {{when}}",
     "rejected": "rejected",
     "rejectedWithReason": "rejected: {{reason}}",
     "withdrawn": "withdrawn"
    },
    "change": {
     "alterTopicConfigs": "Topic configs:",
     "createTopic": "New topic — partitions: {{partitions}}, replication factor: {{rf}}",
     "brokerDefault": "broker default",
     "configs": "configs: {{configs}}",
     "addPartitions": "Partitions: → {{newTotal}}",
     "resetOffsets": "Group offsets: {{strategy}}, {{scope}}"
    }
   }
  },
  "ru": {
   "sourceLlm": "Замечания и предложения LLM",
   "proposals": {
    "title": "Предложения LLM",
    "intro": "LLM-ассистенты, подключённые по MCP, могут предлагать здесь изменения. Ничего не применяется, пока вы не откроете предложение в форме и не подтвердите его.",
    "none": "Предложений LLM нет.",
    "by": "Предложение LLM · {{client}}",
    "openForm": "Открыть в форме",
    "readOnly": "Подключение только для чтения",
    "reject": "Отклонить",
    "reasonPlaceholder": "Причина (необязательно)",
    "rejectConfirm": "Отклонить предложение",
    "deleteAll": "Удалить все предложения LLM",
    "deleteAllConfirm": "Удалить все предложения LLM у этого подключения?",
    "applying": "Применяется предложение LLM: {{title}}",
    "status": {
     "applied": "применено {{when}}",
     "appliedEdited": "применено с правками {{when}}",
     "rejected": "отклонено",
     "rejectedWithReason": "отклонено: {{reason}}",
     "withdrawn": "отозвано"
    },
    "change": {
     "alterTopicConfigs": "Конфиги топика:",
     "createTopic": "Новый топик — партиций: {{partitions}}, фактор репликации: {{rf}}",
     "brokerDefault": "по умолчанию брокера",
     "configs": "конфиги: {{configs}}",
     "addPartitions": "Партиций: → {{newTotal}}",
     "resetOffsets": "Оффсеты группы: {{strategy}}, {{scope}}"
    }
   }
  },
  "de": {
   "sourceLlm": "LLM-Anmerkungen und -Vorschläge",
   "proposals": {
    "title": "LLM-Vorschläge",
    "intro": "Über MCP verbundene LLM-Assistenten können hier Änderungen vorschlagen. Nichts wird angewendet, bis Sie einen Vorschlag im Formular öffnen und bestätigen.",
    "none": "Keine LLM-Vorschläge.",
    "by": "LLM-Vorschlag · {{client}}",
    "openForm": "Im Formular öffnen",
    "readOnly": "Die Verbindung ist schreibgeschützt",
    "reject": "Ablehnen",
    "reasonPlaceholder": "Grund (optional)",
    "rejectConfirm": "Vorschlag ablehnen",
    "deleteAll": "Alle LLM-Vorschläge löschen",
    "deleteAllConfirm": "Alle LLM-Vorschläge dieser Verbindung löschen?",
    "applying": "LLM-Vorschlag wird angewendet: {{title}}",
    "status": {
     "applied": "angewendet {{when}}",
     "appliedEdited": "mit Änderungen angewendet {{when}}",
     "rejected": "abgelehnt",
     "rejectedWithReason": "abgelehnt: {{reason}}",
     "withdrawn": "zurückgezogen"
    },
    "change": {
     "alterTopicConfigs": "Topic-Konfiguration:",
     "createTopic": "Neues Topic — Partitionen: {{partitions}}, Replikationsfaktor: {{rf}}",
     "brokerDefault": "Broker-Standard",
     "configs": "Konfiguration: {{configs}}",
     "addPartitions": "Partitionen: → {{newTotal}}",
     "resetOffsets": "Offsets der Gruppe: {{strategy}}, {{scope}}"
    }
   }
  },
  "fr": {
   "sourceLlm": "Remarques et propositions LLM",
   "proposals": {
    "title": "Propositions LLM",
    "intro": "Les assistants LLM connectés via MCP peuvent proposer ici des modifications. Rien n’est appliqué tant que vous n’ouvrez pas une proposition dans un formulaire et ne la confirmez pas.",
    "none": "Aucune proposition LLM.",
    "by": "Proposition LLM · {{client}}",
    "openForm": "Ouvrir dans le formulaire",
    "readOnly": "La connexion est en lecture seule",
    "reject": "Rejeter",
    "reasonPlaceholder": "Motif (facultatif)",
    "rejectConfirm": "Rejeter la proposition",
    "deleteAll": "Supprimer toutes les propositions LLM",
    "deleteAllConfirm": "Supprimer toutes les propositions LLM de cette connexion ?",
    "applying": "Application de la proposition LLM : {{title}}",
    "status": {
     "applied": "appliquée {{when}}",
     "appliedEdited": "appliquée avec des modifications {{when}}",
     "rejected": "rejetée",
     "rejectedWithReason": "rejetée : {{reason}}",
     "withdrawn": "retirée"
    },
    "change": {
     "alterTopicConfigs": "Configuration du topic :",
     "createTopic": "Nouveau topic — partitions : {{partitions}}, facteur de réplication : {{rf}}",
     "brokerDefault": "valeur par défaut du broker",
     "configs": "configuration : {{configs}}",
     "addPartitions": "Partitions : → {{newTotal}}",
     "resetOffsets": "Offsets du groupe : {{strategy}}, {{scope}}"
    }
   }
  },
  "es": {
   "sourceLlm": "Notas y propuestas LLM",
   "proposals": {
    "title": "Propuestas LLM",
    "intro": "Los asistentes LLM conectados por MCP pueden proponer aquí cambios. Nada se aplica hasta que abras una propuesta en un formulario y la confirmes.",
    "none": "No hay propuestas LLM.",
    "by": "Propuesta LLM · {{client}}",
    "openForm": "Abrir en el formulario",
    "readOnly": "La conexión es de solo lectura",
    "reject": "Rechazar",
    "reasonPlaceholder": "Motivo (opcional)",
    "rejectConfirm": "Rechazar la propuesta",
    "deleteAll": "Eliminar todas las propuestas LLM",
    "deleteAllConfirm": "¿Eliminar todas las propuestas LLM de esta conexión?",
    "applying": "Aplicando la propuesta LLM: {{title}}",
    "status": {
     "applied": "aplicada {{when}}",
     "appliedEdited": "aplicada con cambios {{when}}",
     "rejected": "rechazada",
     "rejectedWithReason": "rechazada: {{reason}}",
     "withdrawn": "retirada"
    },
    "change": {
     "alterTopicConfigs": "Configuración del topic:",
     "createTopic": "Topic nuevo — particiones: {{partitions}}, factor de replicación: {{rf}}",
     "brokerDefault": "predeterminado del broker",
     "configs": "configuración: {{configs}}",
     "addPartitions": "Particiones: → {{newTotal}}",
     "resetOffsets": "Offsets del grupo: {{strategy}}, {{scope}}"
    }
   }
  },
  "it": {
   "sourceLlm": "Note e proposte LLM",
   "proposals": {
    "title": "Proposte LLM",
    "intro": "Gli assistenti LLM connessi via MCP possono proporre qui delle modifiche. Nulla viene applicato finché non apri una proposta in un modulo e la confermi.",
    "none": "Nessuna proposta LLM.",
    "by": "Proposta LLM · {{client}}",
    "openForm": "Apri nel modulo",
    "readOnly": "La connessione è in sola lettura",
    "reject": "Rifiuta",
    "reasonPlaceholder": "Motivo (facoltativo)",
    "rejectConfirm": "Rifiuta la proposta",
    "deleteAll": "Elimina tutte le proposte LLM",
    "deleteAllConfirm": "Eliminare tutte le proposte LLM di questa connessione?",
    "applying": "Applicazione della proposta LLM: {{title}}",
    "status": {
     "applied": "applicata {{when}}",
     "appliedEdited": "applicata con modifiche {{when}}",
     "rejected": "rifiutata",
     "rejectedWithReason": "rifiutata: {{reason}}",
     "withdrawn": "ritirata"
    },
    "change": {
     "alterTopicConfigs": "Configurazione del topic:",
     "createTopic": "Nuovo topic — partizioni: {{partitions}}, fattore di replica: {{rf}}",
     "brokerDefault": "predefinito del broker",
     "configs": "configurazione: {{configs}}",
     "addPartitions": "Partizioni: → {{newTotal}}",
     "resetOffsets": "Offset del gruppo: {{strategy}}, {{scope}}"
    }
   }
  },
  "pl": {
   "sourceLlm": "Notatki i propozycje LLM",
   "proposals": {
    "title": "Propozycje LLM",
    "intro": "Asystenci LLM połączeni przez MCP mogą proponować tu zmiany. Nic nie zostanie zastosowane, dopóki propozycja nie zostanie otwarta w formularzu i potwierdzona.",
    "none": "Brak propozycji LLM.",
    "by": "Propozycja LLM · {{client}}",
    "openForm": "Otwórz w formularzu",
    "readOnly": "Połączenie jest tylko do odczytu",
    "reject": "Odrzuć",
    "reasonPlaceholder": "Powód (opcjonalnie)",
    "rejectConfirm": "Odrzuć propozycję",
    "deleteAll": "Usuń wszystkie propozycje LLM",
    "deleteAllConfirm": "Usunąć wszystkie propozycje LLM tego połączenia?",
    "applying": "Stosowanie propozycji LLM: {{title}}",
    "status": {
     "applied": "zastosowano {{when}}",
     "appliedEdited": "zastosowano ze zmianami {{when}}",
     "rejected": "odrzucono",
     "rejectedWithReason": "odrzucono: {{reason}}",
     "withdrawn": "wycofano"
    },
    "change": {
     "alterTopicConfigs": "Konfiguracja topiku:",
     "createTopic": "Nowy topik — partycje: {{partitions}}, współczynnik replikacji: {{rf}}",
     "brokerDefault": "domyślny brokera",
     "configs": "konfiguracja: {{configs}}",
     "addPartitions": "Partycje: → {{newTotal}}",
     "resetOffsets": "Offsety grupy: {{strategy}}, {{scope}}"
    }
   }
  },
  "pt-BR": {
   "sourceLlm": "Notas e propostas de LLM",
   "proposals": {
    "title": "Propostas de LLM",
    "intro": "Assistentes LLM conectados por MCP podem propor alterações aqui. Nada é aplicado até você abrir a proposta em um formulário e confirmá-la.",
    "none": "Nenhuma proposta de LLM.",
    "by": "Proposta de LLM · {{client}}",
    "openForm": "Abrir no formulário",
    "readOnly": "A conexão é somente leitura",
    "reject": "Rejeitar",
    "reasonPlaceholder": "Motivo (opcional)",
    "rejectConfirm": "Rejeitar a proposta",
    "deleteAll": "Excluir todas as propostas de LLM",
    "deleteAllConfirm": "Excluir todas as propostas de LLM desta conexão?",
    "applying": "Aplicando a proposta de LLM: {{title}}",
    "status": {
     "applied": "aplicada {{when}}",
     "appliedEdited": "aplicada com alterações {{when}}",
     "rejected": "rejeitada",
     "rejectedWithReason": "rejeitada: {{reason}}",
     "withdrawn": "retirada"
    },
    "change": {
     "alterTopicConfigs": "Configurações do tópico:",
     "createTopic": "Novo tópico — partições: {{partitions}}, fator de replicação: {{rf}}",
     "brokerDefault": "padrão do broker",
     "configs": "configurações: {{configs}}",
     "addPartitions": "Partições: → {{newTotal}}",
     "resetOffsets": "Offsets do grupo: {{strategy}}, {{scope}}"
    }
   }
  },
  "zh-CN": {
   "sourceLlm": "LLM 批注和建议",
   "proposals": {
    "title": "LLM 建议",
    "intro": "通过 MCP 连接的 LLM 助手可以在这里提出更改建议。在你于表单中打开建议并确认之前，不会应用任何更改。",
    "none": "没有 LLM 建议。",
    "by": "LLM 建议 · {{client}}",
    "openForm": "在表单中打开",
    "readOnly": "连接为只读",
    "reject": "拒绝",
    "reasonPlaceholder": "原因（可选）",
    "rejectConfirm": "拒绝建议",
    "deleteAll": "删除所有 LLM 建议",
    "deleteAllConfirm": "删除此连接的所有 LLM 建议？",
    "applying": "正在应用 LLM 建议：{{title}}",
    "status": {
     "applied": "已应用，{{when}}",
     "appliedEdited": "已修改后应用，{{when}}",
     "rejected": "已拒绝",
     "rejectedWithReason": "已拒绝：{{reason}}",
     "withdrawn": "已撤回"
    },
    "change": {
     "alterTopicConfigs": "主题配置：",
     "createTopic": "新主题 — 分区：{{partitions}}，副本因子：{{rf}}",
     "brokerDefault": "Broker 默认值",
     "configs": "配置：{{configs}}",
     "addPartitions": "分区：→ {{newTotal}}",
     "resetOffsets": "组偏移量：{{strategy}}，{{scope}}"
    }
   }
  },
  "zh-TW": {
   "sourceLlm": "LLM 註記與建議",
   "proposals": {
    "title": "LLM 建議",
    "intro": "透過 MCP 連線的 LLM 助理可以在這裡提出變更建議。在你於表單中開啟建議並確認之前，不會套用任何變更。",
    "none": "沒有 LLM 建議。",
    "by": "LLM 建議 · {{client}}",
    "openForm": "在表單中開啟",
    "readOnly": "連線為唯讀",
    "reject": "拒絕",
    "reasonPlaceholder": "原因（選填）",
    "rejectConfirm": "拒絕建議",
    "deleteAll": "刪除所有 LLM 建議",
    "deleteAllConfirm": "要刪除此連線的所有 LLM 建議嗎？",
    "applying": "正在套用 LLM 建議：{{title}}",
    "status": {
     "applied": "已套用，{{when}}",
     "appliedEdited": "已修改後套用，{{when}}",
     "rejected": "已拒絕",
     "rejectedWithReason": "已拒絕：{{reason}}",
     "withdrawn": "已撤回"
    },
    "change": {
     "alterTopicConfigs": "主題設定：",
     "createTopic": "新主題 — 分區：{{partitions}}，複寫係數：{{rf}}",
     "brokerDefault": "Broker 預設值",
     "configs": "設定：{{configs}}",
     "addPartitions": "分區：→ {{newTotal}}",
     "resetOffsets": "群組偏移量：{{strategy}}，{{scope}}"
    }
   }
  },
  "ja": {
   "sourceLlm": "LLMの注記と提案",
   "proposals": {
    "title": "LLMの提案",
    "intro": "MCPで接続したLLMアシスタントはここに変更を提案できます。提案をフォームで開いて確認するまで、何も適用されません。",
    "none": "LLMの提案はありません。",
    "by": "LLMの提案 · {{client}}",
    "openForm": "フォームで開く",
    "readOnly": "接続は読み取り専用です",
    "reject": "却下",
    "reasonPlaceholder": "理由（任意）",
    "rejectConfirm": "提案を却下",
    "deleteAll": "LLMの提案をすべて削除",
    "deleteAllConfirm": "この接続のLLMの提案をすべて削除しますか？",
    "applying": "LLMの提案を適用中：{{title}}",
    "status": {
     "applied": "{{when}}に適用",
     "appliedEdited": "{{when}}に修正して適用",
     "rejected": "却下",
     "rejectedWithReason": "却下：{{reason}}",
     "withdrawn": "取り下げ"
    },
    "change": {
     "alterTopicConfigs": "トピック設定：",
     "createTopic": "新しいトピック — パーティション：{{partitions}}、レプリケーションファクター：{{rf}}",
     "brokerDefault": "ブローカーのデフォルト",
     "configs": "設定：{{configs}}",
     "addPartitions": "パーティション：→ {{newTotal}}",
     "resetOffsets": "グループのオフセット：{{strategy}}、{{scope}}"
    }
   }
  },
  "ko": {
   "sourceLlm": "LLM 메모 및 제안",
   "proposals": {
    "title": "LLM 제안",
    "intro": "MCP로 연결된 LLM 어시스턴트가 여기에 변경을 제안할 수 있습니다. 제안을 양식에서 열어 확인하기 전까지는 아무것도 적용되지 않습니다.",
    "none": "LLM 제안이 없습니다.",
    "by": "LLM 제안 · {{client}}",
    "openForm": "양식에서 열기",
    "readOnly": "연결이 읽기 전용입니다",
    "reject": "거부",
    "reasonPlaceholder": "사유(선택)",
    "rejectConfirm": "제안 거부",
    "deleteAll": "LLM 제안 모두 삭제",
    "deleteAllConfirm": "이 연결의 LLM 제안을 모두 삭제하시겠습니까?",
    "applying": "LLM 제안 적용 중: {{title}}",
    "status": {
     "applied": "적용됨, {{when}}",
     "appliedEdited": "수정하여 적용됨, {{when}}",
     "rejected": "거부됨",
     "rejectedWithReason": "거부됨: {{reason}}",
     "withdrawn": "철회됨"
    },
    "change": {
     "alterTopicConfigs": "토픽 설정:",
     "createTopic": "새 토픽 — 파티션: {{partitions}}, 복제 계수: {{rf}}",
     "brokerDefault": "브로커 기본값",
     "configs": "설정: {{configs}}",
     "addPartitions": "파티션: → {{newTotal}}",
     "resetOffsets": "그룹 오프셋: {{strategy}}, {{scope}}"
    }
   }
  }
 }
}
````

Run: `node /tmp/add-keys.mjs src/locales /tmp/g3-task7-health.json`

- [ ] **Step 4: Тесты проходят.**

Run: `pnpm vitest run src/components/health` — Expected: `Tests 32 passed` (5 файлов); `pnpm exec tsc --noEmit`, `pnpm i18n:check` — без замечаний.

- [ ] **Step 5: Commit.**

````bash
git add src/api/types.ts src/api/client.ts src/lib/useHealth.ts src/components/health/findings.ts src/components/health/findings.test.ts src/components/health/proposals.ts src/components/health/proposals.test.ts src/components/health/CheckBar.test.tsx src/components/health/FindingsPanel.test.tsx src/components/health/HealthBlock.test.tsx src/locales/*.json
git commit -m "feat(ui): типы и API предложений, действия useHealth, предложения в значках, описание изменения, строки на 12 языках

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 8: Карточка предложения, раздел «Предложения LLM» на вкладке, предложения в блоках объектов, поле `proposal` у вкладок

**Files:**
- Create: `src/components/health/ProposalItem.tsx`
- Modify: `src/state/workspace.ts` (`ProposalPrefill`, поле `proposal` у `topics`/`topic`/`group`), `src/components/health/proposals.ts` (`proposalTab`), `src/components/health/FindingsPanel.tsx`, `src/components/health/HealthBlock.tsx`, `src/styles.css`, `src/components/health/FindingsPanel.test.tsx`, `src/components/health/HealthBlock.test.tsx`, `src/locales/*.json` (`hints.health.proposals`)

**Interfaces:**
- Consumes: `SeverityMark`, `TargetLink` из `parts.tsx`, `useConnection`, `useWorkspace().open`, `changeSummary`/`proposalStatusText`, `Hint`.
- Produces: `workspace.ts`: `ProposalPrefill = { proposal: Proposal; nonce: number }`, `TabSpec` с `proposal?` у `topics`, `topic`, `group`; `proposals.ts`: `proposalTab(connectionId, proposal, nonce?) -> TabSpec` (конфиги и партиции → вкладка топика, создание → `topics`, сброс → группа); `ProposalItem { connectionId, proposal, busy?, showTarget?, onStatus(status, reason?), onDelete }` — карточка с «Открыть в форме» (неактивна у read-only с подсказкой `health.proposals.readOnly`), «Отклонить» с полем причины и подтверждением, «Открыть снова» у отклонённого, «Удалить»; раздел `aria-labelledby="health-proposals"` на вкладке между находками и замечаниями с подсказкой `hints.health.proposals`, «Закрытые (N)», «Удалить все предложения LLM» с вопросом на странице; `HealthBlock` показывает открытые предложения объекта; CSS `.proposal-change`, `.proposal-reject`.

Спец. G3 §7. Вкладки формы ещё не умеют принимать `proposal` (это задача 9), но поле в `TabSpec` и `proposalTab` появляются здесь, чтобы «Открыть в форме» уже открывал нужную вкладку с заполнением в состоянии. У `createTopic` объект показывается именем без ссылки: топика ещё нет.

- [ ] **Step 1: Тесты.**

Патч тестов `FindingsPanel.test.tsx` (карточка и её подпись, закрытые со статусами, «Открыть в форме» → вкладка `topic:c1:orders` с `proposal`, отклонение с причиной, read-only, фильтр «Правила», «Удалить все»; у старого теста фильтров пустое состояние теперь в двух разделах) и `HealthBlock.test.tsx` (открытые предложения объекта, значок считает их):

<!-- block: patch task8-tests.diff -->
````diff
diff --git a/src/components/health/FindingsPanel.test.tsx b/src/components/health/FindingsPanel.test.tsx
index a3f9aa4..1db4044 100644
--- a/src/components/health/FindingsPanel.test.tsx
+++ b/src/components/health/FindingsPanel.test.tsx
@@ -1,8 +1,10 @@
 import { screen, waitFor, within } from "@testing-library/react";
 import userEvent from "@testing-library/user-event";
 import { beforeEach, describe, expect, it, vi } from "vitest";
-import type { Annotation, Finding, HealthView } from "../../api/types";
+import type { Annotation, Finding, HealthView, Proposal } from "../../api/types";
+import { useWorkspace } from "../../state/workspace";
 import { renderWithClient } from "../../test/render";
+import { newProfile } from "../connections/profile";
 
 vi.mock("../../api/client", async (importOriginal) => {
   const orig = await importOriginal<typeof import("../../api/client")>();
@@ -16,6 +18,9 @@ vi.mock("../../api/client", async (importOriginal) => {
       setAnnotationStatus: vi.fn(),
       deleteAnnotation: vi.fn(),
       deleteAllAnnotations: vi.fn(),
+      setProposalStatus: vi.fn(),
+      deleteProposal: vi.fn(),
+      deleteAllProposals: vi.fn(),
     },
   };
 });
@@ -69,7 +74,27 @@ function view(p: Partial<HealthView> = {}): HealthView {
   };
 }
 
-beforeEach(() => vi.clearAllMocks());
+const proposal = (id: string, status: Proposal["status"] = "open"): Proposal => ({
+  id,
+  severity: "warning",
+  title: `Proposal ${id}`,
+  rationale: "acks=all\nneeds two replicas",
+  target: { kind: "topic", name: "orders" },
+  change: { kind: "alterTopicConfigs", topic: "orders", changes: [{ name: "min.insync.replicas", value: "2" }] },
+  relatedRuleId: "topic.minIsrOne",
+  client: "claude-code",
+  createdAtMs: 1,
+  updatedAtMs: 1,
+  status,
+  ...(status === "applied" ? { closedAtMs: Date.now() - 60_000, appliedAsProposed: false } : {}),
+  ...(status === "rejected" ? { closedAtMs: Date.now() - 60_000, rejectReason: "позже" } : {}),
+});
+
+beforeEach(() => {
+  vi.clearAllMocks();
+  vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "dev-1", environment: "dev" }]);
+  useWorkspace.setState({ tabs: [], activeId: null });
+});
 
 describe("FindingsPanel", () => {
   it("запускает проверку и группирует находки по правилу", async () => {
@@ -123,7 +148,8 @@ describe("FindingsPanel", () => {
     await user.type(screen.getByRole("textbox", { name: "Поиск по имени" }), "bill");
     expect(screen.getByText("Группа billing пуста, но у неё есть лаг")).toBeInTheDocument();
     expect(screen.queryByText("Фактор репликации 1 у orders")).not.toBeInTheDocument();
-    expect(screen.getByText("Под фильтры ничего не подходит.")).toBeInTheDocument();
+    // Пустое состояние под фильтры — у замечаний и у предложений.
+    expect(screen.getAllByText("Под фильтры ничего не подходит.")).toHaveLength(2);
   });
 
   it("закрывает и удаляет замечания LLM", async () => {
@@ -154,4 +180,58 @@ describe("FindingsPanel", () => {
     expect(screen.getByText("Замечаний LLM нет.")).toBeInTheDocument();
     expect(screen.queryByText(/Pro/)).not.toBeInTheDocument();
   });
+
+  it("показывает предложения LLM, открывает форму и отклоняет с причиной", async () => {
+    vi.mocked(api.getHealth).mockResolvedValue(view({ proposals: [proposal("p1"), proposal("p2", "applied"), proposal("p3", "rejected")] }));
+    vi.mocked(api.setProposalStatus).mockResolvedValue(view({ proposals: [proposal("p1", "rejected"), proposal("p2", "applied"), proposal("p3", "rejected")] }));
+    const user = userEvent.setup();
+    renderWithClient(<FindingsPanel connectionId="c1" />);
+    const card = (await screen.findByText("Proposal p1")).closest("li")!;
+    expect(within(card).getByText("Конфиги топика: min.insync.replicas → 2")).toBeInTheDocument();
+    expect(within(card).getByText(/acks=all/)).toHaveClass("note-body");
+    expect(within(card).getByText("Предложение LLM · claude-code · min.insync.replicas равен 1")).toBeInTheDocument();
+    expect(within(card).getByRole("button", { name: "orders" })).toBeInTheDocument();
+    // Закрытые — в свёрнутом разделе со статусами.
+    await user.click(screen.getByText("Закрытые (2)"));
+    expect(screen.getByText(/применено с правками/)).toBeInTheDocument();
+    expect(screen.getByText(/отклонено: позже/)).toBeInTheDocument();
+    expect(screen.getByRole("button", { name: "Открыть снова" })).toBeInTheDocument();
+    // «Открыть в форме» — вкладка топика с предложением.
+    await user.click(within(card).getByRole("button", { name: "Открыть в форме" }));
+    const tab = useWorkspace.getState().tabs.find((x) => x.id === "topic:c1:orders");
+    expect(tab && "proposal" in tab && tab.proposal?.proposal.id).toBe("p1");
+    expect(useWorkspace.getState().activeId).toBe("topic:c1:orders");
+    // «Отклонить» спрашивает причину.
+    await user.click(within(card).getByRole("button", { name: "Отклонить" }));
+    await user.type(within(card).getByRole("textbox", { name: "Причина (необязательно)" }), "позже");
+    await user.click(within(card).getByRole("button", { name: "Отклонить предложение" }));
+    expect(api.setProposalStatus).toHaveBeenCalledWith("c1", "p1", "rejected", "позже");
+    await waitFor(() => expect(screen.getByText("Закрытые (3)")).toBeInTheDocument());
+  });
+
+  it("у read-only подключения «Открыть в форме» неактивна", async () => {
+    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c1", name: "ro", readOnly: true }]);
+    vi.mocked(api.getHealth).mockResolvedValue(view({ proposals: [proposal("p1")] }));
+    renderWithClient(<FindingsPanel connectionId="c1" />);
+    const btn = await screen.findByRole("button", { name: "Открыть в форме" });
+    await waitFor(() => expect(btn).toBeDisabled());
+    expect(btn).toHaveAttribute("title", "Подключение только для чтения");
+  });
+
+  it("фильтр «Правила» прячет предложения; «Удалить все предложения LLM» спрашивает на странице", async () => {
+    vi.mocked(api.getHealth).mockResolvedValue(view({ proposals: [proposal("p1")] }));
+    vi.mocked(api.deleteAllProposals).mockResolvedValue(view());
+    const user = userEvent.setup();
+    renderWithClient(<FindingsPanel connectionId="c1" />);
+    await screen.findByText("Proposal p1");
+    await user.selectOptions(screen.getByRole("combobox", { name: "Источник" }), "rules");
+    expect(screen.queryByText("Proposal p1")).not.toBeInTheDocument();
+    await user.selectOptions(screen.getByRole("combobox", { name: "Источник" }), "llm");
+    expect(screen.getByText("Proposal p1")).toBeInTheDocument();
+    await user.click(screen.getByRole("button", { name: "Удалить все предложения LLM" }));
+    expect(screen.getByRole("alert")).toHaveTextContent("Удалить все предложения LLM у этого подключения?");
+    await user.click(within(screen.getByRole("alert")).getByRole("button", { name: "Удалить все предложения LLM" }));
+    expect(api.deleteAllProposals).toHaveBeenCalledWith("c1");
+    await waitFor(() => expect(screen.getByText("Предложений LLM нет.")).toBeInTheDocument());
+  });
 });
diff --git a/src/components/health/HealthBlock.test.tsx b/src/components/health/HealthBlock.test.tsx
index 2541c52..625c851 100644
--- a/src/components/health/HealthBlock.test.tsx
+++ b/src/components/health/HealthBlock.test.tsx
@@ -12,10 +12,13 @@ vi.mock("../../api/client", async (importOriginal) => {
   return {
     ...orig,
     api: {
+      listConnections: vi.fn().mockResolvedValue([]),
       getHealth: vi.fn(),
       setFindingIgnored: vi.fn(),
       setAnnotationStatus: vi.fn(),
       deleteAnnotation: vi.fn(),
+      setProposalStatus: vi.fn(),
+      deleteProposal: vi.fn(),
     },
   };
 });
@@ -89,6 +92,48 @@ describe("HealthBlock", () => {
   });
 });
 
+describe("HealthBlock: предложения LLM", () => {
+  const proposal = (id: string, name: string, status: "open" | "applied"): HealthView["proposals"][number] => ({
+    id,
+    severity: "critical",
+    title: `More partitions for ${name}`,
+    rationale: "r",
+    target: { kind: "topic", name },
+    change: { kind: "addPartitions", topic: name, newTotal: 6 },
+    client: "claude-code",
+    createdAtMs: 1,
+    updatedAtMs: 1,
+    status,
+  });
+
+  it("показывает открытые предложения объекта, закрытые — нет, и считает их в значке", async () => {
+    vi.mocked(api.getHealth).mockResolvedValue({
+      ...view,
+      proposals: [proposal("p1", "orders", "open"), proposal("p2", "orders", "applied"), proposal("p3", "audit", "open")],
+    });
+    vi.mocked(api.deleteProposal).mockResolvedValue({ ...view, proposals: [] });
+    const user = userEvent.setup();
+    renderWithClient(
+      <>
+        <HealthBadge connectionId="c1" match={isTopic("orders")} />
+        <HealthBlock connectionId="c1" match={isTopic("orders")} />
+      </>,
+    );
+    const block = await screen.findByRole("region", { name: "Замечания" });
+    expect(within(block).getByText("More partitions for orders")).toBeInTheDocument();
+    expect(within(block).getByText("Партиций: → 6")).toBeInTheDocument();
+    expect(within(block).getAllByText(/More partitions/)).toHaveLength(1);
+    expect(within(block).queryByText("More partitions for audit")).not.toBeInTheDocument();
+    // Значок: находка k1 (критично), замечание n1 (предупреждение), предложение p1 (критично).
+    expect(screen.getByTitle(/Требуют внимания: 3/)).toHaveAttribute(
+      "title",
+      "Требуют внимания: 3\nФактор репликации 1 у orders\nMore partitions for orders\nOrders is not replicated",
+    );
+    await user.click(within(block).getAllByRole("button", { name: "Удалить" })[1]);
+    expect(api.deleteProposal).toHaveBeenCalledWith("c1", "p1");
+  });
+});
+
 describe("HealthBadge", () => {
   it("считает критичные и предупреждения, а info — нет", async () => {
     renderWithClient(
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/health/FindingsPanel.test.tsx src/components/health/HealthBlock.test.tsx` — Expected: FAIL — нет текста «Proposal p1», «Конфиги топика: …».

- [ ] **Step 3: Реализация.**

Новый `src/components/health/ProposalItem.tsx`:

<!-- block: file src/components/health/ProposalItem.tsx part=whole -->
````tsx
import { useState } from "react";
import { useTranslation } from "react-i18next";
import type { Proposal } from "../../api/types";
import { currentLocale } from "../../i18n";
import { useConnection } from "../../lib/useConnection";
import { useWorkspace } from "../../state/workspace";
import { SeverityMark, TargetLink } from "./parts";
import { changeSummary, proposalStatusText, proposalTab } from "./proposals";

interface Props {
  connectionId: string;
  proposal: Proposal;
  busy?: boolean;
  showTarget?: boolean;
  onStatus: (status: "rejected" | "open", reason?: string) => void;
  onDelete: () => void;
}

/**
 * Предложение LLM (спец. G3 §7): заголовок, обоснование как есть, описание изменения, подпись клиента
 * и статус. «Открыть в форме» открывает вкладку объекта с заполненной формой; для read-only
 * подключения кнопка неактивна. «Отклонить» спрашивает необязательную причину.
 */
export function ProposalItem({ connectionId, proposal: p, busy, showTarget = true, onStatus, onDelete }: Props) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const connection = useConnection(connectionId);
  const [rejecting, setRejecting] = useState(false);
  const [reason, setReason] = useState("");
  const lang = currentLocale();
  const client = p.client ?? t("mcp.unknownClient");
  const readOnly = connection?.readOnly === true;
  const status = proposalStatusText(p, t, Date.now(), lang);
  const cancelReject = () => {
    setRejecting(false);
    setReason("");
  };
  return (
    <li className={p.status === "open" ? "finding note proposal" : "finding note proposal is-closed"}>
      <div className="finding-head">
        <SeverityMark severity={p.severity} />
        <strong>{p.title}</strong>
        {showTarget &&
          (p.change.kind === "createTopic" ? (
            // Топика ещё нет: имя без ссылки.
            <span className="health-target">
              <span className="muted">{t("health.kind.topic")}</span> {p.target.name}
            </span>
          ) : (
            <TargetLink connectionId={connectionId} target={p.target} />
          ))}
      </div>
      <p className="note-body">{p.rationale}</p>
      <p className="proposal-change">{changeSummary(p.change, t, lang)}</p>
      <p className="muted">
        {t("health.proposals.by", { client })}
        {p.relatedRuleId && <> · {t(`health.ruleNames.${p.relatedRuleId}`)}</>}
        {status && <> · {status}</>}
      </p>
      <div className="finding-actions">
        {p.status === "open" && (
          <>
            <button
              type="button"
              className="link"
              disabled={busy || readOnly}
              title={readOnly ? t("health.proposals.readOnly") : undefined}
              onClick={() => open(proposalTab(connectionId, p))}
            >
              {t("health.proposals.openForm")}
            </button>
            {rejecting ? (
              <span className="proposal-reject">
                <input
                  aria-label={t("health.proposals.reasonPlaceholder")}
                  placeholder={t("health.proposals.reasonPlaceholder")}
                  value={reason}
                  onChange={(e) => setReason(e.target.value)}
                />
                <button
                  type="button"
                  className="link danger"
                  disabled={busy}
                  onClick={() => {
                    onStatus("rejected", reason.trim() || undefined);
                    cancelReject();
                  }}
                >
                  {t("health.proposals.rejectConfirm")}
                </button>
                <button type="button" className="link" onClick={cancelReject}>
                  {t("common.cancel")}
                </button>
              </span>
            ) : (
              <button type="button" className="link" disabled={busy} onClick={() => setRejecting(true)}>
                {t("health.proposals.reject")}
              </button>
            )}
          </>
        )}
        {p.status === "rejected" && (
          <button type="button" className="link" disabled={busy} onClick={() => onStatus("open")}>
            {t("health.reopen")}
          </button>
        )}
        <button type="button" className="link danger" disabled={busy} onClick={onDelete}>
          {t("common.delete")}
        </button>
      </div>
    </li>
  );
}
````

Патч `workspace.ts`, `proposals.ts`, `FindingsPanel.tsx`, `HealthBlock.tsx`, `styles.css`:

<!-- block: patch task8-src.diff -->
````diff
diff --git a/src/components/health/FindingsPanel.tsx b/src/components/health/FindingsPanel.tsx
index 47d099e..ae9c2b6 100644
--- a/src/components/health/FindingsPanel.tsx
+++ b/src/components/health/FindingsPanel.tsx
@@ -7,12 +7,13 @@ import { Hint } from "../common/Hint";
 import { CheckBar } from "./CheckBar";
 import { findingsOf, SEVERITIES } from "./findings";
 import { FindingItem, NoteItem, SeverityMark } from "./parts";
+import { ProposalItem } from "./ProposalItem";
 
 const KINDS: TargetKind[] = ["cluster", "broker", "topic", "group"];
 
 type Source = "all" | "rules" | "llm";
 
-/** Вкладка «Замечания»: находки последней проверки по правилам и замечания LLM (спец. G2 §7). */
+/** Вкладка «Замечания»: находки последней проверки по правилам, замечания и предложения LLM (спец. G2 §7, G3 §7). */
 export function FindingsPanel({ connectionId }: { connectionId: string }) {
   const { t } = useTranslation();
   const health = useHealth(connectionId);
@@ -23,6 +24,7 @@ export function FindingsPanel({ connectionId }: { connectionId: string }) {
   const [query, setQuery] = useState("");
   const [showHidden, setShowHidden] = useState(false);
   const [confirmAll, setConfirmAll] = useState(false);
+  const [confirmAllProposals, setConfirmAllProposals] = useState(false);
 
   const view = health.data;
   const hidden = new Set(view?.ignored ?? []);
@@ -38,9 +40,25 @@ export function FindingsPanel({ connectionId }: { connectionId: string }) {
   const notes = source === "rules" ? [] : (view?.annotations ?? []).filter(matches);
   const open = notes.filter((a) => a.status === "open");
   const closed = notes.filter((a) => a.status !== "open");
+  const proposals = source === "rules" ? [] : (view?.proposals ?? []).filter(matches);
+  const openProposals = proposals.filter((p) => p.status === "open");
+  const closedProposals = proposals.filter((p) => p.status !== "open");
   const busy =
-    actions.ignore.isPending || actions.setStatus.isPending || actions.remove.isPending || actions.removeAll.isPending;
-  const error = actions.ignore.error ?? actions.setStatus.error ?? actions.remove.error ?? actions.removeAll.error;
+    actions.ignore.isPending ||
+    actions.setStatus.isPending ||
+    actions.remove.isPending ||
+    actions.removeAll.isPending ||
+    actions.setProposalStatus.isPending ||
+    actions.removeProposal.isPending ||
+    actions.removeAllProposals.isPending;
+  const error =
+    actions.ignore.error ??
+    actions.setStatus.error ??
+    actions.remove.error ??
+    actions.removeAll.error ??
+    actions.setProposalStatus.error ??
+    actions.removeProposal.error ??
+    actions.removeAllProposals.error;
   const filtered = severity !== "all" || kind !== "all" || q !== "";
 
   return (
@@ -112,6 +130,66 @@ export function FindingsPanel({ connectionId }: { connectionId: string }) {
         </section>
       )}
 
+      {source !== "rules" && (
+        <section aria-labelledby="health-proposals">
+          <h3 id="health-proposals">
+            {t("health.proposals.title")} <Hint k="hints.health.proposals" label={t("health.proposals.title")} />
+          </h3>
+          <p className="hint">{t("health.proposals.intro")}</p>
+          {openProposals.length === 0 && <p className="empty">{filtered ? t("health.nothingMatches") : t("health.proposals.none")}</p>}
+          <ul className="finding-list">
+            {openProposals.map((p) => (
+              <ProposalItem
+                key={p.id}
+                connectionId={connectionId}
+                proposal={p}
+                busy={busy}
+                onStatus={(status, reason) => actions.setProposalStatus.mutate({ id: p.id, status, reason })}
+                onDelete={() => actions.removeProposal.mutate(p.id)}
+              />
+            ))}
+          </ul>
+          {closedProposals.length > 0 && (
+            <details className="finding-group">
+              <summary>{t("health.closedNotes", { count: closedProposals.length })}</summary>
+              <ul className="finding-list">
+                {closedProposals.map((p) => (
+                  <ProposalItem
+                    key={p.id}
+                    connectionId={connectionId}
+                    proposal={p}
+                    busy={busy}
+                    onStatus={(status, reason) => actions.setProposalStatus.mutate({ id: p.id, status, reason })}
+                    onDelete={() => actions.removeProposal.mutate(p.id)}
+                  />
+                ))}
+              </ul>
+            </details>
+          )}
+          {(view?.proposals.length ?? 0) > 0 &&
+            (confirmAllProposals ? (
+              <p role="alert">
+                {t("health.proposals.deleteAllConfirm")}{" "}
+                <button
+                  type="button"
+                  className="danger"
+                  disabled={busy}
+                  onClick={() => actions.removeAllProposals.mutate(undefined, { onSettled: () => setConfirmAllProposals(false) })}
+                >
+                  {t("health.proposals.deleteAll")}
+                </button>{" "}
+                <button type="button" onClick={() => setConfirmAllProposals(false)}>
+                  {t("common.cancel")}
+                </button>
+              </p>
+            ) : (
+              <button type="button" className="link danger" disabled={busy} onClick={() => setConfirmAllProposals(true)}>
+                {t("health.proposals.deleteAll")}
+              </button>
+            ))}
+        </section>
+      )}
+
       {source !== "rules" && (
         <section aria-labelledby="health-notes">
           <h3 id="health-notes">{t("health.notes")}</h3>
diff --git a/src/components/health/HealthBlock.tsx b/src/components/health/HealthBlock.tsx
index 6bbdf8b..70080a4 100644
--- a/src/components/health/HealthBlock.tsx
+++ b/src/components/health/HealthBlock.tsx
@@ -6,6 +6,7 @@ import { useWorkspace } from "../../state/workspace";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { badgeFor, badgeTooltip, itemsFor } from "./findings";
 import { FindingItem, NoteItem, SeverityMark } from "./parts";
+import { ProposalItem } from "./ProposalItem";
 
 type Match = (t: HealthTarget) => boolean;
 
@@ -22,16 +23,22 @@ export function HealthBadge({ connectionId, match }: { connectionId: string; mat
   );
 }
 
-/** Блок «Замечания» на экране объекта: его находки и открытые замечания LLM с действиями. */
+/** Блок «Замечания» на экране объекта: его находки, открытые замечания и предложения LLM с действиями. */
 export function HealthBlock({ connectionId, match, showTarget = false }: { connectionId: string; match: Match; showTarget?: boolean }) {
   const { t } = useTranslation();
   const health = useHealth(connectionId);
   const actions = useHealthActions(connectionId);
   const open = useWorkspace((s) => s.open);
-  const { findings, notes } = itemsFor(health.data, match);
-  if (findings.length === 0 && notes.length === 0) return null;
-  const busy = actions.ignore.isPending || actions.setStatus.isPending || actions.remove.isPending;
-  const error = actions.ignore.error ?? actions.setStatus.error ?? actions.remove.error;
+  const { findings, notes, proposals } = itemsFor(health.data, match);
+  if (findings.length === 0 && notes.length === 0 && proposals.length === 0) return null;
+  const busy =
+    actions.ignore.isPending ||
+    actions.setStatus.isPending ||
+    actions.remove.isPending ||
+    actions.setProposalStatus.isPending ||
+    actions.removeProposal.isPending;
+  const error =
+    actions.ignore.error ?? actions.setStatus.error ?? actions.remove.error ?? actions.setProposalStatus.error ?? actions.removeProposal.error;
   return (
     <section className="health-block" aria-label={t("health.title")}>
       <h3>
@@ -63,6 +70,17 @@ export function HealthBlock({ connectionId, match, showTarget = false }: { conne
             onDelete={() => actions.remove.mutate(a.id)}
           />
         ))}
+        {proposals.map((p) => (
+          <ProposalItem
+            key={p.id}
+            connectionId={connectionId}
+            proposal={p}
+            busy={busy}
+            showTarget={showTarget}
+            onStatus={(status, reason) => actions.setProposalStatus.mutate({ id: p.id, status, reason })}
+            onDelete={() => actions.removeProposal.mutate(p.id)}
+          />
+        ))}
       </ul>
       {error && <ErrorBanner error={error} />}
     </section>
diff --git a/src/components/health/proposals.ts b/src/components/health/proposals.ts
index 20a75e9..29ebfb2 100644
--- a/src/components/health/proposals.ts
+++ b/src/components/health/proposals.ts
@@ -1,7 +1,22 @@
 import type { TFunction } from "i18next";
 import type { Proposal, ProposalChange, ResetScope, ResetStrategy } from "../../api/types";
+import type { TabSpec } from "../../state/workspace";
 import { formatAgo } from "./CheckBar";
 
+/** Вкладка, в которой предложение открывается заполненной формой (спец. G3 §8). */
+export function proposalTab(connectionId: string, proposal: Proposal, nonce = Date.now()): TabSpec {
+  const prefill = { proposal, nonce };
+  switch (proposal.change.kind) {
+    case "alterTopicConfigs":
+    case "addPartitions":
+      return { kind: "topic", connectionId, topic: proposal.change.topic, proposal: prefill };
+    case "createTopic":
+      return { kind: "topics", connectionId, proposal: prefill };
+    case "resetOffsets":
+      return { kind: "group", connectionId, groupId: proposal.change.groupId, proposal: prefill };
+  }
+}
+
 /** Стратегия сброса словами формы сброса: «С начала (earliest)», «По времени 22.10.2026, 10:00:00», «Сдвиг на ±N −5». */
 export function strategyText(s: ResetStrategy, t: TFunction, lang: string): string {
   switch (s.type) {
diff --git a/src/state/workspace.ts b/src/state/workspace.ts
index 8e419c7..0500f70 100644
--- a/src/state/workspace.ts
+++ b/src/state/workspace.ts
@@ -1,19 +1,22 @@
 import { create } from "zustand";
 
-import type { ProduceMessage, ProducerSettings } from "../api/types";
+import type { ProduceMessage, ProducerSettings, Proposal } from "../api/types";
 
 export type ProducerPrefill = { message: ProduceMessage; settings?: ProducerSettings; nonce: number };
 
 export type ResetPrefill = { topic: string; partition: number; offset: number; nonce: number };
 
+/** Предложение LLM, которое открывает заполненную форму вкладки (G3); `nonce` — повторное открытие обновляет заполнение. */
+export type ProposalPrefill = { proposal: Proposal; nonce: number };
+
 export type TabSpec =
   | { kind: "cluster"; connectionId: string }
-  | { kind: "topics"; connectionId: string }
-  | { kind: "topic"; connectionId: string; topic: string }
+  | { kind: "topics"; connectionId: string; proposal?: ProposalPrefill }
+  | { kind: "topic"; connectionId: string; topic: string; proposal?: ProposalPrefill }
   | { kind: "groups"; connectionId: string }
-  /** «Замечания»: находки проверки кластера и замечания LLM (G2). */
+  /** «Замечания»: находки проверки кластера, замечания и предложения LLM (G2, G3). */
   | { kind: "findings"; connectionId: string }
-  | { kind: "group"; connectionId: string; groupId: string; resetPrefill?: ResetPrefill }
+  | { kind: "group"; connectionId: string; groupId: string; resetPrefill?: ResetPrefill; proposal?: ProposalPrefill }
   | { kind: "producer"; connectionId: string; prefill?: ProducerPrefill }
   | { kind: "consumer"; connectionId: string; key: string; topic?: string; groupId?: string }
   /** folderId — папка нового подключения («Новое подключение здесь»). */
diff --git a/src/styles.css b/src/styles.css
index 9c6d074..eb4e9bf 100644
--- a/src/styles.css
+++ b/src/styles.css
@@ -183,6 +183,8 @@ label.inline { display: inline-flex; gap: 4px; align-items: center; white-space:
 .finding-actions { display: flex; gap: 12px; }
 .finding.is-hidden, .finding.is-closed { opacity: 0.65; }
 .note-body { white-space: pre-wrap; overflow-wrap: anywhere; }
+.proposal-change { overflow-wrap: anywhere; }
+.proposal-reject { display: inline-flex; gap: 8px; align-items: center; }
 .health-badge { margin-left: 4px; }
 .health-block { border: 1px solid var(--border); border-radius: 6px; padding: 8px; }
 .health-block h3 { margin: 0 0 4px; font-size: 14px; }
````

Подсказка «?» раздела на 12 языках:

<!-- block: spec /tmp/g3-task8-hints.json -->
````json
{
 "path": "hints.health",
 "after": "mcp",
 "values": {
  "en": {
   "proposals": "A change an LLM assistant suggests. Nothing is applied until you open it in a form and confirm; you can edit the values first."
  },
  "ru": {
   "proposals": "Изменение, которое предлагает LLM-ассистент. Ничего не применяется, пока вы не откроете его в форме и не подтвердите; значения можно сначала поправить."
  },
  "de": {
   "proposals": "Eine Änderung, die ein LLM-Assistent vorschlägt. Nichts wird angewendet, bis Sie sie im Formular öffnen und bestätigen; die Werte können Sie vorher ändern."
  },
  "fr": {
   "proposals": "Une modification proposée par un assistant LLM. Rien n’est appliqué tant que vous ne l’ouvrez pas dans un formulaire et ne la confirmez pas ; vous pouvez d’abord modifier les valeurs."
  },
  "es": {
   "proposals": "Un cambio que sugiere un asistente LLM. Nada se aplica hasta que lo abras en un formulario y lo confirmes; antes puedes editar los valores."
  },
  "it": {
   "proposals": "Una modifica suggerita da un assistente LLM. Nulla viene applicato finché non la apri in un modulo e la confermi; prima puoi modificare i valori."
  },
  "pl": {
   "proposals": "Zmiana zaproponowana przez asystenta LLM. Nic nie zostanie zastosowane, dopóki nie zostanie otwarta w formularzu i potwierdzona; wartości można wcześniej poprawić."
  },
  "pt-BR": {
   "proposals": "Uma alteração sugerida por um assistente LLM. Nada é aplicado até você abri-la em um formulário e confirmar; antes é possível editar os valores."
  },
  "zh-CN": {
   "proposals": "LLM 助手建议的更改。在你于表单中打开并确认之前不会应用；可以先修改其中的值。"
  },
  "zh-TW": {
   "proposals": "LLM 助理建議的變更。在你於表單中開啟並確認之前不會套用；可以先修改其中的值。"
  },
  "ja": {
   "proposals": "LLMアシスタントが提案する変更です。フォームで開いて確認するまで適用されません。値は先に修正できます。"
  },
  "ko": {
   "proposals": "LLM 어시스턴트가 제안하는 변경입니다. 양식에서 열어 확인하기 전까지 적용되지 않으며, 값을 먼저 수정할 수 있습니다."
  }
 }
}
````

Run: `node /tmp/add-keys.mjs src/locales /tmp/g3-task8-hints.json`

- [ ] **Step 4: Тесты проходят.**

Run: `pnpm vitest run src/components/health` — Expected: `Tests 36 passed`; `pnpm exec tsc --noEmit`, `pnpm i18n:check` (значок `<Hint k="hints.health.proposals">` найден) — без замечаний.

- [ ] **Step 5: Commit.**

````bash
git add src/components/health/ProposalItem.tsx src/state/workspace.ts src/components/health/proposals.ts src/components/health/FindingsPanel.tsx src/components/health/FindingsPanel.test.tsx src/components/health/HealthBlock.tsx src/components/health/HealthBlock.test.tsx src/styles.css src/locales/*.json
git commit -m "feat(ui): карточка предложения, раздел «Предложения LLM» на вкладке «Замечания», предложения в блоках объектов

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 9: Формы из предложения: редактор конфигов, создание топика, партиции, сброс оффсетов; `proposalId` в командах; строка над формой

**Files:**
- Create: `src/components/health/ProposalBanner.tsx`
- Modify: `src/components/TabContent.tsx`, `src/components/common/ConfigEditor.tsx`, `src/components/topics/CreateTopicForm.tsx`, `src/components/topics/TopicActions.tsx`, `src/components/topics/TopicDetails.tsx`, `src/components/topics/TopicList.tsx`, `src/components/groups/GroupDetails.tsx`, `src/components/groups/ResetOffsetsPanel.tsx`, `src/components/groups/reset.ts`, `src/state/workspace.ts` (`ResetPrefill` — объединение); тесты: `ConfigEditor.test.tsx`, `CreateTopicForm.test.tsx`, `TopicActions.test.tsx`, `TopicDetails.test.tsx`, `TopicList.test.tsx`, `GroupDetails.test.tsx`, `ResetOffsetsPanel.test.tsx`, `reset.test.ts`, `src/api/client.test.ts`

**Interfaces:**
- Consumes: `ProposalPrefill`, `formatProps` из `connections/profile.ts`, `ConfigEdit` из `configDiff.ts`, `buildResetRequest`.
- Produces: `ProposalBanner { prefill }` — строка `health.proposals.applying`; `ConfigEditor { initialEdits?, proposalId? }`; `CreateTopicForm { initial?: NewTopicSpec, proposalId? }` (конфиги — строками `k=v` в доп. свойствах, пресет «delete», фактор `-1` допустим); `TopicActions { proposal? }` → `AddPartitions { initialTotal?, proposalId? }`; `TopicDetails { proposal? }` — подвкладка «Конфигурация» и редактор с правками из `alterTopicConfigs`, партиции — через `TopicActions`; `TopicList { proposal? }` — форма создания; `ResetPrefill = { topic, partition, offset, nonce } | { strategy, scope, proposalId, nonce }`, `reset.ts::{toLocalInput(ms), defaultResetForm}` заполняет стратегию/область; `ResetOffsetsPanel` передаёт `{ id, request }` в `api.resetOffsets`; `GroupDetails { proposal? }` открывает панель сброса; `TabContent` пробрасывает `tab.proposal`. Формы пересоздаются по `nonce` (`key`), поэтому повторное открытие заменяет заполнение.

Спец. G3 §8. Мутации форм всегда передают `proposalId` (или `null`), поэтому ожидания старых тестов (`toHaveBeenCalledWith(..., null)`) получают ещё один `null` — это часть патча тестов. У сброса в команду уходит запрос предпросмотра: любая правка формы сбрасывает предпросмотр, так что запрос всегда тот самый.

- [ ] **Step 1: Тесты.**

Патч тестов: заполнение и `proposalId` у `ConfigEditor`, `CreateTopicForm`, `TopicActions`, `ResetOffsetsPanel`, `reset.ts` (в том числе местное время `toLocalInput`); открытие из предложения в `TopicDetails`, `TopicList`, `GroupDetails` со строкой «Применяется предложение LLM: …»; `client.test.ts` — `proposalId`/`request` в IPC; старые ожидания с дополнительным `null`:

<!-- block: patch task9-tests.diff -->
````diff
diff --git a/src/api/client.test.ts b/src/api/client.test.ts
index b300157..317e70e 100644
--- a/src/api/client.test.ts
+++ b/src/api/client.test.ts
@@ -99,7 +99,11 @@ describe("api client", () => {
     invoke.mockResolvedValue([]);
     const offsets = [{ topic: "t", partition: 0, offset: 5 }];
     await api.resetOffsets("c1", "g", offsets, null);
-    expect(invoke).toHaveBeenCalledWith("reset_offsets", { connectionId: "c1", groupId: "g", offsets, confirm: null });
+    expect(invoke).toHaveBeenCalledWith("reset_offsets", { connectionId: "c1", groupId: "g", offsets, confirm: null, proposalId: null, request: null });
+    // Из предложения LLM — его id и запрос предпросмотра для сравнения в Rust.
+    const request = { groupId: "g", strategy: { type: "earliest" as const }, scope: { type: "all" as const } };
+    await api.resetOffsets("c1", "g", offsets, "prod-1", { id: "p1", request });
+    expect(invoke).toHaveBeenCalledWith("reset_offsets", { connectionId: "c1", groupId: "g", offsets, confirm: "prod-1", proposalId: "p1", request });
   });
   it("startConsumer передаёт параметры, Channel и подтверждение", async () => {
     invoke.mockResolvedValue("sid");
diff --git a/src/components/common/ConfigEditor.test.tsx b/src/components/common/ConfigEditor.test.tsx
index b2b288a..8090716 100644
--- a/src/components/common/ConfigEditor.test.tsx
+++ b/src/components/common/ConfigEditor.test.tsx
@@ -32,10 +32,30 @@ describe("ConfigEditor", () => {
     await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
     expect(screen.getByText("1000 → 2000")).toBeInTheDocument();
     await user.click(screen.getByRole("button", { name: "Применить" }));
-    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], null);
+    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], null, null);
     await vi.waitFor(() => expect(onDone).toHaveBeenCalled());
   });
 
+  it("правки из предложения LLM уже заполнены, применение передаёт proposalId", async () => {
+    const user = userEvent.setup();
+    renderWithClient(
+      <ConfigEditor
+        connection={dev}
+        topic="t"
+        entries={entries}
+        onDone={() => {}}
+        initialEdits={{ "retention.ms": { kind: "set", value: "2000" } }}
+        proposalId="p1"
+      />,
+    );
+    expect(screen.getByLabelText("Значение retention.ms")).toHaveValue("2000");
+    expect(screen.getByText("изменений: 1")).toBeInTheDocument();
+    await user.click(screen.getByRole("button", { name: "Проверить изменения" }));
+    expect(screen.getByText("1000 → 2000")).toBeInTheDocument();
+    await user.click(screen.getByRole("button", { name: "Применить" }));
+    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], null, "p1");
+  });
+
   it("очищенное поле не отправляет пустой set", async () => {
     const user = userEvent.setup();
     renderWithClient(<ConfigEditor connection={dev} topic="t" entries={entries} onDone={() => {}} />);
@@ -65,7 +85,7 @@ describe("ConfigEditor", () => {
     expect(screen.getByRole("button", { name: "Применить" })).toBeDisabled();
     await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
     await user.click(screen.getByRole("button", { name: "Применить" }));
-    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], "prod-1");
+    expect(api.alterTopicConfigs).toHaveBeenCalledWith("c", "t", [{ op: "set", name: "retention.ms", value: "2000" }], "prod-1", null);
   });
 
   it("подсказка «?» рядом с «Проверить изменения»", async () => {
diff --git a/src/components/groups/GroupDetails.test.tsx b/src/components/groups/GroupDetails.test.tsx
index c81ed6f..5828dbc 100644
--- a/src/components/groups/GroupDetails.test.tsx
+++ b/src/components/groups/GroupDetails.test.tsx
@@ -172,6 +172,29 @@ describe("GroupDetails", () => {
     expect(screen.getByText("Сброс возможен только для группы без участников (сейчас: Stable)")).toBeInTheDocument();
   });
 
+  it("предложение LLM открывает панель сброса с его стратегией и строкой о предложении", async () => {
+    vi.mocked(api.describeGroups).mockResolvedValue([{ ...stable, state: "Empty", members: [] }]);
+    const proposal = {
+      proposal: {
+        id: "p1",
+        severity: "warning" as const,
+        title: "Replay orders",
+        rationale: "r",
+        target: { kind: "group" as const, name: "g" },
+        change: { kind: "resetOffsets" as const, groupId: "g", strategy: { type: "latest" as const }, scope: { type: "all" as const } },
+        client: "claude-code",
+        createdAtMs: 1,
+        updatedAtMs: 1,
+        status: "open" as const,
+      },
+      nonce: 1,
+    };
+    renderWithClient(<GroupDetails connectionId="c" groupId="g" proposal={proposal} />);
+    expect(await screen.findByText("Применяется предложение LLM: Replay orders")).toBeInTheDocument();
+    expect(screen.getByLabelText("Стратегия")).toHaveValue("latest");
+    expect(screen.getByRole("button", { name: "Предпросмотр" })).toBeInTheDocument();
+  });
+
   it("read-only: панель сброса не открывается даже с prefill", async () => {
     vi.mocked(api.listConnections).mockResolvedValue([{ ...dev, readOnly: true }]);
     vi.mocked(api.describeGroups).mockResolvedValue([stable]);
diff --git a/src/components/groups/ResetOffsetsPanel.test.tsx b/src/components/groups/ResetOffsetsPanel.test.tsx
index 6d657e1..471913c 100644
--- a/src/components/groups/ResetOffsetsPanel.test.tsx
+++ b/src/components/groups/ResetOffsetsPanel.test.tsx
@@ -44,10 +44,32 @@ describe("ResetOffsetsPanel", () => {
     await user.click(screen.getByRole("button", { name: "Применить (1)" }));
     await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
     await user.click(screen.getByRole("button", { name: "Сбросить оффсеты" }));
-    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], "prod-1");
+    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], "prod-1", null);
     await vi.waitFor(() => expect(onApplied).toHaveBeenCalled());
   });
 
+  it("из предложения LLM: форма заполнена, в команду уходят proposalId и запрос предпросмотра", async () => {
+    const user = userEvent.setup();
+    const dev = { ...prod, environment: "dev" as const };
+    renderWithClient(
+      <ResetOffsetsPanel
+        connection={dev}
+        groupId="g"
+        prefill={{ strategy: { type: "offset", offset: 0 }, scope: { type: "topics", topics: ["t"] }, proposalId: "p1", nonce: 1 }}
+        onApplied={() => {}}
+        onClose={() => {}}
+      />,
+    );
+    expect(screen.getByLabelText("Стратегия")).toHaveValue("offset");
+    expect(screen.getByLabelText("Оффсет")).toHaveValue(0);
+    expect(screen.getByLabelText("Область")).toHaveValue("topics");
+    await user.click(screen.getByRole("button", { name: "Предпросмотр" }));
+    const request = { groupId: "g", strategy: { type: "offset", offset: 0 }, scope: { type: "topics", topics: ["t"] } };
+    expect(api.previewOffsetReset).toHaveBeenCalledWith("c", request);
+    await user.click(await screen.findByRole("button", { name: "Применить (1)" }));
+    expect(api.resetOffsets).toHaveBeenCalledWith("c", "g", [{ topic: "t", partition: 0, offset: 0 }], null, { id: "p1", request });
+  });
+
   it("blockedReason блокирует «Применить» и показывает подсказку", async () => {
     const user = userEvent.setup();
     const dev = { ...prod, environment: "dev" as const };
diff --git a/src/components/groups/reset.test.ts b/src/components/groups/reset.test.ts
index 23c127a..efed35b 100644
--- a/src/components/groups/reset.test.ts
+++ b/src/components/groups/reset.test.ts
@@ -1,5 +1,5 @@
 import { describe, expect, it } from "vitest";
-import { buildResetRequest, defaultResetForm, targetsFromPreview } from "./reset";
+import { buildResetRequest, defaultResetForm, targetsFromPreview, toLocalInput } from "./reset";
 
 describe("reset", () => {
   it("стратегии и области", () => {
@@ -39,6 +39,29 @@ describe("reset", () => {
     });
   });
 
+  it("предзаполнение из предложения LLM", () => {
+    const at = new Date(2026, 9, 22, 10, 5, 9).getTime();
+    expect(defaultResetForm({ strategy: { type: "timestamp", timestampMs: at }, scope: { type: "topics", topics: ["a", "b"] }, proposalId: "p1", nonce: 1 })).toEqual({
+      strategy: "timestamp",
+      value: "2026-10-22T10:05:09",
+      scope: "topics",
+      scopeText: "a, b",
+    });
+    expect(toLocalInput(at)).toBe("2026-10-22T10:05:09");
+    expect(
+      defaultResetForm({
+        strategy: { type: "shift", delta: -5 },
+        scope: { type: "partitions", partitions: [{ topic: "t", partition: 0 }, { topic: "t", partition: 2 }] },
+        proposalId: "p1",
+        nonce: 1,
+      }),
+    ).toEqual({ strategy: "shift", value: "-5", scope: "partitions", scopeText: "t:0, t:2" });
+    const form = defaultResetForm({ strategy: { type: "earliest" }, scope: { type: "all" }, proposalId: "p1", nonce: 1 });
+    expect(form).toEqual({ strategy: "earliest", value: "", scope: "all", scopeText: "" });
+    // Обратно получается тот же запрос, что предлагала модель.
+    expect(buildResetRequest("g", form)).toEqual({ groupId: "g", strategy: { type: "earliest" }, scope: { type: "all" } });
+  });
+
   it("применяются только строки без ошибок", () => {
     const rows = [
       { topic: "t", partition: 0, current: 1, target: 0, low: 0, high: 5 },
diff --git a/src/components/topics/CreateTopicForm.test.tsx b/src/components/topics/CreateTopicForm.test.tsx
index d8c4e48..dbd824a 100644
--- a/src/components/topics/CreateTopicForm.test.tsx
+++ b/src/components/topics/CreateTopicForm.test.tsx
@@ -43,10 +43,36 @@ describe("CreateTopicForm", () => {
       "c",
       { name: "orders", partitions: 3, replicationFactor: 1, configs: { "cleanup.policy": "compact" } },
       null,
+      null,
     );
     await vi.waitFor(() => expect(onCreated).toHaveBeenCalledWith("orders"));
   });
 
+  it("поля из предложения LLM заполнены, создание передаёт proposalId", async () => {
+    const user = userEvent.setup();
+    renderWithClient(
+      <CreateTopicForm
+        connection={dev}
+        onCreated={() => {}}
+        onCancel={() => {}}
+        initial={{ name: "orders-dlq", partitions: 6, replicationFactor: -1, configs: { "cleanup.policy": "compact", "retention.ms": "1000" } }}
+        proposalId="p1"
+      />,
+    );
+    expect(screen.getByLabelText("Имя топика")).toHaveValue("orders-dlq");
+    expect(screen.getByLabelText("Партиции")).toHaveValue(6);
+    expect(screen.getByLabelText("Фактор репликации")).toHaveValue(-1);
+    expect(screen.getByLabelText("Пресет")).toHaveValue("delete");
+    expect(screen.getByLabelText("Доп. конфиги (key=value)")).toHaveValue("cleanup.policy=compact\nretention.ms=1000");
+    await user.click(screen.getByRole("button", { name: "Создать" }));
+    expect(api.createTopic).toHaveBeenCalledWith(
+      "c",
+      { name: "orders-dlq", partitions: 6, replicationFactor: -1, configs: { "cleanup.policy": "compact", "retention.ms": "1000" } },
+      null,
+      "p1",
+    );
+  });
+
   it("в prod просит имя подключения перед созданием", async () => {
     const user = userEvent.setup();
     renderWithClient(<CreateTopicForm connection={prod} onCreated={() => {}} onCancel={() => {}} />);
@@ -55,7 +81,7 @@ describe("CreateTopicForm", () => {
     expect(api.createTopic).not.toHaveBeenCalled();
     await user.type(screen.getByLabelText("Имя подключения"), "prod-1");
     await user.click(screen.getByRole("button", { name: "Создать топик" }));
-    expect(api.createTopic).toHaveBeenCalledWith("c", expect.objectContaining({ name: "t" }), "prod-1");
+    expect(api.createTopic).toHaveBeenCalledWith("c", expect.objectContaining({ name: "t" }), "prod-1", null);
   });
 
   it("подсказки «?» у полей формы; открытая подсказка показывает текст", async () => {
diff --git a/src/components/topics/TopicActions.test.tsx b/src/components/topics/TopicActions.test.tsx
index 1e81539..bf95a09 100644
--- a/src/components/topics/TopicActions.test.tsx
+++ b/src/components/topics/TopicActions.test.tsx
@@ -49,7 +49,31 @@ describe("TopicActions", () => {
     await user.clear(input);
     await user.type(input, "4");
     await user.click(screen.getByRole("button", { name: "Увеличить" }));
-    expect(api.addPartitions).toHaveBeenCalledWith("c", "orders", 4, null);
+    expect(api.addPartitions).toHaveBeenCalledWith("c", "orders", 4, null, null);
+  });
+
+  it("предложение LLM открывает диалог партиций с его числом и передаёт proposalId", async () => {
+    const user = userEvent.setup();
+    const proposal = {
+      proposal: {
+        id: "p1",
+        severity: "warning" as const,
+        title: "More partitions",
+        rationale: "r",
+        target: { kind: "topic" as const, name: "orders" },
+        change: { kind: "addPartitions" as const, topic: "orders", newTotal: 4 },
+        client: "claude-code",
+        createdAtMs: 1,
+        updatedAtMs: 1,
+        status: "open" as const,
+      },
+      nonce: 1,
+    };
+    renderWithClient(<TopicActions connection={dev} topic={topic} onDeleted={() => {}} proposal={proposal} />);
+    expect(screen.getByText("Применяется предложение LLM: More partitions")).toBeInTheDocument();
+    expect(screen.getByLabelText("Новое число партиций")).toHaveValue(4);
+    await user.click(screen.getByRole("button", { name: "Увеличить" }));
+    expect(api.addPartitions).toHaveBeenCalledWith("c", "orders", 4, null, "p1");
   });
 
   it("purge предупреждает о compact и требует имя топика", async () => {
diff --git a/src/components/topics/TopicDetails.test.tsx b/src/components/topics/TopicDetails.test.tsx
index c46bec7..6c32b33 100644
--- a/src/components/topics/TopicDetails.test.tsx
+++ b/src/components/topics/TopicDetails.test.tsx
@@ -44,6 +44,33 @@ describe("TopicDetails", () => {
   });
 });
 
+describe("TopicDetails: предложение LLM", () => {
+  it("открывает «Конфигурацию» с заполненным редактором и строкой о предложении", async () => {
+    const { api } = await import("../../api/client");
+    vi.mocked(api.listConnections).mockResolvedValue([{ ...newProfile(), id: "c", name: "dev-1", environment: "dev" }]);
+    const proposal = {
+      proposal: {
+        id: "p1",
+        severity: "warning" as const,
+        title: "Compact orders",
+        rationale: "r",
+        target: { kind: "topic" as const, name: "orders" },
+        change: { kind: "alterTopicConfigs" as const, topic: "orders", changes: [{ name: "cleanup.policy", value: "delete" }] },
+        client: "claude-code",
+        createdAtMs: 1,
+        updatedAtMs: 1,
+        status: "open" as const,
+      },
+      nonce: 1,
+    };
+    renderWithClient(<TopicDetails connectionId="c" topic="orders" proposal={proposal} />);
+    expect(await screen.findByText("Применяется предложение LLM: Compact orders")).toBeInTheDocument();
+    expect(screen.getByRole("button", { name: "Конфигурация" })).toHaveAttribute("aria-pressed", "true");
+    expect(screen.getByLabelText("Значение cleanup.policy")).toHaveValue("delete");
+    expect(screen.getByText("изменений: 1")).toBeInTheDocument();
+  });
+});
+
 describe("TopicDetails: недоступные оффсеты", () => {
   it("показывает «—» и ошибку в title", async () => {
     const user = userEvent.setup();
diff --git a/src/components/topics/TopicList.test.tsx b/src/components/topics/TopicList.test.tsx
index 17581ee..e27f3e6 100644
--- a/src/components/topics/TopicList.test.tsx
+++ b/src/components/topics/TopicList.test.tsx
@@ -52,6 +52,28 @@ describe("TopicList", () => {
     expect(screen.getByRole("button", { name: "Удалить (0)" })).toBeDisabled();
   });
 
+  it("предложение LLM открывает форму создания с его полями", async () => {
+    const proposal = {
+      proposal: {
+        id: "p1",
+        severity: "info" as const,
+        title: "DLQ for orders",
+        rationale: "r",
+        target: { kind: "topic" as const, name: "orders-dlq" },
+        change: { kind: "createTopic" as const, name: "orders-dlq", partitions: 3, replicationFactor: 1, configs: {} },
+        client: "claude-code",
+        createdAtMs: 1,
+        updatedAtMs: 1,
+        status: "open" as const,
+      },
+      nonce: 1,
+    };
+    renderWithClient(<TopicList connectionId="c" proposal={proposal} />);
+    expect(await screen.findByText("Применяется предложение LLM: DLQ for orders")).toBeInTheDocument();
+    expect(screen.getByLabelText("Имя топика")).toHaveValue("orders-dlq");
+    expect(screen.getByLabelText("Партиции")).toHaveValue(3);
+  });
+
   it("цель удаления фиксируется при открытии панели", async () => {
     const user = userEvent.setup();
     renderWithClient(<TopicList connectionId="c" />);
````

- [ ] **Step 2: Увидеть падение.**

Run: `pnpm vitest run src/components/common/ConfigEditor.test.tsx src/components/topics src/components/groups src/api/client.test.ts` — Expected: FAIL — поля не заполнены, нет строки о предложении, вызовы без `proposalId`.

- [ ] **Step 3: Реализация.**

Новый `src/components/health/ProposalBanner.tsx`:

<!-- block: file src/components/health/ProposalBanner.tsx part=whole -->
````tsx
import { useTranslation } from "react-i18next";
import type { ProposalPrefill } from "../../state/workspace";

/** Строка над формой, открытой из предложения LLM (спец. G3 §8): какое предложение применяется. */
export function ProposalBanner({ prefill }: { prefill: ProposalPrefill | undefined }) {
  const { t } = useTranslation();
  if (!prefill) return null;
  return <p className="hint proposal-banner">{t("health.proposals.applying", { title: prefill.proposal.title })}</p>;
}
````

Патч форм, вкладок и состояния:

<!-- block: patch task9-src.diff -->
````diff
diff --git a/src/components/TabContent.tsx b/src/components/TabContent.tsx
index 7fde2ab..519f2fa 100644
--- a/src/components/TabContent.tsx
+++ b/src/components/TabContent.tsx
@@ -51,15 +51,15 @@ export function TabContent({ tab }: { tab: Tab }) {
     case "cluster":
       return <ClusterOverview connectionId={tab.connectionId} />;
     case "topics":
-      return <TopicList connectionId={tab.connectionId} />;
+      return <TopicList connectionId={tab.connectionId} proposal={tab.proposal} />;
     case "topic":
-      return <TopicDetails connectionId={tab.connectionId} topic={tab.topic} />;
+      return <TopicDetails connectionId={tab.connectionId} topic={tab.topic} proposal={tab.proposal} />;
     case "groups":
       return <GroupList connectionId={tab.connectionId} />;
     case "findings":
       return <FindingsPanel connectionId={tab.connectionId} />;
     case "group":
-      return <GroupDetails connectionId={tab.connectionId} groupId={tab.groupId} resetPrefill={tab.resetPrefill} />;
+      return <GroupDetails connectionId={tab.connectionId} groupId={tab.groupId} resetPrefill={tab.resetPrefill} proposal={tab.proposal} />;
     case "producer":
       return <ProducerPanel connectionId={tab.connectionId} prefill={tab.prefill} />;
     case "consumer":
diff --git a/src/components/common/ConfigEditor.tsx b/src/components/common/ConfigEditor.tsx
index 91008ff..2eabb55 100644
--- a/src/components/common/ConfigEditor.tsx
+++ b/src/components/common/ConfigEditor.tsx
@@ -17,12 +17,16 @@ interface Props {
   onDirtyChange?: (dirty: boolean) => void;
   /** Подсказки к параметрам: имя → ключ словаря; без него значков нет. */
   hints?: Record<string, string>;
+  /** Начальные правки из предложения LLM (G3): редактор открывается уже заполненным. */
+  initialEdits?: Record<string, ConfigEdit>;
+  /** Предложение LLM, из которого открыт редактор: Rust отметит его применённым после успеха. */
+  proposalId?: string | null;
 }
 
-export function ConfigEditor({ connection, topic, entries, onDone, onDirtyChange, hints }: Props) {
+export function ConfigEditor({ connection, topic, entries, onDone, onDirtyChange, hints, initialEdits, proposalId = null }: Props) {
   const { t } = useTranslation();
   const queryClient = useQueryClient();
-  const [edits, setEdits] = useState<Record<string, ConfigEdit>>({});
+  const [edits, setEdits] = useState<Record<string, ConfigEdit>>(() => initialEdits ?? {});
   // Снимок изменений на момент открытия проверки: дальше он не зависит от живых правок/обновления entries.
   const [review, setReview] = useState<{ changes: ConfigChange[]; rows: DiffRow[] } | null>(null);
   const [query, setQuery] = useState("");
@@ -30,7 +34,7 @@ export function ConfigEditor({ connection, topic, entries, onDone, onDirtyChange
   const invalidSet = new Set(invalid);
   const apply = useMutation({
     mutationFn: (v: { changes: ConfigChange[]; confirm: string | null }) =>
-      api.alterTopicConfigs(connection.id, topic, v.changes, v.confirm),
+      api.alterTopicConfigs(connection.id, topic, v.changes, v.confirm, proposalId),
     onSuccess: () => {
       void queryClient.invalidateQueries({ queryKey: ["topicConfigs", connection.id, topic] });
       void queryClient.invalidateQueries({ queryKey: ["topicPolicies", connection.id] });
diff --git a/src/components/groups/GroupDetails.tsx b/src/components/groups/GroupDetails.tsx
index d92907d..2473dd4 100644
--- a/src/components/groups/GroupDetails.tsx
+++ b/src/components/groups/GroupDetails.tsx
@@ -7,10 +7,11 @@ import { i18n } from "../../i18n";
 import { tr } from "../../i18n/text";
 import { formatCount } from "../../lib/format";
 import { useConnection } from "../../lib/useConnection";
-import { newConsumerKey, type ResetPrefill, tabId, useWorkspace } from "../../state/workspace";
+import { newConsumerKey, type ProposalPrefill, type ResetPrefill, tabId, useWorkspace } from "../../state/workspace";
 import { ConfirmDialog } from "../common/ConfirmDialog";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { HealthBlock } from "../health/HealthBlock";
+import { ProposalBanner } from "../health/ProposalBanner";
 import { groupTotalLag } from "./groupLag";
 import { ResetOffsetsPanel } from "./ResetOffsetsPanel";
 
@@ -26,10 +27,23 @@ interface Props {
   connectionId: string;
   groupId: string;
   resetPrefill?: ResetPrefill;
+  /** Предложение LLM сбросить оффсеты этой группы (G3): панель открывается заполненной. */
+  proposal?: ProposalPrefill;
 }
 
-export function GroupDetails({ connectionId, groupId, resetPrefill }: Props) {
+/** Заполнение панели сброса: предложение LLM по этой группе или «Сбросить группу сюда» из сообщения. */
+function resetPrefillOf(proposal: ProposalPrefill | undefined, groupId: string, resetPrefill: ResetPrefill | undefined): ResetPrefill | undefined {
+  const c = proposal?.proposal.change;
+  if (proposal && c?.kind === "resetOffsets" && c.groupId === groupId) {
+    return { strategy: c.strategy, scope: c.scope, proposalId: proposal.proposal.id, nonce: proposal.nonce };
+  }
+  return resetPrefill;
+}
+
+export function GroupDetails({ connectionId, groupId, resetPrefill: fromMessage, proposal }: Props) {
   const { t } = useTranslation();
+  const resetPrefill = resetPrefillOf(proposal, groupId, fromMessage);
+  const forReset = resetPrefill && "proposalId" in resetPrefill ? proposal : undefined;
   const connection = useConnection(connectionId);
   const canMutate = connection !== undefined && !connection.readOnly;
   const readOnly = connection?.readOnly === true;
@@ -169,7 +183,9 @@ export function GroupDetails({ connectionId, groupId, resetPrefill }: Props) {
         </>
       )}
       {panel === "reset" && connection && canMutate && (
-        <ResetOffsetsPanel
+        <>
+          <ProposalBanner prefill={forReset} />
+          <ResetOffsetsPanel
           key={resetPrefill?.nonce ?? 0}
           connection={connection}
           groupId={groupId}
@@ -177,7 +193,8 @@ export function GroupDetails({ connectionId, groupId, resetPrefill }: Props) {
           blockedReason={resetBlock}
           onApplied={refresh}
           onClose={() => setPanel(null)}
-        />
+          />
+        </>
       )}
 
       <h3>{t("groups.details.partitions")}</h3>
diff --git a/src/components/groups/ResetOffsetsPanel.tsx b/src/components/groups/ResetOffsetsPanel.tsx
index 389885c..3afb919 100644
--- a/src/components/groups/ResetOffsetsPanel.tsx
+++ b/src/components/groups/ResetOffsetsPanel.tsx
@@ -50,9 +50,18 @@ export function ResetOffsetsPanel({ connection, groupId, prefill, blockedReason
     mutationFn: () => api.previewOffsetReset(connection.id, buildResetRequest(groupId, form)),
   });
   const queryClient = useQueryClient();
+  // Форма открыта из предложения LLM: вместе с планом уходит его id и запрос, который показал предпросмотр
+  // (после любой правки формы предпросмотр сбрасывается, так что запрос всегда тот самый).
+  const proposalId = prefill && "proposalId" in prefill ? prefill.proposalId : null;
   const apply = useMutation({
     mutationFn: ({ targets, confirm }: { targets: OffsetTarget[]; confirm: string | null }) =>
-      api.resetOffsets(connection.id, groupId, targets, confirm),
+      api.resetOffsets(
+        connection.id,
+        groupId,
+        targets,
+        confirm,
+        proposalId === null ? null : { id: proposalId, request: buildResetRequest(groupId, form) },
+      ),
     onSuccess: (results) => {
       setConfirming(false);
       invalidateGroup();
diff --git a/src/components/groups/reset.ts b/src/components/groups/reset.ts
index 3cef230..6744050 100644
--- a/src/components/groups/reset.ts
+++ b/src/components/groups/reset.ts
@@ -11,8 +11,23 @@ export interface ResetForm {
   scopeText: string;
 }
 
+/** Значение `datetime-local` (местное время, с секундами) из Unix-миллисекунд. */
+export function toLocalInput(ms: number): string {
+  const d = new Date(ms);
+  const two = (n: number) => String(n).padStart(2, "0");
+  return `${d.getFullYear()}-${two(d.getMonth() + 1)}-${two(d.getDate())}T${two(d.getHours())}:${two(d.getMinutes())}:${two(d.getSeconds())}`;
+}
+
 export function defaultResetForm(prefill?: ResetPrefill): ResetForm {
   if (!prefill) return { strategy: "earliest", value: "", scope: "all", scopeText: "" };
+  if ("strategy" in prefill) {
+    // Предложение LLM: стратегия и область — в поля формы в том виде, в каком их вводит человек.
+    const s = prefill.strategy;
+    const value = s.type === "offset" ? String(s.offset) : s.type === "shift" ? String(s.delta) : s.type === "timestamp" ? toLocalInput(s.timestampMs) : "";
+    const sc = prefill.scope;
+    const scopeText = sc.type === "topics" ? sc.topics.join(", ") : sc.type === "partitions" ? sc.partitions.map((p) => `${p.topic}:${p.partition}`).join(", ") : "";
+    return { strategy: s.type, value, scope: sc.type, scopeText };
+  }
   return { strategy: "offset", value: String(prefill.offset), scope: "partitions", scopeText: `${prefill.topic}:${prefill.partition}` };
 }
 
diff --git a/src/components/topics/CreateTopicForm.tsx b/src/components/topics/CreateTopicForm.tsx
index 955bcf2..a26cae8 100644
--- a/src/components/topics/CreateTopicForm.tsx
+++ b/src/components/topics/CreateTopicForm.tsx
@@ -3,7 +3,7 @@ import { useTranslation } from "react-i18next";
 import { useMutation, useQuery } from "@tanstack/react-query";
 import { api } from "../../api/client";
 import type { ConnectionProfile, NewTopicSpec } from "../../api/types";
-import { parseProps } from "../connections/profile";
+import { formatProps, parseProps } from "../connections/profile";
 import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { Hint } from "../common/Hint";
@@ -13,20 +13,24 @@ interface Props {
   connection: ConnectionProfile;
   onCreated: (name: string) => void;
   onCancel: () => void;
+  /** Поля из предложения LLM (G3): конфиги — строками `k=v` в дополнительных свойствах, пресет «delete». */
+  initial?: NewTopicSpec;
+  /** Предложение LLM, из которого открыта форма: Rust отметит его применённым после успеха. */
+  proposalId?: string | null;
 }
 
-export function CreateTopicForm({ connection, onCreated, onCancel }: Props) {
+export function CreateTopicForm({ connection, onCreated, onCancel, initial, proposalId = null }: Props) {
   const { t } = useTranslation();
   const cluster = useQuery({
     queryKey: ["cluster", connection.id],
     queryFn: () => api.getClusterOverview(connection.id),
   });
   const brokers = cluster.data?.brokers.length;
-  const [name, setName] = useState("");
-  const [partitions, setPartitions] = useState("1");
-  const [rf, setRf] = useState<string | null>(null);
+  const [name, setName] = useState(initial?.name ?? "");
+  const [partitions, setPartitions] = useState(initial ? String(initial.partitions) : "1");
+  const [rf, setRf] = useState<string | null>(initial ? String(initial.replicationFactor) : null);
   const [preset, setPreset] = useState("delete");
-  const [extra, setExtra] = useState("");
+  const [extra, setExtra] = useState(initial ? formatProps(initial.configs) : "");
   const [confirming, setConfirming] = useState(false);
 
   // RF по умолчанию — min(3, число брокеров), пока пользователь не ввёл своё значение.
@@ -51,7 +55,7 @@ export function CreateTopicForm({ connection, onCreated, onCancel }: Props) {
   const valid = !nameError && !extraError && !numbersError;
 
   const create = useMutation({
-    mutationFn: (confirm: string | null) => api.createTopic(connection.id, spec, confirm),
+    mutationFn: (confirm: string | null) => api.createTopic(connection.id, spec, confirm, proposalId),
     onSuccess: () => onCreated(name),
   });
 
diff --git a/src/components/topics/TopicActions.tsx b/src/components/topics/TopicActions.tsx
index baf8f01..f3aaaf9 100644
--- a/src/components/topics/TopicActions.tsx
+++ b/src/components/topics/TopicActions.tsx
@@ -1,12 +1,14 @@
-import { useState } from "react";
+import { useEffect, useState } from "react";
 import { useTranslation } from "react-i18next";
 import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
 import { api, toAppError } from "../../api/client";
 import type { ConnectionProfile, TopicDetails } from "../../api/types";
 import { i18n } from "../../i18n";
 import { tr } from "../../i18n/text";
+import type { ProposalPrefill } from "../../state/workspace";
 import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
 import { ErrorBanner } from "../common/ErrorBanner";
+import { ProposalBanner } from "../health/ProposalBanner";
 import { DeleteTopicsPanel } from "./DeleteTopicsPanel";
 
 type Action = "partitions" | "purge" | "delete" | null;
@@ -15,6 +17,8 @@ interface Props {
   connection: ConnectionProfile;
   topic: TopicDetails;
   onDeleted: () => void;
+  /** Предложение LLM добавить партиции (G3): диалог открывается с его числом. */
+  proposal?: ProposalPrefill;
 }
 
 function useInvalidateTopic(connectionId: string, topic: string) {
@@ -36,9 +40,13 @@ export function purgeBlockReason(cleanup: string | null): string | null {
   return i18n.t("topics.actions.purgeBlocked", { policy: cleanup });
 }
 
-export function TopicActions({ connection, topic, onDeleted }: Props) {
+export function TopicActions({ connection, topic, onDeleted, proposal }: Props) {
   const { t } = useTranslation();
-  const [action, setAction] = useState<Action>(null);
+  const [action, setAction] = useState<Action>(proposal ? "partitions" : null);
+  // Предложение LLM добавить партиции (новый nonce) открывает диалог с его числом.
+  useEffect(() => {
+    if (proposal) setAction("partitions");
+  }, [proposal]);
   const policy = useQuery({
     queryKey: ["topicPolicies", connection.id, [topic.name]],
     queryFn: () => api.getTopicPolicies(connection.id, [topic.name]),
@@ -65,7 +73,19 @@ export function TopicActions({ connection, topic, onDeleted }: Props) {
         </button>
       </div>
       {purgeBlock && <p className="warn">{purgeBlock}</p>}
-      {action === "partitions" && <AddPartitions connection={connection} topic={topic} onClose={() => setAction(null)} />}
+      {action === "partitions" && (
+        <>
+          <ProposalBanner prefill={proposal} />
+          <AddPartitions
+            key={proposal?.nonce ?? 0}
+            connection={connection}
+            topic={topic}
+            initialTotal={proposal?.proposal.change.kind === "addPartitions" ? proposal.proposal.change.newTotal : undefined}
+            proposalId={proposal?.proposal.id ?? null}
+            onClose={() => setAction(null)}
+          />
+        </>
+      )}
       {action === "purge" && <Purge connection={connection} topic={topic} cleanup={cleanup} policyError={policy.error} onClose={() => setAction(null)} />}
       {action === "delete" && (
         <DeleteTopicsPanel
@@ -81,17 +101,30 @@ export function TopicActions({ connection, topic, onDeleted }: Props) {
   );
 }
 
-function AddPartitions({ connection, topic, onClose }: { connection: ConnectionProfile; topic: TopicDetails; onClose: () => void }) {
+function AddPartitions({
+  connection,
+  topic,
+  onClose,
+  initialTotal,
+  proposalId = null,
+}: {
+  connection: ConnectionProfile;
+  topic: TopicDetails;
+  onClose: () => void;
+  /** Число из предложения LLM (G3). */
+  initialTotal?: number;
+  proposalId?: string | null;
+}) {
   const { t } = useTranslation();
   const invalidate = useInvalidateTopic(connection.id, topic.name);
   const current = topic.partitions.length;
-  const [value, setValue] = useState(String(current + 1));
+  const [value, setValue] = useState(String(initialTotal ?? current + 1));
   // Снимок цели на момент открытия подтверждения.
   const [confirming, setConfirming] = useState<number | null>(null);
   const n = Number(value);
   const valid = Number.isInteger(n) && n > current;
   const run = useMutation({
-    mutationFn: (v: { total: number; confirm: string | null }) => api.addPartitions(connection.id, topic.name, v.total, v.confirm),
+    mutationFn: (v: { total: number; confirm: string | null }) => api.addPartitions(connection.id, topic.name, v.total, v.confirm, proposalId),
     onSuccess: () => {
       invalidate();
       onClose();
diff --git a/src/components/topics/TopicDetails.tsx b/src/components/topics/TopicDetails.tsx
index 435ef21..e543c17 100644
--- a/src/components/topics/TopicDetails.tsx
+++ b/src/components/topics/TopicDetails.tsx
@@ -1,4 +1,4 @@
-import { useState } from "react";
+import { useEffect, useState } from "react";
 import { useTranslation } from "react-i18next";
 import { useQuery, useQueryClient } from "@tanstack/react-query";
 import { api } from "../../api/client";
@@ -7,13 +7,15 @@ import { tr } from "../../i18n/text";
 import { formatCount } from "../../lib/format";
 import { useConnection } from "../../lib/useConnection";
 import { useHelpChapter } from "../../state/helpContext";
-import { newConsumerKey, tabId, useWorkspace } from "../../state/workspace";
+import { newConsumerKey, type ProposalPrefill, tabId, useWorkspace } from "../../state/workspace";
 import { BrowsePanel } from "../browse/BrowsePanel";
 import { ConfigEditor } from "../common/ConfigEditor";
+import type { ConfigEdit } from "../common/configDiff";
 import { ConfigTable } from "../common/ConfigTable";
 import { TOPIC_CONFIG_HINTS } from "../common/topicConfigHints";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { HealthBlock } from "../health/HealthBlock";
+import { ProposalBanner } from "../health/ProposalBanner";
 import { TopicActions } from "./TopicActions";
 import { TopicGroups } from "./TopicGroups";
 
@@ -25,9 +27,25 @@ const SUBTABS: { id: SubTab; labelKey: string }[] = [
   { id: "groups", labelKey: "topics.details.tabGroups" },
 ];
 
-export function TopicDetails({ connectionId, topic }: { connectionId: string; topic: string }) {
+/** Предложение LLM по конфигам этого топика (G3); другие виды здесь не открывают редактор. */
+function configProposal(prefill: ProposalPrefill | undefined, topic: string): ProposalPrefill | undefined {
+  const c = prefill?.proposal.change;
+  return c?.kind === "alterTopicConfigs" && c.topic === topic ? prefill : undefined;
+}
+
+function partitionsProposal(prefill: ProposalPrefill | undefined, topic: string): ProposalPrefill | undefined {
+  const c = prefill?.proposal.change;
+  return c?.kind === "addPartitions" && c.topic === topic ? prefill : undefined;
+}
+
+export function TopicDetails({ connectionId, topic, proposal }: { connectionId: string; topic: string; proposal?: ProposalPrefill }) {
   const { t: tt } = useTranslation();
-  const [sub, setSub] = useState<SubTab>("messages");
+  const forConfigs = configProposal(proposal, topic);
+  const [sub, setSub] = useState<SubTab>(forConfigs ? "configs" : "messages");
+  // Новое предложение по конфигам (другой nonce) переключает на подвкладку «Конфигурация».
+  useEffect(() => {
+    if (forConfigs) setSub("configs");
+  }, [forConfigs]);
   useHelpChapter(tabId({ kind: "topic", connectionId, topic }), sub === "messages" ? "browse" : "topics");
   const q = useQuery({ queryKey: ["topic", connectionId, topic], queryFn: () => api.getTopic(connectionId, topic) });
 
@@ -84,6 +102,7 @@ export function TopicDetails({ connectionId, topic }: { connectionId: string; to
         <TopicActions
           connection={connection}
           topic={t}
+          proposal={partitionsProposal(proposal, topic)}
           onDeleted={() => {
             void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
             closeTab(tabId({ kind: "topic", connectionId, topic }));
@@ -102,7 +121,7 @@ export function TopicDetails({ connectionId, topic }: { connectionId: string; to
         <BrowsePanel connectionId={connectionId} topic={topic} />
       </div>
       {sub === "partitions" && <PartitionsTable topic={t} />}
-      {sub === "configs" && <TopicConfigs connectionId={connectionId} topic={topic} />}
+      {sub === "configs" && <TopicConfigs connectionId={connectionId} topic={topic} proposal={forConfigs} />}
       {sub === "groups" && <TopicGroups connectionId={connectionId} topic={topic} />}
     </div>
   );
@@ -142,11 +161,22 @@ function PartitionsTable({ topic }: { topic: TopicDetailsDto }) {
   );
 }
 
-function TopicConfigs({ connectionId, topic }: { connectionId: string; topic: string }) {
+/** Правки редактора из предложения: `value` — установить, `null` — вернуть значение по умолчанию. */
+function editsOf(prefill: ProposalPrefill | undefined): Record<string, ConfigEdit> | undefined {
+  const c = prefill?.proposal.change;
+  if (c?.kind !== "alterTopicConfigs") return undefined;
+  return Object.fromEntries(c.changes.map((x) => [x.name, x.value === null ? { kind: "reset" } : { kind: "set", value: x.value }]));
+}
+
+function TopicConfigs({ connectionId, topic, proposal }: { connectionId: string; topic: string; proposal?: ProposalPrefill }) {
   const { t } = useTranslation();
   const connection = useConnection(connectionId);
-  const [editing, setEditing] = useState(false);
+  const [editing, setEditing] = useState(proposal !== undefined);
   const [dirty, setDirty] = useState(false);
+  // Предложение (новый nonce) включает редактор; редактор пересоздаётся по nonce с новыми правками.
+  useEffect(() => {
+    if (proposal) setEditing(true);
+  }, [proposal]);
   const q = useQuery({
     queryKey: ["topicConfigs", connectionId, topic],
     queryFn: () => api.getTopicConfigs(connectionId, topic),
@@ -169,7 +199,20 @@ function TopicConfigs({ connectionId, topic }: { connectionId: string; topic: st
         </div>
       )}
       {editing && canEdit && connection ? (
-        <ConfigEditor connection={connection} topic={topic} entries={q.data} onDone={() => setEditing(false)} onDirtyChange={setDirty} hints={TOPIC_CONFIG_HINTS} />
+        <>
+          <ProposalBanner prefill={proposal} />
+          <ConfigEditor
+            key={proposal?.nonce ?? 0}
+            connection={connection}
+            topic={topic}
+            entries={q.data}
+            onDone={() => setEditing(false)}
+            onDirtyChange={setDirty}
+            hints={TOPIC_CONFIG_HINTS}
+            initialEdits={editsOf(proposal)}
+            proposalId={proposal?.proposal.id ?? null}
+          />
+        </>
       ) : (
         <ConfigTable entries={q.data} hints={TOPIC_CONFIG_HINTS} />
       )}
diff --git a/src/components/topics/TopicList.tsx b/src/components/topics/TopicList.tsx
index efcda55..5b9a28b 100644
--- a/src/components/topics/TopicList.tsx
+++ b/src/components/topics/TopicList.tsx
@@ -1,4 +1,4 @@
-import { useMemo, useRef, useState } from "react";
+import { useEffect, useMemo, useRef, useState } from "react";
 import { useTranslation } from "react-i18next";
 import { useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
 import { useVirtualizer } from "@tanstack/react-virtual";
@@ -6,12 +6,13 @@ import { api, toAppError } from "../../api/client";
 import { tr } from "../../i18n/text";
 import { activateProps } from "../../lib/activate";
 import { formatCount } from "../../lib/format";
-import { useWorkspace } from "../../state/workspace";
+import { type ProposalPrefill, useWorkspace } from "../../state/workspace";
 import { useConnection } from "../../lib/useConnection";
 import { CreateTopicForm } from "./CreateTopicForm";
 import { DeleteTopicsPanel } from "./DeleteTopicsPanel";
 import { ErrorBanner } from "../common/ErrorBanner";
 import { HealthBadge } from "../health/HealthBlock";
+import { ProposalBanner } from "../health/ProposalBanner";
 import { useDebouncedValue } from "../../lib/useDebouncedValue";
 import {
   buildCounts,
@@ -28,8 +29,14 @@ const REFRESH_OPTIONS = [
   { labelKey: "topics.list.every30", value: 30_000 },
 ];
 
-export function TopicList({ connectionId }: { connectionId: string }) {
+/** Предложение LLM создать топик (G3); другие виды форму не открывают. */
+function createProposal(prefill: ProposalPrefill | undefined): ProposalPrefill | undefined {
+  return prefill?.proposal.change.kind === "createTopic" ? prefill : undefined;
+}
+
+export function TopicList({ connectionId, proposal }: { connectionId: string; proposal?: ProposalPrefill }) {
   const { t: tt } = useTranslation();
+  const forCreate = createProposal(proposal);
   const [query, setQuery] = useState("");
   const [showInternal, setShowInternal] = useState(false);
   const [refreshMs, setRefreshMs] = useState(0);
@@ -38,7 +45,11 @@ export function TopicList({ connectionId }: { connectionId: string }) {
   const open = useWorkspace((s) => s.open);
   const connection = useConnection(connectionId);
   const canMutate = connection !== undefined && !connection.readOnly;
-  const [creating, setCreating] = useState(false);
+  const [creating, setCreating] = useState(forCreate !== undefined);
+  // Новое предложение (другой nonce) открывает форму заново; форма пересоздаётся по nonce.
+  useEffect(() => {
+    if (forCreate) setCreating(true);
+  }, [forCreate]);
   const [selecting, setSelecting] = useState(false);
   const [selected, setSelected] = useState<Set<string>>(new Set());
   const [deleteTargets, setDeleteTargets] = useState<string[] | null>(null);
@@ -198,15 +209,30 @@ export function TopicList({ connectionId }: { connectionId: string }) {
         </span>
       </div>
       {creating && connection && (
-        <CreateTopicForm
-          connection={connection}
-          onCancel={() => setCreating(false)}
-          onCreated={(name) => {
-            setCreating(false);
-            void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
-            open({ kind: "topic", connectionId, topic: name });
-          }}
-        />
+        <>
+          <ProposalBanner prefill={forCreate} />
+          <CreateTopicForm
+            key={forCreate?.nonce ?? 0}
+            connection={connection}
+            initial={
+              forCreate?.proposal.change.kind === "createTopic"
+                ? {
+                    name: forCreate.proposal.change.name,
+                    partitions: forCreate.proposal.change.partitions,
+                    replicationFactor: forCreate.proposal.change.replicationFactor,
+                    configs: forCreate.proposal.change.configs,
+                  }
+                : undefined
+            }
+            proposalId={forCreate?.proposal.id ?? null}
+            onCancel={() => setCreating(false)}
+            onCreated={(name) => {
+              setCreating(false);
+              void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
+              open({ kind: "topic", connectionId, topic: name });
+            }}
+          />
+        </>
       )}
       {deleting && connection && (
         <DeleteTopicsPanel
diff --git a/src/state/workspace.ts b/src/state/workspace.ts
index 0500f70..7b2e6dd 100644
--- a/src/state/workspace.ts
+++ b/src/state/workspace.ts
@@ -1,10 +1,16 @@
 import { create } from "zustand";
 
-import type { ProduceMessage, ProducerSettings, Proposal } from "../api/types";
+import type { ProduceMessage, ProducerSettings, Proposal, ResetScope, ResetStrategy } from "../api/types";
 
 export type ProducerPrefill = { message: ProduceMessage; settings?: ProducerSettings; nonce: number };
 
-export type ResetPrefill = { topic: string; partition: number; offset: number; nonce: number };
+/**
+ * Заполнение формы сброса оффсетов: из просмотра сообщений («Сбросить группу сюда») или из
+ * предложения LLM (стратегия и область; `proposalId` уходит в команду вместе с запросом предпросмотра).
+ */
+export type ResetPrefill =
+  | { topic: string; partition: number; offset: number; nonce: number }
+  | { strategy: ResetStrategy; scope: ResetScope; proposalId: string; nonce: number };
 
 /** Предложение LLM, которое открывает заполненную форму вкладки (G3); `nonce` — повторное открытие обновляет заполнение. */
 export type ProposalPrefill = { proposal: Proposal; nonce: number };
````

- [ ] **Step 4: Тесты проходят.**

Run: `pnpm test` — Expected: `Test Files 69 passed`, `Tests 458 passed`; `pnpm exec tsc --noEmit`, `pnpm build`, `pnpm i18n:check` — без замечаний.

- [ ] **Step 5: Commit.**

````bash
git add src/components/health/ProposalBanner.tsx src/components/TabContent.tsx src/components/common/ConfigEditor.tsx src/components/common/ConfigEditor.test.tsx src/components/topics/CreateTopicForm.tsx src/components/topics/CreateTopicForm.test.tsx src/components/topics/TopicActions.tsx src/components/topics/TopicActions.test.tsx src/components/topics/TopicDetails.tsx src/components/topics/TopicDetails.test.tsx src/components/topics/TopicList.tsx src/components/topics/TopicList.test.tsx src/components/groups/GroupDetails.tsx src/components/groups/GroupDetails.test.tsx src/components/groups/ResetOffsetsPanel.tsx src/components/groups/ResetOffsetsPanel.test.tsx src/components/groups/reset.ts src/components/groups/reset.test.ts src/state/workspace.ts src/api/client.test.ts
git commit -m "feat(ui): формы из предложения LLM — редактор конфигов, создание топика, партиции, сброс оффсетов; proposalId в командах

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 10: Руководство на 12 языках: раздел «Предложения LLM», глава `llm-mcp`; глоссарий; README

**Files:**
- Modify: `docs/guide/<12 языков>/health-check.md` (раздел `{#proposals}` перед `{#with-llm}`), `docs/guide/<12 языков>/llm-mcp.md` (заголовок и абзац раздела `{#health}`, пункт в `{#what-llm-sees}`), `docs/i18n/glossary.md` (четыре строки терминов), `README.md`

**Interfaces:**
- Consumes: метки `[[ui:health.proposals.title]]`, `[[ui:health.proposals.openForm]]`, `[[ui:health.proposals.reject]]`, `[[ui:health.reopen]]`, `[[ui:common.delete]]`, `[[ui:health.proposals.deleteAll]]`, `[[ui:sidebar.findings]]` (все ключи существуют после задач 7–8).
- Produces: раздел `## … {#proposals}` в каждой главе `health-check` (одинаковый якорь во всех языках); раздел `{#health}` главы `llm-mcp` переименован («Проверка кластера, замечания и предложения») и дополнен абзацем о `propose_change`/`list_proposals`; в «Что видит ассистент» — пункт о предложениях и решении; глоссарий: proposal, open in form, applied / applied with edits, reject / withdraw; README — абзацы в «LLM-ассистенты (MCP)», «Проверка кластера и замечания» и «Где хранятся данные».

Спец. G3 §10. Правила глав — `docs/i18n/README.md` («Руководство»): якорь только у `##`, метки `[[ui:…]]`, во французском неразрывные пробелы перед `:`/`;`/`?`, в zh/ja метки без пробелов вокруг. Патч ниже — готовые тексты всех 12 языков; `pnpm i18n:check` проверяет якоря, метки и типографику.

- [ ] **Step 1: Правки.**

Патч руководства, глоссария и README:

<!-- block: patch task10-src.diff -->
````diff
diff --git a/README.md b/README.md
index 939c517..c2a4dec 100644
--- a/README.md
+++ b/README.md
@@ -63,7 +63,7 @@ Windows: если падает `librdkafka_is_built_with_every_compression_codec
   macOS может один раз спросить доступ к старым записям Keychain.
 - Шаблоны сообщений: `templates.json`; история отправки — `history/<id подключения>.json` (последние 100, значения до 64 КБ). История хранится открытым текстом и содержит уже отрисованные сообщения: значения, подставленные из `{{env NAME}}`, и значения заголовков (например, токены) записываются как есть. Файлы данных создаются с правами только для владельца (0600, каталог `history/` — 0700 на macOS/Linux). Если `templates.json` не читается (например, создан более новой версией), недоступны только шаблоны — ошибка показывается в панели шаблонов.
 - MCP: `mcp.json` (адрес и токен сервера, пока он работает) и журнал вызовов `mcp-audit.jsonl` (+ `mcp-audit.1.jsonl`, ротация при 5 МБ) — в каталоге данных; содержимое ответов LLM в журнал не пишется. Сокет — `mcp.sock` там же, а если путь длиннее 100 байт, — `glafira-kafka-mcp-….sock` в `$XDG_RUNTIME_DIR`, а если он не задан, — во временном каталоге; на Windows вместо сокета — именованный канал `\\.\pipe\glafira-kafka-mcp-…`. На macOS/Linux файлы и сокет доступны только владельцу (0600).
-- Проверка кластера и замечания: `health/<id подключения>.json` — последняя проверка, скрытые находки и замечания LLM (каждое живёт 7 дней с создания, открытых — до 200). Каталог `health/` — 0700, файлы — 0600 на macOS/Linux. Удаляется вместе с подключением, в экспорт и импорт не входит; повреждённый файл переименовывается в `<id>.json.corrupt-<время>`.
+- Проверка кластера, замечания и предложения: `health/<id подключения>.json` — последняя проверка, скрытые находки, замечания LLM (каждое живёт 7 дней с создания, открытых — до 200) и предложения LLM (7 дней, открытых — до 50). Каталог `health/` — 0700, файлы — 0600 на macOS/Linux. Удаляется вместе с подключением, в экспорт и импорт не входит; повреждённый файл переименовывается в `<id>.json.corrupt-<время>`.
 - Логи: `RUST_LOG=debug pnpm tauri dev`.
 
 ## Защита от изменений
@@ -107,6 +107,7 @@ kubectl get secret kafka-client -o jsonpath='{.data.ca\.crt}' | base64 -d > ~/ce
 ## LLM-ассистенты (MCP)
 - Glafira — MCP-шлюз, который не меняет кластер: LLM-клиент (Claude Desktop, Claude Code, Cursor) запускает `glafira-kafka mcp`, тот соединяется с запущенной Glafira через локальный сокет (Windows — named pipe) и читает метаданные подключений с флажком «Доступ для LLM (только метаданные)». Изменить кластер через шлюз нельзя: замечания LLM пишутся только в файл Glafira; сообщения, пароли и настройки безопасности не передаются.
 - Включается в «Настройки» → «LLM-ассистенты (MCP)», там же — фрагменты конфига клиентов, подтверждённые клиенты, сессии и журнал. Новый клиент подтверждается диалогом. Политика `mcpEnabled = false` запрещает MCP (`docs/admin.md`).
+- Предложения изменений (`propose_change`, `list_proposals`, `withdraw_proposal`): LLM предлагает конфиги топика, новый топик, партиции или сброс оффсетов; предложение пишется в файл Glafira и показывается рядом с объектом, а применяет его человек заполненной формой с обычными стражами (read-only, ввод имени для prod). Статус «применено» ставит Rust после успеха операции. Удаление и очистку предложить нельзя. Спецификация: `docs/superpowers/specs/2026-10-22-glafira-proposals-g3-design.md`.
 - Протокол, инструменты и лимиты — крейт `crates/glafira-mcp` (не зависит от rdkafka и приложения); связь с приложением — `src-tauri/src/mcp/`. Спецификация: `docs/superpowers/specs/2026-10-18-glafira-mcp-gateway-g1-design.md`.
 - Разработка: отладочный бинарь — `src-tauri/target/debug/kafka-explorer` (`… mcp` — режим шима); Glafira запускайте через `pnpm tauri dev` до подключения клиента — шим не поднимет dev-сервер Vite.
 
@@ -114,6 +115,7 @@ kubectl get secret kafka-client -o jsonpath='{.data.ca\.crt}' | base64 -d > ~/ce
 - «Проверить кластер» (обзор кластера или вкладка «Замечания» у подключения) запускает встроенные правила: партиции и репликация, конфиги топиков и брокеров, consumer groups. Строгость зависит от окружения подключения. Проверка только читает кластер: до 2000 топиков, 500 групп и 45 секунд; запускается по кнопке или по запросу LLM (`health_check`), сама не повторяется.
 - Находки видны на вкладке «Замечания», значками в дереве и списках топиков и групп и блоком на экранах топика и группы. Находку можно скрыть для объекта.
 - LLM-ассистент через MCP оставляет замечания (`annotate`, `list_annotations`, `remove_annotation`): они пишутся в Glafira, кластер не меняется. Текст показывается как есть, с подписью клиента.
+- Предложения LLM на той же вкладке: карточка с обоснованием и описанием изменения, «Открыть в форме» открывает редактор конфигов, создание топика, диалог партиций или сброс оффсетов уже заполненными; «Отклонить» — с необязательной причиной, которую видит модель.
 - Код: правила и снимок — `src-tauri/src/health/` (правила — чистые функции над `ClusterSnapshot`), инструменты — `crates/glafira-mcp/src/server/health_tools.rs`. Спецификация: `docs/superpowers/specs/2026-10-19-glafira-health-annotations-g2-design.md`.
 
 ## Языки
diff --git a/docs/guide/de/health-check.md b/docs/guide/de/health-check.md
index 9cb7cf6..3f0be6b 100644
--- a/docs/guide/de/health-check.md
+++ b/docs/guide/de/health-check.md
@@ -37,6 +37,15 @@ Ein über MCP verbundener LLM-Assistent (siehe [LLM-Assistenten (MCP)](llm-mcp.m
 - [[ui:common.delete]] entfernt eine Anmerkung, [[ui:health.deleteAll]] entfernt alle Anmerkungen der Verbindung.
 - Eine Anmerkung wird 7 Tage nach dem Erstellen automatisch entfernt. Eine Verbindung kann höchstens 200 offene Anmerkungen haben.
 
+## LLM-Vorschläge {#proposals}
+
+Neben Anmerkungen kann ein LLM-Assistent eine Änderung vorschlagen: Topic-Konfiguration, ein neues Topic, mehr Partitionen oder das Zurücksetzen der Offsets einer leeren Consumer Group. Ein Vorschlag erscheint unter [[ui:health.proposals.title]] im Tab [[ui:sidebar.findings]] und auf dem Bildschirm des Topics oder der Gruppe — mit der Begründung des Assistenten, einer Beschreibung der Änderung und dem Namen des Clients. Von selbst wird nichts angewendet; Löschen und Leeren lassen sich gar nicht vorschlagen.
+
+- [[ui:health.proposals.openForm]] öffnet den Tab des Objekts mit dem gewohnten, bereits ausgefüllten Formular: dem Konfigurationseditor mit Vorher-nachher-Prüfung, dem Formular zum Anlegen eines Topics, dem Partitionsdialog oder dem Zurücksetzen der Offsets mit Vorschau. Ändern Sie die Werte bei Bedarf und bestätigen Sie wie gewohnt; bei einer Prod-Verbindung fragt Glafira nach ihrem Namen. Nach einer erfolgreichen Änderung wird der Vorschlag als angewendet markiert (mit Änderungen, falls Sie etwas geändert haben), und der Assistent sieht das in `list_proposals`.
+- [[ui:health.proposals.reject]] schließt einen Vorschlag; Sie können einen Grund angeben, den der Assistent sieht. [[ui:health.reopen]] holt einen abgelehnten Vorschlag zurück.
+- Bei einer schreibgeschützten Verbindung bleibt der Vorschlag erhalten, aber [[ui:health.proposals.openForm]] ist nicht verfügbar, bis Sie den Schreibschutz im Verbindungsformular aufheben; der Assistent erfährt das ebenfalls.
+- [[ui:common.delete]] entfernt einen Vorschlag, [[ui:health.proposals.deleteAll]] entfernt alle Vorschläge der Verbindung. Ein Vorschlag wird 7 Tage nach dem Erstellen automatisch entfernt; eine Verbindung kann höchstens 50 offene Vorschläge haben.
+
 ## Mit einem LLM-Assistenten {#with-llm}
 
 Der Assistent führt dieselbe Prüfung mit dem Tool `health_check` aus und sieht dieselben Befunde, auch ausgeblendete, die markiert sind. Ist die letzte Prüfung jünger als 5 Minuten, erhält der Assistent ihr Ergebnis, ohne den Cluster erneut abzufragen. Bitten Sie zum Beispiel: „Prüfe den Zustand des Clusters und hinterlasse Anmerkungen zu den wichtigsten Problemen.“
diff --git a/docs/guide/de/llm-mcp.md b/docs/guide/de/llm-mcp.md
index f75a48d..5d0da5d 100644
--- a/docs/guide/de/llm-mcp.md
+++ b/docs/guide/de/llm-mcp.md
@@ -42,7 +42,8 @@ Der Assistent sieht:
 - Broker mit Hosts und Ports, den Controller;
 - Topics, Partitionen, Replikate, ISR, Offsets, die ungefähre Anzahl der Nachrichten;
 - Konfigurationen von Brokern und Topics, wobei Passwörter und andere geheime Werte verborgen sind;
-- Consumer Groups: Status, Protokoll, Koordinator, Mitglieder mit Client-ID, Host und `group.instance.id`, Zuweisungen, committete Offsets und Lag.
+- Consumer Groups: Status, Protokoll, Koordinator, Mitglieder mit Client-ID, Host und `group.instance.id`, Zuweisungen, committete Offsets und Lag;
+- seine Vorschläge und Ihre Entscheidung dazu: angewendet (unverändert oder mit Änderungen), abgelehnt mit Ihrem Grund, zurückgezogen.
 
 Der Assistent sieht keine Nachrichten in Topics, keine Bootstrap-Zeichenfolge und keine Sicherheitseinstellungen einer Verbindung, keine Passwörter, Zertifikate und zusätzlichen librdkafka-Eigenschaften, keinen Sendeverlauf und keine Vorlagen.
 
@@ -61,10 +62,12 @@ Solange ein Client verbunden ist, erscheint unten im linken Bereich die Anzeige
 
 Um MCP ganz auszuschalten, deaktivieren Sie [[ui:settings.mcp.enable]]: Die Clients werden getrennt.
 
-## Clusterprüfung und Anmerkungen {#health}
+## Clusterprüfung, Anmerkungen und Vorschläge {#health}
 
 Der Assistent kann die integrierte Clusterprüfung von Glafira (Tool `health_check`) ausführen und sich auf ihre Befunde stützen. Er kann außerdem Anmerkungen zu Objekten hinterlassen: Sie erscheinen im Tab [[ui:sidebar.findings]] und auf den Bildschirmen von Topics und Gruppen, der Cluster wird dabei nicht geändert. Siehe [Clusterprüfung und Anmerkungen](health-check.md#with-llm).
 
+Der Assistent kann mit dem Tool `propose_change` auch eine Änderung vorschlagen: Topic-Konfiguration, ein neues Topic, mehr Partitionen oder das Zurücksetzen von Offsets. Der Vorschlag erscheint neben dem Objekt; Sie wenden ihn selbst in einem ausgefüllten Formular an oder lehnen ihn ab, und der Assistent sieht Ihre Entscheidung in `list_proposals`. Löschen und Leeren lassen sich nicht vorschlagen. Siehe [LLM-Vorschläge](health-check.md#proposals).
+
 ## Wenn es nicht funktioniert {#troubleshooting}
 
 Der Client zeigt eine Meldung von Glafira:
diff --git a/docs/guide/en/health-check.md b/docs/guide/en/health-check.md
index 610032d..66dbb0a 100644
--- a/docs/guide/en/health-check.md
+++ b/docs/guide/en/health-check.md
@@ -37,6 +37,15 @@ An LLM assistant connected over MCP (see [LLM assistants (MCP)](llm-mcp.md#how-i
 - [[ui:common.delete]] removes a note, and [[ui:health.deleteAll]] removes every note of the connection.
 - A note is removed automatically 7 days after it was created. A connection can have at most 200 open notes.
 
+## LLM proposals {#proposals}
+
+Besides notes, an LLM assistant can propose a change: topic configs, a new topic, more partitions or an offset reset of an empty consumer group. A proposal appears in [[ui:health.proposals.title]] on the [[ui:sidebar.findings]] tab and on the topic or group screen, with the assistant's rationale, a description of the change and the client name. Nothing is applied by itself, and deleting or purging cannot be proposed at all.
+
+- [[ui:health.proposals.openForm]] opens the object's tab with the usual form already filled in: the config editor with its before/after review, the create-topic form, the partitions dialog or the offset reset with a preview. Edit the values if you like and confirm as usual; on a prod connection Glafira asks for the connection name. After a successful change the proposal is marked as applied (with edits, if you changed something), and the assistant sees that in `list_proposals`.
+- [[ui:health.proposals.reject]] closes a proposal; you may add a reason, and the assistant will see it. [[ui:health.reopen]] brings a rejected proposal back.
+- On a read-only connection the proposal is kept, but [[ui:health.proposals.openForm]] is unavailable until you clear read-only in the connection form; the assistant is told about that too.
+- [[ui:common.delete]] removes a proposal, [[ui:health.proposals.deleteAll]] removes every proposal of the connection. A proposal is removed automatically 7 days after it was created; a connection can have at most 50 open proposals.
+
 ## With an LLM assistant {#with-llm}
 
 The assistant runs the same check with the `health_check` tool and sees the same findings, including hidden ones, which are marked. If the last check is less than 5 minutes old, the assistant gets its result without querying the cluster again. Ask, for example: "Check the health of the cluster and leave notes on the most important problems."
diff --git a/docs/guide/en/llm-mcp.md b/docs/guide/en/llm-mcp.md
index 97ab394..9165833 100644
--- a/docs/guide/en/llm-mcp.md
+++ b/docs/guide/en/llm-mcp.md
@@ -42,7 +42,8 @@ The assistant sees:
 - brokers with hosts and ports, the controller;
 - topics, partitions, replicas, ISR, offsets, approximate message counts;
 - broker and topic configs, with passwords and other secret values hidden;
-- consumer groups: state, protocol, coordinator, members with client id, host and `group.instance.id`, assignments, committed offsets and lag.
+- consumer groups: state, protocol, coordinator, members with client id, host and `group.instance.id`, assignments, committed offsets and lag;
+- its proposals and your decision on them: applied (unchanged or with edits), rejected with your reason, withdrawn.
 
 The assistant does not see topic messages, the bootstrap string or the security settings of a connection, passwords, certificates, additional librdkafka properties, the send history or templates.
 
@@ -61,10 +62,12 @@ While a client is connected, an `LLM` indicator with the client name is shown at
 
 To turn MCP off completely, clear [[ui:settings.mcp.enable]]: clients are disconnected.
 
-## Cluster check and notes {#health}
+## Cluster check, notes and proposals {#health}
 
 The assistant can run Glafira's built-in cluster check (the `health_check` tool) and rely on its findings. It can also leave notes about objects: they appear on the [[ui:sidebar.findings]] tab and on the topic and group screens, and the cluster is not changed. See [Cluster check and notes](health-check.md#with-llm).
 
+The assistant can also propose a change with the `propose_change` tool: topic configs, a new topic, more partitions or an offset reset. The proposal appears next to the object, and you apply it yourself in a pre-filled form or reject it; the assistant then sees your decision in `list_proposals`. Deleting and purging cannot be proposed. See [LLM proposals](health-check.md#proposals).
+
 ## If it does not work {#troubleshooting}
 
 The client shows a message from Glafira:
diff --git a/docs/guide/es/health-check.md b/docs/guide/es/health-check.md
index ea43104..f279362 100644
--- a/docs/guide/es/health-check.md
+++ b/docs/guide/es/health-check.md
@@ -37,6 +37,15 @@ Un asistente LLM conectado por MCP (consulta [Asistentes LLM (MCP)](llm-mcp.md#h
 - [[ui:common.delete]] elimina una nota y [[ui:health.deleteAll]] elimina todas las notas de la conexión.
 - Una nota se elimina automáticamente 7 días después de crearse. Una conexión puede tener como máximo 200 notas abiertas.
 
+## Propuestas LLM {#proposals}
+
+Además de notas, un asistente LLM puede proponer un cambio: la configuración de un topic, un topic nuevo, más particiones o el restablecimiento de los offsets de un consumer group vacío. La propuesta aparece en [[ui:health.proposals.title]] de la pestaña [[ui:sidebar.findings]] y en la pantalla del topic o del grupo, con la justificación del asistente, una descripción del cambio y el nombre del cliente. Nada se aplica por sí solo; eliminar y vaciar no se pueden proponer en absoluto.
+
+- [[ui:health.proposals.openForm]] abre la pestaña del objeto con el formulario habitual ya relleno: el editor de configuración con la revisión “antes → después”, el formulario de creación de topic, el diálogo de particiones o el restablecimiento de offsets con vista previa. Edita los valores si quieres y confirma como siempre; en una conexión prod, Glafira pide el nombre de la conexión. Tras un cambio correcto la propuesta se marca como aplicada (con cambios, si modificaste algo), y el asistente lo ve en `list_proposals`.
+- [[ui:health.proposals.reject]] cierra una propuesta; puedes indicar un motivo, y el asistente lo verá. [[ui:health.reopen]] recupera una propuesta rechazada.
+- En una conexión de solo lectura la propuesta se conserva, pero [[ui:health.proposals.openForm]] no está disponible hasta que quites la solo lectura en el formulario de conexión; al asistente también se le informa.
+- [[ui:common.delete]] elimina una propuesta, [[ui:health.proposals.deleteAll]] elimina todas las propuestas de la conexión. Una propuesta se elimina automáticamente 7 días después de crearse; una conexión puede tener como máximo 50 propuestas abiertas.
+
 ## Con un asistente LLM {#with-llm}
 
 El asistente ejecuta la misma comprobación con la herramienta `health_check` y ve los mismos hallazgos, incluidos los ocultos, que aparecen marcados. Si la última comprobación tiene menos de 5 minutos, el asistente recibe su resultado sin volver a consultar el clúster. Pide, por ejemplo: “Comprueba el estado del clúster y deja notas sobre los problemas más importantes”.
diff --git a/docs/guide/es/llm-mcp.md b/docs/guide/es/llm-mcp.md
index a1d4ac7..dd722f4 100644
--- a/docs/guide/es/llm-mcp.md
+++ b/docs/guide/es/llm-mcp.md
@@ -42,7 +42,8 @@ El asistente ve:
 - los brokers con sus hosts y puertos, el controlador;
 - los topics, las particiones, las réplicas, ISR, los offsets, el número aproximado de mensajes;
 - las configuraciones de brokers y topics, con las contraseñas y otros valores secretos ocultos;
-- los consumer groups: estado, protocolo, coordinador, miembros con client id, host y `group.instance.id`, asignaciones, offsets con commit y lag.
+- los consumer groups: estado, protocolo, coordinador, miembros con client id, host y `group.instance.id`, asignaciones, offsets con commit y lag;
+- sus propuestas y tu decisión sobre ellas: aplicada (tal cual o con cambios), rechazada con tu motivo, retirada.
 
 El asistente no ve los mensajes de los topics, la cadena bootstrap ni los ajustes de seguridad de una conexión, ni las contraseñas, los certificados, las propiedades adicionales de librdkafka, el historial de envíos ni las plantillas.
 
@@ -61,10 +62,12 @@ Mientras hay un cliente conectado, en la parte inferior del panel izquierdo se m
 
 Para desactivar MCP por completo, desmarca [[ui:settings.mcp.enable]]: los clientes se desconectan.
 
-## Comprobación del clúster y notas {#health}
+## Comprobación del clúster, notas y propuestas {#health}
 
 El asistente puede ejecutar la comprobación integrada del clúster de Glafira (herramienta `health_check`) y basarse en sus hallazgos. También puede dejar notas sobre los objetos: aparecen en la pestaña [[ui:sidebar.findings]] y en las pantallas de topics y grupos, y el clúster no cambia. Consulta [Comprobación del clúster y notas](health-check.md#with-llm).
 
+El asistente también puede proponer un cambio con la herramienta `propose_change`: la configuración de un topic, un topic nuevo, más particiones o un restablecimiento de offsets. La propuesta aparece junto al objeto; la aplicas tú en un formulario ya relleno o la rechazas, y el asistente ve tu decisión en `list_proposals`. Eliminar y vaciar no se pueden proponer. Consulta [Propuestas LLM](health-check.md#proposals).
+
 ## Si no funciona {#troubleshooting}
 
 El cliente muestra un mensaje de Glafira:
diff --git a/docs/guide/fr/health-check.md b/docs/guide/fr/health-check.md
index 8e8ad4b..02ff842 100644
--- a/docs/guide/fr/health-check.md
+++ b/docs/guide/fr/health-check.md
@@ -37,6 +37,15 @@ Un assistant LLM connecté par MCP (voir [Assistants LLM (MCP)](llm-mcp.md#how-i
 - [[ui:common.delete]] supprime une remarque, [[ui:health.deleteAll]] supprime toutes les remarques de la connexion.
 - Une remarque est supprimée automatiquement 7 jours après sa création. Une connexion peut avoir au plus 200 remarques ouvertes.
 
+## Propositions LLM {#proposals}
+
+En plus des remarques, un assistant LLM peut proposer une modification : la configuration d’un topic, un nouveau topic, davantage de partitions ou la réinitialisation des offsets d’un consumer group vide. Une proposition apparaît dans [[ui:health.proposals.title]] de l’onglet [[ui:sidebar.findings]] et sur l’écran du topic ou du groupe, avec la justification de l’assistant, une description de la modification et le nom du client. Rien n’est appliqué tout seul ; la suppression et la purge ne peuvent pas du tout être proposées.
+
+- [[ui:health.proposals.openForm]] ouvre l’onglet de l’objet avec le formulaire habituel déjà rempli : l’éditeur de configuration avec sa vérification avant / après, le formulaire de création de topic, la boîte de dialogue des partitions ou la réinitialisation des offsets avec aperçu. Modifiez les valeurs si besoin et confirmez comme d’habitude ; sur une connexion prod, Glafira demande le nom de la connexion. Après une modification réussie, la proposition est marquée comme appliquée (avec des modifications si vous avez changé quelque chose), et l’assistant le voit dans `list_proposals`.
+- [[ui:health.proposals.reject]] ferme une proposition ; vous pouvez indiquer un motif, que l’assistant verra. [[ui:health.reopen]] rétablit une proposition rejetée.
+- Sur une connexion en lecture seule, la proposition est conservée, mais [[ui:health.proposals.openForm]] est indisponible tant que vous ne désactivez pas la lecture seule dans le formulaire de connexion ; l’assistant en est aussi informé.
+- [[ui:common.delete]] supprime une proposition, [[ui:health.proposals.deleteAll]] supprime toutes les propositions de la connexion. Une proposition est supprimée automatiquement 7 jours après sa création ; une connexion peut avoir au plus 50 propositions ouvertes.
+
 ## Avec un assistant LLM {#with-llm}
 
 L’assistant lance la même vérification avec l’outil `health_check` et voit les mêmes constats, y compris les constats masqués, qui sont signalés. Si la dernière vérification date de moins de 5 minutes, l’assistant reçoit son résultat sans interroger à nouveau le cluster. Demandez par exemple : « Vérifie l’état du cluster et laisse des remarques sur les problèmes les plus importants. »
diff --git a/docs/guide/fr/llm-mcp.md b/docs/guide/fr/llm-mcp.md
index a14f329..6928eb6 100644
--- a/docs/guide/fr/llm-mcp.md
+++ b/docs/guide/fr/llm-mcp.md
@@ -42,7 +42,8 @@ L’assistant voit :
 - les brokers avec leurs hôtes et leurs ports, le contrôleur ;
 - les topics, partitions, répliques, ISR, offsets, le nombre approximatif de messages ;
 - les configurations des brokers et des topics, les mots de passe et autres valeurs secrètes étant masqués ;
-- les consumer groups : état, protocole, coordinateur, membres avec client id, hôte et `group.instance.id`, attributions, offsets commités et lag.
+- les consumer groups : état, protocole, coordinateur, membres avec client id, hôte et `group.instance.id`, attributions, offsets commités et lag;
+- ses propositions et votre décision à leur sujet : appliquée (telle quelle ou avec des modifications), rejetée avec votre motif, retirée.
 
 L’assistant ne voit pas les messages des topics, la chaîne bootstrap ni les paramètres de sécurité d’une connexion, ni les mots de passe, les certificats, les propriétés librdkafka supplémentaires, l’historique d’envoi et les modèles.
 
@@ -61,10 +62,12 @@ Tant qu’un client est connecté, un indicateur `LLM` avec le nom du client s
 
 Pour désactiver complètement MCP, décochez [[ui:settings.mcp.enable]] : les clients sont déconnectés.
 
-## Vérification du cluster et remarques {#health}
+## Vérification du cluster, remarques et propositions {#health}
 
 L’assistant peut lancer la vérification intégrée du cluster de Glafira (outil `health_check`) et s’appuyer sur ses constats. Il peut aussi laisser des remarques sur les objets : elles apparaissent dans l’onglet [[ui:sidebar.findings]] et sur les écrans des topics et des groupes, sans que le cluster soit modifié. Voir [Vérification du cluster et remarques](health-check.md#with-llm).
 
+L’assistant peut aussi proposer une modification avec l’outil `propose_change` : la configuration d’un topic, un nouveau topic, davantage de partitions ou une réinitialisation des offsets. La proposition apparaît à côté de l’objet ; vous l’appliquez vous-même dans un formulaire pré-rempli ou la rejetez, et l’assistant voit votre décision dans `list_proposals`. La suppression et la purge ne peuvent pas être proposées. Voir [Propositions LLM](health-check.md#proposals).
+
 ## En cas de problème {#troubleshooting}
 
 Le client affiche un message de Glafira :
diff --git a/docs/guide/it/health-check.md b/docs/guide/it/health-check.md
index 29aa16a..e60f14b 100644
--- a/docs/guide/it/health-check.md
+++ b/docs/guide/it/health-check.md
@@ -37,6 +37,15 @@ Un assistente LLM collegato tramite MCP (vedi [Assistenti LLM (MCP)](llm-mcp.md#
 - [[ui:common.delete]] elimina una nota e [[ui:health.deleteAll]] elimina tutte le note della connessione.
 - Una nota viene eliminata automaticamente 7 giorni dopo la creazione. Una connessione può avere al massimo 200 note aperte.
 
+## Proposte LLM {#proposals}
+
+Oltre alle note, un assistente LLM può proporre una modifica: la configurazione di un topic, un nuovo topic, più partizioni o il reset degli offset di un consumer group vuoto. La proposta compare in [[ui:health.proposals.title]] nella scheda [[ui:sidebar.findings]] e nella schermata del topic o del gruppo, con la motivazione dell’assistente, una descrizione della modifica e il nome del client. Nulla viene applicato da solo; eliminazione e svuotamento non si possono proporre affatto.
+
+- [[ui:health.proposals.openForm]] apre la scheda dell’oggetto con il modulo consueto già compilato: l’editor di configurazione con il confronto “prima → dopo”, il modulo di creazione del topic, la finestra delle partizioni o il reset degli offset con anteprima. Modifica i valori se vuoi e conferma come al solito; su una connessione prod Glafira chiede il nome della connessione. Dopo una modifica riuscita la proposta viene segnata come applicata (con modifiche, se hai cambiato qualcosa), e l’assistente lo vede in `list_proposals`.
+- [[ui:health.proposals.reject]] chiude una proposta; puoi indicare un motivo, che l’assistente vedrà. [[ui:health.reopen]] ripristina una proposta rifiutata.
+- Su una connessione in sola lettura la proposta resta, ma [[ui:health.proposals.openForm]] non è disponibile finché non togli la sola lettura nel modulo della connessione; anche l’assistente ne viene informato.
+- [[ui:common.delete]] elimina una proposta, [[ui:health.proposals.deleteAll]] elimina tutte le proposte della connessione. Una proposta viene eliminata automaticamente 7 giorni dopo la creazione; una connessione può avere al massimo 50 proposte aperte.
+
 ## Con un assistente LLM {#with-llm}
 
 L’assistente avvia lo stesso controllo con lo strumento `health_check` e vede gli stessi rilievi, compresi quelli nascosti, che sono contrassegnati. Se l’ultimo controllo ha meno di 5 minuti, l’assistente ne riceve il risultato senza interrogare di nuovo il cluster. Chiedi per esempio: “Controlla lo stato del cluster e lascia note sui problemi più importanti”.
diff --git a/docs/guide/it/llm-mcp.md b/docs/guide/it/llm-mcp.md
index e876e12..3043c21 100644
--- a/docs/guide/it/llm-mcp.md
+++ b/docs/guide/it/llm-mcp.md
@@ -42,7 +42,8 @@ L’assistente vede:
 - i broker con host e porte, il controller;
 - i topic, le partizioni, le repliche, ISR, gli offset, il numero approssimativo di messaggi;
 - le configurazioni di broker e topic, con password e altri valori segreti nascosti;
-- i consumer group: stato, protocollo, coordinatore, membri con client id, host e `group.instance.id`, assegnazioni, offset sottoposti a commit e lag.
+- i consumer group: stato, protocollo, coordinatore, membri con client id, host e `group.instance.id`, assegnazioni, offset sottoposti a commit e lag;
+- le sue proposte e la tua decisione in merito: applicata (così com’è o con modifiche), rifiutata con il tuo motivo, ritirata.
 
 L’assistente non vede i messaggi dei topic, la stringa bootstrap né le impostazioni di sicurezza di una connessione, né le password, i certificati, le proprietà aggiuntive di librdkafka, la cronologia di invio e i modelli.
 
@@ -61,10 +62,12 @@ Finché un client è collegato, in fondo al pannello sinistro compare l’indica
 
 Per disattivare del tutto MCP, deseleziona [[ui:settings.mcp.enable]]: i client vengono disconnessi.
 
-## Controllo del cluster e note {#health}
+## Controllo del cluster, note e proposte {#health}
 
 L’assistente può avviare il controllo integrato del cluster di Glafira (strumento `health_check`) e basarsi sui suoi rilievi. Può anche lasciare note sugli oggetti: compaiono nella scheda [[ui:sidebar.findings]] e nelle schermate di topic e gruppi, e il cluster non viene modificato. Vedi [Controllo del cluster e note](health-check.md#with-llm).
 
+L’assistente può anche proporre una modifica con lo strumento `propose_change`: la configurazione di un topic, un nuovo topic, più partizioni o un reset degli offset. La proposta compare accanto all’oggetto; la applichi tu in un modulo già compilato oppure la rifiuti, e l’assistente vede la tua decisione in `list_proposals`. Eliminazione e svuotamento non si possono proporre. Vedi [Proposte LLM](health-check.md#proposals).
+
 ## Se non funziona {#troubleshooting}
 
 Il client mostra un messaggio di Glafira:
diff --git a/docs/guide/ja/health-check.md b/docs/guide/ja/health-check.md
index 9c8a058..e2c1336 100644
--- a/docs/guide/ja/health-check.md
+++ b/docs/guide/ja/health-check.md
@@ -37,6 +37,15 @@ MCPで接続したLLMアシスタント（[LLMアシスタント（MCP）](llm-m
 - [[ui:common.delete]]は注記を1件削除し、[[ui:health.deleteAll]]は接続のすべての注記を削除します。
 - 注記は作成から7日後に自動で削除されます。1つの接続で開いている注記は最大200件です。
 
+## LLMの提案 {#proposals}
+
+注記に加えて、LLMアシスタントは変更を提案できます。トピック設定、新しいトピック、パーティションの追加、空のコンシューマーグループのオフセットのリセットです。提案は[[ui:sidebar.findings]]タブの[[ui:health.proposals.title]]セクションと、トピックまたはグループの画面に、アシスタントの根拠、変更内容の説明、クライアント名とともに表示されます。何も勝手には適用されません。削除とパージはそもそも提案できません。
+
+- [[ui:health.proposals.openForm]]を押すと、オブジェクトのタブが入力済みの通常のフォームで開きます。変更前後を確認できる設定エディター、トピック作成フォーム、パーティションのダイアログ、プレビュー付きのオフセットのリセットです。必要なら値を直し、いつもどおり確認します。本番接続ではGlafiraが接続名の入力を求めます。変更が成功すると提案は適用済み（何かを変えた場合は修正して適用）としてマークされ、アシスタントは`list_proposals`でそれを確認できます。
+- [[ui:health.proposals.reject]]は提案を閉じます。理由を書けば、アシスタントがそれを見ます。[[ui:health.reopen]]は却下した提案を戻します。
+- 読み取り専用の接続では提案は保持されますが、接続フォームで読み取り専用を解除するまで[[ui:health.proposals.openForm]]は使えません。アシスタントにもそのことが伝えられます。
+- [[ui:common.delete]]は提案を1件削除し、[[ui:health.proposals.deleteAll]]は接続のすべての提案を削除します。提案は作成から7日後に自動で削除されます。1つの接続で開いている提案は最大50件です。
+
 ## LLMアシスタントと使う {#with-llm}
 
 アシスタントは`health_check`ツールで同じチェックを実行し、同じ検出事項を確認します。非表示の検出事項も、非表示であることがわかる形で含まれます。前回のチェックから5分たっていなければ、アシスタントはクラスターに再度問い合わせずにその結果を受け取ります。たとえば「クラスターの状態をチェックして、最も重要な問題について注記を残して」と依頼してください。
diff --git a/docs/guide/ja/llm-mcp.md b/docs/guide/ja/llm-mcp.md
index d45fb9f..7c05f56 100644
--- a/docs/guide/ja/llm-mcp.md
+++ b/docs/guide/ja/llm-mcp.md
@@ -43,6 +43,7 @@ Glafiraが起動していない場合は、クライアントがGlafiraを起動
 - トピック、パーティション、レプリカ、ISR、オフセット、メッセージのおおよその件数。
 - ブローカーとトピックの設定（パスワードなどの機密の値は非表示）。
 - コンシューマーグループ：状態、プロトコル、コーディネーター、メンバー（client id、ホスト、`group.instance.id`）、割り当て、コミット済みオフセット、ラグ。
+- 自分の提案と、それに対するあなたの決定（そのまま適用、修正して適用、理由付きで却下、取り下げ）。
 
 アシスタントが見られないのは、トピック内のメッセージ、接続のブートストラップサーバーとセキュリティ設定、パスワード、証明書、librdkafkaの追加プロパティ、送信履歴、テンプレートです。
 
@@ -61,10 +62,12 @@ Glafiraが起動していない場合は、クライアントがGlafiraを起動
 
 MCPを完全にオフにするには、[[ui:settings.mcp.enable]]のチェックを外します。クライアントは切断されます。
 
-## クラスターのチェックと注記 {#health}
+## クラスターのチェック、注記、提案 {#health}
 
 アシスタントはGlafiraに組み込まれたクラスターのチェック（`health_check`ツール）を実行し、その検出事項をもとに判断できます。オブジェクトについての注記も残せます。注記は[[ui:sidebar.findings]]タブとトピック・グループの画面に表示され、クラスターは変更されません。[クラスターのチェックと注記](health-check.md#with-llm)を参照してください。
 
+アシスタントは`propose_change`ツールで変更を提案することもできます。トピック設定、新しいトピック、パーティションの追加、オフセットのリセットです。提案はオブジェクトの横に表示され、入力済みのフォームで適用するか却下するかはあなたが決めます。アシスタントはその決定を`list_proposals`で確認します。削除とパージは提案できません。[LLMの提案](health-check.md#proposals)を参照してください。
+
 ## うまく動かないとき {#troubleshooting}
 
 クライアントにGlafiraからのメッセージが表示されます：
diff --git a/docs/guide/ko/health-check.md b/docs/guide/ko/health-check.md
index 33d515e..0d91b83 100644
--- a/docs/guide/ko/health-check.md
+++ b/docs/guide/ko/health-check.md
@@ -37,6 +37,15 @@ MCP로 연결된 LLM 어시스턴트([LLM 어시스턴트(MCP)](llm-mcp.md#how-i
 - [[ui:common.delete]] 버튼은 메모 하나를, [[ui:health.deleteAll]] 버튼은 연결의 모든 메모를 삭제합니다.
 - 메모는 만든 지 7일이 지나면 자동으로 삭제됩니다. 연결 하나에 열린 메모는 최대 200개입니다.
 
+## LLM 제안 {#proposals}
+
+메모 외에도 LLM 어시스턴트는 변경을 제안할 수 있습니다. 토픽 설정, 새 토픽, 파티션 추가, 비어 있는 컨슈머 그룹의 오프셋 재설정입니다. 제안은 [[ui:sidebar.findings]] 탭의 [[ui:health.proposals.title]] 섹션과 토픽 또는 그룹 화면에 어시스턴트의 근거, 변경 내용 설명, 클라이언트 이름과 함께 표시됩니다. 어떤 변경도 저절로 적용되지 않으며, 삭제와 비우기는 아예 제안할 수 없습니다.
+
+- [[ui:health.proposals.openForm]] 버튼은 개체 탭을 열고 평소 사용하는 양식을 미리 채워 보여 줍니다. 변경 전후를 검토하는 설정 편집기, 토픽 만들기 양식, 파티션 대화 상자, 미리 보기가 있는 오프셋 재설정입니다. 필요하면 값을 고치고 평소처럼 확인하세요. 운영 환경 연결에서는 Glafira가 연결 이름 입력을 요구합니다. 변경이 성공하면 제안은 적용됨(무언가 바꿨다면 수정하여 적용됨)으로 표시되고, 어시스턴트는 `list_proposals`에서 이를 확인합니다.
+- [[ui:health.proposals.reject]] 버튼은 제안을 닫습니다. 사유를 적으면 어시스턴트가 볼 수 있습니다. [[ui:health.reopen]] 버튼은 거부한 제안을 되돌립니다.
+- 읽기 전용 연결에서는 제안이 유지되지만, 연결 양식에서 읽기 전용을 해제하기 전까지 [[ui:health.proposals.openForm]] 버튼을 사용할 수 없습니다. 어시스턴트에게도 이 사실이 전달됩니다.
+- [[ui:common.delete]] 버튼은 제안 하나를, [[ui:health.proposals.deleteAll]] 버튼은 연결의 모든 제안을 삭제합니다. 제안은 만든 지 7일이 지나면 자동으로 삭제됩니다. 연결 하나에 열린 제안은 최대 50개입니다.
+
 ## LLM 어시스턴트와 함께 {#with-llm}
 
 어시스턴트는 `health_check` 도구로 같은 점검을 실행하고 같은 발견 사항을 봅니다. 숨긴 발견 사항도 숨김 표시와 함께 포함됩니다. 마지막 점검이 5분이 지나지 않았다면 어시스턴트는 클러스터를 다시 조회하지 않고 그 결과를 받습니다. 예를 들어 "클러스터 상태를 점검하고 가장 중요한 문제에 메모를 남겨 줘"라고 요청해 보세요.
diff --git a/docs/guide/ko/llm-mcp.md b/docs/guide/ko/llm-mcp.md
index 5ec796c..b7ef07c 100644
--- a/docs/guide/ko/llm-mcp.md
+++ b/docs/guide/ko/llm-mcp.md
@@ -43,6 +43,7 @@ Glafira가 실행 중이 아니면 클라이언트가 직접 실행하고 최대
 - 토픽, 파티션, 복제본, ISR, 오프셋, 메시지의 대략적인 개수.
 - 브로커와 토픽의 구성. 비밀번호와 그 밖의 비밀 값은 숨겨집니다.
 - 컨슈머 그룹: 상태, 프로토콜, 코디네이터, client id·호스트·`group.instance.id`가 있는 멤버, 할당, 커밋된 오프셋, 랙.
+- 자신의 제안과 그에 대한 사용자의 결정: 적용됨(그대로 또는 수정하여), 사유와 함께 거부됨, 철회됨.
 
 어시스턴트는 토픽의 메시지, 연결의 부트스트랩 서버와 보안 설정, 비밀번호, 인증서, librdkafka 추가 속성, 전송 기록과 템플릿을 볼 수 없습니다.
 
@@ -61,10 +62,12 @@ Glafira가 실행 중이 아니면 클라이언트가 직접 실행하고 최대
 
 MCP를 완전히 끄려면 [[ui:settings.mcp.enable]] 확인란의 선택을 해제하세요. 클라이언트의 연결이 끊어집니다.
 
-## 클러스터 점검과 메모 {#health}
+## 클러스터 점검, 메모, 제안 {#health}
 
 어시스턴트는 Glafira 기본 제공 클러스터 점검(`health_check` 도구)을 실행하고 그 발견 사항을 근거로 삼을 수 있습니다. 개체에 대한 메모도 남길 수 있습니다. 메모는 [[ui:sidebar.findings]] 탭과 토픽·그룹 화면에 표시되며 클러스터는 바뀌지 않습니다. [클러스터 점검과 메모](health-check.md#with-llm)를 참고하세요.
 
+어시스턴트는 `propose_change` 도구로 변경을 제안할 수도 있습니다. 토픽 설정, 새 토픽, 파티션 추가, 오프셋 재설정입니다. 제안은 개체 옆에 표시되며, 미리 채워진 양식에서 직접 적용하거나 거부하는 것은 사용자의 몫입니다. 어시스턴트는 그 결정을 `list_proposals`에서 확인합니다. 삭제와 비우기는 제안할 수 없습니다. [LLM 제안](health-check.md#proposals)을 참고하세요.
+
 ## 작동하지 않을 때 {#troubleshooting}
 
 클라이언트에 Glafira의 메시지가 표시됩니다:
diff --git a/docs/guide/pl/health-check.md b/docs/guide/pl/health-check.md
index 0dfe454..995861e 100644
--- a/docs/guide/pl/health-check.md
+++ b/docs/guide/pl/health-check.md
@@ -37,6 +37,15 @@ Asystent LLM połączony przez MCP (zob. [Asystenci LLM (MCP)](llm-mcp.md#how-it
 - [[ui:common.delete]] usuwa notatkę, a [[ui:health.deleteAll]] usuwa wszystkie notatki połączenia.
 - Notatka jest usuwana automatycznie 7 dni po utworzeniu. Połączenie może mieć najwyżej 200 otwartych notatek.
 
+## Propozycje LLM {#proposals}
+
+Oprócz notatek asystent LLM może zaproponować zmianę: konfigurację topiku, nowy topik, więcej partycji lub reset offsetów pustej grupy konsumentów. Propozycja pojawia się w sekcji [[ui:health.proposals.title]] na karcie [[ui:sidebar.findings]] oraz na ekranie topiku lub grupy — z uzasadnieniem asystenta, opisem zmiany i nazwą klienta. Nic nie jest stosowane samo z siebie, a usunięcia i czyszczenia nie da się zaproponować wcale.
+
+- [[ui:health.proposals.openForm]] otwiera kartę obiektu ze zwykłym, już wypełnionym formularzem: edytorem konfiguracji z porównaniem „było → będzie”, formularzem tworzenia topiku, oknem partycji lub resetem offsetów z podglądem. W razie potrzeby popraw wartości i potwierdź jak zwykle; przy połączeniu prod Glafira poprosi o jego nazwę. Po udanej zmianie propozycja zostaje oznaczona jako zastosowana (ze zmianami, jeśli coś poprawiono), a asystent widzi to w `list_proposals`.
+- [[ui:health.proposals.reject]] zamyka propozycję; można podać powód, który zobaczy asystent. [[ui:health.reopen]] przywraca odrzuconą propozycję.
+- Przy połączeniu tylko do odczytu propozycja zostaje zachowana, ale [[ui:health.proposals.openForm]] jest niedostępne, dopóki tryb tylko do odczytu nie zostanie wyłączony w formularzu połączenia; asystent też się o tym dowiaduje.
+- [[ui:common.delete]] usuwa propozycję, [[ui:health.proposals.deleteAll]] — wszystkie propozycje połączenia. Propozycja jest usuwana automatycznie 7 dni po utworzeniu; połączenie może mieć najwyżej 50 otwartych propozycji.
+
 ## Razem z asystentem LLM {#with-llm}
 
 Asystent uruchamia to samo sprawdzenie narzędziem `health_check` i widzi te same ustalenia, w tym ukryte, oznaczone jako ukryte. Jeśli ostatnie sprawdzenie ma mniej niż 5 minut, asystent dostaje jego wynik bez ponownego odpytywania klastra. Można na przykład poprosić: „Sprawdź stan klastra i zostaw notatki o najważniejszych problemach”.
diff --git a/docs/guide/pl/llm-mcp.md b/docs/guide/pl/llm-mcp.md
index e5f8e6a..155eefd 100644
--- a/docs/guide/pl/llm-mcp.md
+++ b/docs/guide/pl/llm-mcp.md
@@ -42,7 +42,8 @@ Asystent widzi:
 - brokery z hostami i portami, kontroler;
 - topiki, partycje, repliki, ISR, offsety, przybliżoną liczbę wiadomości;
 - konfiguracje brokerów i topików, przy czym hasła i inne wartości tajne są ukryte;
-- grupy konsumentów: stan, protokół, koordynator, członków z client id, hostem i `group.instance.id`, przypisania, zatwierdzone offsety i lag.
+- grupy konsumentów: stan, protokół, koordynator, członków z client id, hostem i `group.instance.id`, przypisania, zatwierdzone offsety i lag;
+- swoje propozycje i decyzje w ich sprawie: zastosowano (bez zmian lub ze zmianami), odrzucono z podanym powodem, wycofano.
 
 Asystent nie widzi wiadomości w topikach, ciągu bootstrap ani ustawień zabezpieczeń połączenia, haseł, certyfikatów, dodatkowych właściwości librdkafka, historii wysyłania ani szablonów.
 
@@ -61,10 +62,12 @@ Dopóki klient jest połączony, na dole lewego panelu widać wskaźnik `LLM` z
 
 Aby całkowicie wyłączyć MCP, należy odznaczyć opcję [[ui:settings.mcp.enable]]: klienci zostaną rozłączeni.
 
-## Sprawdzanie klastra i notatki {#health}
+## Sprawdzanie klastra, notatki i propozycje {#health}
 
 Asystent może uruchomić wbudowane sprawdzenie klastra Glafira (narzędzie `health_check`) i opierać się na jego ustaleniach. Może też zostawiać notatki o obiektach: pojawiają się na karcie [[ui:sidebar.findings]] i na ekranach topików i grup, a klaster się nie zmienia. Zob. [Sprawdzanie klastra i notatki](health-check.md#with-llm).
 
+Asystent może też zaproponować zmianę narzędziem `propose_change`: konfigurację topiku, nowy topik, więcej partycji lub reset offsetów. Propozycja pojawia się obok obiektu; stosuje się ją samodzielnie w wypełnionym formularzu albo odrzuca, a asystent widzi decyzję w `list_proposals`. Usunięcia i czyszczenia nie da się zaproponować. Zob. [Propozycje LLM](health-check.md#proposals).
+
 ## Gdy nie działa {#troubleshooting}
 
 Klient wyświetla komunikat z Glafira:
diff --git a/docs/guide/pt-BR/health-check.md b/docs/guide/pt-BR/health-check.md
index fbaf52c..7b6a716 100644
--- a/docs/guide/pt-BR/health-check.md
+++ b/docs/guide/pt-BR/health-check.md
@@ -37,6 +37,15 @@ Um assistente LLM conectado por MCP (consulte [Assistentes LLM (MCP)](llm-mcp.md
 - [[ui:common.delete]] exclui uma nota, e [[ui:health.deleteAll]] exclui todas as notas da conexão.
 - Uma nota é excluída automaticamente 7 dias depois de criada. Uma conexão pode ter no máximo 200 notas abertas.
 
+## Propostas de LLM {#proposals}
+
+Além de notas, um assistente LLM pode propor uma alteração: as configurações de um tópico, um novo tópico, mais partições ou a redefinição dos offsets de um consumer group vazio. A proposta aparece em [[ui:health.proposals.title]] na aba [[ui:sidebar.findings]] e na tela do tópico ou do grupo, com a justificativa do assistente, uma descrição da alteração e o nome do cliente. Nada é aplicado sozinho; exclusão e limpeza não podem ser propostas de forma alguma.
+
+- [[ui:health.proposals.openForm]] abre a aba do objeto com o formulário habitual já preenchido: o editor de configurações com a revisão “antes → depois”, o formulário de criação de tópico, o diálogo de partições ou a redefinição de offsets com pré-visualização. Edite os valores se quiser e confirme como de costume; em uma conexão prod, o Glafira pede o nome da conexão. Depois de uma alteração bem-sucedida a proposta é marcada como aplicada (com alterações, se você mudou algo), e o assistente vê isso em `list_proposals`.
+- [[ui:health.proposals.reject]] fecha uma proposta; você pode informar um motivo, e o assistente o verá. [[ui:health.reopen]] traz de volta uma proposta rejeitada.
+- Em uma conexão somente leitura a proposta é mantida, mas [[ui:health.proposals.openForm]] fica indisponível até você desmarcar somente leitura no formulário da conexão; o assistente também é informado disso.
+- [[ui:common.delete]] exclui uma proposta, [[ui:health.proposals.deleteAll]] exclui todas as propostas da conexão. Uma proposta é excluída automaticamente 7 dias depois de criada; uma conexão pode ter no máximo 50 propostas abertas.
+
 ## Com um assistente LLM {#with-llm}
 
 O assistente executa a mesma verificação com a ferramenta `health_check` e vê as mesmas constatações, inclusive as ocultas, que aparecem marcadas. Se a última verificação tem menos de 5 minutos, o assistente recebe o resultado dela sem consultar o cluster de novo. Peça, por exemplo: “Verifique a saúde do cluster e deixe notas sobre os problemas mais importantes.”
diff --git a/docs/guide/pt-BR/llm-mcp.md b/docs/guide/pt-BR/llm-mcp.md
index b92685c..27357d0 100644
--- a/docs/guide/pt-BR/llm-mcp.md
+++ b/docs/guide/pt-BR/llm-mcp.md
@@ -42,7 +42,8 @@ O assistente vê:
 - os brokers com hosts e portas, o controlador;
 - os tópicos, as partições, as réplicas, ISR, os offsets, o número aproximado de mensagens;
 - as configurações de brokers e tópicos, com senhas e outros valores secretos ocultos;
-- os consumer groups: estado, protocolo, coordenador, membros com client id, host e `group.instance.id`, atribuições, offsets com commit e lag.
+- os consumer groups: estado, protocolo, coordenador, membros com client id, host e `group.instance.id`, atribuições, offsets com commit e lag;
+- as próprias propostas e a sua decisão sobre elas: aplicada (como está ou com alterações), rejeitada com o seu motivo, retirada.
 
 O assistente não vê as mensagens dos tópicos, a string de bootstrap nem as configurações de segurança de uma conexão, nem as senhas, os certificados, as propriedades adicionais do librdkafka, o histórico de envio e os modelos.
 
@@ -61,10 +62,12 @@ Enquanto um cliente está conectado, na parte inferior do painel esquerdo aparec
 
 Para desativar o MCP por completo, desmarque [[ui:settings.mcp.enable]]: os clientes são desconectados.
 
-## Verificação do cluster e notas {#health}
+## Verificação do cluster, notas e propostas {#health}
 
 O assistente pode executar a verificação integrada do cluster do Glafira (ferramenta `health_check`) e se basear nas constatações dela. Ele também pode deixar notas sobre os objetos: elas aparecem na aba [[ui:sidebar.findings]] e nas telas de tópicos e grupos, e o cluster não é alterado. Consulte [Verificação do cluster e notas](health-check.md#with-llm).
 
+O assistente também pode propor uma alteração com a ferramenta `propose_change`: as configurações de um tópico, um novo tópico, mais partições ou uma redefinição de offsets. A proposta aparece ao lado do objeto; você mesmo a aplica em um formulário pré-preenchido ou a rejeita, e o assistente vê sua decisão em `list_proposals`. Exclusão e limpeza não podem ser propostas. Consulte [Propostas de LLM](health-check.md#proposals).
+
 ## Se não funcionar {#troubleshooting}
 
 O cliente mostra uma mensagem do Glafira:
diff --git a/docs/guide/ru/health-check.md b/docs/guide/ru/health-check.md
index 3664c18..f3ca681 100644
--- a/docs/guide/ru/health-check.md
+++ b/docs/guide/ru/health-check.md
@@ -37,6 +37,15 @@ LLM-ассистент, подключённый по MCP (см. [LLM-ассис
 - [[ui:common.delete]] удаляет замечание, [[ui:health.deleteAll]] — все замечания подключения.
 - Замечание удаляется само через 7 дней после создания. У подключения может быть не больше 200 открытых замечаний.
 
+## Предложения LLM {#proposals}
+
+Кроме замечаний, LLM-ассистент может предложить изменение: конфиги топика, новый топик, больше партиций или сброс оффсетов пустой consumer group. Предложение появляется в разделе [[ui:health.proposals.title]] на вкладке [[ui:sidebar.findings]] и на экране топика или группы — с обоснованием ассистента, описанием изменения и именем клиента. Само по себе ничего не применяется, а удаление и очистку предложить нельзя вовсе.
+
+- [[ui:health.proposals.openForm]] открывает вкладку объекта с обычной формой, уже заполненной: редактор конфигов с проверкой «было → станет», форму создания топика, диалог партиций или сброс оффсетов с предпросмотром. Поправьте значения, если нужно, и подтвердите как обычно; у prod-подключения Glafira попросит ввести его имя. После успешного изменения предложение помечается применённым (с правками, если вы что-то меняли), и ассистент видит это в `list_proposals`.
+- [[ui:health.proposals.reject]] закрывает предложение; можно указать причину — ассистент её увидит. [[ui:health.reopen]] возвращает отклонённое предложение.
+- У подключения «только чтение» предложение сохраняется, но [[ui:health.proposals.openForm]] недоступна, пока вы не снимете флажок в форме подключения; ассистенту об этом тоже сообщается.
+- [[ui:common.delete]] удаляет предложение, [[ui:health.proposals.deleteAll]] — все предложения подключения. Предложение удаляется само через 7 дней после создания; у подключения может быть не больше 50 открытых предложений.
+
 ## Вместе с LLM-ассистентом {#with-llm}
 
 Ассистент запускает ту же проверку инструментом `health_check` и видит те же находки, включая скрытые — с пометкой. Если последней проверке меньше 5 минут, ассистент получит её результат без нового запроса к кластеру. Попросите, например: «Проверь здоровье кластера и оставь замечания по самым важным проблемам».
diff --git a/docs/guide/ru/llm-mcp.md b/docs/guide/ru/llm-mcp.md
index c1bb6a6..52fbfb5 100644
--- a/docs/guide/ru/llm-mcp.md
+++ b/docs/guide/ru/llm-mcp.md
@@ -42,7 +42,8 @@ Glafira может работать шлюзом для LLM-ассистента
 - брокеры с хостами и портами, контроллер;
 - топики, партиции, реплики, ISR, оффсеты, примерное число сообщений;
 - конфиги брокеров и топиков, при этом значения паролей и других секретов скрыты;
-- consumer groups: состояние, протокол, координатор, участников с client id, хостом и `group.instance.id`, назначения, закоммиченные оффсеты и лаг.
+- consumer groups: состояние, протокол, координатор, участников с client id, хостом и `group.instance.id`, назначения, закоммиченные оффсеты и лаг;
+- свои предложения и ваше решение по ним: применено (как есть или с правками), отклонено с вашей причиной, отозвано.
 
 Ассистент не видит сообщений в топиках, строки bootstrap и настроек безопасности подключения, паролей, сертификатов, дополнительных свойств librdkafka, истории отправки и шаблонов.
 
@@ -61,10 +62,12 @@ Glafira может работать шлюзом для LLM-ассистента
 
 Чтобы выключить MCP совсем, снимите флажок [[ui:settings.mcp.enable]]: клиенты отключатся.
 
-## Проверка кластера и замечания {#health}
+## Проверка кластера, замечания и предложения {#health}
 
 Ассистент может запустить встроенную проверку кластера Glafira (инструмент `health_check`) и опираться на её находки. Он может также оставлять замечания об объектах: они появляются на вкладке [[ui:sidebar.findings]] и на экранах топиков и групп, а кластер при этом не меняется. См. [Проверка кластера и замечания](health-check.md#with-llm).
 
+Ассистент может также предложить изменение инструментом `propose_change`: конфиги топика, новый топик, больше партиций или сброс оффсетов. Предложение появляется рядом с объектом, а применяете его вы сами в заполненной форме — или отклоняете; решение ассистент видит в `list_proposals`. Удаление и очистку предложить нельзя. См. [Предложения LLM](health-check.md#proposals).
+
 ## Если не работает {#troubleshooting}
 
 Клиент показывает сообщение из Glafira:
diff --git a/docs/guide/zh-CN/health-check.md b/docs/guide/zh-CN/health-check.md
index 5b47144..e4b9190 100644
--- a/docs/guide/zh-CN/health-check.md
+++ b/docs/guide/zh-CN/health-check.md
@@ -37,6 +37,15 @@ Glafira 不会自行轮询集群：只有在你点击按钮或 LLM 助手请求
 - [[ui:common.delete]]删除一条批注，[[ui:health.deleteAll]]删除该连接的所有批注。
 - 批注在创建 7 天后自动删除。一个连接最多可以有 200 条未关闭的批注。
 
+## LLM 建议 {#proposals}
+
+除了批注，LLM 助手还可以提出更改建议：主题配置、新主题、增加分区，或重置空消费者组的偏移量。建议显示在[[ui:sidebar.findings]]标签页的“[[ui:health.proposals.title]]”部分以及主题或消费者组页面中，附带助手的理由、更改说明和客户端名称。不会自行应用任何更改；删除和清空根本无法被建议。
+
+- [[ui:health.proposals.openForm]]会打开对象的标签页，其中常规表单已填好：带“修改前 → 修改后”检查的配置编辑器、创建主题表单、分区对话框，或带预览的偏移量重置。你可以按需修改值并像往常一样确认；对于 prod 连接，Glafira 会要求输入连接名称。更改成功后，建议会标记为已应用（如果你改动了内容，则为已修改后应用），助手可在 `list_proposals` 中看到。
+- [[ui:health.proposals.reject]]会关闭建议；可以填写原因，助手会看到。[[ui:health.reopen]]可恢复被拒绝的建议。
+- 对于只读连接，建议会保留，但在你于连接表单中取消只读之前，[[ui:health.proposals.openForm]]不可用；助手也会得到相应提示。
+- [[ui:common.delete]]删除一条建议，[[ui:health.proposals.deleteAll]]删除该连接的所有建议。建议在创建 7 天后自动删除；一个连接最多可以有 50 条未关闭的建议。
+
 ## 与 LLM 助手配合 {#with-llm}
 
 助手通过 `health_check` 工具运行同样的检查，并看到同样的检查结果，包括带标记的已隐藏结果。如果上次检查不到 5 分钟，助手会直接获得其结果，而不会再次查询集群。例如，可以这样请求：“检查集群的健康状况，并针对最重要的问题留下批注。”
diff --git a/docs/guide/zh-CN/llm-mcp.md b/docs/guide/zh-CN/llm-mcp.md
index ef4c378..7022b8a 100644
--- a/docs/guide/zh-CN/llm-mcp.md
+++ b/docs/guide/zh-CN/llm-mcp.md
@@ -42,7 +42,8 @@ Glafira 可以充当 LLM 助手的网关：Claude Desktop、Claude Code、Cursor
 - Broker 的主机和端口，以及控制器；
 - 主题、分区、副本、ISR、偏移量、消息的大致数量；
 - Broker 和主题的配置，其中密码和其他机密值会被隐藏；
-- 消费者组：状态、协议、协调器、成员（含 client id、主机和 `group.instance.id`）、分配、已提交的偏移量和延迟。
+- 消费者组：状态、协议、协调器、成员（含 client id、主机和 `group.instance.id`）、分配、已提交的偏移量和延迟；
+- 自己的建议以及你对它们的决定：已应用（原样或已修改）、已拒绝（附你的原因）、已撤回。
 
 助手看不到主题中的消息、连接的 Bootstrap 服务器地址和安全设置、密码、证书、librdkafka 的附加属性、发送历史记录和模板。
 
@@ -61,10 +62,12 @@ Glafira 可以充当 LLM 助手的网关：Claude Desktop、Claude Code、Cursor
 
 要完全关闭 MCP，请取消选中[[ui:settings.mcp.enable]]：客户端会被断开。
 
-## 集群检查与批注 {#health}
+## 集群检查、批注与建议 {#health}
 
 助手可以运行 Glafira 内置的集群检查（`health_check` 工具），并以其检查结果为依据。它还可以为对象留下批注：批注显示在[[ui:sidebar.findings]]标签页以及主题和消费者组页面中，集群不会被更改。参见[集群检查与批注](health-check.md#with-llm)。
 
+助手还可以通过 `propose_change` 工具提出更改建议：主题配置、新主题、增加分区或重置偏移量。建议显示在对象旁边，由你在已填好的表单中自行应用或拒绝；助手随后可在 `list_proposals` 中看到你的决定。删除和清空无法被建议。参见[LLM 建议](health-check.md#proposals)。
+
 ## 无法使用时 {#troubleshooting}
 
 客户端会显示来自 Glafira 的消息：
diff --git a/docs/guide/zh-TW/health-check.md b/docs/guide/zh-TW/health-check.md
index 8c66d09..5b82865 100644
--- a/docs/guide/zh-TW/health-check.md
+++ b/docs/guide/zh-TW/health-check.md
@@ -37,6 +37,15 @@ Glafira 不會自行輪詢叢集：只有在你按一下按鈕或 LLM 助理要
 - [[ui:common.delete]]會刪除一則註記，[[ui:health.deleteAll]]會刪除此連線的所有註記。
 - 註記會在建立 7 天後自動刪除。一個連線最多可以有 200 則未關閉的註記。
 
+## LLM 建議 {#proposals}
+
+除了註記，LLM 助理還可以提出變更建議：主題設定、新主題、增加分區，或重設空消費者群組的偏移量。建議會顯示在[[ui:sidebar.findings]]分頁的「[[ui:health.proposals.title]]」區段以及主題或消費者群組畫面中，附有助理的理由、變更說明和用戶端名稱。不會自行套用任何變更；刪除和清空完全無法被建議。
+
+- [[ui:health.proposals.openForm]]會開啟物件的分頁，其中常用表單已填好：含「變更前 → 變更後」檢查的設定編輯器、建立主題表單、分區對話方塊，或含預覽的偏移量重設。你可以視需要修改值並照常確認；對於 prod 連線，Glafira 會要求輸入連線名稱。變更成功後，建議會標記為已套用（若你有改動內容，則為已修改後套用），助理可在 `list_proposals` 中看到。
+- [[ui:health.proposals.reject]]會關閉建議；可以填寫原因，助理會看到。[[ui:health.reopen]]可還原被拒絕的建議。
+- 對於唯讀連線，建議會保留，但在你於連線表單中取消唯讀之前，[[ui:health.proposals.openForm]]無法使用；助理也會收到相應提示。
+- [[ui:common.delete]]會刪除一則建議，[[ui:health.proposals.deleteAll]]會刪除此連線的所有建議。建議會在建立 7 天後自動刪除；一個連線最多可以有 50 則未關閉的建議。
+
 ## 與 LLM 助理搭配 {#with-llm}
 
 助理會使用 `health_check` 工具執行同樣的檢查，並看到同樣的檢查結果，包括帶有標示的已隱藏結果。如果上次檢查不到 5 分鐘，助理會直接取得其結果，不會再次查詢叢集。例如，可以這樣要求：「檢查叢集的健康狀況，並針對最重要的問題留下註記。」
diff --git a/docs/guide/zh-TW/llm-mcp.md b/docs/guide/zh-TW/llm-mcp.md
index dd68f68..988d226 100644
--- a/docs/guide/zh-TW/llm-mcp.md
+++ b/docs/guide/zh-TW/llm-mcp.md
@@ -42,7 +42,8 @@ Glafira 可以作為 LLM 助理的閘道：Claude Desktop、Claude Code、Cursor
 - Broker 的主機和連接埠，以及控制器；
 - 主題、分區、複本、ISR、偏移量、訊息的大致數量；
 - Broker 和主題的設定，其中密碼和其他機密值會被隱藏；
-- 消費者群組：狀態、通訊協定、協調器、成員（含 client id、主機和 `group.instance.id`）、指派、已提交的偏移量和延遲。
+- 消費者群組：狀態、通訊協定、協調器、成員（含 client id、主機和 `group.instance.id`）、指派、已提交的偏移量和延遲；
+- 自己的建議以及你對它們的決定：已套用（原樣或已修改）、已拒絕（附你的原因）、已撤回。
 
 助理看不到主題中的訊息、連線的 Bootstrap 伺服器位址和安全性設定、密碼、憑證、librdkafka 的額外屬性、傳送歷程記錄和範本。
 
@@ -61,10 +62,12 @@ Glafira 可以作為 LLM 助理的閘道：Claude Desktop、Claude Code、Cursor
 
 若要完全關閉 MCP，請取消勾選[[ui:settings.mcp.enable]]：用戶端會被中斷連線。
 
-## 叢集檢查與註記 {#health}
+## 叢集檢查、註記與建議 {#health}
 
 助理可以執行 Glafira 內建的叢集檢查（`health_check` 工具），並以其檢查結果為依據。它還可以為物件留下註記：註記會顯示在[[ui:sidebar.findings]]分頁以及主題與消費者群組畫面中，叢集不會被變更。請參閱[叢集檢查與註記](health-check.md#with-llm)。
 
+助理也可以透過 `propose_change` 工具提出變更建議：主題設定、新主題、增加分區或重設偏移量。建議會顯示在物件旁邊，由你在已填好的表單中自行套用或拒絕；助理隨後可在 `list_proposals` 中看到你的決定。刪除和清空無法被建議。請參閱[LLM 建議](health-check.md#proposals)。
+
 ## 無法運作時 {#troubleshooting}
 
 用戶端會顯示來自 Glafira 的訊息：
diff --git a/docs/i18n/glossary.md b/docs/i18n/glossary.md
index dbeb0df..e770cd8 100644
--- a/docs/i18n/glossary.md
+++ b/docs/i18n/glossary.md
@@ -97,6 +97,10 @@ DNS, TCP, ID, MCP, LLM, stdio, Claude Desktop, Claude Code, Cursor, Build (сб
 | critical / warning / info | критично / предупреждение / информация | Kritisch / Warnung / Hinweis | Critique / Avertissement / Information | Crítico / Advertencia / Información | Critico / Avviso / Informazione | Krytyczne / Ostrzeżenie / Informacja | Crítico / Aviso / Informação | 严重 / 警告 / 信息 | 嚴重 / 警告 / 資訊 | 重大 / 警告 / 情報 | 심각 / 경고 / 정보 |
 | hide for this object | скрыть для этого объекта | für dieses Objekt ausblenden | masquer pour cet objet | ocultar para este objeto | nascondi per questo oggetto | ukryj dla tego obiektu | ocultar para este objeto | 对此对象隐藏 | 對此物件隱藏 | このオブジェクトで非表示 | 이 개체에서 숨기기 |
 | badge (число у объекта в дереве и списках) | значок | Badge (das, Pl. Badges) | badge (m.) | insignia (как цветная метка окружения; не «indicador» — это индикатор `LLM` в панели) | badge (m.) | znaczek | selo (как цветная метка окружения; не «indicador» — это индикатор `LLM` в панели) | 标记 | 標記 | バッジ | 배지 |
+| proposal (предложение LLM) | предложение LLM | LLM-Vorschlag | proposition LLM | propuesta LLM | proposta LLM | propozycja LLM | proposta de LLM | LLM 建议 | LLM 建議 | LLMの提案 | LLM 제안 |
+| open in form (кнопка предложения) | открыть в форме | im Formular öffnen | ouvrir dans le formulaire | abrir en el formulario | apri nel modulo | otwórz w formularzu | abrir no formulário | 在表单中打开 | 在表單中開啟 | フォームで開く | 양식에서 열기 |
+| applied / applied with edits (статус предложения) | применено / применено с правками | angewendet / mit Änderungen angewendet | appliquée / appliquée avec des modifications | aplicada / aplicada con cambios | applicata / applicata con modifiche | zastosowano / zastosowano ze zmianami | aplicada / aplicada com alterações | 已应用 / 已修改后应用 | 已套用 / 已修改後套用 | 適用 / 修正して適用 | 적용됨 / 수정하여 적용됨 |
+| reject / withdraw (предложение) | отклонить / отозвать | ablehnen / zurückziehen | rejeter / retirer | rechazar / retirar | rifiuta / ritira | odrzuć / wycofaj | rejeitar / retirar | 拒绝 / 撤回 | 拒絕 / 撤回 | 却下 / 取り下げ | 거부 / 철회 |
 
 ## Частые слова интерфейса
 
````

- [ ] **Step 2: Проверки.**

Run: `pnpm i18n:check` — Expected: `✓ словари … согласованы`; `pnpm test:scripts` — `# pass 71`; `pnpm exec tsc --noEmit` и `pnpm build` (главы попадают в сборку) — без замечаний.

- [ ] **Step 3: Commit.**

````bash
git add docs/guide docs/i18n/glossary.md README.md
git commit -m "docs(guide): раздел «Предложения LLM» на 12 языках, глава llm-mcp о предложениях, глоссарий, README

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````

### Task 11: Чек-лист GUI-смоука G3 с долгами смоука G1

**Files:**
- Create: `docs/smoke/g3.md`

**Interfaces:**
- Consumes: стенд и уборка смоука G2 (`docs/smoke/g2.md`), подключение клиента из `docs/smoke/g1.md` (п. 11).
- Produces: `docs/smoke/g3.md` — 16 пунктов G3 (хвосты G2, четыре вида предложений, отклонение с причиной, отзыв и повтор, проверка против кластера, read-only, prod, фильтры и удаление, перезапуск, справка и языки, журнал) и раздел «Долги смоука G1» (Claude Desktop, ребаланс, Windows).

Спец. G3 §12. Точные значения и формулировки — как выводит код (ключи `health.proposals.*` ru). Пункт 2 про `cluster.controllerUnknown` на dev-стенде не воспроизводится — так и написано.

- [ ] **Step 1: Файл.**

<!-- block: file docs/smoke/g3.md part=whole -->
````markdown
# Смоук G3 (предложения LLM в формах)

Сборка: `pnpm tauri dev` (Glafira запущена заранее; MCP-клиент — Claude Code из `~`, как в `docs/smoke/g1.md`, п. 11; клиент подтверждён, у подключения dev-стенда отмечен «Доступ для LLM (только метаданные)»). Локальная Kafka: `docker compose -f dev/docker-compose.yml up -d` — один брокер. Стенд и уборка — те же, что в `docs/smoke/g2.md`.

Подготовка (в терминале):

```bash
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic smoke-g2 --partitions 1 --replication-factor 1
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic smoke-g2-isr --partitions 1 --replication-factor 1 --config min.insync.replicas=2
seq 1 3 | docker exec -i ke-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic smoke-g2
docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group smoke-g2-empty --topic smoke-g2 --reset-offsets --to-earliest --execute
```

1. Хвосты G2. Окружение подключения **prod**, «Проверить кластер». Сменить окружение на **dev** и сохранить: в строке «Последняя проверка: … · в Glafira» появилось « · проверено как PROD». Открыть «Обзор кластера» и вкладку «Замечания» рядом, нажать «Проверить кластер» на одной из них: на обеих кнопка стала «Проверка…» и неактивна до конца проверки; после проверки пометки про окружение нет.
2. Контроллер. В «Замечаниях» нет ни «Нет контроллера», ни «Контроллер неизвестен» (брокер один, контроллер известен). Правило «Контроллер неизвестен» (предупреждение) появляется только если `controller id` не пришёл при живых лидерах — на dev-стенде не воспроизводится, проверено автотестами.
3. Предложение по конфигам. В Claude Code: «Проверь здоровье кластера dev и предложи исправление для smoke-g2-isr» → вызовы `health_check` и `propose_change` (вид `alterTopicConfigs`, `min.insync.replicas` → `1`). На вкладке «Замечания» в разделе «Предложения LLM» карточка: заголовок, обоснование как есть, строка «Конфиги топика: min.insync.replicas → 1», подпись «Предложение LLM · claude-code · min.insync.replicas больше фактора репликации». В дереве у «Замечаний» число в точке выросло на 1; на экране топика `smoke-g2-isr` карточка в блоке «Замечания».
4. Применение. «Открыть в форме» → вкладка `smoke-g2-isr`, подвкладка «Конфигурация», редактор открыт, над ним «Применяется предложение LLM: <заголовок>», у `min.insync.replicas` значение `1`, «изменений: 1». «Проверить изменения» → «2 → 1», «Применить». Карточка переехала в «Закрытые (1)» со статусом «применено несколько секунд назад» (или «применено с правками», если значение поправили). Спросить ассистента «какое решение по предложению?» → `list_proposals` со статусом `applied`, `appliedAsProposed: true`.
5. Создание топика. «Предложи создать топик smoke-g3-dlq на 2 партиции с cleanup.policy=compact» → `propose_change` (`createTopic`); карточка «Новый топик — партиций: 2, фактор репликации: по умолчанию брокера, конфиги: cleanup.policy=compact», имя без ссылки. «Открыть в форме» → вкладка «Топики», форма создания с именем `smoke-g3-dlq`, партиций 2, фактор репликации `-1`, пресет «delete», в доп. конфигах `cleanup.policy=compact`; «Создать» → открылась вкладка топика, предложение «применено».
6. Партиции. «Предложи увеличить число партиций smoke-g2 до 3» → карточка «Партиций: → 3». «Открыть в форме» → вкладка топика, диалог «Добавить партиции» с числом 3 и строкой про предложение. Ввести 4 и «Увеличить» → предложение «применено с правками»; `list_proposals` — `appliedAsProposed: false`.
7. Сброс оффсетов. «Предложи сбросить оффсеты группы smoke-g2-empty к началу» → карточка «Оффсеты группы: С начала (earliest), Вся группа». «Открыть в форме» → вкладка группы, панель сброса со стратегией «С начала (earliest)», область «Вся группа», строка про предложение. «Предпросмотр» → план, «Применить (1)» → результат, предложение «применено».
8. Отклонение. «Предложи поставить retention.ms=60000 у smoke-g2» → карточка. «Отклонить» → поле причины, ввести «не сейчас», «Отклонить предложение» → в «Закрытые» статус «отклонено: не сейчас». Спросить ассистента о решении → `rejected`, `rejectReason: "не сейчас"`. «Открыть снова» — карточка снова открыта.
9. Отзыв и повтор. Попросить ассистента отозвать это предложение → `withdraw_proposal`, статус «отозвано». Попросить предложить то же самое дважды: второй вызов отвечает `created: false`, карточка одна.
10. Проверка против кластера. Попросить ассистента предложить изменение конфига несуществующего топика → ответ `not_found`; изменить конфиг `__consumer_offsets` → `invalid_argument` про внутренний топик; уменьшить число партиций `smoke-g2` → `invalid_argument` «newTotal must be greater». Карточек не появилось.
11. Read-only. Отметить у подключения «Только чтение», сохранить. Попросить предложить изменение конфига `smoke-g2` → ответ содержит `readOnly: true`, карточка есть, кнопка «Открыть в форме» неактивна с подсказкой «Подключение только для чтения». Снять «Только чтение» → кнопка активна.
12. Prod. Окружение **prod**, сохранить. «Открыть в форме» у открытого предложения → при применении ConfirmDialog просит ввести имя подключения; ввести и применить → «применено». Вернуть окружение **dev**.
13. Фильтры и удаление. «Источник» → «Правила» — раздел предложений скрыт; «LLM» — только замечания и предложения. «Удалить все предложения LLM» → вопрос на странице «Удалить все предложения LLM у этого подключения?» → подтвердить → «Предложений LLM нет.».
14. Перезапуск. Попросить ещё одно предложение, перезапустить Glafira: карточка на месте. Файл `~/Library/Application Support/io.github.vvnovg.glafira.kafka/health/<id>.json` содержит `proposals` (`-rw-------`).
15. Справка и языки. F1 на вкладке «Замечания» — глава «Проверка кластера и замечания», раздел «Предложения LLM»; F1 в разделе MCP «Настроек» — глава «LLM-ассистенты (MCP)» с разделом «Проверка кластера, замечания и предложения». Переключение языка (12 языков): метки раздела совпадают с интерфейсом, карточка и описание изменения переведены.
16. Журнал MCP: у `propose_change` в аргументах `kind` и `target`, без заголовка, обоснования и значений.

Долги смоука G1 (не проверялись в G1, см. `docs/smoke/g1.md`):

17. Claude Desktop (п. 3 смоука G1): подключение по фрагменту из «Настроек», диалог подтверждения `claude-ai`, вопрос о топиках. После этого п. 3–4 этого смоука можно повторить в Claude Desktop.
18. Ребаланс с мигающим консьюмером (п. 5 смоука G1): `observe_consumer_group` называет входы и выходы участника.
19. Windows (п. 14 смоука G1, release-сборка из CI): шим из GUI-бинаря отвечает, закрытие Claude Desktop не закрывает Glafira; на Windows дополнительно п. 3–4 этого смоука.

Уборка:

```bash
docker exec ke-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --delete --group smoke-g2-empty
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic smoke-g2
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic smoke-g2-isr
docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic smoke-g3-dlq
```
````

- [ ] **Step 2: Проверки.**

Run: `pnpm i18n:check` — Expected: без замечаний (смоук не входит в проверку глав, но команда должна оставаться зелёной). Прочитайте чек-лист подряд: каждый пункт исполним на одноброкерном стенде.

- [ ] **Step 3: Commit.**

````bash
git add docs/smoke/g3.md
git commit -m "docs(smoke): чек-лист GUI-смоука G3 с долгами смоука G1

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
````
