# G4.1 «Хвосты G4» — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** закрыть хвосты G4 — дыры маскировки, ответ «не найден» вместо «Недостаточно прав», недостающие тесты, мелочи доступности интерфейса и неточности документации — без новых функций.

**Architecture:** маскировка меняется только в `crates/glafira-mcp/src/mask.rs` (текстовый режим, карты, JSON-числа). Права Describe различает общая функция `topic_partitions` в `src-tauri/src/browse/session.rs`: через неё идут экран «Сообщения», `read_messages`, `summarize_messages` и история коммитов. Ответ истории, запас бюджета и копия ключа меняются в крейте (`message_tools.rs`, `GatewayOptions`, `KEY_PARSE_LIMIT`) и в `src-tauri/src/mcp/messages.rs`. Интерфейс — форма подключения, значки дерева, журнал MCP, стили и два ключа словарей. Документация — README, смоук G4, спека G4, новый смоук G4.1.

**Tech Stack:** Rust (regex, serde_json, rmcp 3.5.1, tokio), rdkafka 0.37, Tauri 2, React 19, TanStack Query 5, react-i18next, Vitest.

**Спецификация:** `docs/superpowers/specs/2026-10-29-glafira-g41-g4-tails-design.md`. Уточнения по итогам прототипа внесены в неё этим же коммитом:
- имя пары без предела длины (§2.5): `{0,63}` поверх классов Unicode замедлял `tests/server.rs` с 5 до 84 с;
- пометка о недоступном статусе — только когда данных статуса нет (§4.2);
- точная фраза README (§5.1);
- фальшивому брокеру хватает Metadata v1 (§6.2);
- клиент rmcp сам ставит `progressToken` и затирает чужой (§6.3).

**Проверено до написания плана.** Весь код плана собран и прогнан в отдельном worktree (ветка `proto/g41`, по два коммита на задачу: тесты, затем реализация). Повторное применение всех блоков плана в чистом worktree от коммита спеки даёт то же дерево побайтно. Проверки после каждой задачи — зелёные:
- крейт `glafira-mcp` — 72 unit-теста (было 65), `tests/server.rs` — 58 (было 55, прогон ~2 с вместо ~5,5 с), `manifest`, `gui_stdio`, `catalog`, clippy, fmt;
- приложение — 524 unit-теста (было 521), clippy, fmt; интеграционные против локальной Kafka: `it_browse` 6, `it_mcp` 2, `it_groups` 5, `it_health` 3;
- фронтенд — 521 тест в 71 файле (было 520), `tsc`, `i18n:check` (вызовов из Rust 272), сборка.

Код переносится как есть. Перед каждым блоком кода стоит HTML-комментарий с его назначением:
- `block: patch <имя>` — патч `git diff` против текущего состояния ветки, применять `git apply --recount`. Патч тестов задачи накладывается первым, патч реализации — после него;
- `block: spec /tmp/<имя>.json` — строки словарей для скрипта `add-keys.mjs`;
- `block: helper` — сам скрипт.

Ограждения блоков — четыре обратные кавычки: внутри встречаются тройные. Блоки извлекайте программно, по комментариям (например, `python3 .superpowers/sdd/g4-extract.py <файл> get "<маркер>" <куда>`), не перепечатывайте.

## Global Constraints

- Крейт `glafira-mcp` по-прежнему не зависит от `rdkafka`, `tauri` и `kafka-explorer` (тест `tests/manifest.rs`). Содержимое кластера уходит к LLM только через `mask::Masker`.
- Режим заголовков и маскировка JSON-члена по имени не меняются. Правила G4.1 относятся к тексту: значения `string`, JSON, ушедший в текстовый режим, строковые значения внутри JSON.
- Значение до конца строки — только у имён, которые после нормализации равны `cookie` или `setcookie` либо оканчиваются на `authorization`, только у значения без кавычек и только если имя есть в списке подключения. Конец строки — `\n`, `\r` или конец текста; это одна замена.
- Карта: проверка Луна, 13–19 цифр, первая цифра 2–6 — без изменений. Формы окна: одна группа из 13–19 цифр; 4-4-4-4 с необязательной группой из 1–3 цифр; 4-6-5; 4-6-4; 4-4-4-3; 4-3-3-3; 6-13; 8-8.
- `KEY_PARSE_LIMIT` = 16 КиБ (`16 * 1024`); `SCAN_GRACE` = 5 с — значение по умолчанию поля `GatewayOptions::scan_grace`.
- Тексты для LLM — по-английски. У истории, которая не помещается и без серий: `response_too_large` «The commit history does not fit into 100 KB even without commit runs; narrow it: pass topic or a shorter window».
- Топик без права Describe (`TOPIC_AUTHORIZATION_FAILED` в метаданных) — `ErrorCode::NotAuthorized` с существующим ключом `errors.kafka.notAuthorized`; нет топика — `errors.topics.notFound`, как раньше. Новых строк Rust нет.
- Значки LLM: цвет `--llm-badge` — `#2563eb` в светлой теме, `#60a5fa` в тёмной; закрашенный значок — фон и рамка `#2563eb`, текст `#fff`. `--accent` не меняется.
- Новых ключей интерфейса ровно два: `connections.form.llmStatusUnavailable` и `settings.mcp.audit.openConnectionsFailed`, во всех 12 словарях (en — эталон ключей, ru — эталон смысла, остальные — по `docs/i18n/glossary.md`), скриптом `add-keys.mjs` из spec-файлов плана. Во французском — U+00A0 перед `:` и внутри « ».
- Проверки после каждой задачи (что применимо):
  - крейт, в `crates/glafira-mcp`: `cargo test`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`;
  - приложение, в `src-tauri`: `cargo test --lib`, `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check`;
  - фронтенд: `pnpm test`, `pnpm exec tsc --noEmit`, `pnpm i18n:check`, `pnpm build`.

  Интеграционные тесты — `KE_IT_BOOTSTRAP=localhost:9092 cargo test --test <имя>` (dev-стенд `docker compose -f dev/docker-compose.yml up -d`, один брокер).
- Тесты Rust не используют Unix-пути вида `/nonexistent/…`: на Windows они не абсолютны.
- `src-tauri/target` может быть символической ссылкой: не удалять и не заменять, без `cargo clean`. `CARGO_TARGET_DIR` не задавать.
- Коммиты — на русском в стиле репозитория, `git commit -s` (на репозитории работает DCO). Последняя строка сообщения — `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (Signed-off-by git добавит сам). Файлы добавлять поимённо: в рабочем дереве есть чужие неотслеживаемые `dev/certs/out/*` и `docs/habr-*`. Push — только с согласия пользователя.

## Вспомогательный скрипт для словарей

Строки на 12 языках добавляются скриптом `add-keys.mjs`. Его кладут во временный каталог, в репозиторий он не коммитится. Тот же скрипт был в плане G4.

Spec-файл: `{ "path": "a.b", "values": { "<язык>": { … } } }`. Объект `values[язык]` добавляется в раздел по пути `path`, новые ключи встают в конец раздела. Формат файла (2 пробела, порядок ключей) сохраняется.

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

| Файл | Что меняется |
|---|---|
| `crates/glafira-mcp/src/mask.rs` | правило «до конца строки», имя следующей пары `name=`, имена пар на любом алфавите, карты на границе цифр и новые формы, JSON-числа, `KEY_PARSE_LIMIT`, тесты |
| `crates/glafira-mcp/src/messages.rs`, `src/server/mod.rs` | `SCAN_GRACE`, поле `GatewayOptions::scan_grace` |
| `crates/glafira-mcp/src/server/message_tools.rs` | запас бюджета из настроек, `fit_history` → `response_too_large`, тест |
| `crates/glafira-mcp/tests/server.rs` | фальшивый ход `progress`, клиент с журналом хода, тесты прогресса, занятости и аргументов обзора, `scan_grace` в тесте таймаута |
| `src-tauri/src/browse/session.rs` | `topic_not_authorized`, ошибка топика в `topic_partitions` |
| `src-tauri/src/mcp/messages.rs` | `offsets_partitions` через `topic_partitions`, без `offsets_partition_count`; `raw_data` с пределом, ключ до `KEY_PARSE_LIMIT`; фальшивый брокер с метаданными, тесты |
| `src-tauri/src/mcp/backend.rs` | тест подсказки про Read — через `topic_not_authorized` |
| `src-tauri/src/storage/connections.rs` | список масок проверяется только на уровне `messages`, тесты |
| `src/components/connections/ConnectionForm.tsx`, `tree/ConnectionNode.tsx`, `src/components/settings/McpSection.tsx`, `src/styles.css` | живая область предупреждения о prod, пометка о статусе MCP, роль и имя значков, цвета значков, причина «Открыть в „Сообщениях“» при ошибке списка подключений |
| `src/components/**/*.test.tsx` | тесты интерфейса |
| `src/locales/*.json` | два ключа на 12 языках |
| `README.ru.md`, `docs/smoke/g4.md`, `docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md` | фраза о журнале, подтверждённые числа смоука, правила G4.1 в спеке G4 |
| `docs/smoke/g41.md` (новый) | чек-лист GUI-смоука G4.1 |

---

### Task 1: Маскировка строк и пар (`mask.rs`)

**Files:**
- Modify: `crates/glafira-mcp/src/mask.rs` (регулярки пар и учётных данных, `credential_len`, `Masker::text`, модуль `tests`)

**Interfaces:**
- Consumes: —
- Produces: публичный API крейта не меняется. В `mask.rs` появляются приватные `SCHEMES`, `is_scheme(&str) -> bool`, `masks_line(&str) -> bool`, `line_len(&str) -> usize`, регулярки `OPEN_PAIR` и `SPACED_VALUE`. `credential_len` теряет параметр `name`: `credential_len(value: &str, rest: &str) -> usize`. `SAME_LINE_CREDENTIAL` удаляется.

Спец. §2.1, §2.2, §2.5:
- у кук и заголовков авторизации значение без кавычек заменяется до конца строки;
- если значение — одно слово-схема `Bearer`/`Basic`/`Digest`/`Token` и дальше в строке только пробелы, учётные данные на следующей строке берутся по прежнему правилу;
- на следующей строке слово `name=` со значением после пробелов — имя следующей пары, а не учётные данные; в той же строке слово с `=` в конце по-прежнему учётные данные (base64 с выравниванием);
- имя пары без кавычек — буквы и цифры любого алфавита, без предела длины.

Четыре ожидания существующего теста `any_authorization_scheme_is_masked_with_its_credential` меняются: соседние пары и текст в той же строке после `authorization` теперь тоже скрыты (`authorization=ApiKey s3cr3tKEY, next` → `authorization=***`). Это цена правила, спец. §2.1.

- [ ] **Step 1: Тесты.**

Патч меняет четыре ожидания и добавляет тесты `cookies_and_authorization_are_masked_to_the_end_of_the_line`, `next_line_pair_with_a_spaced_value_is_not_a_credential`, `pair_names_may_be_in_any_script`:

<!-- block: patch task1-tests.diff -->
````diff
diff --git a/crates/glafira-mcp/src/mask.rs b/crates/glafira-mcp/src/mask.rs
index 0bfd93f..07bc967 100644
--- a/crates/glafira-mcp/src/mask.rs
+++ b/crates/glafira-mcp/src/mask.rs
@@ -924,9 +924,10 @@ mod tests {
                     "Authorization: ***",
                     1,
                 ),
+                // У `…authorization` — до конца строки, вместе с соседними парами.
                 (
                     "authorization=ApiKey s3cr3tKEY, next",
-                    "authorization=***, next",
+                    "authorization=***",
                     1,
                 ),
                 // Только в той же строке; у других имён следующее слово остаётся.
@@ -972,14 +973,14 @@ mod tests {
                 ),
                 (
                     "authorization=abc password: hunter2",
-                    "authorization=*** password: ***",
-                    2,
+                    "authorization=***",
+                    1,
                 ),
                 ("token: Bearer\nuser=ann", "token: ***\nuser=ann", 1),
                 // Токен — слово целиком: с `:`, `@` и `=` внутри.
                 (
                     "Authorization: Basic dXNlcg== from 10.0.0.1",
-                    "Authorization: *** from 10.0.0.1",
+                    "Authorization: ***",
                     1,
                 ),
                 (
@@ -989,7 +990,7 @@ mod tests {
                 ),
                 (
                     "Authorization: Basic alice:hunter2 from 10.0.0.1",
-                    "Authorization: *** from 10.0.0.1",
+                    "Authorization: ***",
                     1,
                 ),
                 ("token: Token id42:s3cr3tKEY", "token: ***", 1),
@@ -1023,6 +1024,75 @@ mod tests {
         );
     }
 
+    #[test]
+    fn cookies_and_authorization_are_masked_to_the_end_of_the_line() {
+        texts(
+            &standard(),
+            &[
+                ("Cookie: a=1; sessionid=xyz", "Cookie: ***", 1),
+                ("cookie: a=1\nuser=ann", "cookie: ***\nuser=ann", 1),
+                (
+                    r#"Authorization: Digest username="bob", realm="r", response="abc""#,
+                    "Authorization: ***",
+                    1,
+                ),
+                (
+                    "Authorization: AWS4-HMAC-SHA256 Credential=AKIA/2026/s3, SignedHeaders=host, Signature=abc",
+                    "Authorization: ***",
+                    1,
+                ),
+                ("Authorization: Bearer abc:", "Authorization: ***", 1),
+                (
+                    "Authorization: Bearer\n  abcdef12345",
+                    "Authorization: ***",
+                    1,
+                ),
+                // Значение в кавычках — по-прежнему в пределах кавычек.
+                (
+                    r#"Cookie: "a=1; b=2" user=ann"#,
+                    r#"Cookie: "***" user=ann"#,
+                    1,
+                ),
+                ("set-cookie: sid=1; Path=/\r\nok", "set-cookie: ***\r\nok", 1),
+            ],
+        );
+        let (out, n) = json(&standard(), r#"{"h":"Set-Cookie: sid=1; Path=/"}"#);
+        assert_eq!((out.as_str(), n), (r#"{"h":"Set-Cookie: ***"}"#, 1));
+    }
+
+    #[test]
+    fn next_line_pair_with_a_spaced_value_is_not_a_credential() {
+        texts(
+            &standard(),
+            &[
+                (
+                    "pwd: basic\npassword= hunter2",
+                    "pwd: ***\npassword= ***",
+                    2,
+                ),
+                // В той же строке слово с `=` в конце — учётные данные (base64 с выравниванием).
+                ("X-Auth-Token: Basic dXNlcjpwYXM=", "X-Auth-Token: ***", 1),
+                // На следующей строке без значения после `=` — тоже учётные данные.
+                ("token: Basic\ndXNlcjpwYXM=", "token: ***", 1),
+            ],
+        );
+    }
+
+    #[test]
+    fn pair_names_may_be_in_any_script() {
+        let own = Masker::new(&MaskConfig {
+            fields: Some(vec!["пароль".into(), "token".into()]),
+        });
+        texts(
+            &own,
+            &[
+                ("пароль=x", "пароль=***", 1),
+                ("логин=ann Пароль: секрет", "логин=ann Пароль: ***", 1),
+                ("token: Basic\nпароль= x", "token: ***\nпароль= ***", 2),
+            ],
+        );
+    }
+
     #[test]
     fn proxy_authorization_is_in_the_standard_list() {
         let m = standard();
````

- [ ] **Step 2: Тесты падают.**

Run: `cd crates/glafira-mcp && cargo test --lib mask`
Expected: `test result: FAILED. 15 passed; 4 failed`. Падают `cookies_and_authorization_are_masked_to_the_end_of_the_line` (`"Cookie: a=1; sessionid=xyz" → "Cookie: ***; sessionid=xyz"`), `next_line_pair_with_a_spaced_value_is_not_a_credential`, `pair_names_may_be_in_any_script` (`"пароль=x" → "пароль=x" (0)`) и `any_authorization_scheme_is_masked_with_its_credential`.

- [ ] **Step 3: Реализация.**

<!-- block: patch task1-src.diff -->
````diff
diff --git a/crates/glafira-mcp/src/mask.rs b/crates/glafira-mcp/src/mask.rs
index 07bc967..449e8ea 100644
--- a/crates/glafira-mcp/src/mask.rs
+++ b/crates/glafira-mcp/src/mask.rs
@@ -118,7 +118,7 @@ static DIGIT_RUN: LazyLock<Regex> =
 /// Имя пары `name=value`, `name: value`, `"name": value` в тексте вместе с разделителем.
 static PAIR: LazyLock<Regex> = LazyLock::new(|| {
     Regex::new(
-        r#"(?:"(?P<n1>[^"\\]{1,64})"|'(?P<n2>[^'\\]{1,64})'|(?P<n3>[A-Za-z_][A-Za-z0-9_.\-]{0,63}))\s*[:=]\s*"#,
+        r#"(?:"(?P<n1>[^"\\]{1,64})"|'(?P<n2>[^'\\]{1,64})'|(?P<n3>[\p{L}_][\p{L}\p{N}_.\-]*))\s*[:=]\s*"#,
     )
     .expect("pair regex")
 });
@@ -126,52 +126,62 @@ static PAIR: LazyLock<Regex> = LazyLock::new(|| {
 static VALUE: LazyLock<Regex> = LazyLock::new(|| {
     Regex::new(r#"^(?:"(?:[^"\\]|\\.)*"|'[^']*'|[^\s,;&}\]]+)"#).expect("value regex")
 });
-/// Слово учётных данных после известной схемы авторизации — и на следующей строке.
+/// Слово учётных данных после схемы авторизации — и на следующей строке.
 static CREDENTIAL: LazyLock<Regex> =
     LazyLock::new(|| Regex::new(r"^\s+[^\s,;&}\]]+").expect("credential regex"));
-/// Слово учётных данных после любой другой схемы — только в той же строке.
-static SAME_LINE_CREDENTIAL: LazyLock<Regex> =
-    LazyLock::new(|| Regex::new(r"^[ \t]+[^\s,;&}\]]+").expect("same-line credential regex"));
 /// Разделитель пары сразу за словом: слово — имя следующей пары.
 static NEXT_PAIR: LazyLock<Regex> =
     LazyLock::new(|| Regex::new(r"^\s*[:=]").expect("next pair regex"));
 /// Слово начинается с пары `name=value`.
 static GLUED_PAIR: LazyLock<Regex> =
-    LazyLock::new(|| Regex::new(r"^[A-Za-z_][\w.-]*=[^=]").expect("glued pair regex"));
+    LazyLock::new(|| Regex::new(r"^[\p{L}_][\p{L}\p{N}_.\-]*=[^=]").expect("glued pair regex"));
+/// Слово — имя пары с `=` в конце (`password=`).
+static OPEN_PAIR: LazyLock<Regex> =
+    LazyLock::new(|| Regex::new(r"^[\p{L}_][\p{L}\p{N}_.\-]*=$").expect("open pair regex"));
+/// За словом в той же строке после пробелов стоит значение.
+static SPACED_VALUE: LazyLock<Regex> =
+    LazyLock::new(|| Regex::new(r"^[ \t]+[^\s]").expect("spaced value regex"));
 
-/// Сколько байтов `rest` занимают учётные данные, если значение пары — схема авторизации
-/// (`Authorization: Negotiate YII…`, иначе они остались бы за `***`): `Bearer`, `Basic`, `Digest`,
-/// `Token` — у любого имени из списка, токен может быть и на следующей строке; любая другая схема
-/// (токен RFC 7235) — только у имён, оканчивающихся на `authorization`, и в той же строке.
+/// Схемы авторизации, после которых учётные данные берутся следующим словом.
+const SCHEMES: [&str; 4] = ["bearer", "basic", "digest", "token"];
+
+fn is_scheme(value: &str) -> bool {
+    SCHEMES.iter().any(|s| value.eq_ignore_ascii_case(s))
+}
+
+/// Имена, значение которых без кавычек маскируется до конца строки: куки и заголовки
+/// авторизации (`Cookie: a=1; b=2`, `Authorization: Digest username="u", response="…"`).
+fn masks_line(name: &str) -> bool {
+    let n = normalize(name);
+    n == "cookie" || n == "setcookie" || n.ends_with("authorization")
+}
+
+/// Длина строки `s` до `\n` или `\r` (или до конца).
+fn line_len(s: &str) -> usize {
+    s.find(['\n', '\r']).unwrap_or(s.len())
+}
+
+/// Сколько байтов `rest` занимают учётные данные, если значение пары — схема `Bearer`, `Basic`,
+/// `Digest` или `Token` (иначе они остались бы за `***`); токен может быть и на следующей строке.
 /// Учётные данные — слово целиком (`user:p4ss`, `abc@x`, `a=b`), но не имя следующей пары: слово
 /// с `:` в конце или перед `:`/`=` (`access_token: x`), а на следующей строке и слово,
-/// начинающееся с `name=value` (`user=ann`; в той же строке это параметр схемы).
-fn credential_len(name: &str, value: &str, rest: &str) -> usize {
-    let re = if ["bearer", "basic", "digest", "token"]
-        .iter()
-        .any(|s| value.eq_ignore_ascii_case(s))
-    {
-        &*CREDENTIAL
-    } else if normalize(name).ends_with("authorization")
-        && value
-            .bytes()
-            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
-    {
-        &*SAME_LINE_CREDENTIAL
-    } else {
+/// начинающееся с `name=value` (`user=ann`), и слово `name=` со значением после пробелов
+/// (`password= x`); в той же строке такие слова — параметр схемы или base64 с `=`.
+fn credential_len(value: &str, rest: &str) -> usize {
+    if !is_scheme(value) {
         return 0;
-    };
-    let Some(cred) = re.find(rest) else {
+    }
+    let Some(cred) = CREDENTIAL.find(rest) else {
         return 0;
     };
     let word = cred.as_str().trim_start();
+    let after = &rest[cred.end()..];
     let next_line = !cred.as_str()[..cred.len() - word.len()]
         .bytes()
         .all(|b| b == b' ' || b == b'\t');
-    if word.ends_with(':')
-        || NEXT_PAIR.is_match(&rest[cred.end()..])
-        || (next_line && GLUED_PAIR.is_match(word))
-    {
+    let next_pair =
+        GLUED_PAIR.is_match(word) || (OPEN_PAIR.is_match(word) && SPACED_VALUE.is_match(after));
+    if word.ends_with(':') || NEXT_PAIR.is_match(after) || (next_line && next_pair) {
         0
     } else {
         cred.end()
@@ -349,8 +359,9 @@ impl Masker {
         !n.is_empty() && self.fields.iter().any(|p| glob(p, &n))
     }
 
-    /// Текстовый режим: значения пар `name=value` из списка → `***`, затем шаблоны. Значение пары
-    /// с именем не из списка просматривается дальше — в нём бывают свои пары
+    /// Текстовый режим: значения пар `name=value` из списка → `***`, затем шаблоны. У кук и
+    /// заголовков авторизации ([`masks_line`]) значение без кавычек — до конца строки. Значение
+    /// пары с именем не из списка просматривается дальше — в нём бывают свои пары
     /// (`url=…?password=x`, `note: token=y`).
     pub fn text(&self, s: &str, count: &mut usize) -> String {
         let mut out = String::with_capacity(s.len());
@@ -383,9 +394,18 @@ impl Masker {
                     out.push_str(MASK);
                     out.push(q);
                 }
+                None if masks_line(name) => {
+                    out.push_str(MASK);
+                    let end = head + line_len(&s[head..]);
+                    let rest_blank = s[pos..end].bytes().all(|b| b == b' ' || b == b'\t');
+                    pos = end;
+                    if rest_blank {
+                        pos += credential_len(v.as_str(), &s[pos..]);
+                    }
+                }
                 None => {
                     out.push_str(MASK);
-                    pos += credential_len(name, v.as_str(), &s[pos..]);
+                    pos += credential_len(v.as_str(), &s[pos..]);
                 }
             }
         }
@@ -1053,7 +1073,11 @@ mod tests {
                     r#"Cookie: "***" user=ann"#,
                     1,
                 ),
-                ("set-cookie: sid=1; Path=/\r\nok", "set-cookie: ***\r\nok", 1),
+                (
+                    "set-cookie: sid=1; Path=/\r\nok",
+                    "set-cookie: ***\r\nok",
+                    1,
+                ),
             ],
         );
         let (out, n) = json(&standard(), r#"{"h":"Set-Cookie: sid=1; Path=/"}"#);
````

- [ ] **Step 4: Тесты проходят.**

Run: `cd crates/glafira-mcp && cargo test --lib mask` — Expected: `test result: ok. 19 passed`. Затем `cargo test` (unit 68, `tests/server.rs` 55 — прогон около 5 с; если он идёт больше минуты, регулярка пар снова получила ограниченное повторение), `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` — чисто.

- [ ] **Step 5: Commit.**

````bash
git add crates/glafira-mcp/src/mask.rs
git commit -s -m "fix(mcp): куки и Authorization маскируются до конца строки; имя следующей пары не берётся как токен; имена пар на любом алфавите

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
````

### Task 2: Маскировка карт и JSON-чисел (`mask.rs`)

**Files:**
- Modify: `crates/glafira-mcp/src/mask.rs` (`DIGIT_RUN`, `card_shaped`, `Masker::node`, новая `integer_digits`, модуль `tests`)

**Interfaces:**
- Consumes: Task 1 (тот же файл).
- Produces: публичный API не меняется. Приватная `integer_digits(&serde_json::Number) -> Option<String>`.

Спец. §2.3, §2.4, §2.6:
- серия цифр ищется по границам цифр, а не слова: `id4111…` и `x_4111…` — карты;
- к формам окна добавляются 4-4-4-3, 4-3-3-3, 6-13, 8-8;
- JSON-число — карта, если цифры его модуля проходят проверку: отрицательные целые и целые значения с дробной частью или экспонентой меньше 10¹⁹.

Плюс два теста покрытия из бэклога, которые проходят сразу:
- `data()` с JSON длиннее `MASK_PARSE_LIMIT` (целиком и частично) идёт в текстовый режим;
- заголовок `Proxy-Authorization` в `header()`.

- [ ] **Step 1: Тесты.**

<!-- block: patch task2-tests.diff -->
````diff
diff --git a/crates/glafira-mcp/src/mask.rs b/crates/glafira-mcp/src/mask.rs
index 449e8ea..a0aff2b 100644
--- a/crates/glafira-mcp/src/mask.rs
+++ b/crates/glafira-mcp/src/mask.rs
@@ -1117,6 +1117,71 @@ mod tests {
         );
     }
 
+    #[test]
+    fn cards_glued_to_words_and_rare_groupings_are_found() {
+        texts(
+            &standard(),
+            &[
+                ("id4111111111111111", "id[card]", 1),
+                ("x_4111111111111111", "x_[card]", 1),
+                ("4111111111111111abc", "[card]abc", 1),
+                // Окна новых форм рядом с числом: 4-4-4-3, 4-3-3-3, 6-13, 8-8.
+                ("qty 2 4111 1111 1111 116", "qty 2 [card]", 1),
+                ("qty 2 4222 222 222 222", "qty 2 [card]", 1),
+                ("qty 2 411111 1111111111110", "qty 2 [card]", 1),
+                ("qty 2 41111111 11111111", "qty 2 [card]", 1),
+                // Без верной суммы Луна приклеенные цифры остаются.
+                ("id4111111111111112", "id4111111111111112", 0),
+            ],
+        );
+    }
+
+    #[test]
+    fn negative_and_float_json_card_numbers_are_masked() {
+        let m = standard();
+        for (input, want, want_n) in [
+            (r#"{"n":-4111111111111111}"#, r#"{"n":"[card]"}"#, 1),
+            (r#"{"n":4.111111111111111e15}"#, r#"{"n":"[card]"}"#, 1),
+            (r#"{"n":4111111111111111.0}"#, r#"{"n":"[card]"}"#, 1),
+            (r#"{"n":-12,"m":1.5}"#, r#"{"n":-12,"m":1.5}"#, 0),
+        ] {
+            assert_eq!(json(&m, input), (want.to_string(), want_n), "{input}");
+        }
+    }
+
+    #[test]
+    fn json_longer_than_the_parse_limit_is_masked_as_text() {
+        let m = standard();
+        let head = br#"{"password":"x","pad":""#;
+        let mut whole = head.to_vec();
+        whole.resize(MASK_PARSE_LIMIT + 10, b'a');
+        whole.extend(br#""}"#);
+        // Целиком передано, но длиннее предела разбора.
+        let mut n = 0;
+        let d = m.data(&whole, whole.len(), DataFormat::Json, 64, &mut n);
+        assert!(
+            d.text.starts_with(r#"{"password":"***","pad":"aaa"#),
+            "{}",
+            d.text
+        );
+        assert_eq!((d.truncated, d.size, n), (true, whole.len(), 1));
+        // Передан только предел разбора.
+        let mut n = 0;
+        let d = m.data(
+            &whole[..MASK_PARSE_LIMIT],
+            whole.len(),
+            DataFormat::Json,
+            64,
+            &mut n,
+        );
+        assert!(
+            d.text.starts_with(r#"{"password":"***","pad":"aaa"#),
+            "{}",
+            d.text
+        );
+        assert_eq!((d.truncated, n), (true, 1));
+    }
+
     #[test]
     fn proxy_authorization_is_in_the_standard_list() {
         let m = standard();
@@ -1136,6 +1201,12 @@ mod tests {
             (out.as_str(), n),
             (r#"{"Proxy-Authorization":"***","via":"proxy"}"#, 1)
         );
+        let mut n = 0;
+        assert_eq!(
+            m.header("Proxy-Authorization", "Basic dXNlcjpwNHNz", &mut n),
+            MASK
+        );
+        assert_eq!(n, 1);
         // Отдельное имя, а не `*authorization`: другие имена с этим окончанием в список не попадают.
         assert!(!m.field_matches("preauthorization"));
     }
````

- [ ] **Step 2: Тесты падают.**

Run: `cd crates/glafira-mcp && cargo test --lib mask`
Expected: `test result: FAILED. 20 passed; 2 failed`. Падают `cards_glued_to_words_and_rare_groupings_are_found` (`"id4111111111111111" → "id4111111111111111" (0)`) и `negative_and_float_json_card_numbers_are_masked`. `json_longer_than_the_parse_limit_is_masked_as_text` и дополненный `proxy_authorization_is_in_the_standard_list` проходят: это покрытие, поведение не меняется.

- [ ] **Step 3: Реализация.**

<!-- block: patch task2-src.diff -->
````diff
diff --git a/crates/glafira-mcp/src/mask.rs b/crates/glafira-mcp/src/mask.rs
index a0aff2b..d5951f8 100644
--- a/crates/glafira-mcp/src/mask.rs
+++ b/crates/glafira-mcp/src/mask.rs
@@ -112,9 +112,10 @@ static BEARER: LazyLock<Regex> =
 static EMAIL: LazyLock<Regex> = LazyLock::new(|| {
     Regex::new(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}").expect("email regex")
 });
-/// Серия групп цифр (кандидаты в номер карты): между группами одиночный пробел или дефис.
+/// Серия групп цифр (кандидаты в номер карты): между группами одиночный пробел или дефис; по
+/// краям — не цифра (буква рядом не мешает: `id4111…`).
 static DIGIT_RUN: LazyLock<Regex> =
-    LazyLock::new(|| Regex::new(r"\b[0-9]+(?:[ -][0-9]+)*\b").expect("digit run regex"));
+    LazyLock::new(|| Regex::new(r"[0-9]+(?:[ -][0-9]+)*").expect("digit run regex"));
 /// Имя пары `name=value`, `name: value`, `"name": value` в тексте вместе с разделителем.
 static PAIR: LazyLock<Regex> = LazyLock::new(|| {
     Regex::new(
@@ -213,11 +214,13 @@ fn is_card(digits: &[u8]) -> bool {
 }
 
 /// Длины групп окна в форме номера карты: одна группа из 13–19 цифр, 4-4-4-4 с необязательной
-/// последней группой из 1–3 цифр, 4-6-5 (Amex) или 4-6-4 (Diners).
+/// последней группой из 1–3 цифр, 4-6-5 (Amex), 4-6-4 (Diners), 4-4-4-3, 4-3-3-3, 6-13, 8-8.
 fn card_shaped(lens: &[usize]) -> bool {
     match lens {
         [n] => (13..=19).contains(n),
-        [4, 4, 4, 4] | [4, 6, 5] | [4, 6, 4] => true,
+        [4, 4, 4, 4] | [4, 6, 5] | [4, 6, 4] | [4, 4, 4, 3] | [4, 3, 3, 3] | [6, 13] | [8, 8] => {
+            true
+        }
         [4, 4, 4, 4, n] => (1..=3).contains(n),
         _ => false,
     }
@@ -318,6 +321,19 @@ fn patterns_in_place(run: &mut [u8], count: &mut usize) {
     }
 }
 
+/// Цифры модуля целого значения числа JSON: у отрицательного — без знака; у числа с дробной
+/// частью или экспонентой — если значение целое и по модулю меньше 10¹⁹.
+fn integer_digits(n: &serde_json::Number) -> Option<String> {
+    if let Some(u) = n.as_u64() {
+        return Some(u.to_string());
+    }
+    if let Some(i) = n.as_i64() {
+        return Some(i.unsigned_abs().to_string());
+    }
+    let f = n.as_f64()?.abs();
+    (f.fract() == 0.0 && f < 1e19).then(|| (f as u64).to_string())
+}
+
 /// Обрезает текст до `max` байтов по границе символа; true — обрезан.
 pub(crate) fn cut_utf8(mut s: String, max: usize) -> (String, bool) {
     if s.len() <= max {
@@ -425,8 +441,7 @@ impl Masker {
         match node {
             Node::String(s) => *s = self.text(s, count),
             Node::Number(n) => {
-                let digits = n.to_string();
-                if digits.bytes().all(|b| b.is_ascii_digit()) && is_card(digits.as_bytes()) {
+                if integer_digits(n).is_some_and(|d| is_card(d.as_bytes())) {
                     *count += 1;
                     *node = Node::String("[card]".into());
                 }
````

- [ ] **Step 4: Тесты проходят.**

Run: `cd crates/glafira-mcp && cargo test --lib mask` — Expected: `test result: ok. 22 passed`. Затем `cargo test` (unit 71, `tests/server.rs` 55), `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` — чисто.

- [ ] **Step 5: Commit.**

````bash
git add crates/glafira-mcp/src/mask.rs
git commit -s -m "fix(mcp): номера карт рядом с буквами и в редких группировках; отрицательные и дробные JSON-числа

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
````

### Task 3: Топик без права Describe — «Недостаточно прав»

**Files:**
- Modify: `src-tauri/src/browse/session.rs` (`topic_not_authorized`, `topic_partitions`, импорты)
- Modify: `src-tauri/src/mcp/messages.rs` (`offsets_partitions`, удаление `offsets_partition_count`, импорты; тесты: фальшивый брокер с метаданными, `summarize()`)
- Modify: `src-tauri/src/mcp/backend.rs` (тест `offsets_topic_without_describe_gets_the_read_hint`)

**Interfaces:**
- Consumes: —
- Produces:
  - `pub(crate) fn crate::browse::session::topic_not_authorized() -> AppError` — код `NotAuthorized`, ключ `errors.kafka.notAuthorized`;
  - `topic_partitions` с прежней сигнатурой;
  - `crate::mcp::messages::offsets_partition_count` удаляется;
  - в тестах `mcp::messages`: `fake_broker(reply)`, `metadata_broker(topic, error)`, `summarize()`, константы `API_VERSIONS`/`METADATA`, `api_versions_reply(correlation_id, metadata_max)`.

Спец. §3.1, §6.2:
- `topic_partitions` берёт ошибку топика из метаданных до `convert`, который её отбрасывал: `TOPIC_AUTHORIZATION_FAILED` → `topic_not_authorized()`, нет топика или партиций → `topic_not_found`;
- `offsets_partitions` считает партиции `__consumer_offsets` через `topic_partitions`; подсказка про Read остаётся в `scan_to_backend`;
- фальшивый брокер отвечает на ApiVersions (Metadata до v1) и на Metadata: один брокер (он сам) и топик с заданной ошибкой без партиций.

- [ ] **Step 1: Тесты.**

<!-- block: patch task3-tests.diff -->
````diff
diff --git a/src-tauri/src/mcp/backend.rs b/src-tauri/src/mcp/backend.rs
index a12aeca..670258b 100644
--- a/src-tauri/src/mcp/backend.rs
+++ b/src-tauri/src/mcp/backend.rs
@@ -1637,12 +1637,7 @@ mod tests {
     #[test]
     fn offsets_topic_without_describe_gets_the_read_hint() {
         // Нет никаких ACL на `__consumer_offsets`: ошибка уже в метаданных, до чтения.
-        let denied = crate::mcp::messages::offsets_partition_count(
-            Some(rdkafka::types::RDKafkaErrorCode::TopicAuthorizationFailed),
-            0,
-        )
-        .unwrap_err();
-        let e = scan_to_backend(denied, true);
+        let e = scan_to_backend(crate::browse::session::topic_not_authorized(), true);
         assert_eq!(e.code, "NOT_AUTHORIZED");
         assert!(
             e.message.ends_with(
diff --git a/src-tauri/src/mcp/messages.rs b/src-tauri/src/mcp/messages.rs
index 2ec868f..fd25b06 100644
--- a/src-tauri/src/mcp/messages.rs
+++ b/src-tauri/src/mcp/messages.rs
@@ -631,6 +631,7 @@ pub fn local_utc_offset_minutes() -> i32 {
 mod tests {
     use std::io::{Read, Write};
     use std::net::{SocketAddr, TcpListener};
+    use std::sync::Arc;
     use std::sync::atomic::Ordering;
 
     use super::*;
@@ -818,29 +819,21 @@ mod tests {
         assert_eq!((kept.len(), truncated), (MAX_HEADERS, false));
     }
 
-    #[test]
-    fn offsets_topic_without_describe_is_an_authorization_error() {
-        // Без права Describe брокер отвечает TOPIC_AUTHORIZATION_FAILED без партиций.
-        let e = offsets_partition_count(Some(RDKafkaErrorCode::TopicAuthorizationFailed), 0)
-            .unwrap_err();
-        assert_eq!(e.code, ErrorCode::NotAuthorized);
-        assert_eq!(offsets_partition_count(None, 50).ok(), Some(50));
-        assert_eq!(
-            offsets_partition_count(None, 0).map_err(|e| e.code),
-            Err(ErrorCode::NotFound)
-        );
-    }
+    const API_VERSIONS: i16 = 18;
+    const METADATA: i16 = 3;
 
-    /// Брокер, который отвечает только на ApiVersions (соединение готово), а метаданные и
-    /// ListOffsets оставляет без ответа: каждый запрос планирования ждёт свой таймаут целиком.
-    /// Недоступный адрес для этого не годится: `fetch_metadata` без готового брокера сразу
-    /// после первой порции ожидания отвечает «Broker transport failure».
-    fn stalled_broker() -> SocketAddr {
+    /// Фальшивый брокер на 127.0.0.1: на каждый запрос `reply(api_key, correlation_id, port)`
+    /// отвечает готовым ответом или молчит.
+    fn fake_broker(
+        reply: impl Fn(i16, &[u8], u16) -> Option<Vec<u8>> + Send + Sync + 'static,
+    ) -> SocketAddr {
         let listener = TcpListener::bind("127.0.0.1:0").unwrap();
         let addr = listener.local_addr().unwrap();
+        let reply = Arc::new(reply);
         std::thread::spawn(move || {
             for conn in listener.incoming() {
                 let Ok(mut conn) = conn else { return };
+                let reply = reply.clone();
                 std::thread::spawn(move || {
                     let mut size = [0u8; 4];
                     while conn.read_exact(&mut size).is_ok() {
@@ -848,9 +841,13 @@ mod tests {
                         if conn.read_exact(&mut req).is_err() {
                             return;
                         }
-                        // Заголовок запроса: api_key (18 — ApiVersions), версия, correlation_id.
-                        if req.len() >= 8 && req[..2] == [0, 18] {
-                            let _ = conn.write_all(&api_versions_reply(&req[4..8]));
+                        // Заголовок запроса: api_key, версия, correlation_id.
+                        if req.len() < 8 {
+                            continue;
+                        }
+                        let api_key = i16::from_be_bytes([req[0], req[1]]);
+                        if let Some(out) = reply(api_key, &req[4..8], addr.port()) {
+                            let _ = conn.write_all(&out);
                         }
                     }
                 });
@@ -859,12 +856,33 @@ mod tests {
         addr
     }
 
-    /// Ответ ApiVersions v3 (заголовок ответа v0): брокер знает только ApiVersions и Metadata.
-    fn api_versions_reply(correlation_id: &[u8]) -> Vec<u8> {
+    /// Брокер, который отвечает только на ApiVersions (соединение готово), а метаданные и
+    /// ListOffsets оставляет без ответа: каждый запрос планирования ждёт свой таймаут целиком.
+    /// Недоступный адрес для этого не годится: `fetch_metadata` без готового брокера сразу
+    /// после первой порции ожидания отвечает «Broker transport failure».
+    fn stalled_broker() -> SocketAddr {
+        fake_broker(|api_key, correlation_id, _| {
+            (api_key == API_VERSIONS).then(|| api_versions_reply(correlation_id, 12))
+        })
+    }
+
+    /// Брокер с метаданными: на любой запрос Metadata — один брокер (он сам) и топик `topic` с
+    /// кодом ошибки `error` без партиций (без права Describe брокер отвечает так: 29).
+    fn metadata_broker(topic: &'static str, error: i16) -> SocketAddr {
+        fake_broker(move |api_key, correlation_id, port| match api_key {
+            API_VERSIONS => Some(api_versions_reply(correlation_id, 1)),
+            METADATA => Some(metadata_reply(correlation_id, port, topic, error)),
+            _ => None,
+        })
+    }
+
+    /// Ответ ApiVersions v3 (заголовок ответа v0): брокер знает только ApiVersions и Metadata
+    /// до версии `metadata_max`.
+    fn api_versions_reply(correlation_id: &[u8], metadata_max: i16) -> Vec<u8> {
         let mut body = correlation_id.to_vec();
         body.extend(0i16.to_be_bytes()); // error_code
         body.push(3); // compact array: 2 элемента + 1
-        for (api_key, max_version) in [(18i16, 3i16), (3, 12)] {
+        for (api_key, max_version) in [(API_VERSIONS, 3i16), (METADATA, metadata_max)] {
             body.extend(api_key.to_be_bytes());
             body.extend(0i16.to_be_bytes());
             body.extend(max_version.to_be_bytes());
@@ -872,6 +890,33 @@ mod tests {
         }
         body.extend(0i32.to_be_bytes()); // throttle_time_ms
         body.push(0); // tagged fields
+        framed(body)
+    }
+
+    /// Ответ Metadata v1 (заголовок ответа v0): брокер 1 на `127.0.0.1:port`, он же контроллер,
+    /// и один топик без партиций.
+    fn metadata_reply(correlation_id: &[u8], port: u16, topic: &str, error: i16) -> Vec<u8> {
+        let mut body = correlation_id.to_vec();
+        body.extend(1i32.to_be_bytes()); // brokers: 1
+        body.extend(1i32.to_be_bytes()); // node_id
+        put_str(&mut body, "127.0.0.1");
+        body.extend(i32::from(port).to_be_bytes());
+        body.extend((-1i16).to_be_bytes()); // rack: null
+        body.extend(1i32.to_be_bytes()); // controller_id
+        body.extend(1i32.to_be_bytes()); // topics: 1
+        body.extend(error.to_be_bytes());
+        put_str(&mut body, topic);
+        body.push(u8::from(topic == OFFSETS_TOPIC)); // is_internal
+        body.extend(0i32.to_be_bytes()); // partitions: 0
+        framed(body)
+    }
+
+    fn put_str(buf: &mut Vec<u8>, s: &str) {
+        buf.extend((s.len() as i16).to_be_bytes());
+        buf.extend(s.as_bytes());
+    }
+
+    fn framed(body: Vec<u8>) -> Vec<u8> {
         let mut out = (body.len() as u32).to_be_bytes().to_vec();
         out.extend(body);
         out
@@ -905,6 +950,21 @@ mod tests {
         }
     }
 
+    fn summarize() -> MessagesOp {
+        MessagesOp::Summarize {
+            connection_id: "a".into(),
+            topic: "orders".into(),
+            from_ms: 0,
+            to_ms: 1_000,
+            partitions: None,
+            filter: MessageFilter::default(),
+            key_format: FormatArg::Auto,
+            value_format: FormatArg::Auto,
+            bucket_ms: 60_000,
+            top_keys: 10,
+        }
+    }
+
     fn commits() -> MessagesOp {
         MessagesOp::CommitHistory {
             connection_id: "a".into(),
@@ -921,18 +981,6 @@ mod tests {
         let expired = ScanCtx::new(Duration::ZERO);
         let cancelled = ScanCtx::new(Duration::from_secs(40));
         cancelled.cancel.store(true, Ordering::Relaxed);
-        let summarize = MessagesOp::Summarize {
-            connection_id: "a".into(),
-            topic: "orders".into(),
-            from_ms: 0,
-            to_ms: 1_000,
-            partitions: None,
-            filter: MessageFilter::default(),
-            key_format: FormatArg::Auto,
-            value_format: FormatArg::Auto,
-            bucket_ms: 60_000,
-            top_keys: 10,
-        };
         let offsets = Range::Offsets {
             partition: 0,
             start: 0,
@@ -941,7 +989,7 @@ mod tests {
         let cases = [
             ("read window, expired", read(window()), &expired),
             ("read offsets, cancelled", read(offsets), &cancelled),
-            ("summarize, expired", summarize, &expired),
+            ("summarize, expired", summarize(), &expired),
             ("commits, expired", commits(), &expired),
             ("commits, cancelled", commits(), &cancelled),
         ];
@@ -978,4 +1026,35 @@ mod tests {
             "{e:?}"
         );
     }
+
+    #[test]
+    fn topic_without_describe_is_not_authorized_and_a_missing_one_is_not_found() {
+        let cases = [
+            (29, ErrorCode::NotAuthorized, "errors.kafka.notAuthorized"),
+            (3, ErrorCode::NotFound, "errors.topics.notFound"),
+        ];
+        for (error, code, key) in cases {
+            let p = profile_at(metadata_broker("orders", error), 5_000);
+            for (name, op) in [("read", read(window())), ("summarize", summarize())] {
+                let ctx = ScanCtx::new(Duration::from_secs(10));
+                let e = scan(&p, &MemoryStore::default(), op, &ctx).expect_err(name);
+                assert_eq!(
+                    (e.code, e.key.as_str()),
+                    (code, key),
+                    "{name}, {error}: {e:?}"
+                );
+            }
+        }
+    }
+
+    #[test]
+    fn offsets_topic_without_describe_is_not_authorized() {
+        let p = profile_at(metadata_broker(OFFSETS_TOPIC, 29), 5_000);
+        let ctx = ScanCtx::new(Duration::from_secs(10));
+        let e = scan(&p, &MemoryStore::default(), commits(), &ctx).expect_err("нет Describe");
+        assert_eq!(
+            (e.code, e.key.as_str()),
+            (ErrorCode::NotAuthorized, "errors.kafka.notAuthorized")
+        );
+    }
 }
````

- [ ] **Step 2: Тесты падают.**

Run: `cd src-tauri && cargo test --lib mcp::messages`
Expected: компиляция падает — `error[E0425]: cannot find function topic_not_authorized in module crate::browse::session`.

- [ ] **Step 3: Реализация.**

<!-- block: patch task3-src.diff -->
````diff
diff --git a/src-tauri/src/browse/session.rs b/src-tauri/src/browse/session.rs
index 2f1af95..5bcb92a 100644
--- a/src-tauri/src/browse/session.rs
+++ b/src-tauri/src/browse/session.rs
@@ -6,6 +6,7 @@ use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
 use rdkafka::consumer::{BaseConsumer, Consumer};
 use rdkafka::error::{KafkaError, KafkaResult};
 use rdkafka::message::{BorrowedMessage, Headers, Message};
+use rdkafka::types::RDKafkaErrorCode;
 use rdkafka::{ClientConfig, Offset, TopicPartitionList};
 use serde::Deserialize;
 
@@ -19,8 +20,7 @@ use crate::browse::types::{BrowseEvent, BrowsedMessage, FinishReason, HeaderView
 use crate::error::{AppError, AppResult, ErrorCode, from_kafka};
 use crate::kafka::config::build_client_config;
 use crate::kafka::context::CapturingContext;
-use crate::kafka::errors::{fetch_metadata_fast_fail, op_error};
-use crate::kafka::metadata::convert;
+use crate::kafka::errors::{fetch_metadata_fast_fail, item_error, op_error};
 use crate::kafka::recovery::{PROBE_TIMEOUT, RecoveryMonitor, is_transient};
 use crate::kafka::topics::fetch_bounds;
 use crate::secrets::SecretStore;
@@ -266,7 +266,21 @@ pub(crate) fn load_bounds(
     partition_bounds(consumer, topic, &selected, timeout, kerr)
 }
 
-/// Выбранные партиции топика по метаданным (`partitions` пусто или `None` — все).
+/// Отказ в правах на топик: без права Describe брокер отвечает `TOPIC_AUTHORIZATION_FAILED`
+/// без партиций.
+pub(crate) fn topic_not_authorized() -> AppError {
+    item_error(
+        RDKafkaErrorCode::TopicAuthorizationFailed,
+        "",
+        ltext!(
+            "errors.kafka.notAuthorized",
+            "Not authorized to perform the operation"
+        ),
+    )
+}
+
+/// Выбранные партиции топика по метаданным (`partitions` пусто или `None` — все). Ошибка топика
+/// в метаданных различает отказ в правах и «топика нет» (спец. G4.1 §3.1).
 pub(crate) fn topic_partitions(
     consumer: &BaseConsumer<CapturingContext>,
     topic: &str,
@@ -274,12 +288,18 @@ pub(crate) fn topic_partitions(
     timeout: Duration,
 ) -> AppResult<Vec<i32>> {
     let md = fetch_metadata_fast_fail(consumer, Some(topic), timeout)?;
-    let (_, topics) = convert(&md);
-    let found = topics
-        .into_iter()
-        .find(|t| t.name == topic && !t.partitions.is_empty())
-        .ok_or_else(|| crate::kafka::admin::topic_not_found(topic))?;
-    let all: Vec<i32> = found.partitions.iter().map(|p| p.id).collect();
+    let meta = md.topics().iter().find(|t| t.name() == topic);
+    if meta.and_then(|t| t.error()).map(RDKafkaErrorCode::from)
+        == Some(RDKafkaErrorCode::TopicAuthorizationFailed)
+    {
+        return Err(topic_not_authorized());
+    }
+    let all: Vec<i32> = meta
+        .map(|t| t.partitions().iter().map(|p| p.id()).collect())
+        .unwrap_or_default();
+    if all.is_empty() {
+        return Err(crate::kafka::admin::topic_not_found(topic));
+    }
     match partitions {
         Some(list) if !list.is_empty() => {
             if let Some(bad) = list.iter().find(|p| !all.contains(p)) {
diff --git a/src-tauri/src/mcp/messages.rs b/src-tauri/src/mcp/messages.rs
index fd25b06..60000fc 100644
--- a/src-tauri/src/mcp/messages.rs
+++ b/src-tauri/src/mcp/messages.rs
@@ -18,7 +18,6 @@ use rdkafka::ClientConfig;
 use rdkafka::consumer::{BaseConsumer, Consumer};
 use rdkafka::error::KafkaError;
 use rdkafka::message::{BorrowedMessage, Message, Timestamp};
-use rdkafka::types::RDKafkaErrorCode;
 
 use crate::browse::filter::{CompiledFilter, FilterSpec, HeaderMatch, TextMatch};
 use crate::browse::format::DataFormat;
@@ -30,7 +29,7 @@ use crate::browse::session::{
 use crate::browse::types::{BrowsedMessage, HeaderView};
 use crate::error::{AppError, AppResult, ErrorCode};
 use crate::kafka::context::CapturingContext;
-use crate::kafka::errors::{fetch_metadata_fast_fail, item_error, op_error};
+use crate::kafka::errors::op_error;
 use crate::kafka::offsets_log::{self, OFFSETS_TOPIC, OffsetsRecord};
 use crate::kafka::recovery::{PROBE_TIMEOUT, RecoveryMonitor, is_transient};
 use crate::secrets::SecretStore;
@@ -58,27 +57,6 @@ fn mcp_tune(profile: &ConnectionProfile) -> impl FnOnce(&mut ClientConfig) {
     }
 }
 
-/// Число партиций `__consumer_offsets` по его метаданным: ошибка топика и число партиций
-/// (спец. §6.3). Без права Describe брокер отвечает `TOPIC_AUTHORIZATION_FAILED` без партиций —
-/// это отказ в правах (вызывающий добавит подсказку про Read), а не «топика нет».
-pub(crate) fn offsets_partition_count(
-    error: Option<RDKafkaErrorCode>,
-    partitions: usize,
-) -> AppResult<i32> {
-    match error {
-        Some(code @ RDKafkaErrorCode::TopicAuthorizationFailed) => Err(item_error(
-            code,
-            "",
-            ltext!(
-                "errors.kafka.notAuthorized",
-                "Not authorized to perform the operation"
-            ),
-        )),
-        _ if partitions == 0 => Err(crate::kafka::admin::topic_not_found(OFFSETS_TOPIC)),
-        _ => Ok(partitions as i32),
-    }
-}
-
 /// Отмена или дедлайн до конца планирования: прочитать ещё ничего не успели, поэтому ошибка,
 /// а не частичный ответ (спец. §6.2).
 pub(crate) fn planning_stopped() -> AppError {
@@ -342,15 +320,11 @@ impl<'a> Scan<'a> {
         )
     }
 
-    /// Число партиций `__consumer_offsets` по метаданным — с ошибкой топика, которую
-    /// `topic_partitions` не различает (нет права Describe выглядит как «топика нет»).
+    /// Число партиций `__consumer_offsets` по метаданным; без права Describe — отказ в правах
+    /// (вызывающий добавит подсказку про Read), а не «топика нет».
     fn offsets_partitions(&self) -> AppResult<i32> {
-        let md = fetch_metadata_fast_fail(&self.consumer, Some(OFFSETS_TOPIC), self.wait()?)?;
-        let topic = md.topics().iter().find(|t| t.name() == OFFSETS_TOPIC);
-        offsets_partition_count(
-            topic.and_then(|t| t.error()).map(RDKafkaErrorCode::from),
-            topic.map_or(0, |t| t.partitions().len()),
-        )
+        let all = topic_partitions(&self.consumer, OFFSETS_TOPIC, None, self.wait()?)?;
+        Ok(all.len() as i32)
     }
 
     fn offsets_at(
````

- [ ] **Step 4: Тесты проходят.**

Run: `cd src-tauri && cargo test --lib without_describe` — Expected: `test result: ok. 3 passed` (два новых и тест подсказки в `backend.rs`, около 0,1 с). Затем:
- `cargo test --lib` — 522;
- `cargo clippy --all-targets -- -D warnings`, `cargo fmt --check` — чисто;
- на стенде: `KE_IT_BOOTSTRAP=localhost:9092 cargo test --test it_browse` (6) и `--test it_mcp` (2).

Проверка, что тесты ловят ошибку: если в `topic_partitions` сравнивать ошибку не с `TopicAuthorizationFailed`, оба новых теста падают с `(NotFound, "errors.topics.notFound")`.

- [ ] **Step 5: Commit.**

````bash
git add src-tauri/src/browse/session.rs src-tauri/src/mcp/messages.rs src-tauri/src/mcp/backend.rs
git commit -s -m "fix(messages): топик без права Describe — «Недостаточно прав», а не «не найден» (экран «Сообщения» и MCP)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
````

### Task 4: История без серий, запас бюджета, копия ключа, тесты крейта

**Files:**
- Modify: `crates/glafira-mcp/src/messages.rs` (`SCAN_GRACE`)
- Modify: `crates/glafira-mcp/src/server/mod.rs` (`GatewayOptions::scan_grace`)
- Modify: `crates/glafira-mcp/src/server/message_tools.rs` (запас из настроек, `HISTORY_TOO_LARGE`, `fit_history`, импорт `Duration`; тест)
- Modify: `crates/glafira-mcp/src/mask.rs` (`KEY_PARSE_LIMIT`)
- Modify: `crates/glafira-mcp/tests/server.rs` (ход `progress` в фальшивом бэкенде, `ProgressLog`, `call_with`, три теста, `scan_grace` в тесте таймаута)
- Modify: `src-tauri/src/mcp/messages.rs` (`raw_data` с пределом, ключ до `KEY_PARSE_LIMIT`; тест)

**Interfaces:**
- Consumes: Task 3 (`src-tauri/src/mcp/messages.rs` уже без `offsets_partition_count`).
- Produces:
  - `glafira_mcp::mask::KEY_PARSE_LIMIT: usize = 16384`;
  - `glafira_mcp::messages::SCAN_GRACE: Duration = 5 с`;
  - `GatewayOptions::scan_grace: Duration` (по умолчанию `SCAN_GRACE`);
  - приватная `fit_history(&mut HistoryBody) -> Result<bool, BackendError>`;
  - в приложении `raw_data(bytes: &[u8], format: DataFormat, limit: usize) -> RawData`.

Спец. §3.2–3.4, §6.3, §6.4:
- `fit_history` вызывает `fit_runs` и, если ответ не помещается и без `runs`, отвечает `response_too_large`; `history_body` использует её;
- страховочный таймаут берёт запас из `GatewayOptions`;
- ключ копируется для маскировки не длиннее 16 КиБ.

Тесты крейта:
- прогресс: клиент rmcp сам ставит `progressToken` = 0 у первого запроса;
- занятость `observe_consumer_group` при идущем `read_messages`: `durationSeconds` 10 — меньше проверяется раньше занятости;
- неверные аргументы `summarize_messages` не доходят до бэкенда.

- [ ] **Step 1: Тесты.**

<!-- block: patch task4-tests.diff -->
````diff
diff --git a/crates/glafira-mcp/src/server/message_tools.rs b/crates/glafira-mcp/src/server/message_tools.rs
index eec1a8e..f8ecd85 100644
--- a/crates/glafira-mcp/src/server/message_tools.rs
+++ b/crates/glafira-mcp/src/server/message_tools.rs
@@ -1093,6 +1093,8 @@ impl GatewayServer {
 
 #[cfg(test)]
 mod tests {
+    use std::time::Duration;
+
     use super::*;
     use crate::commits::MAX_RUNS;
 
@@ -1176,4 +1178,19 @@ mod tests {
                 .all(|p| p.runs.len() == 10 && !p.runs_truncated)
         );
     }
+
+    #[test]
+    fn history_that_does_not_fit_even_without_runs_is_too_large() {
+        let mut body = history(3000, 1);
+        let e = fit_history(&mut body).unwrap_err();
+        assert_eq!(e.code, "response_too_large");
+        // Без `runs` помещается — ответ с обрезкой, как раньше.
+        let mut body = history(1200, 2);
+        assert_eq!(fit_history(&mut body).ok(), Some(true));
+        assert!(
+            body.partitions
+                .iter()
+                .all(|p| p.runs.is_empty() && p.runs_truncated)
+        );
+    }
 }
diff --git a/crates/glafira-mcp/tests/server.rs b/crates/glafira-mcp/tests/server.rs
index 4263efa..65242a3 100644
--- a/crates/glafira-mcp/tests/server.rs
+++ b/crates/glafira-mcp/tests/server.rs
@@ -20,10 +20,10 @@ use glafira_mcp::{
 };
 use rmcp::model::{
     CallToolRequest, CallToolRequestParams, CallToolResult, ClientCapabilities, ClientConfig,
-    ClientRequest, Implementation,
+    ClientRequest, Implementation, NumberOrString, ProgressNotificationParam, ProgressToken,
 };
-use rmcp::service::{PeerRequestOptions, RunningService};
-use rmcp::{RoleClient, ServiceExt};
+use rmcp::service::{NotificationContext, PeerRequestOptions, RunningService};
+use rmcp::{ClientHandler, RoleClient, ServiceExt};
 use serde_json::{Value, json};
 
 const TOKEN: &str = "test-token";
@@ -671,6 +671,14 @@ impl Backend for Fake {
                             "messages_not_allowed",
                             "LLM access to this connection is limited to metadata",
                         )),
+                        // Три сообщения о ходе, затем пустой ответ.
+                        "progress" => {
+                            for i in 1..=3 {
+                                (ctx.progress)(i, 3);
+                                tokio::time::sleep(Duration::from_millis(30)).await;
+                            }
+                            Ok(read(vec![], ScanStats::default()))
+                        }
                         "slow" => {
                             for i in 0..500 {
                                 if ctx.cancelled() {
@@ -938,6 +946,15 @@ async fn named_client(
 }
 
 async fn call(c: &RunningService<RoleClient, ()>, tool: &str, args: Value) -> CallToolResult {
+    call_with(c, tool, args).await
+}
+
+/// `call` для клиента с любым обработчиком.
+async fn call_with<H: ClientHandler>(
+    c: &RunningService<RoleClient, H>,
+    tool: &str,
+    args: Value,
+) -> CallToolResult {
     let mut p = CallToolRequestParams::new(tool.to_string());
     if let Value::Object(m) = args {
         p = p.with_arguments(m);
@@ -2725,11 +2742,12 @@ async fn scan_deadline_counts_from_the_start_of_the_call() {
 async fn timed_out_scan_is_told_to_stop() {
     let fake = Fake::new(SessionAccess::Approved);
     let e = env_with(fake.clone(), |o| {
-        o.scan_deadline = Duration::from_millis(50)
+        o.scan_deadline = Duration::from_millis(50);
+        o.scan_grace = Duration::from_millis(100);
     })
     .await;
     let c = client(&e.ep).await;
-    // Страховочный таймаут — бюджет + 5 с.
+    // Страховочный таймаут — бюджет + `scan_grace`.
     let msg = err(&call(&c, "read_messages", window_args("stuck")).await);
     assert!(msg.starts_with("TIMEOUT:"), "{msg}");
     let mut stopped = false;
@@ -2854,3 +2872,91 @@ async fn backends_without_messages_answer_not_supported() {
         .unwrap_err();
     assert_eq!(e.code, "not_supported");
 }
+
+/// Клиент, который запоминает уведомления о ходе.
+#[derive(Clone, Default)]
+struct ProgressLog(Arc<Mutex<Vec<ProgressNotificationParam>>>);
+
+impl ClientHandler for ProgressLog {
+    async fn on_progress(
+        &self,
+        params: ProgressNotificationParam,
+        _: NotificationContext<RoleClient>,
+    ) {
+        self.0.lock().unwrap().push(params);
+    }
+}
+
+#[tokio::test]
+async fn scan_progress_reaches_the_client_with_its_token() {
+    let e = env(Fake::new(SessionAccess::Approved)).await;
+    let mut s = endpoint::connect(&e.ep).await.unwrap();
+    handshake::client(&mut s, TOKEN).await.unwrap();
+    let log = ProgressLog::default();
+    let c = log.clone().serve(s).await.unwrap();
+    // rmcp сам кладёт в `_meta` запроса свой progressToken: у первого запроса клиента — 0.
+    ok(call_with(&c, "read_messages", window_args("progress")).await);
+    // Обработчик уведомлений клиента может отставать от ответа.
+    for _ in 0..100 {
+        if !log.0.lock().unwrap().is_empty() {
+            break;
+        }
+        tokio::time::sleep(Duration::from_millis(10)).await;
+    }
+    let notes = log.0.lock().unwrap().clone();
+    assert!(!notes.is_empty(), "уведомлений о ходе нет");
+    let token = ProgressToken(NumberOrString::Number(0));
+    assert!(notes.iter().all(|n| n.progress_token == token), "{notes:?}");
+    assert!(notes.iter().all(|n| n.total == Some(3.0)));
+    assert!(
+        notes.windows(2).all(|w| w[0].progress <= w[1].progress),
+        "ход не убывает"
+    );
+}
+
+#[tokio::test]
+async fn observation_is_busy_while_a_scan_runs() {
+    let e = env(Fake::new(SessionAccess::Approved)).await;
+    let c = client(&e.ep).await;
+    let Value::Object(args) = window_args("slow") else {
+        unreachable!()
+    };
+    let request = ClientRequest::CallToolRequest(CallToolRequest::new(
+        CallToolRequestParams::new("read_messages").with_arguments(args),
+    ));
+    let handle = c
+        .send_cancellable_request(request, PeerRequestOptions::no_options())
+        .await
+        .unwrap();
+    tokio::time::sleep(Duration::from_millis(100)).await;
+    let msg = err(&call(
+        &c,
+        "observe_consumer_group",
+        json!({"connectionId": "c1", "groupId": "orders-svc", "durationSeconds": 10}),
+    )
+    .await);
+    assert!(msg.starts_with("busy:"), "{msg}");
+    handle.cancel(Some("test".into())).await.unwrap();
+}
+
+#[tokio::test]
+async fn bad_summarize_arguments_do_not_reach_the_backend() {
+    let fake = Fake::new(SessionAccess::Approved);
+    let e = env(fake.clone()).await;
+    let c = client(&e.ep).await;
+    let base = window_args("orders");
+    let cases = [
+        json!({"bucketSeconds": 0}),
+        json!({"bucketSeconds": 86401}),
+        json!({"topKeys": 101}),
+        json!({"from": "2026-10-24T14:31:00"}),
+        // Два дня по минуте — больше 1000 корзин.
+        json!({"to": "2026-10-26T14:40:00+03:00", "bucketSeconds": 60}),
+    ];
+    for extra in cases {
+        let args = with(base.clone(), extra);
+        let msg = err(&call(&c, "summarize_messages", args.clone()).await);
+        assert!(msg.starts_with("invalid_argument:"), "{args}: {msg}");
+    }
+    assert!(fake.message_ops.lock().unwrap().is_empty());
+}
diff --git a/src-tauri/src/mcp/messages.rs b/src-tauri/src/mcp/messages.rs
index 60000fc..8d1a931 100644
--- a/src-tauri/src/mcp/messages.rs
+++ b/src-tauri/src/mcp/messages.rs
@@ -1031,4 +1031,16 @@ mod tests {
             (ErrorCode::NotAuthorized, "errors.kafka.notAuthorized")
         );
     }
+
+    #[test]
+    fn long_keys_are_copied_up_to_the_key_parse_limit() {
+        let key = vec![b'k'; KEY_PARSE_LIMIT + 10];
+        let d = raw_data(&key, DataFormat::Json, KEY_PARSE_LIMIT);
+        assert_eq!(
+            (d.bytes.len(), d.size),
+            (KEY_PARSE_LIMIT, KEY_PARSE_LIMIT + 10)
+        );
+        let d = raw_data(&key, DataFormat::Json, MASK_PARSE_LIMIT);
+        assert_eq!((d.bytes.len(), d.size), (key.len(), key.len()));
+    }
 }
````

- [ ] **Step 2: Тесты падают.**

Run: `cd crates/glafira-mcp && cargo test --no-run`
Expected: компиляция падает — `cannot find function fit_history`, затем (после неё) нет поля `scan_grace` у `GatewayOptions`. В приложении `cd src-tauri && cargo test --lib --no-run` — нет `KEY_PARSE_LIMIT`, у `raw_data` два аргумента.

- [ ] **Step 3: Реализация.**

<!-- block: patch task4-src.diff -->
````diff
diff --git a/crates/glafira-mcp/src/mask.rs b/crates/glafira-mcp/src/mask.rs
index d5951f8..070fa65 100644
--- a/crates/glafira-mcp/src/mask.rs
+++ b/crates/glafira-mcp/src/mask.rs
@@ -14,6 +14,9 @@ use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
 pub const MASK: &str = "***";
 /// Сколько первых байтов значения разбирается как JSON; длиннее — текстовый режим.
 pub const MASK_PARSE_LIMIT: usize = 256 * 1024;
+/// Сколько первых байтов ключа приложение передаёт на маскировку: ключ в ответе короткий
+/// (`MAX_KEY_TEXT`), а ключ длиннее маскируется как частичный, текстом.
+pub const KEY_PARSE_LIMIT: usize = 16 * 1024;
 /// Пределы списка полей подключения (проверяет приложение при сохранении).
 pub const MAX_MASK_FIELDS: usize = 200;
 pub const MAX_MASK_FIELD_CHARS: usize = 64;
diff --git a/crates/glafira-mcp/src/messages.rs b/crates/glafira-mcp/src/messages.rs
index a97cf9a..b8cb78e 100644
--- a/crates/glafira-mcp/src/messages.rs
+++ b/crates/glafira-mcp/src/messages.rs
@@ -18,6 +18,8 @@ use crate::summary::Summary;
 /// Бюджет одного просмотра: время и число записей (спец. §3.1).
 pub const SCAN_DEADLINE: Duration = Duration::from_secs(40);
 pub const SCAN_RECORD_LIMIT: u64 = 1_000_000;
+/// Запас сверх бюджета просмотра: приложение закрывает консьюмер и возвращает собранное.
+pub const SCAN_GRACE: Duration = Duration::from_secs(5);
 /// Отступ до начала окна в истории коммитов: состояние группы на начало окна.
 pub const COMMIT_LOOKBACK_MS: i64 = 10 * 60 * 1000;
 pub const DEFAULT_LIMIT: u32 = 50;
diff --git a/crates/glafira-mcp/src/server/message_tools.rs b/crates/glafira-mcp/src/server/message_tools.rs
index f8ecd85..60256ab 100644
--- a/crates/glafira-mcp/src/server/message_tools.rs
+++ b/crates/glafira-mcp/src/server/message_tools.rs
@@ -4,7 +4,7 @@
 
 use std::sync::Arc;
 use std::sync::atomic::Ordering;
-use std::time::{Duration, Instant};
+use std::time::Instant;
 
 use rmcp::handler::server::wrapper::Parameters;
 use rmcp::model::{CallToolResult, ProgressNotificationParam};
@@ -26,9 +26,6 @@ use crate::summary::{
     Summary,
 };
 
-/// Запас сверх бюджета просмотра: приложение закрывает консьюмер и возвращает собранное.
-const SCAN_GRACE: Duration = Duration::from_secs(5);
-
 const INCOMPLETE_HINT: &str = "The scan stopped at the time budget or the record limit (1,000,000): the results cover only the scanned part (see scannedTo). Narrow the window, add filters or select partitions.";
 /// `topic` здесь не поможет: партиция `__consumer_offsets` читается целиком (спец. §6.3).
 const HISTORY_INCOMPLETE_HINT: &str = "The scan of __consumer_offsets stopped at the time budget or the record limit (1,000,000): later commits of the window may be missing. Narrow the window.";
@@ -37,6 +34,7 @@ const PAGE_CUT_HINT: &str =
 const SUMMARY_CUT_HINT: &str =
     "The response was cut to 100 KB: headerNames and keys.top were shortened.";
 const RUNS_CUT_HINT: &str = "The response was cut to 100 KB: the oldest runs were dropped; narrow the window or pass topic.";
+const HISTORY_TOO_LARGE: &str = "The commit history does not fit into 100 KB even without commit runs; narrow it: pass topic or a shorter window";
 const NO_COMMITS_HINT: &str = "No commits of this group in the window: the group may not exist, may store offsets elsewhere (assign() with external storage), or the history was compacted.";
 
 // ---- Параметры (описания полей уходят в JSON Schema для LLM) ----
@@ -845,7 +843,7 @@ fn fit_runs(body: &mut HistoryBody) -> bool {
     let capped: Vec<bool> = body.partitions.iter().map(|p| p.runs_truncated).collect();
     let mut dropped: Vec<Vec<RunOut>> = body.partitions.iter().map(|_| Vec::new()).collect();
     // Наибольший `k` в 0..longest, при котором ответ помещается; не помещается и без `runs` —
-    // остаётся 0, а обвязка ответит response_too_large.
+    // остаётся 0, а `fit_history` ответит response_too_large.
     let (mut lo, mut hi) = (0, longest - 1);
     while lo < hi {
         let mid = (lo + hi).div_ceil(2);
@@ -860,6 +858,17 @@ fn fit_runs(body: &mut HistoryBody) -> bool {
     true
 }
 
+/// Укладывает историю в 100 КБ ([`fit_runs`]); не помещается и без `runs` — `response_too_large`,
+/// а не история, похожая на «коммитов не было». `Ok(true)` — что-то отброшено.
+fn fit_history(body: &mut HistoryBody) -> Result<bool, BackendError> {
+    let cut = fit_runs(body);
+    if fits(body) {
+        Ok(cut)
+    } else {
+        Err(BackendError::new("response_too_large", HISTORY_TOO_LARGE))
+    }
+}
+
 fn history_body(op: &MessagesOp, out: MessagesOutput) -> Result<Value, BackendError> {
     let (
         MessagesOp::CommitHistory {
@@ -921,7 +930,7 @@ fn history_body(op: &MessagesOp, out: MessagesOutput) -> Result<Value, BackendEr
         truncated: false,
         hint: None,
     };
-    let cut = fit_runs(&mut body);
+    let cut = fit_history(&mut body)?;
     body.truncated = cut;
     body.hint = hints(&[
         (!body.complete).then_some(HISTORY_INCOMPLETE_HINT),
@@ -960,7 +969,8 @@ impl GatewayServer {
     ) -> Result<MessagesOutput, BackendError> {
         let started = Instant::now();
         let budget = self.shared.options.scan_deadline;
-        let safety = tokio::time::Instant::from_std(started + budget + SCAN_GRACE);
+        let safety =
+            tokio::time::Instant::from_std(started + budget + self.shared.options.scan_grace);
         let timeout = || BackendError::new("TIMEOUT", "Reading messages did not finish in time");
         let _long = self.begin_long()?;
         let _slots = tokio::time::timeout_at(safety, self.slots())
diff --git a/crates/glafira-mcp/src/server/mod.rs b/crates/glafira-mcp/src/server/mod.rs
index a9d05a0..e1a896b 100644
--- a/crates/glafira-mcp/src/server/mod.rs
+++ b/crates/glafira-mcp/src/server/mod.rs
@@ -78,6 +78,9 @@ pub struct GatewayOptions {
     pub check_wait: Duration,
     /// Бюджет просмотра сообщений (спец. G4 §3.1); приложение останавливается по дедлайну.
     pub scan_deadline: Duration,
+    /// Страховочный запас сверх бюджета: потом вызов отвечает `TIMEOUT` и просит приложение
+    /// остановиться (в тестах — миллисекунды).
+    pub scan_grace: Duration,
 }
 
 impl GatewayOptions {
@@ -88,6 +91,7 @@ impl GatewayOptions {
             observe_unit: Duration::from_secs(1),
             check_wait: Duration::from_secs(50),
             scan_deadline: crate::messages::SCAN_DEADLINE,
+            scan_grace: crate::messages::SCAN_GRACE,
         }
     }
 }
diff --git a/src-tauri/src/mcp/messages.rs b/src-tauri/src/mcp/messages.rs
index 8d1a931..338ac0f 100644
--- a/src-tauri/src/mcp/messages.rs
+++ b/src-tauri/src/mcp/messages.rs
@@ -6,7 +6,7 @@ use std::collections::HashMap;
 use std::time::{Duration, Instant};
 
 use glafira_mcp::commits::{CommitHistoryBuilder, CommitRecord, CommitStats};
-use glafira_mcp::mask::{self as mm, MASK_PARSE_LIMIT};
+use glafira_mcp::mask::{self as mm, KEY_PARSE_LIMIT, MASK_PARSE_LIMIT};
 use glafira_mcp::messages::{
     COMMIT_LOOKBACK_MS, FormatArg, MAX_HEADERS, MessageFilter, MessagesBody, MessagesOp,
     PartitionRange, Range, RawData, RawMessage, SCAN_RECORD_LIMIT, ScanCtx, ScanStats, StopReason,
@@ -159,9 +159,10 @@ fn timestamp_type(t: Timestamp) -> TimestampType {
     }
 }
 
-fn raw_data(bytes: &[u8], format: DataFormat) -> RawData {
+/// Первые `limit` байтов ключа или значения для маскировки в крейте и исходный размер.
+fn raw_data(bytes: &[u8], format: DataFormat, limit: usize) -> RawData {
     RawData {
-        bytes: bytes[..bytes.len().min(MASK_PARSE_LIMIT)].to_vec(),
+        bytes: bytes[..bytes.len().min(limit)].to_vec(),
         size: bytes.len(),
         format: mask_format(format),
     }
@@ -187,11 +188,11 @@ fn raw_message(msg: &BorrowedMessage<'_>, m: &BrowsedMessage) -> RawMessage {
         key: msg
             .key()
             .zip(m.key.as_ref())
-            .map(|(k, d)| raw_data(k, d.format)),
+            .map(|(k, d)| raw_data(k, d.format, KEY_PARSE_LIMIT)),
         value: msg
             .payload()
             .zip(m.value.as_ref())
-            .map(|(v, d)| raw_data(v, d.format)),
+            .map(|(v, d)| raw_data(v, d.format, MASK_PARSE_LIMIT)),
         headers,
         headers_truncated,
     }
````

- [ ] **Step 4: Тесты проходят.**

Run: `cd crates/glafira-mcp && cargo test` — Expected: unit 72, `tests/server.rs` 58 (около 2 с: тест таймаута ждёт 100 мс вместо 5 с); clippy и fmt чисто. Затем `cd src-tauri && cargo test --lib` — 523; clippy и fmt чисто.

- [ ] **Step 5: Commit.**

````bash
git add crates/glafira-mcp/src/mask.rs crates/glafira-mcp/src/messages.rs crates/glafira-mcp/src/server/message_tools.rs crates/glafira-mcp/src/server/mod.rs crates/glafira-mcp/tests/server.rs src-tauri/src/mcp/messages.rs
git commit -s -m "fix(mcp): история, не помещающаяся и без серий, — response_too_large; запас бюджета в настройках шлюза; ключ на маскировку до 16 КиБ; тесты хода, занятости и аргументов обзора

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
````

### Task 5: Интерфейс и доступность

**Files:**
- Modify: `src-tauri/src/storage/connections.rs` (`validate_profile`; тесты)
- Modify: `src/components/connections/ConnectionForm.tsx`, `src/components/connections/tree/ConnectionNode.tsx`, `src/components/settings/McpSection.tsx`, `src/styles.css`
- Modify: `src/locales/*.json` (12 файлов, скриптом)
- Test: `src/components/connections/ConnectionForm.test.tsx`, `src/components/connections/tree/ConnectionTree.test.tsx`, `src/components/settings/McpSection.test.tsx`

**Interfaces:**
- Consumes: —
- Produces:
  - ключи `connections.form.llmStatusUnavailable`, `settings.mcp.audit.openConnectionsFailed`;
  - `openBlocked(canPrefill, e, s, connections, connectionsFailed: boolean)`;
  - у `AuditDetails` новый проп `connectionsFailed: boolean`.

Спец. §4:
1. `#cf-llm-prod` с `role="status"` есть в форме всегда, текст вписывается при prod и уровне «Метаданные и сообщения»; `aria-describedby` — только пока текст есть.
2. Пометка «Статус MCP недоступен…» — когда статус не загрузился и данных нет.
3. Значки: `role="img"` и `aria-label` = тексту `title`; цвета значков по Global Constraints.
4. `validate_profile` проверяет список масок только при `llm_access == Messages`.
5. «Открыть в „Сообщениях“»: при ошибке списка подключений — подсказка `openConnectionsFailed`.

Существующие тесты, которые опирались на старое поведение, меняются:
- предупреждение о prod — теперь проверка постоянной области;
- `it.each` о статусе MCP — пометка только для `error`;
- два Rust-теста списка масок — уровень `Messages`.

- [ ] **Step 1: Тесты.**

<!-- block: patch task5-tests.diff -->
````diff
diff --git a/src-tauri/src/storage/connections.rs b/src-tauri/src/storage/connections.rs
index c8453d3..ae8534a 100644
--- a/src-tauri/src/storage/connections.rs
+++ b/src-tauri/src/storage/connections.rs
@@ -896,6 +896,7 @@ mod tests {
         let dir = tempfile::tempdir().unwrap();
         let store = ConnectionStore::open(dir.path()).unwrap();
         let mut p = sample_profile("a");
+        p.llm_access = LlmAccess::Messages;
         // Ровно предельное число имён — допустимо.
         p.llm_mask_fields = Some((0..MAX_MASK_FIELDS).map(|i| format!("f{i}")).collect());
         assert_eq!(
@@ -930,6 +931,7 @@ mod tests {
             "стандартный список не пишется"
         );
         let mut p = sample_profile("a");
+        p.llm_access = LlmAccess::Messages;
         p.llm_mask_fields = Some(vec![" email ".into(), "".into(), "inn".into()]);
         let saved = store.upsert(p.clone()).unwrap();
         assert_eq!(
@@ -953,6 +955,25 @@ mod tests {
         assert_eq!(e.key, "errors.connections.maskFieldTooLong");
     }
 
+    #[test]
+    fn mask_fields_are_checked_only_at_the_messages_level() {
+        use glafira_mcp::mask::MAX_MASK_FIELDS;
+        let dir = tempfile::tempdir().unwrap();
+        let store = ConnectionStore::open(dir.path()).unwrap();
+        let mut p = sample_profile("a");
+        // Список скрыт в форме на других уровнях и не применяется: сохранение он не блокирует.
+        let too_many: Vec<String> = (0..=MAX_MASK_FIELDS).map(|i| format!("f{i}")).collect();
+        p.llm_mask_fields = Some(too_many.clone());
+        for level in [LlmAccess::Off, LlmAccess::Metadata] {
+            p.llm_access = level;
+            let saved = store.upsert(p.clone()).unwrap();
+            assert_eq!(saved.llm_mask_fields.as_ref(), Some(&too_many), "{level:?}");
+        }
+        p.llm_access = LlmAccess::Messages;
+        let e = store.upsert(p).unwrap_err();
+        assert_eq!(e.key, "errors.connections.maskFieldsTooMany");
+    }
+
     #[test]
     fn old_tls_profiles_get_defaults() {
         let tls: TlsConfig =
diff --git a/src/components/connections/ConnectionForm.test.tsx b/src/components/connections/ConnectionForm.test.tsx
index 07c90a4..c262f94 100644
--- a/src/components/connections/ConnectionForm.test.tsx
+++ b/src/components/connections/ConnectionForm.test.tsx
@@ -190,15 +190,18 @@ describe("ConnectionForm", () => {
     expect(within(group).getByLabelText("Имена полей для маскирования, по одному на строку")).toHaveValue("inn");
   });
 
-  it("сообщения production-кластера — предупреждение", async () => {
+  it("сообщения production-кластера — предупреждение в постоянной живой области", async () => {
     const user = userEvent.setup();
     const initial = { ...newProfile(), id: "p", name: "p", environment: "prod" as const };
     renderWithClient(<ConnectionForm initial={initial} tabId="t" />);
-    expect(screen.queryByText(/production-кластера/)).not.toBeInTheDocument();
+    // Область есть с самого начала: программа чтения экрана объявит текст, когда он появится.
+    const live = screen.getByRole("status");
+    expect(live).toBeEmptyDOMElement();
+    const levels = screen.getByRole("radiogroup", { name: "Доступ для LLM" });
+    expect(levels).not.toHaveAttribute("aria-describedby");
     await user.click(await messagesLevel());
-    const warning = screen.getByText("Содержимое сообщений этого production-кластера будет уходить провайдеру LLM");
-    expect(warning).toHaveAttribute("role", "status");
-    expect(screen.getByRole("radiogroup", { name: "Доступ для LLM" })).toHaveAccessibleDescription(warning.textContent ?? "");
+    expect(live).toHaveTextContent("Содержимое сообщений этого production-кластера будет уходить провайдеру LLM");
+    expect(levels).toHaveAccessibleDescription(live.textContent ?? "");
   });
 
   it("политика запрещает чтение сообщений", async () => {
@@ -235,9 +238,9 @@ describe("ConnectionForm", () => {
   });
 
   it.each([
-    ["пока статус MCP не пришёл", () => new Promise<never>(() => {}), "pending"],
-    ["если статус MCP не загрузился", () => Promise.reject(new Error("нет ответа")), "error"],
-  ] as const)("%s: третий уровень недоступен без пометок, сохранённый уровень не меняется", async (_, status, state) => {
+    ["пока статус MCP не пришёл", () => new Promise<never>(() => {}), "pending", false],
+    ["если статус MCP не загрузился", () => Promise.reject(new Error("нет ответа")), "error", true],
+  ] as const)("%s: третий уровень недоступен, сохранённый уровень не меняется", async (_, status, state, note) => {
     vi.mocked(api.getMcpStatus).mockImplementation(status);
     vi.mocked(api.saveConnection).mockResolvedValue({ ...newProfile(), id: "p", name: "p" });
     const user = userEvent.setup();
@@ -250,6 +253,9 @@ describe("ConnectionForm", () => {
     expect(screen.queryByText(/отключено вашей организацией/)).not.toBeInTheDocument();
     expect(screen.queryByText(/production-кластера/)).not.toBeInTheDocument();
     expect(screen.queryByRole("group", { name: "Маскирование" })).not.toBeInTheDocument();
+    // Пометка — только когда статус не загрузился, а не пока он грузится.
+    const unavailable = screen.queryByText("Статус MCP недоступен: уровень «Метаданные и сообщения» сейчас нельзя выбрать");
+    expect(unavailable !== null).toBe(note);
     await user.click(screen.getByRole("button", { name: "Сохранить" }));
     expect(api.saveConnection).toHaveBeenCalledWith(
       expect.objectContaining({ llmAccess: "messages", llmMaskFields: ["inn"] }),
diff --git a/src/components/connections/tree/ConnectionTree.test.tsx b/src/components/connections/tree/ConnectionTree.test.tsx
index 890bb9b..571b207 100644
--- a/src/components/connections/tree/ConnectionTree.test.tsx
+++ b/src/components/connections/tree/ConnectionTree.test.tsx
@@ -87,8 +87,9 @@ describe("ConnectionTree", () => {
       { ...CONNS[1], llmAccess: "messages" },
     ]);
     const { unmount } = renderWithClient(<ConnectionTree />);
-    expect(await screen.findByTitle("Доступ для LLM: метаданные и сообщения")).toHaveClass("llm-badge--messages");
-    expect(screen.getByTitle("Открыто LLM-ассистентам по MCP")).not.toHaveClass("llm-badge--messages");
+    // Значки различает и программа чтения экрана: у них роль изображения и полное название.
+    expect(await screen.findByRole("img", { name: "Доступ для LLM: метаданные и сообщения" })).toHaveClass("llm-badge--messages");
+    expect(screen.getByRole("img", { name: "Открыто LLM-ассистентам по MCP" })).not.toHaveClass("llm-badge--messages");
     unmount();
     vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus({ messagesAllowed: false }));
     renderWithClient(<ConnectionTree />);
diff --git a/src/components/settings/McpSection.test.tsx b/src/components/settings/McpSection.test.tsx
index ad54f02..ae728e2 100644
--- a/src/components/settings/McpSection.test.tsx
+++ b/src/components/settings/McpSection.test.tsx
@@ -174,6 +174,16 @@ describe("McpSection", () => {
     expect(button).toBeDisabled();
   });
 
+  it("журнал: список подключений не загрузился — своя подсказка у кнопки", async () => {
+    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
+    vi.mocked(api.listConnections).mockRejectedValue(new Error("нет ответа"));
+    vi.mocked(api.getMcpAudit).mockResolvedValue([read("c1", "orders")]);
+    renderWithClient(<McpSection />);
+    const [button] = await openButtons();
+    await waitFor(() => expect(button).toHaveAttribute("title", "Недоступно: не удалось загрузить список подключений"));
+    expect(button).toBeDisabled();
+  });
+
   it("журнал: пока список подключений грузится, кнопка неактивна без подсказки", async () => {
     vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
     vi.mocked(api.listConnections).mockReturnValue(new Promise(() => {}));
````

- [ ] **Step 2: Тесты падают.**

Run: `cd src-tauri && cargo test --lib mask_field` — Expected: `FAILED. 3 passed; 1 failed` (`mask_fields_are_checked_only_at_the_messages_level`).
Run: `pnpm exec vitest run src/components/connections/ConnectionForm.test.tsx src/components/connections/tree/ConnectionTree.test.tsx src/components/settings/McpSection.test.tsx` — Expected: 4 failed:
- «сообщения production-кластера — предупреждение в постоянной живой области»;
- «если статус MCP не загрузился: …»;
- «значок LLM показывает уровень доступа с учётом политики»;
- «журнал: список подключений не загрузился — своя подсказка у кнопки».

- [ ] **Step 3: Реализация.**

<!-- block: patch task5-src.diff -->
````diff
diff --git a/src-tauri/src/storage/connections.rs b/src-tauri/src/storage/connections.rs
index ae8534a..6220484 100644
--- a/src-tauri/src/storage/connections.rs
+++ b/src-tauri/src/storage/connections.rs
@@ -365,7 +365,10 @@ pub fn validate_profile(p: &ConnectionProfile) -> AppResult<()> {
             "Request timeout must be at least 1000 ms"
         ));
     }
-    if let Some(fields) = &p.llm_mask_fields {
+    // На других уровнях список скрыт в форме и не применяется (спец. G4.1 §4).
+    if p.llm_access == LlmAccess::Messages
+        && let Some(fields) = &p.llm_mask_fields
+    {
         validate_mask_fields(fields)?;
     }
     Ok(())
diff --git a/src/components/connections/ConnectionForm.tsx b/src/components/connections/ConnectionForm.tsx
index 8edae2c..5236e51 100644
--- a/src/components/connections/ConnectionForm.tsx
+++ b/src/components/connections/ConnectionForm.tsx
@@ -30,9 +30,10 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
   // Подтверждение удаления внутри формы: нативный window.confirm в Tauri 2 не показывается и возвращает false.
   const [confirmingDelete, setConfirmingDelete] = useState(false);
   // Политика может запретить чтение сообщений (G4). Строго: третий уровень и блок «Маскирование» — только когда статус
-  // пришёл и разрешает (true), пометки политики — только при явном запрете (false). Пока статус грузится или не
-  // загрузился, третий уровень недоступен без пометки, а сохранение оставляет сохранённый уровень как есть.
-  const messagesAllowed = useMcpStatus().data?.messagesAllowed;
+  // пришёл и разрешает (true), пометки политики — только при явном запрете (false). Пока статус грузится, третий уровень
+  // недоступен без пометки, не загрузился — с пометкой; сохранение оставляет сохранённый уровень как есть.
+  const mcpStatus = useMcpStatus();
+  const messagesAllowed = mcpStatus.data?.messagesAllowed;
   const prodWarning = messagesAllowed === true && profile.llmAccess === "messages" && profile.environment === "prod";
   const [maskText, setMaskText] = useState((initial.llmMaskFields ?? STANDARD_MASK_FIELDS).join("\n"));
   const queryClient = useQueryClient();
@@ -186,11 +187,13 @@ export function ConnectionForm({ initial, tabId }: { initial: ConnectionProfile;
               {profile.llmAccess === "messages" ? t("connections.form.llmMessagesAsMetadata") : t("connections.form.llmMessagesPolicy")}
             </span>
           )}
-          {prodWarning && (
-            <span id="cf-llm-prod" className="warn" role="status">
-              {t("connections.form.llmProdWarning")}
-            </span>
+          {messagesAllowed === undefined && mcpStatus.isError && (
+            <span className="muted">{t("connections.form.llmStatusUnavailable")}</span>
           )}
+          {/* Живая область есть всегда: текст, вписанный в уже существующую область, программа чтения экрана объявит. */}
+          <span id="cf-llm-prod" className="warn" role="status">
+            {prodWarning ? t("connections.form.llmProdWarning") : ""}
+          </span>
         </div>
 
         {messagesAllowed === true && profile.llmAccess === "messages" && (
diff --git a/src/components/connections/tree/ConnectionNode.tsx b/src/components/connections/tree/ConnectionNode.tsx
index 18a6560..88f54e9 100644
--- a/src/components/connections/tree/ConnectionNode.tsx
+++ b/src/components/connections/tree/ConnectionNode.tsx
@@ -25,12 +25,12 @@ export function ConnectionNode({ connection: c, rowProps }: { connection: Connec
         <span className="tree-name">{c.name}</span>
         <EnvBadge environment={c.environment} readOnly={c.readOnly} />
         {level === "metadata" && (
-          <span className="llm-badge" title={t("sidebar.llmAccess")}>
+          <span className="llm-badge" role="img" title={t("sidebar.llmAccess")} aria-label={t("sidebar.llmAccess")}>
             LLM
           </span>
         )}
         {level === "messages" && (
-          <span className="llm-badge llm-badge--messages" title={t("sidebar.llmMessages")}>
+          <span className="llm-badge llm-badge--messages" role="img" title={t("sidebar.llmMessages")} aria-label={t("sidebar.llmMessages")}>
             LLM
           </span>
         )}
diff --git a/src/components/settings/McpSection.tsx b/src/components/settings/McpSection.tsx
index 6199e66..4b42c3e 100644
--- a/src/components/settings/McpSection.tsx
+++ b/src/components/settings/McpSection.tsx
@@ -78,26 +78,37 @@ function auditWhat(e: McpAuditEntry, t: (k: string) => string): string {
 }
 
 /**
- * Почему «Открыть в „Сообщениях“» неактивна: ключ подсказки; "" — пока неизвестно (список подключений грузится
- * или не загрузился), без подсказки; null — кнопка активна.
+ * Почему «Открыть в „Сообщениях“» неактивна: ключ подсказки; "" — пока список подключений грузится, без подсказки;
+ * null — кнопка активна.
  */
 function openBlocked(
   canPrefill: boolean,
   e: McpAuditEntry,
   s: McpStatus,
   connections: ConnectionProfile[] | undefined,
+  connectionsFailed: boolean,
 ): string | null {
   if (!canPrefill) return "settings.mcp.audit.openArgsCut";
   // Политика действует поверх уровня подключения: сохранённое «сообщения» работает как «Только метаданные».
   if (!s.messagesAllowed) return "settings.mcp.audit.openPolicy";
-  if (!connections) return "";
+  if (!connections) return connectionsFailed ? "settings.mcp.audit.openConnectionsFailed" : "";
   const conn = connections.find((c) => c.id === e.connection?.id);
   if (!conn) return "settings.mcp.audit.openNoConnection";
   return conn.llmAccess === "messages" ? null : "settings.mcp.audit.openNotMessages";
 }
 
 /** Подробности вызова чтения сообщений (G4): цель, отдано (по шаблону инструмента), просмотрено, замаскировано. */
-function AuditDetails({ e, s, connections }: { e: McpAuditEntry; s: McpStatus; connections: ConnectionProfile[] | undefined }) {
+function AuditDetails({
+  e,
+  s,
+  connections,
+  connectionsFailed,
+}: {
+  e: McpAuditEntry;
+  s: McpStatus;
+  connections: ConnectionProfile[] | undefined;
+  connectionsFailed: boolean;
+}) {
   const { t } = useTranslation();
   const open = useWorkspace((st) => st.open);
   if (e.scanned === undefined) return null;
@@ -108,7 +119,7 @@ function AuditDetails({ e, s, connections }: { e: McpAuditEntry; s: McpStatus; c
   const details = target ? t(keys.target, { target, ...counts }) : t(keys.noTarget, counts);
   if (!isBrowsable(e)) return <>{details}</>;
   const prefill = browsePrefillFromAudit(e);
-  const blocked = openBlocked(prefill !== null, e, s, connections);
+  const blocked = openBlocked(prefill !== null, e, s, connections, connectionsFailed);
   return (
     <>
       {details}{" "}
@@ -314,7 +325,7 @@ export function McpSection() {
                 <td>{e.outcome === "error" ? e.code ?? t("settings.mcp.audit.error") : e.outcome === "ok" ? t("settings.mcp.audit.ok") : ""}</td>
                 <td>{e.ms ?? ""}</td>
                 <td>
-                  <AuditDetails e={e} s={s} connections={connections.data} />
+                  <AuditDetails e={e} s={s} connections={connections.data} connectionsFailed={connections.isError} />
                 </td>
               </tr>
             ))}
diff --git a/src/styles.css b/src/styles.css
index d6293b7..20755af 100644
--- a/src/styles.css
+++ b/src/styles.css
@@ -5,6 +5,8 @@
   --muted: #6b7280;
   --border: #e3e5e8;
   --accent: #3b82f6;
+  /* Значки LLM в дереве: контраст текста не ниже 4,5:1 (#2563eb — 5,2:1 на белом, 4,7:1 на --bg-alt). */
+  --llm-badge: #2563eb;
   --danger: #dc2626;
   --warn: #f59e0b;
   font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
@@ -19,6 +21,7 @@
     --fg: #e6e8eb;
     --muted: #9aa0a6;
     --border: #33373d;
+    --llm-badge: #60a5fa;
   }
 }
 * { box-sizing: border-box; }
@@ -173,8 +176,9 @@ label.inline { display: inline-flex; gap: 4px; align-items: center; white-space:
 .help-content pre { background: var(--bg-alt); border: 1px solid var(--border); border-radius: 4px; padding: 8px; overflow: auto; }
 .help-content pre code { padding: 0; background: none; }
 .help-content a { color: var(--accent); }
-.llm-badge { font-size: 9px; padding: 0 3px; border: 1px solid var(--accent); border-radius: 3px; color: var(--accent); }
-.llm-badge--messages { background: var(--accent); color: #fff; }
+.llm-badge { font-size: 9px; padding: 0 3px; border: 1px solid var(--llm-badge); border-radius: 3px; color: var(--llm-badge); }
+/* Белый текст на #2563eb — 5,2:1 в обеих темах. */
+.llm-badge--messages { background: #2563eb; border-color: #2563eb; color: #fff; }
 .form .llm-levels label, .form .llm-mask label { color: var(--fg); margin-right: 12px; }
 .llm-levels, .llm-mask { display: flex; flex-wrap: wrap; gap: 4px 0; align-items: center; }
 .llm-mask { flex-direction: column; align-items: stretch; }
````

- [ ] **Step 4: Строки словарей.**

Положить скрипт в `/tmp/add-keys.mjs`, spec-файлы — в `/tmp/g41-t5-form.json` и `/tmp/g41-t5-audit.json`, затем `node /tmp/add-keys.mjs src/locales /tmp/g41-t5-form.json && node /tmp/add-keys.mjs src/locales /tmp/g41-t5-audit.json`:

<!-- block: spec /tmp/g41-t5-form.json -->
````json
{
  "path": "connections.form",
  "values": {
    "en": {
      "llmStatusUnavailable": "MCP status is unavailable: “Metadata and messages” cannot be selected right now"
    },
    "ru": {
      "llmStatusUnavailable": "Статус MCP недоступен: уровень «Метаданные и сообщения» сейчас нельзя выбрать"
    },
    "de": {
      "llmStatusUnavailable": "MCP-Status nicht verfügbar: „Metadaten und Nachrichten“ kann gerade nicht ausgewählt werden"
    },
    "fr": {
      "llmStatusUnavailable": "Statut MCP indisponible : « Métadonnées et messages » ne peut pas être sélectionné pour le moment"
    },
    "es": {
      "llmStatusUnavailable": "El estado de MCP no está disponible: ahora no se puede elegir “Metadatos y mensajes”"
    },
    "it": {
      "llmStatusUnavailable": "Stato MCP non disponibile: al momento non è possibile scegliere “Metadati e messaggi”"
    },
    "pl": {
      "llmStatusUnavailable": "Stan MCP jest niedostępny: nie można teraz wybrać „Metadane i wiadomości”"
    },
    "pt-BR": {
      "llmStatusUnavailable": "O status do MCP está indisponível: não é possível escolher “Metadados e mensagens” agora"
    },
    "zh-CN": {
      "llmStatusUnavailable": "MCP 状态不可用：现在无法选择“元数据和消息”"
    },
    "zh-TW": {
      "llmStatusUnavailable": "MCP 狀態無法取得：目前無法選擇「中繼資料和訊息」"
    },
    "ja": {
      "llmStatusUnavailable": "MCPの状態を取得できません：現在は「メタデータとメッセージ」を選択できません"
    },
    "ko": {
      "llmStatusUnavailable": "MCP 상태를 확인할 수 없음: 지금은 '메타데이터 및 메시지'를 선택할 수 없습니다"
    }
  }
}
````

<!-- block: spec /tmp/g41-t5-audit.json -->
````json
{
  "path": "settings.mcp.audit",
  "values": {
    "en": {
      "openConnectionsFailed": "Unavailable: could not load the connection list"
    },
    "ru": {
      "openConnectionsFailed": "Недоступно: не удалось загрузить список подключений"
    },
    "de": {
      "openConnectionsFailed": "Nicht verfügbar: Die Verbindungsliste konnte nicht geladen werden"
    },
    "fr": {
      "openConnectionsFailed": "Indisponible : impossible de charger la liste des connexions"
    },
    "es": {
      "openConnectionsFailed": "No disponible: no se pudo cargar la lista de conexiones"
    },
    "it": {
      "openConnectionsFailed": "Non disponibile: impossibile caricare l’elenco delle connessioni"
    },
    "pl": {
      "openConnectionsFailed": "Niedostępne: nie udało się wczytać listy połączeń"
    },
    "pt-BR": {
      "openConnectionsFailed": "Indisponível: não foi possível carregar a lista de conexões"
    },
    "zh-CN": {
      "openConnectionsFailed": "不可用：无法加载连接列表"
    },
    "zh-TW": {
      "openConnectionsFailed": "無法使用：無法載入連線清單"
    },
    "ja": {
      "openConnectionsFailed": "利用できません：接続の一覧を読み込めませんでした"
    },
    "ko": {
      "openConnectionsFailed": "사용할 수 없음: 연결 목록을 불러오지 못했습니다"
    }
  }
}
````

- [ ] **Step 5: Тесты проходят.**

Run:
- `cd src-tauri && cargo test --lib` — 524; clippy и fmt чисто;
- `pnpm test` — `Test Files 71 passed`, `Tests 521 passed`;
- `pnpm exec tsc --noEmit`;
- `pnpm i18n:check` — словари согласованы, вызовов из Rust 272;
- `pnpm build`.

- [ ] **Step 6: Commit.**

````bash
git add src-tauri/src/storage/connections.rs src/components/connections/ConnectionForm.tsx src/components/connections/ConnectionForm.test.tsx src/components/connections/tree/ConnectionNode.tsx src/components/connections/tree/ConnectionTree.test.tsx src/components/settings/McpSection.tsx src/components/settings/McpSection.test.tsx src/styles.css src/locales/*.json
git commit -s -m "fix(ui): живая область предупреждения о prod, пометка о недоступном статусе MCP, значки LLM для программ чтения экрана, скрытый список масок не мешает сохранению

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
````

### Task 6: Документация

**Files:**
- Modify: `README.ru.md` (раздел MCP), `docs/smoke/g4.md` (п. 7 и 9), `docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md` (§4, §5, §7)
- Create: `docs/smoke/g41.md`

**Interfaces:**
- Consumes: Tasks 1–5 (тексты описывают сделанное).
- Produces: —

Спец. §5 и §8:
- README — фраза о журнале;
- смоук G4 — числа, подтверждённые на стенде 2026-10-25, вместо «выведено из кода — проверить»; у обзора N зависит от окна: 11 за 10 минут с шагом 60 с, 61 за ровно час;
- спека G4 — правила G4.1 в §5, сохранённый уровень в §4, две ошибки в §7;
- новый чек-лист смоука G4.1 из пяти шагов (шаг 2 не выполняется: на стенде нет ACL, его покрывают тесты задачи 3).

- [ ] **Step 1: Правки.**

<!-- block: patch task6-src.diff -->
````diff
diff --git a/README.ru.md b/README.ru.md
index 6909997..2025d0d 100644
--- a/README.ru.md
+++ b/README.ru.md
@@ -110,7 +110,7 @@ kubectl get secret kafka-client -o jsonpath='{.data.ca\.crt}' | base64 -d > ~/ce
 - Glafira — MCP-шлюз, который не меняет кластер: LLM-клиент (Claude Desktop, Claude Code, Cursor) запускает `glafira-kafka mcp`, тот соединяется с запущенной Glafira через локальный сокет (Windows — named pipe) и читает подключения, открытые для LLM в форме подключения («Доступ для LLM»: «Только метаданные» или «Метаданные и сообщения»). Изменить кластер через шлюз нельзя: замечания и предложения LLM пишутся только в файл Glafira; пароли и настройки безопасности не передаются.
 - Включается в «Настройки» → «LLM-ассистенты (MCP)», там же — фрагменты конфига клиентов, подтверждённые клиенты, сессии и журнал. Новый клиент подтверждается диалогом. Политика `mcpEnabled = false` запрещает MCP, `mcpMessagesEnabled = false` — чтение сообщений (`docs/admin.md`).
 - Предложения изменений (`propose_change`, `list_proposals`, `withdraw_proposal`): LLM предлагает конфиги топика, новый топик, партиции или сброс оффсетов; предложение пишется в файл Glafira и показывается рядом с объектом, а применяет его человек заполненной формой с обычными стражами (read-only, ввод имени для prod). Статус «применено» ставит Rust после успеха операции. Удаление и очистку предложить нельзя. Спецификация: `docs/superpowers/specs/2026-10-22-glafira-proposals-g3-design.md`.
-- Чтение сообщений (`read_messages`, `summarize_messages`, `get_group_commit_history`) — только у подключений с уровнем «Метаданные и сообщения»: сообщения за окно времени или диапазон оффсетов с фильтрами экрана «Сообщения», обзор окна без содержимого, история коммитов группы из `__consumer_offsets`. Перед ответом значения маскируются: список полей подключения и шаблоны (email, номера карт, JWT, Bearer); двоичные форматы маскируются лишь частично, а фильтры работают по немаскированным данным. Читает отдельный консьюмер без группы с `client.id` `glafira-kafka-mcp` (или своим из `extraProperties` подключения), не дольше 40 секунд и не больше 1 000 000 записей за вызов. Журнал MCP показывает, сколько записей просмотрено и отдано, и кнопку «Открыть в „Сообщениях“». Спецификация: `docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md`.
+- Чтение сообщений (`read_messages`, `summarize_messages`, `get_group_commit_history`) — только у подключений с уровнем «Метаданные и сообщения»: сообщения за окно времени или диапазон оффсетов с фильтрами экрана «Сообщения», обзор окна без содержимого, история коммитов группы из `__consumer_offsets`. Перед ответом значения маскируются: список полей подключения и шаблоны (email, номера карт, JWT, Bearer); двоичные форматы маскируются лишь частично, а фильтры работают по немаскированным данным. Читает отдельный консьюмер без группы с `client.id` `glafira-kafka-mcp` (или своим из `extraProperties` подключения), не дольше 40 секунд и не больше 1 000 000 записей за вызов. Журнал MCP показывает, сколько просмотрено и сколько отдано: сообщений, корзин обзора или серий коммитов; у чтения и обзора там же кнопка «Открыть в „Сообщениях“». Спецификация: `docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md`.
 - Протокол, инструменты и лимиты — крейт `crates/glafira-mcp` (не зависит от rdkafka и приложения); связь с приложением — `src-tauri/src/mcp/`. Спецификация: `docs/superpowers/specs/2026-10-18-glafira-mcp-gateway-g1-design.md`.
 - Сервер-каталог для реестров MCP (Glama и др.): бинарь `glafira-mcp-catalog` крейта — тот же сервер на stdio с настоящим списком инструментов, каждый вызов отвечает `desktop_app_required` со ссылкой на установщики. Образ: `docker build -f crates/glafira-mcp/Dockerfile .`; владелец листинга на Glama — `glama.json`.
 - Разработка: отладочный бинарь — `src-tauri/target/debug/kafka-explorer` (`… mcp` — режим шима); Glafira запускайте через `pnpm tauri dev` до подключения клиента — шим не поднимет dev-сервер Vite.
diff --git a/docs/smoke/g4.md b/docs/smoke/g4.md
index a7d9b60..76b8b27 100644
--- a/docs/smoke/g4.md
+++ b/docs/smoke/g4.md
@@ -21,9 +21,9 @@ printf '%s\n' 'k1|{"orderId":123,"email":"ann@example.com","note":"card 4111 111
    ```
 
    «Что успела обработать группа smoke-g4 за последний час?» → `get_group_commit_history`: по партициям `smoke-g4` — `runs` с оффсетами и временем, `offsetsTopicPartition` — число. Затем «Покажи сообщения партиции 1, которые группа обработала» (оба ключа, `k1` и `k2`, попадают в партицию 1, партиция 0 пуста) → `read_messages` в режиме оффсетов (`partition` 1, `startOffset` 0, `endOffset` 3, то есть оффсеты 0–2) — три сообщения. Подсказка о праве Read на `__consumer_offsets` не проверяется на dev-стенде (нет ACL), покрыто автотестом.
-7. Журнал и «Открыть в «Сообщениях»». В «Настройках» → «Журнал MCP» (при необходимости «Обновить») в колонке «Подробности» у строк `read_messages` — «smoke-g4: N из M, замаскировано: K», у строк `summarize_messages` — «smoke-g4: корзин: N, просмотрено: M, замаскировано: K», у обеих кнопка «Открыть в «Сообщениях»»; у `get_group_commit_history` — «smoke-g4: серий коммитов: N, просмотрено записей: M, замаскировано: K» без кнопки (серия — подряд идущие коммиты одного оффсета; просмотренные записи — записи `__consumer_offsets`, а не сообщения smoke-g4). Ожидаемые числа (выведены из кода — проверить): у `read_messages` из п. 3 — «smoke-g4: 3 из 3, замаскировано: 4» (email дважды, номер карты, пароль); у `summarize_messages` — «smoke-g4: корзин: 61, просмотрено: 3, замаскировано: 0» (корзин около 60; 61 — за ровно час при шаге по умолчанию); у `get_group_commit_history` из п. 6 — серий коммитов обычно 2, по одной на партицию (консольный консьюмер коммитит одни и те же оффсеты), замаскировано: 0. Если в п. 3 ассистент не вызвал `read_messages`, повторить п. 3, настаивая на вызове. Нажать кнопку у `read_messages` из п. 3 → вкладка топика `smoke-g4`, подвкладка «Сообщения»: начальная позиция «С времени», заполнены «Время с» и «Время по» (границы округлены до секунды); чтение не началось. «Читать» → те же сообщения без маскирования (видны email и пароль). У строки из п. 5 после «Читать» — одно сообщение, в поле «Значение содержит» `bob@example.com`. Усечённые аргументы: попросить «Сгенерируй строку из 1100 букв x подряд и найди в smoke-g4 за последний час сообщения, значение которых содержит эту строку. Вызови read_messages с этой строкой в фильтре value» (Claude Code соберёт строку командой оболочки; в журнале `args` у вызова — строка длиной около 1 КБ) → у новой строки «0 из 3, замаскировано: 0» без `smoke-g4:` (аргументы длиннее 1 КБ журнал усекает, цели в них не найти), кнопка «Открыть в «Сообщениях»» неактивна, подсказка при наведении — «Недоступно: аргументы вызова в журнале усечены».
+7. Журнал и «Открыть в «Сообщениях»». В «Настройках» → «Журнал MCP» (при необходимости «Обновить») в колонке «Подробности» у строк `read_messages` — «smoke-g4: N из M, замаскировано: K», у строк `summarize_messages` — «smoke-g4: корзин: N, просмотрено: M, замаскировано: K», у обеих кнопка «Открыть в «Сообщениях»»; у `get_group_commit_history` — «smoke-g4: серий коммитов: N, просмотрено записей: M, замаскировано: K» без кнопки (серия — подряд идущие коммиты одного оффсета; просмотренные записи — записи `__consumer_offsets`, а не сообщения smoke-g4). Ожидаемые числа (подтверждены на стенде 2026-10-25): у `read_messages` из п. 3 — «smoke-g4: 3 из 3, замаскировано: 4» (email дважды, номер карты, пароль), из п. 5 — «smoke-g4: 1 из 3, замаскировано: 1»; у `summarize_messages` — «smoke-g4: корзин: N, просмотрено: 3, замаскировано: 0», где N зависит от окна, которое выбрал ассистент: за 10 минут с шагом 60 с — 11 (так было на стенде), за ровно час с шагом по умолчанию — 61; у `get_group_commit_history` из п. 6 — «серий коммитов: 2» (по одной на партицию: консольный консьюмер коммитит одни и те же оффсеты), замаскировано: 0. Если в п. 3 ассистент не вызвал `read_messages`, повторить п. 3, настаивая на вызове. Нажать кнопку у `read_messages` из п. 3 → вкладка топика `smoke-g4`, подвкладка «Сообщения»: начальная позиция «С времени», заполнены «Время с» и «Время по» (границы округлены до секунды); чтение не началось. «Читать» → те же сообщения без маскирования (видны email и пароль). У строки из п. 5 после «Читать» — одно сообщение, в поле «Значение содержит» `bob@example.com`. Усечённые аргументы: попросить «Сгенерируй строку из 1100 букв x подряд и найди в smoke-g4 за последний час сообщения, значение которых содержит эту строку. Вызови read_messages с этой строкой в фильтре value» (Claude Code соберёт строку командой оболочки; в журнале `args` у вызова — строка длиной около 1 КБ) → у новой строки «0 из 3, замаскировано: 0» без `smoke-g4:` (аргументы длиннее 1 КБ журнал усекает, цели в них не найти), кнопка «Открыть в «Сообщениях»» неактивна, подсказка при наведении — «Недоступно: аргументы вызова в журнале усечены».
 8. Пометки о неточном переносе. У строки `read_messages` из п. 6 (режим оффсетов, без фильтра) нажать «Открыть в «Сообщениях»» → начальная позиция «С оффсета», оффсет 0, в поле партиций 1, «Лимит» 3 (`endOffset` − `startOffset`); пометок под формой нет. Попросить «Покажи сообщения smoke-g4 из партиции 1 с оффсета 0 до 3, не включая 3, только со статусом FAILED» (настаивать на вызове `read_messages` с `endOffset`) → «Открыть в «Сообщениях»» у новой строки: под формой «Лимит перенесён неточно: в форме он считает найденные сообщения, а не оффсеты, поэтому чтение может зайти за конечный оффсет вызова». Попросить «Найди в smoke-g4 за последний час сообщения с ключом по regex `k[12]` и значением, содержащим FAILED, без regex» (настаивать на вызове `read_messages`) → «Открыть в «Сообщениях»» у новой строки: под формой «Фильтры перенесены неточно: в форме флаги regex и регистра общие для ключа, значения и значения заголовка», флажок regex снят (флаги взяты у фильтра значения).
-9. Свой список масок. В форме подключения снять «Стандартный список»: открылось поле со стандартными именами по одному на строку (последнее — `proxyauthorization`). Двойной щелчок по подписи «Маскирование» флажок не переключает и поле не сбрасывает. Дописать строку из 65 букв `a` (`printf 'a%.0s' {1..65} | pbcopy`) и сохранить → ошибка «Имя поля для маскирования длиннее 64 символов: …»; убрать эту строку. Заменить содержимое поля на `orderId`, сохранить. Повторить вопрос п. 3: `"orderId":"***"`, а email и пароль — по шаблонам: email — `[email]`, `password` больше не в списке и виден как есть (это ожидаемо: свой список заменяет стандартный). В блоке «Журнал MCP» у новой строки `read_messages` — «smoke-g4: 3 из 3, замаскировано: 6» (`orderId` трижды, email дважды, номер карты; выведено из кода — проверить). Список пока не возвращать: он нужен в п. 10.
+9. Свой список масок. В форме подключения снять «Стандартный список»: открылось поле со стандартными именами по одному на строку (последнее — `proxyauthorization`). Двойной щелчок по подписи «Маскирование» флажок не переключает и поле не сбрасывает. Дописать строку из 65 букв `a` (`printf 'a%.0s' {1..65} | pbcopy`) и сохранить → ошибка «Имя поля для маскирования длиннее 64 символов: …»; убрать эту строку. Заменить содержимое поля на `orderId`, сохранить. Повторить вопрос п. 3: `"orderId":"***"`, а email и пароль — по шаблонам: email — `[email]`, `password` больше не в списке и виден как есть (это ожидаемо: свой список заменяет стандартный). В блоке «Журнал MCP» у новой строки `read_messages` — «smoke-g4: 3 из 3, замаскировано: 6» (`orderId` трижды, email дважды, номер карты; подтверждено на стенде 2026-10-25). Список пока не возвращать: он нужен в п. 10.
 10. Экспорт и импорт. Пока у подключения «Метаданные и сообщения» и список `orderId`, в меню дерева подключений «Экспортировать всё…» и сохранить файл: в файле у подключения нет `llmAccess` и `llmMaskFields`. Переименовать подключение (например, дописать к имени `-2`), сохранить, затем в меню дерева «Импортировать…» этот файл с отмеченным «Обновлять существующие подключения»: подключение — в группе «Обновятся»; «Импортировать» → имя прежнее, а уровень «Метаданные и сообщения» и список `orderId` не изменились. Необязательно (удаление стирает историю отправки, шаблоны, замечания и предложения LLM подключения): удалить подключение — в блоке «Журнал MCP» кнопка у его строк `read_messages` неактивна, подсказка «Недоступно: подключения больше нет»; дописать в файл у подключения `"llmAccess": "messages",` (например, после строки с `"id"`) и импортировать его → подключение вернулось с уровнем «Нет»; выставить «Метаданные и сообщения». Вернуть «Стандартный список», сохранить.
 11. Уровень на лету. Выбрать «Только метаданные», сохранить (MCP не переподключать). Попросить ещё раз прочитать smoke-g4 → ответ инструмента `messages_not_allowed: LLM access to this connection is limited to metadata…`, ассистент просит переключить уровень в Glafira. Значок `LLM` в дереве не закрашен. В блоке «Журнал MCP» кнопка «Открыть в «Сообщениях»» у строк `read_messages` из п. 3 неактивна, подсказка при наведении — «Недоступно: у подключения уровень не «Метаданные и сообщения»». Вернуть «Метаданные и сообщения» — кнопка снова активна.
 12. Политика (macOS). Выйти из Glafira, затем:
diff --git a/docs/smoke/g41.md b/docs/smoke/g41.md
new file mode 100644
index 0000000..bb24a91
--- /dev/null
+++ b/docs/smoke/g41.md
@@ -0,0 +1,17 @@
+# Смоук G4.1 (хвосты G4)
+
+Сборка: `pnpm tauri dev` из ветки `feature/g41-g4-tails` (Glafira запущена заранее). MCP-клиент — Claude Code из `~`, клиент подтверждён, как в `docs/smoke/g4.md`. Стенд — тот же: `docker compose -f dev/docker-compose.yml up -d`, топик `smoke-g4` из подготовки смоука G4 (если его нет — создать командами оттуда). У подключения dev-стенда уровень «Метаданные и сообщения» и стандартный список масок. Во всех запросах к ассистенту называйте подключение и свой часовой пояс («время — UTC+3»).
+
+Подготовка (в терминале):
+
+```bash
+printf '%s\n' 'k3|Cookie: a=1; sessionid=xyz' | docker exec -i ke-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic smoke-g4 --property parse.key=true --property key.separator='|'
+```
+
+1. Куки до конца строки. «Покажи сообщения с ключом k3 в smoke-g4 за последний час. Время — UTC+3» → `read_messages` с фильтром по ключу `k3`; значение сообщения — `Cookie: ***`, `sessionid` и `xyz` не видны ни в ответе ассистента, ни в выводе инструмента (раскрыть вызов в Claude Code). В «Настройках» → «Журнал MCP» у этой строки — «замаскировано: 1».
+2. Топик без права Describe. На dev-стенде ACL нет, поэтому шаг не выполняется: ответ «Недостаточно прав» вместо «не найден» на экране «Сообщения» и в `read_messages`/`summarize_messages`/`get_group_commit_history` проверяют тесты `topic_without_describe_is_not_authorized_and_a_missing_one_is_not_found` и `offsets_topic_without_describe_is_not_authorized` (фальшивый брокер с ответом `TOPIC_AUTHORIZATION_FAILED`).
+3. Предупреждение о prod для программы чтения экрана. Открыть форму подключения dev-стенда. Включить VoiceOver (Cmd+F5). Если уровень не «Метаданные и сообщения», выбрать его. Сменить «Окружение» на **prod** → VoiceOver произносит «Содержимое сообщений этого production-кластера будет уходить провайдеру LLM» (раньше молчал). Вернуть **dev**, форму не сохранять (закрыть вкладку). Выключить VoiceOver (Cmd+F5).
+4. Значки LLM. С включённым VoiceOver перейти в дереве подключений к значку `LLM` у подключения dev-стенда (VO+стрелки: Ctrl+Option+→) → VoiceOver читает «Доступ для LLM: метаданные и сообщения» (у подключения с уровнем «Только метаданные» — «Открыто LLM-ассистентам по MCP»), а не просто «LLM». Выключить VoiceOver. В «Системных настройках» → «Оформление» переключить светлую и тёмную тему: в обеих текст обоих значков читается чётко (закрашенный — белый на синем, контурный — синий).
+5. Скрытый список масок не мешает сохранению. В форме подключения при уровне «Метаданные и сообщения» снять «Стандартный список» и дописать в поле строку из 65 букв `a` (`printf 'a%.0s' {1..65} | pbcopy`, вставить новой строкой). Выбрать уровень «Только метаданные» (блок «Маскирование» скрылся) → «Сохранить» → сохранено без ошибки. Снова открыть форму, выбрать «Метаданные и сообщения» → в поле видна строка из 65 букв; «Сохранить» → ошибка «Имя поля для маскирования длиннее 64 символов: …». Убрать эту строку, отметить «Стандартный список», сохранить.
+
+Уборка: в «Журнале MCP» ничего чистить не нужно; сообщение `k3` в `smoke-g4` можно оставить или удалить топик целиком (`docker exec ke-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic smoke-g4`).
diff --git a/docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md b/docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md
index c8106ab..0618db4 100644
--- a/docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md
+++ b/docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md
@@ -289,7 +289,8 @@ exist, may store offsets elsewhere (assign() with external storage), or the hist
    (иначе `not_allowed`);
 2. политика `mcpMessagesEnabled` (иначе `messages_disabled`: «Reading messages is disabled by the organization's
    policy»);
-3. уровень подключения `messages` (иначе `messages_not_allowed`: «LLM access to this connection is limited to
+3. сохранённый уровень подключения — `messages`; политика к этому моменту уже проверена на шагах 1–2 (иначе
+   `messages_not_allowed`: «LLM access to this connection is limited to
    metadata. Ask the user to switch it to "Metadata and messages" in the connection settings in Glafira»).
 
 Изменение уровня, списка полей и политики действует со следующего вызова. Проверки делает реализация `Backend`
@@ -324,28 +325,34 @@ M считается с учётом политики.
 | Шаблон | Что ищет | Замена |
 |---|---|---|
 | email | `[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}` | `[email]` |
-| карта | серия групп цифр (между группами одиночный пробел или дефис, серия — на границах слова); кандидаты в ней — вся серия, если в ней 13–19 цифр (при любой группировке: `4222 222 222 222`, `41111111 11111111`), и все окна из подряд идущих целых групп в форме номера карты (одна группа из 13–19 цифр; 4-4-4-4 с необязательной последней группой из 1–3 цифр; 4-6-5, Amex; 4-6-4, Diners); карта — кандидат с верной контрольной суммой Луна и первой цифрой 2–6 (метки времени в миллисекундах до 2033 года начинаются с 1 и картой не считаются); пересекающиеся совпадения объединяются в одну замену. Соседние числа остаются на виду и не мешают (`4111 1111 1111 1111 12/25` → `[card] 12/25`, `qty 2 4111111111111111` → `qty 2 [card]`, `630000 4111 1111 1111 1111` → `630000 [card]`); списки чисел (серии длиннее 19 цифр) картой не считаются, если в них нет окна в форме номера карты (`ids 304 512 618 220 431 559 602 377 248 316` не меняется); группа не делится (23 цифры подряд — не карта) | `[card]` |
+| карта | серия групп цифр (между группами одиночный пробел или дефис, по краям серии — не цифра, буква рядом не мешает: `id4111111111111111` → `id[card]`; G4.1); кандидаты в ней — вся серия, если в ней 13–19 цифр (при любой группировке: `4222 222 222 222`, `41111111 11111111`), и все окна из подряд идущих целых групп в форме номера карты (одна группа из 13–19 цифр; 4-4-4-4 с необязательной последней группой из 1–3 цифр; 4-6-5, Amex; 4-6-4, Diners; 4-4-4-3, 4-3-3-3, 6-13, 8-8 — G4.1); карта — кандидат с верной контрольной суммой Луна и первой цифрой 2–6 (метки времени в миллисекундах до 2033 года начинаются с 1 и картой не считаются); пересекающиеся совпадения объединяются в одну замену. Соседние числа остаются на виду и не мешают (`4111 1111 1111 1111 12/25` → `[card] 12/25`, `qty 2 4111111111111111` → `qty 2 [card]`, `630000 4111 1111 1111 1111` → `630000 [card]`); списки чисел (серии длиннее 19 цифр) картой не считаются, если в них нет окна в форме номера карты (`ids 304 512 618 220 431 559 602 377 248 316` не меняется); группа не делится (23 цифры подряд — не карта) | `[card]` |
 | JWT | `eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*` | `[jwt]` |
 | Bearer | `(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}` | `Bearer [token]` |
 
 **Режим по формату** (формат определяет приложение: заданный или найденный `browse::format::detect` при `auto`):
-- **JSON.** Разбираются первые 256 КиБ байтов значения (`MASK_PARSE_LIMIT`); не разобралось или длиннее —
+- **JSON.** Разбираются первые 256 КиБ байтов значения (`MASK_PARSE_LIMIT`), у ключа — первые 16 КиБ
+  (`KEY_PARSE_LIMIT`, G4.1: ключ в ответе не длиннее 512 байт); не разобралось или длиннее —
   текстовый режим. Значение члена объекта с совпавшим именем — любого типа, включая объект и массив — заменяется на
   строку `"***"`. Остальные строковые значения проходят текстовый режим — пары и шаблоны
-  (`{"log":"user=ann password=x"}` → `{"log":"user=ann password=***"}`), имена членов — только шаблоны; числа из 13–19
-  цифр проверяются шаблоном карты и при совпадении становятся строкой `"[card]"`. Результат сериализуется компактно.
+  (`{"log":"user=ann password=x"}` → `{"log":"user=ann password=***"}`), имена членов — только шаблоны; число, цифры
+  модуля которого проходят шаблон карты (целое, в том числе отрицательное, или с дробной частью и экспонентой, если
+  значение целое и меньше 10¹⁹ — G4.1), становится строкой `"[card]"`. Результат сериализуется компактно.
 - **Текст** (`string`, а также JSON, ушедший в текстовый режим): сначала пары `name=value`, `name: value`,
-  `"name": value` — если нормализованное имя совпадает со списком, значение (строка в кавычках целиком, иначе до
-  разделителя `,`, `;`, `&`, пробела, `}` или `]`) заменяется на `***`. У имён, которые после нормализации оканчиваются
-  на `authorization` (`Authorization`, `Proxy-Authorization`), значение без кавычек — схема (любой токен RFC 7235) —
-  берётся вместе с учётными данными: следующим словом целиком (до `,`, `;`, `&`, `}`, `]` или пробела: `user:p4ss`,
-  `abc@x`, `a=b`) после пробелов или табуляций в той же строке (`Authorization: Negotiate YII…` →
-  `Authorization: ***`). Схемы `Bearer`, `Basic`, `Digest`, `Token` так берутся у любого имени из списка, и токен может
-  стоять на следующей строке (`X-Auth-Token: Bearer x` → `X-Auth-Token: ***`, `Authorization: Bearer` и токен строкой
-  ниже → `Authorization: ***`); у остальных имён другое значение — одно слово.
+  `"name": value` (имя без кавычек — буква любого алфавита или `_`, затем буквы, цифры, `_`, `.`, `-`; G4.1) — если
+  нормализованное имя совпадает со списком, значение (строка в кавычках целиком, иначе до разделителя `,`, `;`, `&`,
+  пробела, `}` или `]`) заменяется на `***`. У кук и заголовков авторизации — имён, которые после нормализации равны
+  `cookie` или `setcookie` либо оканчиваются на `authorization` (`Authorization`, `Proxy-Authorization`), — значение без
+  кавычек заменяется до конца строки (G4.1): `Cookie: a=1; sessionid=x` → `Cookie: ***`,
+  `Authorization: Digest username="u", response="r"` → `Authorization: ***`; соседние пары той же строки тоже скрыты.
+  Схемы `Bearer`, `Basic`, `Digest`, `Token` берутся вместе с учётными данными — следующим словом целиком (до `,`, `;`,
+  `&`, `}`, `]` или пробела: `user:p4ss`, `abc@x`, `a=b`) — у любого имени из списка, и токен может стоять на следующей
+  строке (`X-Auth-Token: Bearer x` → `X-Auth-Token: ***`, `Authorization: Bearer` и токен строкой ниже →
+  `Authorization: ***`); у остальных имён другое значение — одно слово.
   Не берётся слово — имя следующей пары: с `:` в конце или перед `:` или `=` (`token_type: Bearer` и строкой ниже
   `access_token: x` → `token_type: ***` и `access_token: ***`), а на следующей строке — и слово, начинающееся с
-  `name=value` (`user=ann`); в той же строке такое слово — параметр схемы, оно берётся.
+  `name=value` (`user=ann`), и слово `name=` со значением после пробелов (`pwd: basic` и строкой ниже
+  `password= x` → `pwd: ***` и `password= ***`; G4.1); в той же строке такие слова — параметр схемы или base64 с
+  выравниванием (`Basic dXNlcjpwYXM=`), они берутся.
   Пары ищутся и внутри значения пары с именем не из списка: `url=https://x/?password=y` → `url=https://x/?password=***`,
   `note: token=y` → `note: token=***`. Затем шаблоны.
 - **Двоичные** (`hex`, `base64`): шаблоны применяются к печатным ASCII-фрагментам длиной от 6 байт в исходных байтах —
@@ -424,11 +431,13 @@ __consumer_offsets requires the Read permission on this internal topic» — и
 `headerNames`, затем `keys.top`. `get_group_commit_history`: усекаются самые старые `runs` пропорционально — у каждой
 партиции остаются не больше `k` самых новых `runs` с наибольшим `k`, при котором ответ помещается (двоичный поиск),
 поэтому первыми теряют старые `runs` самые длинные партиции; у потерявших — `runsTruncated: true`. Не помещается и после
-усечения — `response_too_large` (G1).
+усечения — `response_too_large` (G1); у истории коммитов — и когда ответ не помещается даже без `runs`
+(G4.1), а не пустая история.
 
 **Ошибки** (результат с `isError: true`, текст `<code>: <message>`): новые `messages_not_allowed`,
 `messages_disabled`; существующие `not_allowed`, `client_not_approved`, `invalid_argument`, `not_found`, `busy`,
-`rate_limited`, `response_too_large`, `internal`, коды Kafka из `ErrorCode`.
+`rate_limited`, `response_too_large`, `internal`, коды Kafka из `ErrorCode`. Топик без права Describe (в метаданных
+`TOPIC_AUTHORIZATION_FAILED`) — `NOT_AUTHORIZED`, а не `not_found` (G4.1); так же отвечает экран «Сообщения».
 
 **Журнал** `mcp-audit.jsonl`: событие `call` получает необязательные поля `scanned`, `returned` (сообщений, `runs` или
 корзин) и `masked`. Аргументы, как прежде, усечены до 1 КБ; содержимое ответов не пишется.
````

- [ ] **Step 2: Проверка.**

`git diff --check` — без замечаний. Сверить с кодом:
- тексты ошибок в `docs/smoke/g41.md` п. 5 — с `errors.connections.maskFieldTooLong` в `src/locales/ru.json`;
- имена тестов в п. 2 — с задачей 3.

`pnpm i18n:check` эти файлы не проверяет.

- [ ] **Step 3: Commit.**

````bash
git add README.ru.md docs/smoke/g4.md docs/smoke/g41.md docs/superpowers/specs/2026-10-24-glafira-messages-g4-design.md
git commit -s -m "docs: G4.1 — журнал MCP в README, подтверждённые числа смоука G4, правила G4.1 в спеке G4, смоук G4.1

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
````

## После задач

- Финальное ревью всей ветки (`git merge-base main HEAD`..`HEAD`).
- CI на трёх платформах: `gh workflow run ci.yml --ref feature/g41-g4-tails` — только после согласия владельца на push.
- GUI-смоук владельца по `docs/smoke/g41.md`.
- Слияние и выпуск 0.5.2: CHANGELOG и `docs/changelog/<код>.md` на 12 языках, по `docs/release-checklist.md`.
