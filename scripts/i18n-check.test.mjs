import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { checkDictionaries, checkFrontendKeys, checkLanguageFiles, checkSameAsEn, checkTypography, languageCodes, lengthReport, tags, checkRustKeys, frontendKeys, expandRustPaths, findCyrillic, findRustCyrillic, flatten, placeholders, rustKeys } from "./i18n-check.mjs";

test("flatten и placeholders", () => {
  assert.deepEqual([...flatten({ a: { b: "x", c: { d: "y" } } }).keys()], ["a.b", "a.c.d"]);
  assert.deepEqual(placeholders("Подключение «{{name}}» ({{ count }})").sort(), ["count", "name"]);
});

test("паритет ключей и параметров", () => {
  const en = { a: "Hello {{name}}", b: "x", items_one: "{{count}} item", items_other: "{{count}} items" };
  const ru = { a: "Привет, {{name}}", b: "х", items_one: "{{count}} элемент", items_few: "{{count}} элемента", items_many: "{{count}} элементов", items_other: "{{count}} элемента" };
  assert.deepEqual(checkDictionaries({ en, ru }), []);
  const broken = checkDictionaries({ en, ru: { ...ru, a: "Привет" } });
  assert.ok(broken.some((p) => p.includes("ru") && p.includes("a") && p.includes("name")), broken.join("\n"));
  const missing = checkDictionaries({ en, ru: { b: "х", items_one: "1", items_few: "2", items_many: "5", items_other: "x" } });
  assert.ok(missing.some((p) => p.includes("нет ключа a")), missing.join("\n"));
});

test("плюральные формы считаются по правилам языка", () => {
  const en = { n_one: "{{count}} x", n_other: "{{count}} xs" };
  // ru без _few/_many — ошибка
  const p = checkDictionaries({ en, ru: { n_one: "{{count}} х", n_other: "{{count}} х" } });
  assert.ok(p.some((x) => x.includes("ru") && x.includes("n_few")), p.join("\n"));
});

test("кириллица в коде вне словарей и тестов", () => {
  const files = {
    "src/a.tsx": "const x = 1;\n<button>Сохранить</button>\n",
    "src/b.tsx": "// комментарий по-русски допустим\nconst y = 2;\n",
    "src/a.test.tsx": "expect('Сохранить')\n",
    "src/i18n/languages.ts": "{ nativeName: \"Русский\" }\n",
  };
  assert.deepEqual(findCyrillic(files), ["src/a.tsx:2"]);
});

const keysOf = (r) => r.calls.map((c) => c.key).sort();

test("ключи из Rust", () => {
  const rust = rustKeys({
    "a.rs": 'app_err!(ErrorCode::ReadOnly, "errors.guard.readOnly", "Connection “{{name}}” is read-only", name = n)',
    "b.rs": 'let t = ltext!("messages.x", "Hi");',
  });
  assert.deepEqual(rust.problems, []);
  assert.deepEqual(keysOf(rust), ["errors.guard.readOnly", "messages.x"]);
  const en = flatten({ errors: { guard: { readOnly: "Connection “{{name}}” is read-only" } }, messages: { x: "Hello" } });
  const p = checkRustKeys(rust.calls, en);
  assert.deepEqual(p.length, 1);
  assert.ok(p[0].includes("messages.x") && p[0].includes("b.rs:1"), p[0]);
});

test("ключи из Rust: многострочный вызов после rustfmt и экранированные кавычки", () => {
  const rust = rustKeys({
    "c.rs": 'fn f() {\n    Err(app_err!(\n            ErrorCode::ConfirmationRequired,\n            "errors.guard.typeToConfirm",\n            "Type \\"{{expected}}\\" to confirm",\n            expected = expected,\n        ))\n}',
  });
  assert.deepEqual(rust.problems, []);
  assert.deepEqual(rust.calls, [
    { key: "errors.guard.typeToConfirm", template: 'Type "{{expected}}" to confirm', params: ["expected"], where: "c.rs:2" },
  ]);
  assert.deepEqual(checkRustKeys(rust.calls, new Map()), ["en: нет ключа errors.guard.typeToConfirm (используется в Rust, c.rs:2)"]);
});

test("ключи из Rust: комментарии, модуль тестов и тела macro_rules не считаются", () => {
  const rust = rustKeys({
    "d.rs": [
      '/// `ltext!("messages.doc", "Doc")` → LocalizedText',
      "macro_rules! app_err {",
      "    ($code:expr, $key:literal, $en:literal) => { $crate::ltext!($key, $en) };",
      "}",
      'let a = ltext!("messages.real", "Real");',
      "#[cfg(test)]",
      "mod tests {",
      '    let t = ltext!("messages.test.only", "Test");',
      "}",
    ].join("\n"),
  });
  assert.deepEqual(rust.problems, []);
  assert.deepEqual(keysOf(rust), ["messages.real"]);
});

test("ключи из Rust: escape-последовательности и перенос строки через \\", () => {
  const rust = rustKeys({
    "e.rs": 'ltext!("m.a", "Line 1\\nLine 2 \\\\ back \\u{2014} dash \\\n        continued");',
  });
  assert.deepEqual(rust.problems, []);
  assert.equal(rust.calls[0].template, "Line 1\nLine 2 \\ back — dash continued");
});

test("ключи из Rust: макрос, который не разобрать, — ошибка с файлом и строкой", () => {
  const rust = rustKeys({
    "f.rs": [
      'let ok = ltext!("m.ok", "Ok");',
      "let a = ltext!(KEY, \"Text\");",
      'let b = app_err!(ErrorCode::Internal, "m.b", format!("x {}", y));',
      'let c = ltext!("m.c", r"raw");',
      'let d = ltext!("m.d", "Bad \\q escape");',
      'let e = ltext!["m.e", "brackets"];',
      'let f = ltext!("m.f", "Text", name);',
      'let g = ltext!("m.g", "unclosed"',
    ].join("\n"),
  });
  assert.deepEqual(keysOf(rust), ["m.ok"]);
  const lines = rust.problems.map((p) => p.match(/^f\.rs:(\d+): макрос (ltext|app_err)!, который проверка не может разобрать/)?.[1]);
  assert.deepEqual(lines, ["2", "3", "4", "5", "6", "7", "8"], rust.problems.join("\n"));
  assert.ok(rust.problems[2].includes("обычный строковый литерал"), rust.problems[2]);
  assert.ok(rust.problems[3].includes("\\q"), rust.problems[3]);
});

test("параметры вызова совпадают с {{…}} шаблона в обе стороны", () => {
  const rust = rustKeys({
    "g.rs": [
      'ltext!("m.ok", "Hi {{name}} and {{ count }}", count = n, name = x)',
      'ltext!("m.missing", "Hi {{name}} and {{count}}", name = x)',
      'ltext!("m.extra", "Hi {{name}}", name = x, count = n)',
      'ltext!("m.dup", "Hi {{name}}", name = x, name = y)',
    ].join("\n"),
  });
  assert.deepEqual(rust.problems, []);
  const en = new Map([
    ["m.ok", "Hi {{name}} and {{ count }}"],
    ["m.missing", "Hi {{name}} and {{count}}"],
    ["m.extra", "Hi {{name}}"],
    ["m.dup", "Hi {{name}}"],
  ]);
  const p = checkRustKeys(rust.calls, en);
  assert.equal(p.length, 3, p.join("\n"));
  assert.ok(p.some((x) => x.startsWith("g.rs:2: m.missing") && x.includes("нет параметра count")), p.join("\n"));
  assert.ok(p.some((x) => x.startsWith("g.rs:3: m.extra") && x.includes("лишний параметр count")), p.join("\n"));
  assert.ok(p.some((x) => x.startsWith("g.rs:4: m.dup") && x.includes("дважды")), p.join("\n"));
});

test("кириллица в литералах Rust: код — находка, комментарии и #[cfg(test)] — нет", () => {
  const src = [
    "//! Модуль: описание по-русски",
    "/// Документация «в кавычках»",
    'fn a() -> String { "Ошибка".into() } // комментарий "в кавычках"',
    'fn b() -> &\'static str { "ok" /* "блок" */ }',
    'fn c() { let s = r#"сырой"#; let ch = \'ё\'; }',
    "fn d<'a>(x: &'a str) -> &'a str { x }",
    'tracing::warn!("лог {x}");',
    "#[cfg(test)]",
    'pub fn helper() -> &\'static str { "тестовый помощник" }',
    "#[cfg(test)]",
    "mod tests {",
    '    #[test] fn t() { assert_eq!(x, "по-русски {"); }',
    "}",
    'const AFTER: &str = "после тестов";',
  ].join("\n");
  assert.deepEqual(findRustCyrillic({ "x.rs": src }), ["x.rs:3", "x.rs:5", "x.rs:7", "x.rs:14"]);
  assert.deepEqual(findRustCyrillic({ "y.rs": 'let s = "multi\nстрока";' }), ["y.rs:1"]);
});

test("--rust-cyrillic: каталог раскрывается во все .rs рекурсивно, файл — как есть", () => {
  const dir = mkdtempSync(join(tmpdir(), "i18n-check-"));
  try {
    mkdirSync(join(dir, "sub"));
    writeFileSync(join(dir, "a.rs"), "");
    writeFileSync(join(dir, "sub", "b.rs"), "");
    writeFileSync(join(dir, "sub", "c.txt"), "");
    const single = join(dir, "sub", "c.txt");
    assert.deepEqual(expandRustPaths([dir]).sort(), [join(dir, "a.rs"), join(dir, "sub", "b.rs")]);
    assert.deepEqual(expandRustPaths([single]), [single]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("ключи фронтенда: статические проверяются, динамические и тесты пропускаются", () => {
  const files = {
    "src/a.tsx": [
      'const a = t("ok.key"); const b = i18n.t("typo.key", { x: 1 });',
      "const c = t(`dyn.${name}`); const d = tt('plural.stem');",
      '{ labelKey: "tabs.missing" }, <Trans i18nKey="trans.key" />',
      'const e = t("concat." + x);',
    ].join("\n"),
    "src/a.test.tsx": 't("only.in.test")',
  };
  const keys = frontendKeys(files);
  assert.deepEqual(
    keys.map((k) => k.key),
    ["ok.key", "typo.key", "plural.stem", "tabs.missing", "trans.key"],
  );
  const en = new Map([["ok.key", "x"], ["plural.stem_one", "a"], ["plural.stem_other", "b"], ["trans.key", "t"]]);
  const p = checkFrontendKeys(keys, en);
  assert.deepEqual(p.map((x) => x.split(" ")[3]), ["typo.key", "tabs.missing"]);
  assert.ok(p[0].includes("src/a.tsx:1"), p[0]);
});

test("теги совпадают с en", () => {
  assert.deepEqual(tags("a <code>{{x}}</code> <offset/> <file>"), ["</code>", "<code>", "<file>", "<offset/>"]);
  const en = { a: "Type <code>{{name}}</code>", n_one: "<total/> {{count}}", n_other: "<total/> {{count}}s" };
  const ok = { a: "Tippen <code>{{name}}</code>", n_one: "<total/> {{count}}", n_other: "<total/> {{count}}" };
  assert.deepEqual(checkDictionaries({ en, de: ok }), []);
  const p = checkDictionaries({ en, de: { ...ok, a: "Tippen {{name}}", n_other: "{{count}}" } });
  assert.ok(p.some((x) => x.includes("de: a") && x.includes("<code>")), p.join("\n"));
  assert.ok(p.some((x) => x.includes("de: n_other") && x.includes("<total/>")), p.join("\n"));
});

test("строка совпадает с en без разрешения — ошибка", () => {
  const en = { a: "Save", b: "Kafka", c: "{{message}}", n_one: "{{count}} topic", n_other: "{{count}} topics" };
  const de = { a: "Save", b: "Kafka", c: "{{message}}", n_one: "{{count}} Topic", n_other: "{{count}} topics" };
  const p = checkSameAsEn({ en, de }, { de: ["b"] });
  assert.ok(p.some((x) => x.includes("de: a ")), p.join("\n"));
  assert.ok(!p.some((x) => x.includes("de: b ")), "b разрешён");
  assert.ok(!p.some((x) => x.includes("de: c ")), "без букв вне параметров — не ошибка");
  assert.ok(p.some((x) => x.includes("de: n_other")), "плюральная форма сравнивается с en той же формы или _other");
  const stale = checkSameAsEn({ en, de: { ...de, a: "Speichern", n_other: "{{count}} Topics" } }, { de: ["a", "b"] });
  assert.deepEqual(stale, ["de: a в docs/i18n/same-as-en.json, но перевод отличается от en — уберите из списка"]);
});

test("типографика: многоточие и пробелы во французском", () => {
  const p = checkTypography({
    en: { a: "Loading...", b: "Loading…" },
    fr: { a: "Chargement…", b: "Supprimer ?", c: "Nom : x", d: "« x »", e: "Supprimer ?", f: "« x »", g: "localhost:9092" },
  });
  assert.deepEqual(p.sort(), [
    "en: a — «...» вместо «…»",
    "fr: b — обычный пробел вместо U+00A0 перед «?»",
    "fr: c — обычный пробел вместо U+00A0 перед «:»",
    "fr: d — обычный пробел вместо U+00A0 у кавычек « »",
  ]);
});

test("языки из languages.ts и файлы словарей", () => {
  const ts = 'export const LANGUAGES = [\n  { code: "en", nativeName: "English", pro: false },\n  { code: "pt-BR", nativeName: "Português (Brasil)", pro: true },\n];';
  assert.deepEqual(languageCodes(ts), ["en", "pt-BR"]);
  assert.deepEqual(checkLanguageFiles(["en", "ru", "de"], ["en", "ru"], { requireAll: false }), []);
  assert.deepEqual(checkLanguageFiles(["en", "ru", "de"], ["en", "ru"], { requireAll: true }), ["нет словаря src/locales/de.json"]);
  assert.deepEqual(checkLanguageFiles(["en", "ru"], ["en", "ru", "xx"], { requireAll: false }), ["словарь src/locales/xx.json — языка xx нет в src/i18n/languages.ts"]);
});

test("отчёт о длине: только заметно длиннее en", () => {
  const r = lengthReport({ en: { a: "Delete topic", b: "OK", c: "Save" }, de: { a: "Das Topic endgültig löschen", b: "Einverstanden", c: "Speich" } });
  assert.deepEqual(r, ["de: a — 27 / 12 (×2.3) «Das Topic endgültig löschen»"]);
});
