import { test } from "node:test";
import assert from "node:assert/strict";
import { checkDictionaries, checkRustKeys, findCyrillic, flatten, placeholders, rustKeys } from "./i18n-check.mjs";

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

test("ключи из Rust", () => {
  const rust = rustKeys({
    "a.rs": 'app_err!(ErrorCode::ReadOnly, "errors.guard.readOnly", "Connection “{{name}}” is read-only", name = n)',
    "b.rs": 'let t = ltext!("messages.x", "Hi");',
  });
  assert.deepEqual([...rust.keys()].sort(), ["errors.guard.readOnly", "messages.x"]);
  const en = flatten({ errors: { guard: { readOnly: "Connection “{{name}}” is read-only" } }, messages: { x: "Hello" } });
  const p = checkRustKeys(rust, en);
  assert.deepEqual(p.length, 1);
  assert.ok(p[0].includes("messages.x"));
});

test("ключи из Rust: многострочный вызов после rustfmt и экранированные кавычки", () => {
  const rust = rustKeys({
    "c.rs": 'Err(app_err!(\n            ErrorCode::ConfirmationRequired,\n            "errors.guard.typeToConfirm",\n            "Type \\"{{expected}}\\" to confirm",\n            expected = expected\n        ))',
  });
  assert.equal(rust.get("errors.guard.typeToConfirm"), 'Type "{{expected}}" to confirm');
  assert.deepEqual(checkRustKeys(rust, new Map()), ["en: нет ключа errors.guard.typeToConfirm (используется в Rust)"]);
});

test("ключи из Rust: комментарии и модуль тестов не считаются", () => {
  const rust = rustKeys({
    "d.rs": [
      '/// `ltext!("messages.doc", "Doc")` → LocalizedText',
      'let a = ltext!("messages.real", "Real");',
      "#[cfg(test)]",
      "mod tests {",
      '    let t = ltext!("messages.test.only", "Test");',
      "}",
    ].join("\n"),
  });
  assert.deepEqual([...rust.keys()], ["messages.real"]);
});
