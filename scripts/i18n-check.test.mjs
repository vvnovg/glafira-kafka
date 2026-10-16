import { test } from "node:test";
import assert from "node:assert/strict";
import { checkDictionaries, findCyrillic, flatten, placeholders } from "./i18n-check.mjs";

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
