#!/usr/bin/env node
// Проверки локализации: одинаковые ключи и параметры во всех словарях, ключи из Rust, забытая кириллица в коде.
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const PLURAL = /_(zero|one|two|few|many|other)$/;

export function flatten(obj, prefix = "", out = new Map()) {
  for (const [k, v] of Object.entries(obj)) {
    const key = prefix ? `${prefix}.${k}` : k;
    if (v && typeof v === "object") flatten(v, key, out);
    else out.set(key, String(v));
  }
  return out;
}

export function placeholders(text) {
  return [...new Set([...text.matchAll(/\{\{\s*([\w.]+)\s*\}\}/g)].map((m) => m[1]))];
}

function pluralForms(code) {
  const rules = new Intl.PluralRules(code);
  return rules.resolvedOptions().pluralCategories;
}

/** dicts: { en: {...}, ru: {...} } — первый (en) эталон. Возвращает список проблем. */
export function checkDictionaries(dicts) {
  const problems = [];
  const flat = Object.fromEntries(Object.entries(dicts).map(([c, d]) => [c, flatten(d)]));
  const base = flat.en;
  // Базовые ключи без плюрального суффикса и их параметры (по форме _other или самому ключу).
  const baseKeys = new Map();
  for (const [k, v] of base) {
    const stem = k.replace(PLURAL, "");
    const params = new Set(placeholders(v));
    const prev = baseKeys.get(stem);
    baseKeys.set(stem, { plural: PLURAL.test(k), params: prev ? new Set([...prev.params, ...params]) : params });
  }
  for (const [code, map] of Object.entries(flat)) {
    if (code === "en") continue;
    const forms = pluralForms(code);
    for (const [stem, info] of baseKeys) {
      const keys = info.plural ? forms.map((f) => `${stem}_${f}`) : [stem];
      for (const key of keys) {
        if (!map.has(key)) {
          problems.push(`${code}: нет ключа ${key}`);
          continue;
        }
        const got = new Set(placeholders(map.get(key)));
        // В плюральных формах «count» может отсутствовать в _one у некоторых языков — требуем остальные параметры.
        for (const p of info.params) {
          if (!got.has(p) && !(info.plural && p === "count")) problems.push(`${code}: ${key} без параметра {{${p}}}`);
        }
        for (const p of got) if (!info.params.has(p)) problems.push(`${code}: ${key} с лишним параметром {{${p}}}`);
      }
    }
    for (const k of map.keys()) {
      if (!baseKeys.has(k.replace(PLURAL, ""))) problems.push(`${code}: лишний ключ ${k} (нет в en)`);
    }
  }
  return problems;
}

/** files: { path: content }. Кириллица в строках кода фронтенда вне словарей, тестов и комментариев. */
export function findCyrillic(files) {
  const hits = [];
  for (const [path, content] of Object.entries(files)) {
    if (/\.test\.[tj]sx?$/.test(path) || path.includes("/locales/") || path.includes("/test/")) continue;
    if (path.endsWith("src/i18n/languages.ts")) continue; // самоназвания языков — данные
    content.split("\n").forEach((line, i) => {
      const code = line.replace(/\/\/.*$/, "").replace(/\/\*.*?\*\//g, "");
      if (/^\s*\*/.test(line)) return; // строки JSDoc-блоков
      if (/[А-Яа-яЁё]/.test(code)) hits.push(`${path}:${i + 1}`);
    });
  }
  return hits;
}

/** Код Rust без строк-комментариев и без модуля тестов в конце файла (там бывают тестовые ключи). */
function rustCode(content) {
  const tests = content.search(/^#\[cfg\(test\)\]\s*mod tests\b/m);
  const code = tests >= 0 ? content.slice(0, tests) : content;
  return code
    .split("\n")
    .filter((line) => !/^\s*\/\//.test(line))
    .join("\n");
}

/** Ключи и английские шаблоны из макросов Rust: app_err!(…, "key", "English {{p}}" …) и ltext!("key", "English" …). */
export function rustKeys(files /* {path: content} */) {
  const out = new Map();
  const re = /(?:app_err!\(\s*[^,]+,|ltext!\()\s*"([\w.]+)"\s*,\s*"((?:[^"\\]|\\.)*)"/g;
  for (const content of Object.values(files)) {
    for (const m of rustCode(content).matchAll(re)) out.set(m[1], m[2].replace(/\\"/g, '"'));
  }
  return out;
}

/** Каждый ключ Rust есть в en.json, текст en совпадает с шаблоном Rust (единый английский). */
export function checkRustKeys(rust, enFlat) {
  const problems = [];
  for (const [key, template] of rust) {
    if (!enFlat.has(key)) problems.push(`en: нет ключа ${key} (используется в Rust)`);
    else if (enFlat.get(key) !== template)
      problems.push(`en: ${key} отличается от шаблона в Rust: «${enFlat.get(key)}» ≠ «${template}»`);
  }
  return problems;
}

function walk(dir, exts, out = []) {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p, exts, out);
    else if (exts.some((e) => p.endsWith(e))) out.push(p);
  }
  return out;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const root = process.cwd();
  const locales = join(root, "src/locales");
  const dicts = Object.fromEntries(
    readdirSync(locales)
      .filter((f) => f.endsWith(".json"))
      .map((f) => [f.replace(/\.json$/, ""), JSON.parse(readFileSync(join(locales, f), "utf8"))]),
  );
  const problems = checkDictionaries(dicts);
  const rustFiles = Object.fromEntries(
    walk(join(root, "src-tauri/src"), [".rs"]).map((p) => [relative(root, p), readFileSync(p, "utf8")]),
  );
  const rust = rustKeys(rustFiles);
  problems.push(...checkRustKeys(rust, flatten(dicts.en)));
  if (!process.argv.includes("--no-cyrillic-scan")) {
    const files = Object.fromEntries(
      walk(join(root, "src"), [".ts", ".tsx"]).map((p) => [relative(root, p), readFileSync(p, "utf8")]),
    );
    for (const hit of findCyrillic(files)) problems.push(`кириллица в коде: ${hit}`);
  }
  if (problems.length) {
    console.error(problems.map((p) => `✗ ${p}`).join("\n"));
    process.exit(1);
  }
  console.log(`✓ словари ${Object.keys(dicts).join(", ")} согласованы, ключей из Rust: ${rust.size}`);
}
