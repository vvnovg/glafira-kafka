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

/** Код Rust без строк-комментариев и без модуля тестов в конце файла (там бывают тестовые ключи).
 * Строки-комментарии заменяются пустыми, чтобы номера строк в сообщениях совпадали с файлом. */
function rustCode(content) {
  const tests = content.search(/^#\[cfg\(test\)\]\s*mod tests\b/m);
  const code = tests >= 0 ? content.slice(0, tests) : content;
  return code
    .split("\n")
    .map((line) => (/^\s*\/\//.test(line) ? "" : line))
    .join("\n");
}

/** Конец обычного строкового литерала Rust, начинающегося с `"` в позиции i (индекс после закрывающей кавычки) или -1. */
function stringEnd(src, i) {
  for (let j = i + 1; j < src.length; j++) {
    if (src[j] === "\\") j++;
    else if (src[j] === '"') return j + 1;
  }
  return -1;
}

/** Аргументы макроса верхнего уровня, начиная сразу после `(`; null — если скобки не сошлись. */
function splitArgs(src, pos) {
  const args = [];
  let depth = 0;
  let start = pos;
  for (let i = pos; i < src.length; ) {
    const c = src[i];
    const raw = /^r(#*)"/.exec(src.slice(i, i + 64));
    if (raw && !/\w/.test(src[i - 1] ?? "")) {
      const close = src.indexOf(`"${raw[1]}`, i + raw[0].length);
      if (close < 0) return null;
      i = close + 1 + raw[1].length;
    } else if (c === '"') {
      i = stringEnd(src, i);
      if (i < 0) return null;
    } else if (c === "'") {
      const ch = /^'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^\\'\n])'/.exec(src.slice(i, i + 16));
      i += ch ? ch[0].length : 1;
    } else if (src.startsWith("//", i)) {
      const nl = src.indexOf("\n", i);
      i = nl < 0 ? src.length : nl;
    } else if (src.startsWith("/*", i)) {
      const e = src.indexOf("*/", i + 2);
      if (e < 0) return null;
      i = e + 2;
    } else {
      if ("([{".includes(c)) depth++;
      else if (")]}".includes(c)) {
        if (depth === 0) {
          if (c !== ")") return null;
          args.push(src.slice(start, i));
          if (args.length && args[args.length - 1].trim() === "") args.pop(); // запятая в конце
          return { args };
        }
        depth--;
      } else if (c === "," && depth === 0) {
        args.push(src.slice(start, i));
        start = i + 1;
      }
      i++;
    }
  }
  return null;
}

/** Значение обычного строкового литерала Rust (весь аргумент — ровно один литерал) или { error }. */
export function rustString(arg) {
  const a = arg.trim();
  if (/^(?:b|c)?r#*"/.test(a) || /^[bc]"/.test(a)) return { error: "нужен обычный строковый литерал (не r\"…\", b\"…\", c\"…\")" };
  if (!a.startsWith('"') || stringEnd(a, 0) !== a.length) return { error: "нужен один строковый литерал" };
  const body = a.slice(1, -1);
  let out = "";
  for (let i = 0; i < body.length; i++) {
    if (body[i] !== "\\") {
      out += body[i];
      continue;
    }
    const n = body[++i];
    const simple = { n: "\n", t: "\t", r: "\r", 0: "\0", "\\": "\\", '"': '"', "'": "'" };
    if (n in simple) out += simple[n];
    else if (n === "\n" || n === "\r") {
      // Перенос строки после `\`: Rust пропускает его и пробелы в начале следующей строки.
      while (/\s/.test(body[i + 1] ?? "")) i++;
    } else if (n === "x" && /^[0-7][0-9a-fA-F]$/.test(body.slice(i + 1, i + 3))) {
      out += String.fromCharCode(parseInt(body.slice(i + 1, i + 3), 16));
      i += 2;
    } else {
      const u = n === "u" ? /^\{([0-9a-fA-F_]{1,8})\}/.exec(body.slice(i + 1)) : null;
      if (!u) return { error: `неизвестная escape-последовательность \\${n}` };
      out += String.fromCodePoint(parseInt(u[1].replace(/_/g, ""), 16));
      i += u[0].length;
    }
  }
  return { value: out };
}

/**
 * Вызовы app_err!(код, "key", "English {{p}}", p = …) и ltext!("key", "English", p = …) в коде Rust.
 * Каждое вхождение `app_err!` / `ltext!` либо разбирается, либо попадает в problems — макрос не пропадает молча.
 * Возвращает { calls: [{ key, template, params, where }], problems }.
 */
export function rustKeys(files /* {path: content} */) {
  const calls = [];
  const problems = [];
  for (const [path, content] of Object.entries(files)) {
    const src = rustCode(content);
    for (const m of src.matchAll(/\b(app_err|ltext)!\s*([([{])/g)) {
      const where = `${path}:${src.slice(0, m.index).split("\n").length}`;
      const fail = (why) => problems.push(`${where}: макрос ${m[1]}!, который проверка не может разобрать: ${why}`);
      if (m[2] !== "(") {
        fail("вызывайте с круглыми скобками");
        continue;
      }
      const parsed = splitArgs(src, m.index + m[0].length);
      if (!parsed) {
        fail("не найдена закрывающая скобка");
        continue;
      }
      const args = parsed.args.map((a) => a.trim());
      const rest = m[1] === "app_err" ? args.slice(1) : args;
      if (rest[0]?.startsWith("$")) continue; // тело другого macro_rules (метапеременные), не вызов
      if (rest.length < 2) {
        fail("ожидаются ключ и английский шаблон");
        continue;
      }
      const key = rustString(rest[0]);
      const template = rustString(rest[1]);
      if (key.error || !/^[\w.]+$/.test(key.value)) {
        fail(`ключ: ${key.error ?? `недопустимый ключ «${key.value}»`}`);
        continue;
      }
      if (template.error) {
        fail(`шаблон ${key.value}: ${template.error}`);
        continue;
      }
      const params = [];
      let bad = null;
      for (const p of rest.slice(2)) {
        const pm = /^([A-Za-z_]\w*)\s*=(?!=)\s*\S/.exec(p);
        if (pm) params.push(pm[1]);
        else bad = p;
      }
      if (bad !== null) {
        fail(`${key.value}: параметр «${bad}» не в виде name = значение`);
        continue;
      }
      calls.push({ key: key.value, template: template.value, params, where });
    }
  }
  return { calls, problems };
}

/** Каждый ключ Rust есть в en.json, текст en совпадает с шаблоном Rust, параметры вызова = {{…}} шаблона. */
export function checkRustKeys(calls, enFlat) {
  const problems = [];
  for (const { key, template, params, where } of calls) {
    if (!enFlat.has(key)) problems.push(`en: нет ключа ${key} (используется в Rust, ${where})`);
    else if (enFlat.get(key) !== template)
      problems.push(`en: ${key} отличается от шаблона в Rust (${where}): «${enFlat.get(key)}» ≠ «${template}»`);
    const used = new Set(placeholders(template));
    for (const p of used) if (!params.includes(p)) problems.push(`${where}: ${key} — нет параметра ${p} для {{${p}}}`);
    for (const p of params) if (!used.has(p)) problems.push(`${where}: ${key} — лишний параметр ${p}, в шаблоне нет {{${p}}}`);
    const dup = params.filter((p, i) => params.indexOf(p) !== i);
    for (const p of new Set(dup)) problems.push(`${where}: ${key} — параметр ${p} передан дважды`);
  }
  return problems;
}

const CYR = /[А-Яа-яЁё]/;

/** Индекс сразу после элемента, помеченного `#[cfg(test)]` (атрибут кончается в позиции pos):
 * до `;` или до парной `}` первого блока на верхнем уровне. */
function skipItem(src, pos) {
  let depth = 0;
  for (let i = pos; i < src.length; ) {
    const lit = literalAt(src, i);
    if (lit) {
      i = lit.end;
      continue;
    }
    const c = src[i];
    if (c === "{") depth++;
    else if (c === "}") {
      depth--;
      if (depth === 0) return i + 1;
    } else if (c === ";" && depth === 0) return i + 1;
    i++;
  }
  return src.length;
}

/** Литерал или комментарий Rust в позиции i: { end, text?, comment? } или null. */
function literalAt(src, i) {
  if (src.startsWith("//", i)) {
    const nl = src.indexOf("\n", i);
    return { end: nl < 0 ? src.length : nl, comment: true };
  }
  if (src.startsWith("/*", i)) {
    const e = src.indexOf("*/", i + 2);
    return { end: e < 0 ? src.length : e + 2, comment: true };
  }
  const raw = /^(?:[bc])?r(#*)"/.exec(src.slice(i, i + 64));
  if (raw && !/\w/.test(src[i - 1] ?? "")) {
    const close = src.indexOf(`"${raw[1]}`, i + raw[0].length);
    const end = close < 0 ? src.length : close + 1 + raw[1].length;
    return { end, text: src.slice(i, end) };
  }
  if (src[i] === '"') {
    const end = stringEnd(src, i);
    return { end: end < 0 ? src.length : end, text: src.slice(i, end < 0 ? src.length : end) };
  }
  if (src[i] === "'") {
    const ch = /^'(?:\\(?:u\{[0-9a-fA-F_]+\}|x[0-9a-fA-F]{2}|.)|[^\\'\n])'/u.exec(src.slice(i, i + 16));
    if (ch) return { end: i + ch[0].length, text: ch[0] };
  }
  return null;
}

/**
 * files: { path: content }. Строковые и символьные литералы Rust с кириллицей вне комментариев
 * и вне элементов под `#[cfg(test)]` (модули тестов, тестовые помощники). Возвращает ["path:line"].
 */
export function findRustCyrillic(files) {
  const hits = [];
  for (const [path, content] of Object.entries(files)) {
    const lines = new Set();
    for (let i = 0; i < content.length; ) {
      if (content.startsWith("#[cfg(test)]", i)) {
        i = skipItem(content, i + "#[cfg(test)]".length);
        continue;
      }
      const lit = literalAt(content, i);
      if (!lit) {
        i++;
        continue;
      }
      if (lit.text && CYR.test(lit.text)) lines.add(content.slice(0, i).split("\n").length);
      i = Math.max(lit.end, i + 1);
    }
    for (const l of [...lines].sort((a, b) => a - b)) hits.push(`${path}:${l}`);
  }
  return hits;
}

function walk(dir, exts, out = []) {
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    if (statSync(p).isDirectory()) walk(p, exts, out);
    else if (exts.some((e) => p.endsWith(e))) out.push(p);
  }
  return out;
}

/** Аргументы `--rust-cyrillic`: файлы как есть, каталоги — все `.rs` внутри (рекурсивно). */
export function expandRustPaths(paths) {
  return paths.flatMap((p) => (statSync(p).isDirectory() ? walk(p, [".rs"]) : [p]));
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
  problems.push(...rust.problems, ...checkRustKeys(rust.calls, flatten(dicts.en)));
  if (!process.argv.includes("--no-cyrillic-scan")) {
    const files = Object.fromEntries(
      walk(join(root, "src"), [".ts", ".tsx"]).map((p) => [relative(root, p), readFileSync(p, "utf8")]),
    );
    for (const hit of findCyrillic(files)) problems.push(`кириллица в коде: ${hit}`);
  }
  const rcIdx = process.argv.indexOf("--rust-cyrillic");
  if (rcIdx >= 0) {
    const paths = process.argv.slice(rcIdx + 1).filter((a) => !a.startsWith("--"));
    const expanded = expandRustPaths(paths);
    const files = Object.fromEntries(expanded.map((p) => [p, readFileSync(p, "utf8")]));
    for (const hit of findRustCyrillic(files)) problems.push(`кириллица в Rust: ${hit}`);
  }
  if (problems.length) {
    console.error(problems.map((p) => `✗ ${p}`).join("\n"));
    process.exit(1);
  }
  console.log(`✓ словари ${Object.keys(dicts).join(", ")} согласованы, вызовов из Rust: ${rust.calls.length}`);
}
