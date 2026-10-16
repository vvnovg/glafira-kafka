#!/usr/bin/env node
// latest.json для tauri-plugin-updater (артефакты v2) из собранных файлов и их .sig.
// macOS-архив Tauri называется «<productName>.app.tar.gz» без архитектуры —
// release.yml переименовывает его в «…_<версия>_<aarch64|x64>.app.tar.gz» до загрузки.
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

function platformOf(name) {
  if (name.endsWith(".app.tar.gz")) {
    if (/aarch64|arm64/.test(name)) return "darwin-aarch64";
    if (/x64|x86_64/.test(name)) return "darwin-x86_64";
    throw new Error(`в имени ${name} нет архитектуры (aarch64/x64)`);
  }
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
    if (platforms[platform]) throw new Error(`две сборки для ${platform}: ${name}`);
    platforms[platform] = { signature: signature.trim(), url: `${baseUrl}/${encodeURIComponent(name)}` };
  }
  return { version, notes, pub_date: pubDate, platforms };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({
    options: { version: { type: "string" }, "notes-file": { type: "string" }, "base-url": { type: "string" }, dir: { type: "string" } },
  });
  for (const k of ["version", "notes-file", "base-url", "dir"]) {
    if (!values[k]) {
      console.error("использование: make-latest-json.mjs --version <v> --notes-file <f> --base-url <url> --dir <артефакты>");
      process.exit(2);
    }
  }
  const dir = values.dir;
  const names = readdirSync(dir);
  const artifacts = names.filter((n) => platformOf(n));
  // Артефакт без .sig — ошибка: значит, сборка шла без ключа подписи updater.
  const files = Object.fromEntries(
    artifacts.map((n) => [n, names.includes(`${n}.sig`) ? readFileSync(join(dir, `${n}.sig`), "utf8") : ""]),
  );
  const latest = buildLatest({
    version: values.version,
    notes: readFileSync(values["notes-file"], "utf8").trim(),
    baseUrl: values["base-url"].replace(/\/+$/, ""),
    files,
    pubDate: new Date().toISOString(),
  });
  if (Object.keys(latest.platforms).length === 0) {
    console.error(`в ${dir} нет артефактов updater`);
    process.exit(1);
  }
  process.stdout.write(JSON.stringify(latest, null, 2) + "\n");
}
