#!/usr/bin/env node
// Проверки перед релизом: версии, боевые ключи, раздел CHANGELOG.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const escapeRe = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export function checkRelease({ tag, files }) {
  const problems = [];
  const version = tag.replace(/^v/, "");
  const pkg = JSON.parse(files.packageJson).version;
  const cargo = /^version\s*=\s*"([^"]+)"/m.exec(files.cargoToml)?.[1];
  const tauri = JSON.parse(files.tauriConf);
  for (const [name, v] of [["package.json", pkg], ["src-tauri/Cargo.toml", cargo], ["src-tauri/tauri.conf.json", tauri.version]]) {
    if (v !== version) problems.push(`${name}: версия ${v ?? "не найдена"}, а тег ${tag}`);
  }
  if (files.licensePublic.trim() === files.devPublic.trim()) {
    problems.push("src-tauri/license-public.key — тестовый dev-ключ лицензий: сгенерируйте боевой (docs/licensing.md)");
  }
  const pubkey = tauri.plugins?.updater?.pubkey;
  if (!pubkey || pubkey === "REPLACE_WITH_UPDATER_PUBKEY") {
    problems.push("tauri.conf.json: не задан открытый ключ updater (plugins.updater.pubkey)");
  }
  if (!new RegExp(`^## ${escapeRe(version)}\\s*$`, "m").test(files.changelog)) {
    problems.push(`CHANGELOG.md: нет раздела «## ${version}»`);
  }
  return problems;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const tag = process.argv[2];
  if (!tag) {
    console.error("использование: check-release.mjs <tag>");
    process.exit(2);
  }
  const read = (p) => readFileSync(p, "utf8");
  const problems = checkRelease({
    tag,
    files: {
      packageJson: read("package.json"),
      cargoToml: read("src-tauri/Cargo.toml"),
      tauriConf: read("src-tauri/tauri.conf.json"),
      licensePublic: read("src-tauri/license-public.key"),
      devPublic: read("crates/license-format/testdata/dev-public.key"),
      changelog: read("CHANGELOG.md"),
    },
  });
  if (problems.length) {
    console.error(problems.map((p) => `✗ ${p}`).join("\n"));
    process.exit(1);
  }
  console.log(`✓ ${tag}: можно выпускать`);
}
