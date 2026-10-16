import { test } from "node:test";
import assert from "node:assert/strict";
import { buildLatest } from "./make-latest-json.mjs";

test("платформы по именам файлов", () => {
  const files = {
    "App_aarch64.app.tar.gz": "SIG-A",
    "App_x64.app.tar.gz": "SIG-X",
    "App_0.2.0_x64_en-US.msi": "SIG-W",
    "app_0.2.0_amd64.AppImage": "SIG-L",
  };
  const latest = buildLatest({ version: "0.2.0", notes: "n", baseUrl: "https://g/r/releases/download/v0.2.0", files, pubDate: "2026-11-01T00:00:00Z" });
  assert.equal(latest.version, "0.2.0");
  assert.deepEqual(Object.keys(latest.platforms).sort(), ["darwin-aarch64", "darwin-x86_64", "linux-x86_64", "windows-x86_64"]);
  assert.equal(latest.platforms["windows-x86_64"].url, "https://g/r/releases/download/v0.2.0/App_0.2.0_x64_en-US.msi");
  assert.equal(latest.platforms["darwin-aarch64"].signature, "SIG-A");
  assert.equal(latest.platforms["darwin-x86_64"].signature, "SIG-X");
});

test("нет подписи — ошибка", () => {
  assert.throws(() => buildLatest({ version: "1", notes: "", baseUrl: "u", files: { "a.msi": "" }, pubDate: "d" }), /подпис/);
});

test("имя с пробелами кодируется в URL", () => {
  const latest = buildLatest({ version: "1", notes: "", baseUrl: "https://x", files: { "My App_1_x64_en-US.msi": "S" }, pubDate: "d" });
  assert.equal(latest.platforms["windows-x86_64"].url, "https://x/My%20App_1_x64_en-US.msi");
});

test("macOS-архив без архитектуры в имени — ошибка", () => {
  assert.throws(() => buildLatest({ version: "1", notes: "", baseUrl: "u", files: { "App.app.tar.gz": "S" }, pubDate: "d" }), /архитектур/);
});

test("две сборки на одну платформу — ошибка", () => {
  assert.throws(
    () => buildLatest({ version: "1", notes: "", baseUrl: "u", files: { "a_x64.msi": "S", "b_x64.msi": "S" }, pubDate: "d" }),
    /windows-x86_64/,
  );
});
