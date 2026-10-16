import { test } from "node:test";
import assert from "node:assert/strict";
import { checkRelease } from "./check-release.mjs";

const files = (o = {}) => ({
  packageJson: JSON.stringify({ version: "0.2.0" }),
  cargoToml: '[package]\nname = "x"\nversion = "0.2.0"\n',
  tauriConf: JSON.stringify({ version: "0.2.0", plugins: { updater: { pubkey: "REALKEY" } } }),
  licensePublic: "REAL\n",
  devPublic: "DEV\n",
  changelog: "# Changelog\n\n## 0.2.0\n- первое\n",
  ...o,
});

test("всё в порядке", () => assert.deepEqual(checkRelease({ tag: "v0.2.0", files: files() }), []));
test("разные версии", () => {
  const p = checkRelease({ tag: "v0.2.1", files: files() });
  assert.equal(p.length, 4); // три файла + CHANGELOG
});
test("dev-ключ лицензий и заглушка updater", () => {
  const p = checkRelease({
    tag: "v0.2.0",
    files: files({ licensePublic: "DEV\n", tauriConf: JSON.stringify({ version: "0.2.0", plugins: { updater: { pubkey: "REPLACE_WITH_UPDATER_PUBKEY" } } }) }),
  });
  assert.ok(p.some((x) => x.includes("лицензий")));
  assert.ok(p.some((x) => x.includes("updater")));
});
test("предрелиз", () => {
  const f = files({
    packageJson: JSON.stringify({ version: "0.2.0-beta.1" }),
    cargoToml: 'version = "0.2.0-beta.1"\n',
    tauriConf: JSON.stringify({ version: "0.2.0-beta.1", plugins: { updater: { pubkey: "K" } } }),
    changelog: "## 0.2.0-beta.1\n",
  });
  assert.deepEqual(checkRelease({ tag: "v0.2.0-beta.1", files: f }), []);
});
test("раздел CHANGELOG ищется точно: 0.2.0 не засчитывается разделом 0.2.0-beta.1", () => {
  const p = checkRelease({ tag: "v0.2.0", files: files({ changelog: "## 0.2.0-beta.1\n" }) });
  assert.deepEqual(p.length, 1);
  assert.ok(p[0].includes("CHANGELOG"));
});
