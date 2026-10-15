import { describe, expect, it } from "vitest";
import type { ConfigEntry } from "../../api/types";
import { diffConfigs, isEditable } from "./configDiff";

const entry = (name: string, value: string | null, isDefault: boolean, extra: Partial<ConfigEntry> = {}): ConfigEntry => ({
  name,
  value,
  source: isDefault ? "Default" : "DynamicTopic",
  isDefault,
  isReadOnly: false,
  isSensitive: false,
  ...extra,
});

describe("diffConfigs", () => {
  const entries = [entry("retention.ms", "1000", false), entry("cleanup.policy", "delete", true), entry("segment.ms", "5", true)];

  it("SET для изменённых, DELETE для сброса, пустые правки отбрасываются", () => {
    const { changes, rows } = diffConfigs(entries, {
      "cleanup.policy": { kind: "set", value: "compact" },
      "retention.ms": { kind: "reset" },
      "segment.ms": { kind: "set", value: "5" },
    });
    expect(changes).toEqual([
      { op: "set", name: "cleanup.policy", value: "compact" },
      { op: "delete", name: "retention.ms" },
    ]);
    expect(rows).toEqual([
      { name: "cleanup.policy", before: "delete", after: "compact" },
      { name: "retention.ms", before: "1000", after: null },
    ]);
  });

  it("сброс default-параметра — не изменение", () => {
    expect(diffConfigs(entries, { "segment.ms": { kind: "reset" } }).changes).toEqual([]);
  });

  it("read-only и sensitive не редактируются", () => {
    expect(isEditable(entry("a", "1", true))).toBe(true);
    expect(isEditable(entry("a", "1", true, { isReadOnly: true }))).toBe(false);
    expect(isEditable(entry("a", null, true, { isSensitive: true }))).toBe(false);
  });
});
