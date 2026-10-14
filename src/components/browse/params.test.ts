import { describe, expect, it } from "vitest";
import { buildBrowseParams, defaultBrowseForm, prettyText, previewText } from "./params";

describe("buildBrowseParams", () => {
  it("значения по умолчанию — последние 100", () => {
    const p = buildBrowseParams(defaultBrowseForm, "c", "t");
    expect(p).toEqual({
      connectionId: "c",
      topic: "t",
      partitions: null,
      start: { type: "latest", count: 100 },
      limit: 500,
      live: false,
      keyFormat: "auto",
      valueFormat: "auto",
      filter: {},
    });
  });

  it("партиции, фильтры, время", () => {
    const p = buildBrowseParams(
      {
        ...defaultBrowseForm,
        startType: "timestamp",
        timestamp: "2026-01-01T10:00",
        partitions: "0, 2",
        valuePattern: "FAILED",
        regex: true,
        headerName: "trace",
        headerValue: "t1",
        jsonPath: "$.a == 1",
        fromTime: "2026-01-01T09:00",
      },
      "c",
      "t",
    );
    expect(p.partitions).toEqual([0, 2]);
    expect(p.start).toEqual({ type: "timestamp", timestampMs: new Date("2026-01-01T10:00").getTime() });
    expect(p.filter).toEqual({
      value: { pattern: "FAILED", regex: true, caseSensitive: false },
      header: { name: "trace", value: { pattern: "t1", regex: true, caseSensitive: false } },
      jsonPath: "$.a == 1",
      fromTimestamp: new Date("2026-01-01T09:00").getTime(),
    });
  });

  it("ошибки ввода", () => {
    expect(() => buildBrowseParams({ ...defaultBrowseForm, partitions: "a" }, "c", "t")).toThrow("Партиции");
    expect(() => buildBrowseParams({ ...defaultBrowseForm, startType: "timestamp", timestamp: "" }, "c", "t")).toThrow("время");
    expect(() => buildBrowseParams({ ...defaultBrowseForm, limit: 0 }, "c", "t")).toThrow("Лимит");
  });
});

describe("text helpers", () => {
  it("prettyText форматирует JSON", () => {
    expect(prettyText({ format: "json", text: '{"a":1}', size: 7, truncated: false })).toBe('{\n  "a": 1\n}');
    expect(prettyText({ format: "string", text: "x", size: 1, truncated: false })).toBe("x");
    expect(prettyText(null)).toBe("");
  });

  it("previewText в одну строку с обрезкой", () => {
    expect(previewText({ format: "string", text: "a\n  b", size: 5, truncated: false })).toBe("a b");
    expect(previewText({ format: "string", text: "x".repeat(300), size: 300, truncated: false }, 10)).toBe("xxxxxxxxxx…");
    expect(previewText(null)).toBe("null");
  });
});
