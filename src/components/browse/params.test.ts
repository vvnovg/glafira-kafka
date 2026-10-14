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

describe("валидация и превью", () => {
  const b = (o: Partial<typeof defaultBrowseForm>) => buildBrowseParams({ ...defaultBrowseForm, ...o }, "c", "t");

  it("превью 1 МБ ограничено", () => {
    const p = previewText({ format: "text", text: "a ".repeat(500_000), size: 1_000_000, truncated: false, error: null } as never, 200);
    expect(p.length).toBe(201);
    expect(p.endsWith("…")).toBe(true);
  });

  it("партиции: пустые части и не-десятичные формы отклоняются, дубли убираются", () => {
    expect(() => b({ partitions: "1," })).toThrow();
    expect(() => b({ partitions: "0,,2" })).toThrow();
    expect(() => b({ partitions: "0x1" })).toThrow();
    expect(() => b({ partitions: "1e1" })).toThrow();
    expect(() => b({ partitions: "-1" })).toThrow();
    expect(b({ partitions: "2, 0, 2" }).partitions).toEqual([2, 0]);
  });

  it("count/offset/minutes валидируются", () => {
    expect(() => b({ startType: "latest", count: 0 })).toThrow(/Количество/);
    expect(() => b({ startType: "latest", count: 1.5 })).toThrow();
    expect(() => b({ startType: "offset", offset: -1 })).toThrow(/Offset/);
    expect(b({ startType: "offset", offset: 0 }).start).toEqual({ type: "offset", offset: 0 });
    expect(() => b({ startType: "lastMinutes", minutes: 0 })).toThrow(/Минуты/);
    expect(() => b({ startType: "lastMinutes", minutes: 5_256_001 })).toThrow(/Минуты/);
    expect(b({ startType: "lastMinutes", minutes: 5_256_000 }).start).toEqual({ type: "lastMinutes", minutes: 5_256_000 });
  });

  it("fromTime не позже toTime", () => {
    expect(() => b({ fromTime: "2026-01-02T00:00", toTime: "2026-01-01T00:00" })).toThrow(/«с»/);
    expect(() => b({ fromTime: "2026-01-01T00:00", toTime: "2026-01-02T00:00" })).not.toThrow();
  });
});
