import { describe, expect, it } from "vitest";
import { formatBytes, formatCount, formatTimestamp } from "./format";

describe("format", () => {
  it("timestamp в UTC с миллисекундами", () => {
    expect(formatTimestamp(Date.UTC(2026, 0, 2, 3, 4, 5, 6), true)).toBe("2026-01-02 03:04:05.006");
    expect(formatTimestamp(null)).toBe("—");
  });

  it("bytes", () => {
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(2048)).toBe("2.0 KB");
    expect(formatBytes(3 * 1024 * 1024)).toBe("3.0 MB");
  });

  it("count: загрузка и недоступно", () => {
    expect(formatCount(undefined)).toBe("…");
    expect(formatCount(null)).toBe("—");
  });
});
