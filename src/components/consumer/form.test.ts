import { describe, expect, it } from "vitest";
import { buildConsumerParams, defaultConsumerForm } from "./form";

describe("consumer form", () => {
  it("список топиков", () => {
    const f = { ...defaultConsumerForm({ topic: "a, b\nc", groupId: "g" }), valuePattern: "FAILED" };
    const p = buildConsumerParams(f, "conn");
    expect(p).toMatchObject({
      connectionId: "conn",
      groupId: "g",
      topics: ["a", "b", "c"],
      topicPattern: null,
      autoOffsetReset: "latest",
      enableAutoCommit: false,
      isolationLevel: "readCommitted",
      filter: { value: { pattern: "FAILED", regex: false, caseSensitive: false } },
    });
  });

  it("regex-подписка и ошибки", () => {
    const f = { ...defaultConsumerForm({ groupId: "g" }), useRegex: true, pattern: "orders\\..*" };
    expect(buildConsumerParams(f, "c")).toMatchObject({ topics: [], topicPattern: "orders\\..*" });
    expect(() => buildConsumerParams({ ...f, pattern: "(" }, "c")).toThrow("регулярное выражение");
    expect(() => buildConsumerParams(defaultConsumerForm({ groupId: "g" }), "c")).toThrow("Укажите топики");
    expect(() => buildConsumerParams(defaultConsumerForm({ topic: "t" }), "c")).toThrow("Укажите group.id");
    expect(() => buildConsumerParams({ ...defaultConsumerForm({ topic: "t", groupId: "g" }), limit: 0 }, "c")).toThrow("Буфер");
  });
});
