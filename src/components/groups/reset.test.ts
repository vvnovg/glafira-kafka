import { describe, expect, it } from "vitest";
import { buildResetRequest, defaultResetForm, targetsFromPreview, toLocalInput } from "./reset";

describe("reset", () => {
  it("стратегии и области", () => {
    const base = defaultResetForm();
    expect(base).toEqual({ strategy: "earliest", value: "", scope: "all", scopeText: "" });
    expect(buildResetRequest("g", base)).toEqual({ groupId: "g", strategy: { type: "earliest" }, scope: { type: "all" } });
    expect(buildResetRequest("g", { ...base, strategy: "shift", value: "-10" }).strategy).toEqual({ type: "shift", delta: -10 });
    expect(buildResetRequest("g", { ...base, strategy: "offset", value: "42" }).strategy).toEqual({ type: "offset", offset: 42 });
    const ts = buildResetRequest("g", { ...base, strategy: "timestamp", value: "2026-10-15T12:00" }).strategy;
    expect(ts).toEqual({ type: "timestamp", timestampMs: new Date("2026-10-15T12:00").getTime() });
    expect(buildResetRequest("g", { ...base, scope: "topics", scopeText: "a, b\nc" }).scope).toEqual({ type: "topics", topics: ["a", "b", "c"] });
    expect(buildResetRequest("g", { ...base, scope: "partitions", scopeText: "orders.v1:0, x:12" }).scope).toEqual({
      type: "partitions",
      partitions: [
        { topic: "orders.v1", partition: 0 },
        { topic: "x", partition: 12 },
      ],
    });
  });

  it("ошибки ввода", () => {
    const base = defaultResetForm();
    expect(() => buildResetRequest("g", { ...base, strategy: "offset", value: "-1" })).toThrow("Оффсет — целое число ≥ 0");
    expect(() => buildResetRequest("g", { ...base, strategy: "offset", value: "9007199254740993" })).toThrow("Оффсет — целое число ≥ 0");
    expect(() => buildResetRequest("g", { ...base, strategy: "shift", value: "0" })).toThrow("Сдвиг — ненулевое целое число");
    expect(() => buildResetRequest("g", { ...base, strategy: "timestamp", value: "" })).toThrow("Укажите дату и время");
    expect(() => buildResetRequest("g", { ...base, scope: "topics", scopeText: " " })).toThrow("Укажите хотя бы один топик");
    expect(() => buildResetRequest("g", { ...base, scope: "partitions", scopeText: "orders" })).toThrow("«orders»: ожидается топик:партиция");
  });

  it("предзаполнение из сообщения", () => {
    expect(defaultResetForm({ topic: "t", partition: 2, offset: 7, nonce: 1 })).toEqual({
      strategy: "offset",
      value: "7",
      scope: "partitions",
      scopeText: "t:2",
    });
  });

  it("предзаполнение из предложения LLM", () => {
    const at = new Date(2026, 9, 22, 10, 5, 9).getTime();
    expect(defaultResetForm({ strategy: { type: "timestamp", timestampMs: at }, scope: { type: "topics", topics: ["a", "b"] }, proposalId: "p1", nonce: 1 })).toEqual({
      strategy: "timestamp",
      value: "2026-10-22T10:05:09",
      scope: "topics",
      scopeText: "a, b",
    });
    expect(toLocalInput(at)).toBe("2026-10-22T10:05:09");
    expect(
      defaultResetForm({
        strategy: { type: "shift", delta: -5 },
        scope: { type: "partitions", partitions: [{ topic: "t", partition: 0 }, { topic: "t", partition: 2 }] },
        proposalId: "p1",
        nonce: 1,
      }),
    ).toEqual({ strategy: "shift", value: "-5", scope: "partitions", scopeText: "t:0, t:2" });
    const form = defaultResetForm({ strategy: { type: "earliest" }, scope: { type: "all" }, proposalId: "p1", nonce: 1 });
    expect(form).toEqual({ strategy: "earliest", value: "", scope: "all", scopeText: "" });
    // Обратно получается тот же запрос, что предлагала модель.
    expect(buildResetRequest("g", form)).toEqual({ groupId: "g", strategy: { type: "earliest" }, scope: { type: "all" } });
  });

  it("применяются только строки без ошибок", () => {
    const rows = [
      { topic: "t", partition: 0, current: 1, target: 0, low: 0, high: 5 },
      {
        topic: "t",
        partition: 1,
        current: null,
        target: null,
        low: 0,
        high: 5,
        error: { key: "errors.groups.noCommittedOffset", params: {}, text: "No committed offset — cannot shift" },
      },
    ];
    expect(targetsFromPreview(rows)).toEqual([{ topic: "t", partition: 0, offset: 0 }]);
  });
});
