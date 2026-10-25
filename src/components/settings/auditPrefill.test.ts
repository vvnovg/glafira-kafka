import { describe, expect, it } from "vitest";
import type { McpAuditEntry } from "../../api/types";
import { toLocalInput } from "../groups/reset";
import { auditTarget, browsePrefillFromAudit } from "./auditPrefill";

const call = (args: unknown, patch: Partial<McpAuditEntry> = {}): McpAuditEntry => ({
  ts: 1,
  event: "call",
  tool: "read_messages",
  outcome: "ok",
  connection: { id: "c1", name: "dev" },
  args,
  scanned: 7,
  returned: 7,
  masked: 2,
  ...patch,
});

describe("browsePrefillFromAudit", () => {
  it("окно по времени: границы наружу до секунды, партиции и фильтры", () => {
    const from = Date.parse("2026-10-24T14:31:00.250+03:00");
    const to = 1_792_841_525_123;
    const r = browsePrefillFromAudit(
      call({
        connectionId: "c1",
        topic: "orders",
        from: "2026-10-24T14:31:00.250+03:00",
        to,
        partitions: [2, 0],
        value: { pattern: "FAILED", regex: true },
        header: { name: "trace", value: { pattern: "t-1" } },
        jsonPath: "$.a",
        valueFormat: "json",
      }),
    );
    expect(r).not.toBeNull();
    expect(r?.connectionId).toBe("c1");
    expect(r?.topic).toBe("orders");
    expect(r?.prefill.inexact).toBe(false);
    expect(r?.prefill.form).toMatchObject({
      startType: "timestamp",
      timestamp: toLocalInput(Math.floor(from / 1000) * 1000),
      fromTime: toLocalInput(Math.floor(from / 1000) * 1000),
      toTime: toLocalInput(Math.ceil(to / 1000) * 1000),
      partitions: "2, 0",
      valuePattern: "FAILED",
      regex: true,
      caseSensitive: false,
      headerName: "trace",
      headerValue: "t-1",
      jsonPath: "$.a",
      keyFormat: "auto",
      valueFormat: "json",
    });
  });

  it("режим «офсеты»: начало, партиция и лимит", () => {
    const r = browsePrefillFromAudit(call({ topic: "orders", partition: 1, startOffset: 10, endOffset: 25 }));
    expect(r?.prefill.form).toMatchObject({ startType: "offset", offset: 10, partitions: "1", limit: 15 });
    const open = browsePrefillFromAudit(call({ topic: "orders", partition: 1, startOffset: 10 }));
    expect(open?.prefill.form.limit).toBeUndefined();
  });

  it("разные флаги ключа и значения — берутся флаги значения, перенос неточный", () => {
    const r = browsePrefillFromAudit(
      call({ topic: "t", from: 0, to: 1, key: { pattern: "k", regex: true }, value: { pattern: "v", caseSensitive: true } }),
    );
    expect(r?.prefill.inexact).toBe(true);
    expect(r?.prefill.form).toMatchObject({ keyPattern: "k", valuePattern: "v", regex: false, caseSensitive: true });
  });

  it("повторить нельзя: усечённые аргументы, ошибка, другой инструмент, нет времени", () => {
    expect(browsePrefillFromAudit(call('{"topic":"orders"…'))).toBeNull();
    expect(browsePrefillFromAudit(call({ topic: "t", from: 0, to: 1 }, { outcome: "error" }))).toBeNull();
    expect(browsePrefillFromAudit(call({ groupId: "g", from: 0, to: 1 }, { tool: "get_group_commit_history" }))).toBeNull();
    expect(browsePrefillFromAudit(call({ topic: "t", from: "yesterday", to: 1 }))).toBeNull();
    expect(browsePrefillFromAudit(call({ topic: "t", from: 0, to: 1 }, { connection: null }))).toBeNull();
    expect(browsePrefillFromAudit(call({ topic: "t", from: 0, to: 1 }, { tool: "summarize_messages" }))).not.toBeNull();
  });
});

describe("auditTarget", () => {
  it("топик или группа из аргументов", () => {
    expect(auditTarget(call({ topic: "orders" }))).toBe("orders");
    expect(auditTarget(call({ groupId: "svc" }))).toBe("svc");
    expect(auditTarget(call("усечено…"))).toBe("");
  });
});
