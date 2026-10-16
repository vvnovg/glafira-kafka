import { describe, expect, it } from "vitest";
import { batchReducer, fileFormatOf, initialBatchState } from "./batch";

describe("batchReducer", () => {
  it("прогресс, ошибки, завершение", () => {
    let s = batchReducer(initialBatchState, { type: "start" });
    expect(s.status).toBe("running");
    s = batchReducer(s, { type: "event", event: { type: "progress", sent: 5, failed: 1, total: 10 } });
    expect([s.sent, s.failed, s.total]).toEqual([5, 1, 10]);
    s = batchReducer(s, { type: "event", event: { type: "errors", errors: [{ line: 3, message: { key: "errors.generic", params: { message: "плохо" }, text: "плохо" } }] } });
    expect(s.errors).toHaveLength(1);
    s = batchReducer(s, { type: "event", event: { type: "finished", sent: 9, failed: 1, notSent: 4, cancelled: true } });
    expect(s).toMatchObject({ status: "finished", sent: 9, failed: 1, notSent: 4, cancelled: true });
    const err = { code: "INVALID_INPUT" as const, message: "x", retryable: false };
    expect(batchReducer(s, { type: "event", event: { type: "error", error: err } }).error).toEqual(err);
  });

  it("failed переводит в error, stopped завершает только выполняющийся пакет", () => {
    const err = { code: "INVALID_INPUT" as const, message: "x", retryable: false };
    expect(batchReducer(initialBatchState, { type: "failed", error: err })).toMatchObject({ status: "error", error: err });
    const running = batchReducer(initialBatchState, { type: "start" });
    expect(batchReducer(running, { type: "stopped" })).toMatchObject({ status: "finished", cancelled: true });
    expect(batchReducer(initialBatchState, { type: "stopped" })).toBe(initialBatchState);
    const done = batchReducer(running, { type: "event", event: { type: "finished", sent: 1, failed: 0, notSent: 0, cancelled: false } });
    expect(batchReducer(done, { type: "stopped" })).toBe(done);
  });

  it("формат по расширению", () => {
    expect(fileFormatOf("/a/b.JSONL")).toBe("jsonl");
    expect(fileFormatOf("/a/b.ndjson")).toBe("jsonl");
    expect(fileFormatOf("c:\\x.csv")).toBe("csv");
    expect(fileFormatOf("/a/b.txt")).toBeNull();
  });
});
