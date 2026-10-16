import { describe, expect, it } from "vitest";
import type { BrowsedMessage } from "../../api/types";
import { appendMessages, browseReducer, initialBrowseState } from "./state";

const msg = (offset: number): BrowsedMessage => ({ topic: "t", partition: 0, offset, timestamp: null, key: null, value: null, headers: [], size: 0 });

describe("browseReducer", () => {
  it("кольцевой буфер держит последние limit сообщений", () => {
    let s = browseReducer(initialBrowseState, { type: "start", limit: 3 });
    s = browseReducer(s, { type: "event", event: { type: "messages", messages: [msg(1), msg(2)] } });
    s = browseReducer(s, { type: "event", event: { type: "messages", messages: [msg(3), msg(4)] } });
    expect(s.messages.map((m) => m.offset)).toEqual([2, 3, 4]);
    expect(s.status).toBe("running");
  });

  it("progress, reachedEnd, finished, error", () => {
    let s = browseReducer(initialBrowseState, { type: "start", limit: 10 });
    s = browseReducer(s, { type: "event", event: { type: "progress", scanned: 5, matched: 2 } });
    expect([s.scanned, s.matched]).toEqual([5, 2]);
    s = browseReducer(s, { type: "event", event: { type: "reachedEnd" } });
    expect(s.reachedEnd).toBe(true);
    s = browseReducer(s, { type: "event", event: { type: "finished", scanned: 9, matched: 3, reason: "limit" } });
    expect(s.status).toBe("finished");
    expect(s.finishReason).toBe("limit");
    const err = { code: "NOT_FOUND" as const, message: "нет", retryable: false };
    s = browseReducer(s, { type: "event", event: { type: "error", error: err } });
    expect(s.status).toBe("error");
    expect(s.error).toEqual(err);
  });

  it("start сбрасывает предыдущие результаты", () => {
    let s = browseReducer(initialBrowseState, { type: "start", limit: 10 });
    s = browseReducer(s, { type: "event", event: { type: "messages", messages: [msg(1)] } });
    s = browseReducer(s, { type: "start", limit: 10 });
    expect(s.messages).toEqual([]);
    expect(s.scanned).toBe(0);
  });

  it("stopped: running -> finished/cancelled, иначе no-op", () => {
    let s = browseReducer(initialBrowseState, { type: "start", limit: 10 });
    s = browseReducer(s, { type: "stopped" });
    expect(s.status).toBe("finished");
    expect(s.finishReason).toBe("cancelled");
    const err = { code: "NOT_FOUND" as const, message: "нет", retryable: false };
    let e = browseReducer(initialBrowseState, { type: "failed", error: err });
    e = browseReducer(e, { type: "stopped" });
    expect(e.status).toBe("error");
    expect(browseReducer(initialBrowseState, { type: "stopped" })).toBe(initialBrowseState);
  });

  it("reconnecting/recovered и сброс статуса переподключения", () => {
    const reconnect = { type: "event" as const, event: { type: "reconnecting" as const, message: { key: "errors.generic", params: { message: "нет связи" }, text: "нет связи" } } };
    let s = browseReducer(initialBrowseState, { type: "start", limit: 10 });
    expect(s.reconnecting).toBeNull();
    s = browseReducer(s, reconnect);
    expect(s.reconnecting?.text).toBe("нет связи");
    expect(s.status).toBe("running");
    s = browseReducer(s, { type: "event", event: { type: "recovered" } });
    expect(s.reconnecting).toBeNull();

    s = browseReducer(browseReducer(s, reconnect), { type: "start", limit: 10 });
    expect(s.reconnecting).toBeNull();
    s = browseReducer(browseReducer(s, reconnect), {
      type: "event",
      event: { type: "finished", scanned: 0, matched: 0, reason: "limit" },
    });
    expect(s.reconnecting).toBeNull();
    const err = { code: "TIMEOUT" as const, message: "t", retryable: true };
    s = browseReducer(browseReducer(browseReducer(s, { type: "start", limit: 1 }), reconnect), {
      type: "event",
      event: { type: "error", error: err },
    });
    expect(s.reconnecting).toBeNull();
    s = browseReducer(browseReducer(browseReducer(s, { type: "start", limit: 1 }), reconnect), { type: "stopped" });
    expect(s.reconnecting).toBeNull();
  });
});

describe("appendMessages", () => {
  it("держит последние limit сообщений", () => {
    expect(appendMessages([msg(1), msg(2)], [msg(3)], 5).map((m) => m.offset)).toEqual([1, 2, 3]);
    expect(appendMessages([msg(1), msg(2)], [msg(3), msg(4)], 3).map((m) => m.offset)).toEqual([2, 3, 4]);
  });

  it("большой пакет вытесняет весь буфер", () => {
    const batch = [msg(5), msg(6), msg(7), msg(8)];
    expect(appendMessages([msg(1)], batch, 3).map((m) => m.offset)).toEqual([6, 7, 8]);
  });

  it("пустой пакет не создаёт новый массив", () => {
    const buf = [msg(1)];
    expect(appendMessages(buf, [], 3)).toBe(buf);
  });
});
