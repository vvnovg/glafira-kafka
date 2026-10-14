import { describe, expect, it } from "vitest";
import type { BrowsedMessage } from "../../api/types";
import { browseReducer, initialBrowseState } from "./state";

const msg = (offset: number): BrowsedMessage => ({ partition: 0, offset, timestamp: null, key: null, value: null, headers: [], size: 0 });

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
});
