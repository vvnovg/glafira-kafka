import { describe, expect, it } from "vitest";
import type { BrowsedMessage } from "../../api/types";
import { consumerReducer, initialConsumerState } from "./state";

const msg = (offset: number): BrowsedMessage => ({ topic: "t", partition: 0, offset, timestamp: null, key: null, value: null, headers: [], size: 0 });
const tp = (partition: number) => ({ topic: "t", partition });

describe("consumerReducer", () => {
  it("сообщения, назначения, пауза, коммит", () => {
    let s = consumerReducer(initialConsumerState, { type: "start", limit: 2 });
    s = consumerReducer(s, { type: "event", event: { type: "messages", messages: [msg(1), msg(2), msg(3)] } });
    expect(s.messages.map((m) => m.offset)).toEqual([2, 3]);
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "assigned", partitions: [tp(0), tp(1)], at: 1 } } });
    expect(s.assignment).toEqual([tp(0), tp(1)]);
    s = consumerReducer(s, { type: "event", event: { type: "paused", partitions: [tp(1)] } });
    expect(s.paused).toEqual(["t:1"]);
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "revoked", partitions: [tp(1)], at: 2 } } });
    expect(s.assignment).toEqual([tp(0)]);
    expect(s.paused, "отозванная партиция снимается с паузы").toEqual([]);
    expect(s.log).toHaveLength(2);
    s = consumerReducer(s, { type: "event", event: { type: "committed", offsets: [{ topic: "t", partition: 0, offset: 4 }] } });
    expect(s.lastCommit).toEqual([{ topic: "t", partition: 0, offset: 4 }]);
    s = consumerReducer(s, { type: "event", event: { type: "finished", consumed: 3, matched: 3 } });
    expect(s.status).toBe("finished");
    expect(s.assignment).toEqual([]);
  });

  it("заново назначенная партиция начинает без паузы; пауза — только для назначенных", () => {
    let s = consumerReducer(initialConsumerState, { type: "start", limit: 10 });
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "assigned", partitions: [tp(0)], at: 1 } } });
    s = consumerReducer(s, { type: "event", event: { type: "paused", partitions: [tp(0), tp(5)] } });
    expect(s.paused).toEqual(["t:0"]);
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "assigned", partitions: [tp(0), tp(1)], at: 2 } } });
    expect(s.paused).toEqual([]);
    expect(s.assignment).toEqual([tp(0), tp(1)]);
  });

  it("ошибка ребаланса только пишется в журнал", () => {
    let s = consumerReducer(initialConsumerState, { type: "start", limit: 10 });
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "assigned", partitions: [tp(0)], at: 1 } } });
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "error", message: "boom", at: 2 } } });
    expect(s.assignment).toEqual([tp(0)]);
    expect(s.log).toHaveLength(2);
  });

  it("остановка без терминального события сбрасывает сессию", () => {
    let s = consumerReducer(initialConsumerState, { type: "start", limit: 10 });
    s = consumerReducer(s, { type: "started", sessionId: "sid" });
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "assigned", partitions: [tp(0)], at: 1 } } });
    s = consumerReducer(s, { type: "event", event: { type: "paused", partitions: [tp(0)] } });
    s = consumerReducer(s, { type: "event", event: { type: "reconnecting", message: "нет связи" } });
    s = consumerReducer(s, { type: "stopped" });
    expect(s).toMatchObject({ status: "finished", assignment: [], paused: [], reconnecting: null });
    expect(consumerReducer(s, { type: "stopped" })).toBe(s);
  });

  it("ошибка сессии", () => {
    let s = consumerReducer(initialConsumerState, { type: "start", limit: 10 });
    s = consumerReducer(s, { type: "event", event: { type: "rebalance", record: { kind: "assigned", partitions: [tp(0)], at: 1 } } });
    const error = { code: "INTERNAL" as const, message: "Внутренняя ошибка consumer-сессии", retryable: false };
    s = consumerReducer(s, { type: "event", event: { type: "error", error } });
    expect(s).toMatchObject({ status: "error", error, assignment: [], paused: [] });
  });
});
