import { beforeEach, describe, expect, it } from "vitest";
import { newConsumerKey, tabId, useWorkspace } from "./workspace";

beforeEach(() => {
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("workspace", () => {
  it("consumer-сессии открываются параллельно в отдельных вкладках", () => {
    const { open } = useWorkspace.getState();
    const a = newConsumerKey();
    const b = newConsumerKey();
    expect(a).not.toBe(b);
    open({ kind: "consumer", connectionId: "c", key: a, topic: "orders" });
    open({ kind: "consumer", connectionId: "c", key: b, topic: "orders" });
    expect(useWorkspace.getState().tabs.map((t) => t.id)).toEqual([`consumer:c:${a}`, `consumer:c:${b}`]);
    expect(tabId({ kind: "consumer", connectionId: "c", key: "k" })).toBe("consumer:c:k");
  });

  it("повторное открытие активирует существующую вкладку", () => {
    const { open } = useWorkspace.getState();
    open({ kind: "topics", connectionId: "c" });
    open({ kind: "topic", connectionId: "c", topic: "t" });
    open({ kind: "topics", connectionId: "c" });
    const s = useWorkspace.getState();
    expect(s.tabs).toHaveLength(2);
    expect(s.activeId).toBe("topics:c");
  });

  it("закрытие активной вкладки активирует соседнюю", () => {
    const { open, close, activate } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "a" });
    open({ kind: "cluster", connectionId: "b" });
    open({ kind: "cluster", connectionId: "c" });
    activate("cluster:b");
    close("cluster:b");
    expect(useWorkspace.getState().activeId).toBe("cluster:c");
    close("cluster:c");
    expect(useWorkspace.getState().activeId).toBe("cluster:a");
    close("cluster:a");
    expect(useWorkspace.getState().activeId).toBeNull();
  });

  it("id форм подключения", () => {
    expect(tabId({ kind: "connection-form", connectionId: null })).toBe("connection-form:new");
    expect(tabId({ kind: "topic", connectionId: "c", topic: "x" })).toBe("topic:c:x");
  });

  it("closeForConnection закрывает все вкладки подключения", () => {
    const { open, activate, closeForConnection } = useWorkspace.getState();
    open({ kind: "cluster", connectionId: "a" });
    open({ kind: "topics", connectionId: "b" });
    open({ kind: "topic", connectionId: "b", topic: "t" });
    open({ kind: "connection-form", connectionId: "b" });
    open({ kind: "cluster", connectionId: "c" });
    activate("topic:b:t");
    closeForConnection("b");
    let s = useWorkspace.getState();
    expect(s.tabs.map((t) => t.id)).toEqual(["cluster:a", "cluster:c"]);
    expect(s.activeId).toBe("cluster:c");
    activate("cluster:a");
    closeForConnection("c");
    s = useWorkspace.getState();
    expect(s.activeId).toBe("cluster:a");
    closeForConnection("a");
    expect(useWorkspace.getState()).toMatchObject({ tabs: [], activeId: null });
  });

  it("повторное open обновляет spec открытой вкладки", () => {
    const { open } = useWorkspace.getState();
    open({ kind: "group", connectionId: "c", groupId: "g" });
    open({ kind: "topics", connectionId: "c" });
    const prefill = { topic: "t", partition: 1, offset: 42, nonce: 1 };
    open({ kind: "group", connectionId: "c", groupId: "g", resetPrefill: prefill });
    const s = useWorkspace.getState();
    expect(s.tabs).toHaveLength(2);
    expect(s.activeId).toBe("group:c:g");
    expect(s.tabs[0]).toMatchObject({ resetPrefill: prefill });
    expect(tabId({ kind: "groups", connectionId: "c" })).toBe("groups:c");
  });
});
