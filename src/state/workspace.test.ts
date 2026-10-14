import { beforeEach, describe, expect, it } from "vitest";
import { tabId, useWorkspace } from "./workspace";

beforeEach(() => {
  useWorkspace.setState({ tabs: [], activeId: null });
});

describe("workspace", () => {
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
});
