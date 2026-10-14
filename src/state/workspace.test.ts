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
});
