import { act, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../../test/render";
import { newProfile } from "../profile";

vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return { ...orig, api: { ...orig.api, getMcpStatus: vi.fn(), getHealth: vi.fn() } };
});

import { api } from "../../../api/client";
import { useWorkspace } from "../../../state/workspace";
import { mcpStatus } from "../../../test/mcp";
import { ConnectionNode } from "./ConnectionNode";

const conn = { ...newProfile(), id: "c1", name: "Local" };

beforeEach(() => {
  vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
  vi.mocked(api.getHealth).mockResolvedValue({ lastCheck: null, ignored: [], annotations: [], proposals: [] });
  useWorkspace.setState({ tabs: [], activeId: null });
});

const current = () => screen.queryAllByRole("button", { current: "page" }).map((b) => b.textContent);

describe("ConnectionNode", () => {
  it("пункт дерева подсвечен для вкладки своего списка и для вкладки объекта из него", () => {
    renderWithClient(<ConnectionNode connection={conn} />);
    expect(current()).toEqual([]);

    act(() => useWorkspace.getState().open({ kind: "topics", connectionId: "c1" }));
    expect(current()).toEqual(["Топики"]);

    act(() => useWorkspace.getState().open({ kind: "topic", connectionId: "c1", topic: "orders" }));
    expect(current()).toEqual(["Топики"]);

    act(() => useWorkspace.getState().open({ kind: "group", connectionId: "c1", groupId: "order-service" }));
    expect(current()).toEqual(["Consumer groups"]);

    // Вкладка другого подключения узел не подсвечивает.
    act(() => useWorkspace.getState().open({ kind: "topics", connectionId: "c2" }));
    expect(current()).toEqual([]);
  });
});
