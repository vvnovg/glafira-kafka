import { act, fireEvent, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionProfile } from "../api/types";
import { useHelpContext } from "../state/helpContext";
import { useWorkspace } from "../state/workspace";
import { renderWithClient } from "../test/render";
import { Layout } from "./Layout";

vi.mock("../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api/client")>();
  const conn = {
    id: "c1",
    name: "prod-1",
    color: "#888",
    environment: "prod",
    readOnly: true,
    bootstrapServers: "localhost:9092",
    security: { type: "plaintext" },
    extraProperties: {},
    requestTimeoutMs: 5000,
  } as unknown as ConnectionProfile;
  return { ...actual, api: { ...actual.api, listConnections: vi.fn().mockResolvedValue([conn]), listFolders: vi.fn().mockResolvedValue([]) } };
});

vi.mock("./TabContent", () => ({ TabContent: () => <div /> }));

describe("Layout tabs", () => {
  beforeEach(() => {
    useWorkspace.setState({
      tabs: [
        { kind: "topics", connectionId: "c1", id: "topics:c1" },
        { kind: "cluster", connectionId: "c1", id: "cluster:c1" },
      ],
      activeId: "topics:c1",
    });
  });

  it("shows env badge and lock in the tab, with accessible tab roles", async () => {
    renderWithClient(<Layout />);
    const list = screen.getByRole("tablist");
    const tabs = within(list).getAllByRole("tab");
    expect(tabs[0].getAttribute("aria-selected")).toBe("true");
    expect(tabs[1].getAttribute("aria-selected")).toBe("false");
    expect((await within(list).findAllByText("PROD")).length).toBe(2);
    expect(within(list).getAllByRole("img", { name: "Только чтение" }).length).toBe(2);
  });

  it("activates a tab via its button", () => {
    renderWithClient(<Layout />);
    const tabs = within(screen.getByRole("tablist")).getAllByRole("tab");
    fireEvent.click(tabs[1]);
    expect(tabs[1].getAttribute("aria-selected")).toBe("true");
    expect(tabs[0].getAttribute("aria-selected")).toBe("false");
  });

  it("F1 opens the guide chapter of the active screen", () => {
    useWorkspace.setState({
      tabs: [{ kind: "producer", connectionId: "c1", id: "producer:c1" }],
      activeId: "producer:c1",
    });
    renderWithClient(<Layout />);
    const ev = new KeyboardEvent("keydown", { key: "F1", bubbles: true, cancelable: true });
    act(() => {
      window.dispatchEvent(ev);
    });
    expect(ev.defaultPrevented).toBe(true);
    const { tabs, activeId } = useWorkspace.getState();
    expect(activeId).toBe("help");
    expect(tabs.find((t) => t.id === "help")).toMatchObject({ kind: "help", chapter: "producer" });
    expect(within(screen.getByRole("tablist")).getByRole("tab", { name: "Справка" })).toBeInTheDocument();
  });

  it("F1 prefers the chapter the active screen reports", () => {
    useWorkspace.setState({
      tabs: [{ kind: "topic", connectionId: "c1", topic: "orders", id: "topic:c1:orders" }],
      activeId: "topic:c1:orders",
    });
    useHelpContext.setState({ byTab: { "topic:c1:orders": "browse" } });
    renderWithClient(<Layout />);
    fireEvent.keyDown(window, { key: "F1" });
    expect(useWorkspace.getState().tabs.find((t) => t.id === "help")).toMatchObject({ chapter: "browse" });
    useHelpContext.setState({ byTab: {} });
  });

  it("opens the guide from the sidebar", () => {
    renderWithClient(<Layout />);
    fireEvent.click(screen.getByRole("button", { name: "Справка" }));
    expect(useWorkspace.getState().activeId).toBe("help");
  });
});
