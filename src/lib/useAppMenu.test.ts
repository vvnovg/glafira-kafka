import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

type Opts = { id?: string; text?: string; accelerator?: string; action?: (id: string) => void; item?: unknown; items?: Item[] };
type Item = { kind: string; opts: Opts };

// Мок меню Tauri: записывает созданные пункты и последнее меню приложения.
const h = vi.hoisted(() => ({
  menus: [] as { items: Item[]; removed: string[]; closed: boolean }[],
  appMenu: null as null | { items: Item[] },
  helpForNSApp: [] as string[],
}));

vi.mock("@tauri-apps/api/menu", () => {
  class MenuItem {
    kind = "item";
    constructor(public opts: Opts) {}
    static async new(opts: Opts) {
      return new MenuItem(opts);
    }
  }
  class PredefinedMenuItem {
    kind = "predefined";
    constructor(public opts: Opts) {}
    static async new(opts: Opts) {
      return new PredefinedMenuItem(opts);
    }
  }
  class Submenu {
    kind = "submenu";
    constructor(public opts: Opts) {}
    static async new(opts: Opts) {
      return new Submenu(opts);
    }
    async setAsHelpMenuForNSApp() {
      h.helpForNSApp.push(this.opts.text ?? "");
    }
  }
  class Menu {
    state = {
      items: [new Submenu({ id: "__tauri_window_menu__", text: "Window" }), new Submenu({ id: "__tauri_help_menu__", text: "Help" })] as Item[],
      removed: [] as string[],
      closed: false,
    };
    static async default() {
      const m = new Menu();
      h.menus.push(m.state);
      return m;
    }
    async get(id: string) {
      return this.state.items.find((i) => i.opts.id === id) ?? null;
    }
    async remove(item: Item) {
      this.state.items = this.state.items.filter((i) => i !== item);
      this.state.removed.push(item.opts.id ?? "");
    }
    async append(item: Item) {
      this.state.items.push(item);
    }
    async setAsAppMenu() {
      h.appMenu = this.state;
      return null;
    }
    async close() {
      this.state.closed = true;
    }
  }
  return { Menu, Submenu, MenuItem, PredefinedMenuItem };
});

import { i18n } from "../i18n";
import { useDiagnosticsRequest } from "../state/diagnosticsRequest";
import { useWorkspace } from "../state/workspace";
import { useAppMenu } from "./useAppMenu";

const helpSubmenu = () => h.appMenu?.items.find((i) => i.kind === "submenu" && i.opts.id === "glafira-help");
const labels = () => helpSubmenu()?.opts.items?.map((i) => (i.kind === "predefined" ? "—" : i.opts.text));
const click = (id: string) => {
  const item = helpSubmenu()?.opts.items?.find((i) => i.opts.id === id);
  act(() => item?.opts.action?.(id));
};

const realUA = navigator.userAgent;
const setUA = (ua: string) => Object.defineProperty(window.navigator, "userAgent", { value: ua, configurable: true });

beforeEach(() => {
  h.menus = [];
  h.appMenu = null;
  h.helpForNSApp = [];
  useWorkspace.setState({ tabs: [], activeId: null });
  useDiagnosticsRequest.setState({ pending: false });
});

afterEach(async () => {
  setUA(realUA);
  await i18n.changeLanguage("ru");
});

describe("useAppMenu", () => {
  it("на macOS — меню приложения с подменю «Справка» на русском", async () => {
    setUA("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)");
    renderHook(() => useAppMenu());
    await waitFor(() => expect(helpSubmenu()).toBeDefined());
    expect(helpSubmenu()?.opts.text).toBe("Справка");
    expect(labels()).toEqual(["Руководство Glafira", "Решение проблем", "—", "О программе", "Собрать диагностику…"]);
    // Стандартное пустое Help из меню по умолчанию заменено нашим; остальные подменю на месте.
    expect(h.appMenu?.items.some((i) => i.opts.id === "__tauri_help_menu__")).toBe(false);
    expect(h.appMenu?.items.some((i) => i.opts.id === "__tauri_window_menu__")).toBe(true);
    expect(helpSubmenu()?.opts.items?.[0].opts.accelerator).toBe("CmdOrCtrl+Shift+/");
    expect(h.helpForNSApp).toEqual(["Справка"]);
  });

  it("после смены языка меню перестроено с английскими подписями", async () => {
    setUA("Macintosh");
    renderHook(() => useAppMenu());
    await waitFor(() => expect(helpSubmenu()?.opts.text).toBe("Справка"));
    await act(() => i18n.changeLanguage("en"));
    await waitFor(() => expect(helpSubmenu()?.opts.text).toBe("Help"));
    expect(labels()).toEqual(["Glafira Guide", "Troubleshooting", "—", "About Glafira", "Collect Diagnostics…"]);
    // Прежнее меню освобождено.
    expect(h.menus[0].closed).toBe(true);
  });

  it("пункты открывают справку, решение проблем, «О программе» и сбор диагностики", async () => {
    setUA("Macintosh");
    renderHook(() => useAppMenu());
    await waitFor(() => expect(helpSubmenu()).toBeDefined());

    click("guide");
    expect(useWorkspace.getState().tabs).toEqual([{ id: "help", kind: "help" }]);

    click("troubleshooting");
    expect(useWorkspace.getState().tabs.find((t) => t.id === "help")).toMatchObject({ kind: "help", chapter: "troubleshooting" });

    click("about");
    expect(useWorkspace.getState().activeId).toBe("about");
    expect(useDiagnosticsRequest.getState().pending).toBe(false);

    useWorkspace.setState({ tabs: [], activeId: null });
    click("diagnostics");
    expect(useWorkspace.getState().activeId).toBe("about");
    expect(useDiagnosticsRequest.getState().pending).toBe(true);
  });

  it("вне macOS меню не строится", async () => {
    setUA("Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
    renderHook(() => useAppMenu());
    await act(() => i18n.changeLanguage("en"));
    expect(h.menus).toEqual([]);
    expect(h.appMenu).toBeNull();
  });

  it("после размонтирования смена языка меню не трогает", async () => {
    setUA("Macintosh");
    const { unmount } = renderHook(() => useAppMenu());
    await waitFor(() => expect(helpSubmenu()).toBeDefined());
    unmount();
    const built = h.menus.length;
    await act(() => i18n.changeLanguage("en"));
    expect(h.menus.length).toBe(built);
  });
});
