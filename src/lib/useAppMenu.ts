import { useEffect } from "react";
import { Menu, MenuItem, PredefinedMenuItem, Submenu } from "@tauri-apps/api/menu";
import { i18n } from "../i18n";
import { useDiagnosticsRequest } from "../state/diagnosticsRequest";
import { useWorkspace } from "../state/workspace";

/** id пустого подменю Help в меню Tauri по умолчанию (`tauri::menu::HELP_SUBMENU_ID`). */
const TAURI_HELP_ID = "__tauri_help_menu__";

/** Меню Help строится только на macOS: на Windows и Linux у окна нет строки меню. */
export function isMacOS(): boolean {
  return typeof navigator !== "undefined" && navigator.userAgent.includes("Mac");
}

/** Меню приложения: стандартные подменю Tauri + Help с пунктами Glafira (подписи — из словаря текущего языка). */
async function buildAppMenu(): Promise<Menu> {
  const t = i18n.t.bind(i18n);
  const { open } = useWorkspace.getState();
  const menu = await Menu.default();
  const stock = await menu.get(TAURI_HELP_ID);
  if (stock) await menu.remove(stock);
  const help = await Submenu.new({
    id: "glafira-help",
    text: t("menu.help"),
    items: [
      // ⌘? — на раскладке US это ⇧⌘/; символ «?» как клавишу Tauri не принимает.
      await MenuItem.new({ id: "guide", text: t("menu.guide"), accelerator: "CmdOrCtrl+Shift+/", action: () => open({ kind: "help" }) }),
      await MenuItem.new({
        id: "troubleshooting",
        text: t("menu.troubleshooting"),
        action: () => open({ kind: "help", chapter: "troubleshooting" }),
      }),
      await PredefinedMenuItem.new({ item: "Separator" }),
      await MenuItem.new({ id: "about", text: t("menu.about"), action: () => open({ kind: "about" }) }),
      await MenuItem.new({
        id: "diagnostics",
        text: t("menu.diagnostics"),
        action: () => {
          useDiagnosticsRequest.getState().request();
          open({ kind: "about" });
        },
      }),
    ],
  });
  await menu.append(help);
  await menu.setAsAppMenu();
  await help.setAsHelpMenuForNSApp();
  return menu;
}

/** Меню Help на macOS; перестраивается при смене языка интерфейса. */
export function useAppMenu(): void {
  useEffect(() => {
    if (!isMacOS()) return;
    let disposed = false;
    let current: Menu | null = null;
    // Сборки идут по очереди: при быстрой смене языка последним установится меню последнего языка.
    let queue: Promise<void> = Promise.resolve();
    const rebuild = () => {
      queue = queue.then(async () => {
        if (disposed) return;
        try {
          const next = await buildAppMenu();
          const previous = current;
          current = next;
          await previous?.close();
        } catch (e) {
          console.warn("app menu", e);
        }
      });
    };
    rebuild();
    i18n.on("languageChanged", rebuild);
    return () => {
      disposed = true;
      i18n.off("languageChanged", rebuild);
    };
  }, []);
}
