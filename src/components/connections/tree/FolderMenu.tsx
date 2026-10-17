import { type KeyboardEvent, useEffect, useRef } from "react";

export interface FolderMenuItem {
  label: string;
  onSelect: () => void;
}

/**
 * Выпадающее меню дерева (у папки и в заголовке панели): абсолютный `role="menu"`,
 * закрывается по Esc и кликом вне. Рендерится внутри якоря (`.tree-menu-anchor`) рядом с кнопкой,
 * которая его открывает. Фокус — на первом пункте, стрелки переходят между пунктами.
 */
export function FolderMenu({ label, items, onClose }: { label: string; items: FolderMenuItem[]; onClose: () => void }) {
  const ref = useRef<HTMLDivElement>(null);
  // Свежий onClose без переподписки слушателей при каждом рендере родителя.
  const closeRef = useRef(onClose);
  closeRef.current = onClose;

  useEffect(() => {
    ref.current?.querySelector<HTMLButtonElement>("[role=menuitem]")?.focus();
    const onDown = (e: MouseEvent) => {
      // Якорь (родитель меню вместе с кнопкой «⋯») — не «вне»: повторный клик по кнопке сам закроет меню.
      const anchor = ref.current?.parentElement ?? ref.current;
      if (anchor && !anchor.contains(e.target as Node)) closeRef.current();
    };
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (e.key === "Escape") closeRef.current();
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, []);

  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
    e.preventDefault();
    const list = [...(ref.current?.querySelectorAll<HTMLButtonElement>("[role=menuitem]") ?? [])];
    const idx = list.indexOf(document.activeElement as HTMLButtonElement);
    const next = e.key === "ArrowDown" ? (idx + 1) % list.length : (idx - 1 + list.length) % list.length;
    list[next]?.focus();
  };

  return (
    <div className="tree-menu" role="menu" aria-label={label} ref={ref} onKeyDown={onKeyDown}>
      {items.map((item) => (
        <button
          key={item.label}
          type="button"
          role="menuitem"
          onClick={() => {
            onClose();
            item.onSelect();
          }}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}
