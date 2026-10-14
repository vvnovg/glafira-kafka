import { create } from "zustand";

export type TabSpec =
  | { kind: "cluster"; connectionId: string }
  | { kind: "topics"; connectionId: string }
  | { kind: "topic"; connectionId: string; topic: string }
  | { kind: "connection-form"; connectionId: string | null };

export type Tab = TabSpec & { id: string };

export function tabId(spec: TabSpec): string {
  switch (spec.kind) {
    case "topic":
      return `topic:${spec.connectionId}:${spec.topic}`;
    case "connection-form":
      return `connection-form:${spec.connectionId ?? "new"}`;
    default:
      return `${spec.kind}:${spec.connectionId}`;
  }
}

interface WorkspaceState {
  tabs: Tab[];
  activeId: string | null;
  open: (spec: TabSpec) => void;
  close: (id: string) => void;
  /** Закрывает все вкладки подключения (например, после его удаления). */
  closeForConnection: (connectionId: string) => void;
  activate: (id: string) => void;
}

export const useWorkspace = create<WorkspaceState>((set) => ({
  tabs: [],
  activeId: null,
  open: (spec) =>
    set((s) => {
      const id = tabId(spec);
      if (s.tabs.some((t) => t.id === id)) return { activeId: id };
      return { tabs: [...s.tabs, { ...spec, id } as Tab], activeId: id };
    }),
  close: (id) =>
    set((s) => {
      const idx = s.tabs.findIndex((t) => t.id === id);
      if (idx < 0) return s;
      const tabs = s.tabs.filter((t) => t.id !== id);
      const activeId = s.activeId !== id ? s.activeId : (tabs[idx] ?? tabs[idx - 1])?.id ?? null;
      return { tabs, activeId };
    }),
  closeForConnection: (connectionId) =>
    set((s) => {
      const firstIdx = s.tabs.findIndex((t) => t.connectionId === connectionId);
      if (firstIdx < 0) return s;
      const tabs = s.tabs.filter((t) => t.connectionId !== connectionId);
      const activeId = tabs.some((t) => t.id === s.activeId)
        ? s.activeId
        : (tabs[firstIdx] ?? tabs[tabs.length - 1])?.id ?? null;
      return { tabs, activeId };
    }),
  activate: (id) => set({ activeId: id }),
}));
