import { create } from "zustand";

import type { ProduceMessage, ProducerSettings } from "../api/types";

export type ProducerPrefill = { message: ProduceMessage; settings?: ProducerSettings; nonce: number };

export type ResetPrefill = { topic: string; partition: number; offset: number; nonce: number };

export type TabSpec =
  | { kind: "cluster"; connectionId: string }
  | { kind: "topics"; connectionId: string }
  | { kind: "topic"; connectionId: string; topic: string }
  | { kind: "groups"; connectionId: string }
  | { kind: "group"; connectionId: string; groupId: string; resetPrefill?: ResetPrefill }
  | { kind: "producer"; connectionId: string; prefill?: ProducerPrefill }
  | { kind: "consumer"; connectionId: string; key: string; topic?: string; groupId?: string }
  /** folderId — папка нового подключения («Новое подключение здесь»). */
  | { kind: "connection-form"; connectionId: string | null; folderId?: string | null }
  | { kind: "license" }
  | { kind: "about" }
  /** Справка: глава и раздел руководства (F1, ссылки). */
  | { kind: "help"; chapter?: string; anchor?: string }
  | { kind: "settings" };

export type Tab = TabSpec & { id: string };

export function tabId(spec: TabSpec): string {
  switch (spec.kind) {
    case "topic":
      return `topic:${spec.connectionId}:${spec.topic}`;
    case "group":
      return `group:${spec.connectionId}:${spec.groupId}`;
    case "consumer":
      return `consumer:${spec.connectionId}:${spec.key}`;
    case "connection-form":
      return `connection-form:${spec.connectionId ?? "new"}`;
    case "license":
      return "license";
    case "about":
      return "about";
    case "help":
      return "help";
    case "settings":
      return "settings";
    default:
      return `${spec.kind}:${spec.connectionId}`;
  }
}

/** Новая вкладка consumer-сессии (каждый вызов — отдельная сессия). */
export const newConsumerKey = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

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
      const tab = { ...spec, id } as Tab;
      // Уже открытая вкладка получает новый spec (например, предзаполненный сброс оффсетов).
      if (s.tabs.some((t) => t.id === id)) return { tabs: s.tabs.map((t) => (t.id === id ? tab : t)), activeId: id };
      return { tabs: [...s.tabs, tab], activeId: id };
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
      const firstIdx = s.tabs.findIndex((t) => ("connectionId" in t ? t.connectionId : null) === connectionId);
      if (firstIdx < 0) return s;
      const tabs = s.tabs.filter((t) => ("connectionId" in t ? t.connectionId : null) !== connectionId);
      const activeId = tabs.some((t) => t.id === s.activeId)
        ? s.activeId
        : (tabs[firstIdx] ?? tabs[tabs.length - 1])?.id ?? null;
      return { tabs, activeId };
    }),
  activate: (id) => set({ activeId: id }),
}));
