import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import type { ConnectionProfile } from "../api/types";
import { ENV_COLORS } from "../lib/env";
import { type Tab, useWorkspace } from "../state/workspace";
import { Sidebar } from "./Sidebar";
import { TabContent } from "./TabContent";

function tabTitle(tab: Tab, byId: Map<string, ConnectionProfile>): string {
  const conn = tab.connectionId ? byId.get(tab.connectionId)?.name ?? "?" : "";
  switch (tab.kind) {
    case "connection-form":
      return tab.connectionId ? `Подключение: ${conn}` : "Новое подключение";
    case "cluster":
      return `${conn} · Обзор`;
    case "topics":
      return `${conn} · Топики`;
    case "topic":
      return `${conn} · ${tab.topic}`;
  }
}

export function Layout() {
  const { tabs, activeId, activate, close } = useWorkspace();
  const { data: connections = [] } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const byId = new Map(connections.map((c) => [c.id, c]));
  const active = tabs.find((t) => t.id === activeId);
  const activeConn = active?.connectionId ? byId.get(active.connectionId) : undefined;

  return (
    <div className="layout">
      <Sidebar />
      <main className="workspace">
        <div className="env-stripe" style={{ background: activeConn ? ENV_COLORS[activeConn.environment] : "transparent" }} />
        <nav className="tabbar">
          {tabs.map((t) => (
            <div key={t.id} className={t.id === activeId ? "tab active" : "tab"} onClick={() => activate(t.id)}>
              <span>{tabTitle(t, byId)}</span>
              <button
                type="button"
                className="icon"
                aria-label="Закрыть вкладку"
                onClick={(e) => {
                  e.stopPropagation();
                  close(t.id);
                }}
              >
                ×
              </button>
            </div>
          ))}
        </nav>
        {tabs.length === 0 && <p className="empty">Выберите подключение слева или добавьте новое.</p>}
        {/* Неактивные вкладки скрыты, но не размонтированы — browse-сессии и состояние форм сохраняются. */}
        {tabs.map((t) => (
          <section key={t.id} className="tab-content" hidden={t.id !== activeId}>
            <TabContent tab={t} />
          </section>
        ))}
      </main>
    </div>
  );
}
