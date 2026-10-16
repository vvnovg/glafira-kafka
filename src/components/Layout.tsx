import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import type { ConnectionProfile } from "../api/types";
import { ENV_COLORS } from "../lib/env";
import { type Tab, useWorkspace } from "../state/workspace";
import { EnvBadge } from "./common/EnvBadge";
import { LicenseNotice } from "./license/LicenseNotice";
import { UpdateNotice } from "./about/UpdateNotice";
import { Sidebar } from "./Sidebar";
import { TabContent } from "./TabContent";

function tabTitle(tab: Tab, byId: Map<string, ConnectionProfile>): string {
  const connId = "connectionId" in tab ? tab.connectionId : null;
  const conn = connId ? byId.get(connId)?.name ?? "?" : "";
  switch (tab.kind) {
    case "license":
      return "Лицензия";
    case "about":
      return "О программе";
    case "connection-form":
      return tab.connectionId ? `Подключение: ${conn}` : "Новое подключение";
    case "cluster":
      return `${conn} · Обзор`;
    case "topics":
      return `${conn} · Топики`;
    case "topic":
      return `${conn} · ${tab.topic}`;
    case "groups":
      return `${conn} · Группы`;
    case "group":
      return `${conn} · ${tab.groupId}`;
    case "producer":
      return `${conn} · Producer`;
    case "consumer":
      return `${conn} · Сессия ${tab.groupId ?? tab.topic ?? ""}`.trim();
  }
}

export function Layout() {
  const { tabs, activeId, activate, close } = useWorkspace();
  const { data: connections = [] } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const byId = new Map(connections.map((c) => [c.id, c]));
  const active = tabs.find((t) => t.id === activeId);
  const activeConnId = active && "connectionId" in active ? active.connectionId : null;
  const activeConn = activeConnId ? byId.get(activeConnId) : undefined;

  return (
    <div className="layout">
      <Sidebar />
      <main className="workspace">
        <div className="env-stripe" style={{ background: activeConn ? ENV_COLORS[activeConn.environment] : "transparent" }} />
        <LicenseNotice />
        <UpdateNotice />
        <nav className="tabbar" role="tablist">
          {tabs.map((t) => {
            const tConnId = "connectionId" in t ? t.connectionId : null;
            const conn = tConnId ? byId.get(tConnId) : undefined;
            const isActive = t.id === activeId;
            return (
              <div key={t.id} className={isActive ? "tab active" : "tab"}>
                {conn && <EnvBadge environment={conn.environment} readOnly={conn.readOnly} />}
                <button type="button" role="tab" aria-selected={isActive} onClick={() => activate(t.id)}>
                  {tabTitle(t, byId)}
                </button>
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
            );
          })}
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
