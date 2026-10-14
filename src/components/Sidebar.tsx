import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { ENV_COLORS, ENV_LABELS } from "../lib/env";
import { useWorkspace } from "../state/workspace";
import { ErrorBanner } from "./common/ErrorBanner";

export function Sidebar() {
  const { data: connections = [], error } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const open = useWorkspace((s) => s.open);

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <span>Подключения</span>
        <button type="button" title="Новое подключение" onClick={() => open({ kind: "connection-form", connectionId: null })}>
          +
        </button>
      </div>
      {error && <ErrorBanner error={error} />}
      {connections.length === 0 && !error && (
        <p className="empty">
          Добавьте первое подключение. Для локальной Kafka: <code>docker compose -f dev/docker-compose.yml up -d</code> и
          bootstrap <code>localhost:9092</code>.
        </p>
      )}
      <ul className="tree">
        {connections.map((c) => (
          <li key={c.id}>
            <div className="tree-conn">
              <span className="dot" style={{ background: c.color }} />
              <span className="tree-name">{c.name}</span>
              <span className="env-badge" style={{ background: ENV_COLORS[c.environment] }}>
                {ENV_LABELS[c.environment]}
              </span>
              {c.readOnly && <span title="Только чтение">🔒</span>}
              <button type="button" className="icon" title="Изменить подключение" onClick={() => open({ kind: "connection-form", connectionId: c.id })}>
                ✎
              </button>
            </div>
            <ul>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "cluster", connectionId: c.id })}>
                  Обзор кластера
                </button>
              </li>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "topics", connectionId: c.id })}>
                  Топики
                </button>
              </li>
            </ul>
          </li>
        ))}
      </ul>
    </aside>
  );
}
