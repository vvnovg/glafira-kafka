import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import { newConsumerKey, useWorkspace } from "../state/workspace";
import { PRO_HINT, useLicense } from "../lib/useLicense";
import { ProLock } from "./common/ProLock";
import { ErrorBanner } from "./common/ErrorBanner";
import { EnvBadge } from "./common/EnvBadge";

export function Sidebar() {
  const { data: connections = [], error, isSuccess } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const open = useWorkspace((s) => s.open);
  const { status, locked } = useLicense();

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <span>Подключения</span>
        <button type="button" title="Новое подключение" onClick={() => open({ kind: "connection-form", connectionId: null })}>
          +
        </button>
      </div>
      {error && <ErrorBanner error={error} />}
      {isSuccess && connections.length === 0 && (
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
              <EnvBadge environment={c.environment} readOnly={c.readOnly} />
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
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "groups", connectionId: c.id })}>
                  Consumer groups
                </button>
              </li>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "producer", connectionId: c.id })}>
                  Producer
                </button>
              </li>
              <li>
                <button
                  type="button"
                  className="link"
                  disabled={locked}
                  title={locked ? PRO_HINT : undefined}
                  onClick={() => open({ kind: "consumer", connectionId: c.id, key: newConsumerKey() })}
                >
                  Consumer-сессия
                  {locked && (
                    <>
                      {" "}
                      <ProLock />
                    </>
                  )}
                </button>
              </li>
            </ul>
          </li>
        ))}
      </ul>
      <div className="sidebar-footer">
        <button type="button" className="link" onClick={() => open({ kind: "license" })}>
          Лицензия: {status ? (status.tier === "pro" ? "Pro" : "Free") : "…"}
        </button>
        <button type="button" className="link" onClick={() => open({ kind: "about" })}>
          О программе
        </button>
      </div>
    </aside>
  );
}
