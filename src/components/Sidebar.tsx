import { useQuery } from "@tanstack/react-query";
import { Trans, useTranslation } from "react-i18next";
import { api } from "../api/client";
import { newConsumerKey, useWorkspace } from "../state/workspace";
import { proHint, useLicense } from "../lib/useLicense";
import { ProLock } from "./common/ProLock";
import { ErrorBanner } from "./common/ErrorBanner";
import { EnvBadge } from "./common/EnvBadge";

export function Sidebar() {
  const { t } = useTranslation();
  const { data: connections = [], error, isSuccess } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const open = useWorkspace((s) => s.open);
  const { status, locked } = useLicense();

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <span>{t("sidebar.connections")}</span>
        <button type="button" title={t("sidebar.newConnection")} onClick={() => open({ kind: "connection-form", connectionId: null })}>
          +
        </button>
      </div>
      {error && <ErrorBanner error={error} />}
      {isSuccess && connections.length === 0 && (
        <p className="empty">
          <Trans i18nKey="sidebar.emptyHint" components={{ code: <code /> }} />
        </p>
      )}
      <ul className="tree">
        {connections.map((c) => (
          <li key={c.id}>
            <div className="tree-conn">
              <span className="dot" style={{ background: c.color }} />
              <span className="tree-name">{c.name}</span>
              <EnvBadge environment={c.environment} readOnly={c.readOnly} />
              <button type="button" className="icon" title={t("sidebar.editConnection")} onClick={() => open({ kind: "connection-form", connectionId: c.id })}>
                ✎
              </button>
            </div>
            <ul>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "cluster", connectionId: c.id })}>
                  {t("sidebar.cluster")}
                </button>
              </li>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "topics", connectionId: c.id })}>
                  {t("sidebar.topics")}
                </button>
              </li>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "groups", connectionId: c.id })}>
                  {t("sidebar.groups")}
                </button>
              </li>
              <li>
                <button type="button" className="link" onClick={() => open({ kind: "producer", connectionId: c.id })}>
                  {t("sidebar.producer")}
                </button>
              </li>
              <li>
                <button
                  type="button"
                  className="link"
                  disabled={locked}
                  title={locked ? proHint() : undefined}
                  onClick={() => open({ kind: "consumer", connectionId: c.id, key: newConsumerKey() })}
                >
                  {t("sidebar.consumer")}
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
        <button type="button" className="link" onClick={() => open({ kind: "settings" })}>
          {t("sidebar.settings")}
        </button>
        <button type="button" className="link" onClick={() => open({ kind: "license" })}>
          {t("sidebar.license", { tier: status ? (status.tier === "pro" ? "Pro" : "Free") : "…" })}
        </button>
        <button type="button" className="link" onClick={() => open({ kind: "about" })}>
          {t("sidebar.about")}
        </button>
      </div>
    </aside>
  );
}
