import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { ConnectionProfile, McpAuditEntry, McpServerState, McpStatus } from "../../api/types";
import { tr } from "../../i18n/text";
import { formatCount, formatTimestamp } from "../../lib/format";
import { MCP_STATUS_KEY, useMcpStatus } from "../../lib/useMcpStatus";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { auditTarget, browsePrefillFromAudit, isBrowsable } from "./auditPrefill";

const STATE_KEYS: Record<McpServerState, string> = {
  off: "settings.mcp.state.off",
  running: "settings.mcp.state.running",
  otherInstance: "settings.mcp.state.otherInstance",
  error: "settings.mcp.state.error",
  disabledByPolicy: "settings.mcp.state.disabledByPolicy",
};

const EVENT_KEYS: Record<string, string> = {
  sessionStarted: "settings.mcp.audit.sessionStarted",
  sessionEnded: "settings.mcp.audit.sessionEnded",
  approved: "settings.mcp.audit.approved",
  rejected: "settings.mcp.audit.rejected",
  revoked: "settings.mcp.audit.revoked",
  serverStarted: "settings.mcp.audit.serverStarted",
  serverStopped: "settings.mcp.audit.serverStopped",
};

const AUDIT_LIMIT = 200;
/** Как `glafira_mcp::rules::RESPONSE_LIMITS_KB`. */
const RESPONSE_LIMITS_KB = [100, 48, 24];

/**
 * Подробности вызова чтения по инструменту (G4): `returned` — сообщения у `read_messages`, корзины обзора у
 * `summarize_messages`, серии коммитов (подряд идущие коммиты одного оффсета) у `get_group_commit_history`.
 * Без цели — для усечённых аргументов.
 */
const DETAILS_KEYS: Record<string, { target: string; noTarget: string }> = {
  read_messages: { target: "settings.mcp.audit.scanDetails", noTarget: "settings.mcp.audit.scanDetailsNoTarget" },
  summarize_messages: { target: "settings.mcp.audit.scanDetailsSummary", noTarget: "settings.mcp.audit.scanDetailsSummaryNoTarget" },
  get_group_commit_history: { target: "settings.mcp.audit.scanDetailsHistory", noTarget: "settings.mcp.audit.scanDetailsHistoryNoTarget" },
};

function CopyBlock({ id, label, text }: { id: string; label: string; text: string }) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);
  const copy = () => {
    try {
      void navigator.clipboard
        .writeText(text)
        .then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 2000);
        })
        .catch(() => {});
    } catch {
      /* буфер обмена недоступен */
    }
  };
  return (
    <div className="mcp-config">
      <div className="toolbar">
        <strong id={id}>{label}</strong>
        <button type="button" aria-describedby={id} onClick={copy}>
          {copied ? t("settings.mcp.copied") : t("settings.mcp.copy")}
        </button>
      </div>
      <pre aria-labelledby={id}>{text}</pre>
    </div>
  );
}

function auditWhat(e: McpAuditEntry, t: (k: string) => string): string {
  if (e.event === "call") return e.tool ?? "";
  const key = EVENT_KEYS[e.event];
  return key ? t(key) : e.event;
}

/**
 * Почему «Открыть в „Сообщениях“» неактивна: ключ подсказки; "" — пока список подключений грузится, без подсказки;
 * null — кнопка активна.
 */
function openBlocked(
  canPrefill: boolean,
  e: McpAuditEntry,
  s: McpStatus,
  connections: ConnectionProfile[] | undefined,
  connectionsFailed: boolean,
): string | null {
  if (!canPrefill) return "settings.mcp.audit.openArgsCut";
  // Политика действует поверх уровня подключения: сохранённое «сообщения» работает как «Только метаданные».
  if (!s.messagesAllowed) return "settings.mcp.audit.openPolicy";
  if (!connections) return connectionsFailed ? "settings.mcp.audit.openConnectionsFailed" : "";
  const conn = connections.find((c) => c.id === e.connection?.id);
  if (!conn) return "settings.mcp.audit.openNoConnection";
  return conn.llmAccess === "messages" ? null : "settings.mcp.audit.openNotMessages";
}

/** Подробности вызова чтения сообщений (G4): цель, отдано (по шаблону инструмента), просмотрено, замаскировано. */
function AuditDetails({
  e,
  s,
  connections,
  connectionsFailed,
}: {
  e: McpAuditEntry;
  s: McpStatus;
  connections: ConnectionProfile[] | undefined;
  connectionsFailed: boolean;
}) {
  const { t } = useTranslation();
  const open = useWorkspace((st) => st.open);
  if (e.scanned === undefined) return null;
  const counts = { returned: formatCount(e.returned ?? 0), scanned: formatCount(e.scanned), masked: formatCount(e.masked ?? 0) };
  const keys = DETAILS_KEYS[e.tool ?? ""] ?? DETAILS_KEYS.read_messages;
  // У усечённых аргументов цели нет — строка без неё, а не «: 3 из 10».
  const target = auditTarget(e);
  const details = target ? t(keys.target, { target, ...counts }) : t(keys.noTarget, counts);
  if (!isBrowsable(e)) return <>{details}</>;
  const prefill = browsePrefillFromAudit(e);
  const blocked = openBlocked(prefill !== null, e, s, connections, connectionsFailed);
  return (
    <>
      {details}{" "}
      <button
        type="button"
        className="link"
        disabled={blocked !== null}
        title={blocked ? t(blocked) : undefined}
        onClick={() =>
          prefill &&
          open({ kind: "topic", connectionId: prefill.connectionId, topic: prefill.topic, browsePrefill: { ...prefill.prefill, nonce: Date.now() } })
        }
      >
        {t("settings.mcp.audit.openInBrowse")}
      </button>
    </>
  );
}

export function McpSection() {
  const { t } = useTranslation();
  const qc = useQueryClient();
  const status = useMcpStatus();
  const configs = useQuery({ queryKey: ["mcp-configs"], queryFn: api.getMcpClientConfigs, staleTime: Infinity });
  const audit = useQuery({ queryKey: ["mcp-audit"], queryFn: () => api.getMcpAudit(AUDIT_LIMIT) });
  const connections = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  const apply = (s: McpStatus) => qc.setQueryData(MCP_STATUS_KEY, s);
  const toggle = useMutation({ mutationFn: (enabled: boolean) => api.setMcpEnabled(enabled), onSuccess: apply });
  const refreshAudit = () => void qc.invalidateQueries({ queryKey: ["mcp-audit"] });
  const revoke = useMutation({
    mutationFn: (name: string) => api.revokeMcpClient(name),
    onSuccess: (s) => {
      apply(s);
      refreshAudit();
    },
  });
  const limit = useMutation({ mutationFn: (kb: number) => api.setMcpResponseLimit(kb), onSuccess: apply });
  const compact = useMutation({
    mutationFn: ({ name, on }: { name: string; on: boolean }) => api.setMcpClientCompact(name, on),
    onSuccess: apply,
  });
  const disconnect = useMutation({
    mutationFn: (sessionId: string | null) => api.disconnectMcpSessions(sessionId),
    onSuccess: (s) => {
      apply(s);
      refreshAudit();
    },
  });
  // Клиент протокола 2026-07-28 не шлёт initialize: без кнопки диалог появился бы только при первом вызове.
  const requestApproval = useMutation({
    mutationFn: (sessionId: string) => api.requestMcpApproval(sessionId),
    onSuccess: apply,
  });
  const reveal = useMutation({ mutationFn: () => api.revealMcpAudit() });

  if (status.error) return <ErrorBanner error={status.error} onRetry={() => status.refetch()} />;
  const s = status.data;
  if (!s) return <p className="empty">{t("common.loading")}</p>;
  const error =
    toggle.error ??
    limit.error ??
    revoke.error ??
    compact.error ??
    disconnect.error ??
    requestApproval.error ??
    reveal.error;

  return (
    <section aria-labelledby="mcp-title">
      <h3 id="mcp-title">{t("settings.mcp.section")}</h3>
      <p className="hint">{t("settings.mcp.intro")}</p>
      <div className="form">
        <span className="field-label">
          <label htmlFor="mcp-enabled">{t("settings.mcp.enable")}</label>
          <Hint k="hints.settings.mcpEnable" label={t("settings.mcp.enable")} />
        </span>
        <span>
          <input
            id="mcp-enabled"
            type="checkbox"
            checked={s.enabled && s.policyAllows}
            disabled={!s.policyAllows || toggle.isPending}
            onChange={(e) => toggle.mutate(e.target.checked)}
          />
        </span>
        <span className="field-label">
          <label htmlFor="mcp-response-limit">{t("settings.mcp.responseLimit")}</label>
          <Hint k="hints.settings.mcpResponseLimit" label={t("settings.mcp.responseLimit")} />
        </span>
        <span>
          <select
            id="mcp-response-limit"
            value={s.responseLimitKb}
            disabled={limit.isPending}
            onChange={(e) => limit.mutate(Number(e.target.value))}
          >
            {RESPONSE_LIMITS_KB.map((kb) => (
              <option key={kb} value={kb}>
                {t("settings.mcp.responseLimitKb", { kb })}
              </option>
            ))}
          </select>
        </span>
      </div>
      {!s.policyAllows && <p className="hint">{t("settings.mcp.policy")}</p>}
      <p role="status">
        {t(STATE_KEYS[s.state])}
        {s.error && <> — {tr(s.error)}</>}
      </p>
      {s.policyAllows && <p>{t("settings.mcp.openCounts", { n: s.allowedConnections, m: s.messageConnections })}</p>}
      {s.policyAllows && !s.messagesAllowed && <p className="hint">{t("settings.mcp.messagesPolicy")}</p>}
      {error && <ErrorBanner error={error} />}

      {s.policyAllows && configs.error && <ErrorBanner error={configs.error} onRetry={() => configs.refetch()} />}
      {s.policyAllows && configs.data && (
        <>
          <h4>{t("settings.mcp.connectTitle")}</h4>
          <p className="hint">{t("settings.mcp.connectHint")}</p>
          <CopyBlock id="mcp-cfg-desktop" label={t("settings.mcp.claudeDesktop")} text={configs.data.claudeDesktop} />
          <CopyBlock id="mcp-cfg-code" label={t("settings.mcp.claudeCode")} text={configs.data.claudeCode} />
          <CopyBlock id="mcp-cfg-other" label={t("settings.mcp.otherClient")} text={configs.data.otherClient} />
        </>
      )}

      <h4>{t("settings.mcp.sessions")}</h4>
      {s.sessions.length === 0 ? (
        <p className="muted">{t("settings.mcp.noSessions")}</p>
      ) : (
        <>
          <table>
            <thead>
              <tr>
                <th>{t("settings.mcp.colClient")}</th>
                <th>{t("settings.mcp.colSince")}</th>
                <th>{t("settings.mcp.colCalls")}</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {s.sessions.map((x) => (
                <tr key={x.id}>
                  <td>
                    {x.clientName ? `${x.clientName} ${x.clientVersion ?? ""}` : t("mcp.unknownClient")}
                    {!x.approved && <span className="muted"> · {t("settings.mcp.waiting")}</span>}
                  </td>
                  <td>{formatTimestamp(x.startedMs)}</td>
                  <td>{x.calls}</td>
                  <td>
                    <div className="toolbar">
                      {!x.approved && (
                        <button type="button" disabled={requestApproval.isPending} onClick={() => requestApproval.mutate(x.id)}>
                          {t("settings.mcp.approve")}
                        </button>
                      )}
                      <button type="button" disabled={disconnect.isPending} onClick={() => disconnect.mutate(x.id)}>
                        {t("settings.mcp.disconnect")}
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <button type="button" disabled={disconnect.isPending} onClick={() => disconnect.mutate(null)}>
            {t("settings.mcp.disconnectAll")}
          </button>
        </>
      )}

      <h4>{t("settings.mcp.clients")}</h4>
      {s.approvedClients.length === 0 ? (
        <p className="muted">{t("settings.mcp.noClients")}</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>{t("settings.mcp.colClient")}</th>
              <th>{t("settings.mcp.colFirstSeen")}</th>
              <th>{t("settings.mcp.colLastSeen")}</th>
              <th>
                <span className="field-label">
                  {t("settings.mcp.colCompact")}
                  <Hint k="hints.settings.mcpCompact" label={t("settings.mcp.colCompact")} />
                </span>
              </th>
              <th />
            </tr>
          </thead>
          <tbody>
            {s.approvedClients.map((c) => (
              <tr key={c.name}>
                <td>
                  {c.name} {c.version}
                </td>
                <td>{formatTimestamp(c.firstSeenMs)}</td>
                <td>{formatTimestamp(c.lastSeenMs)}</td>
                <td>
                  <input
                    type="checkbox"
                    aria-label={t("settings.mcp.compactFor", { name: c.name })}
                    checked={c.compactTools}
                    disabled={compact.isPending}
                    onChange={(e) => compact.mutate({ name: c.name, on: e.target.checked })}
                  />
                </td>
                <td>
                  <button type="button" disabled={revoke.isPending} onClick={() => revoke.mutate(c.name)}>
                    {t("settings.mcp.revoke")}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <div className="toolbar">
        <h4>{t("settings.mcp.audit.title")}</h4>
        <button type="button" onClick={() => audit.refetch()}>
          {t("settings.mcp.audit.refresh")}
        </button>
        <button type="button" onClick={() => reveal.mutate()}>
          {t("settings.mcp.audit.reveal")}
        </button>
      </div>
      {audit.error && <ErrorBanner error={audit.error} onRetry={() => audit.refetch()} />}
      {audit.data && audit.data.length === 0 && <p className="muted">{t("settings.mcp.audit.empty")}</p>}
      {audit.data && audit.data.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>{t("settings.mcp.audit.colTime")}</th>
              <th>{t("settings.mcp.colClient")}</th>
              <th>{t("settings.mcp.audit.colWhat")}</th>
              <th>{t("settings.mcp.audit.colConnection")}</th>
              <th>{t("settings.mcp.audit.colResult")}</th>
              <th>{t("settings.mcp.audit.colMs")}</th>
              <th>{t("settings.mcp.audit.colDetails")}</th>
            </tr>
          </thead>
          <tbody>
            {audit.data.map((e, i) => (
              <tr key={`${e.ts}-${i}`}>
                <td>{formatTimestamp(e.ts)}</td>
                <td>{e.client ?? ""}</td>
                <td>{auditWhat(e, t)}</td>
                <td>{e.connection?.name ?? ""}</td>
                <td>{e.outcome === "error" ? e.code ?? t("settings.mcp.audit.error") : e.outcome === "ok" ? t("settings.mcp.audit.ok") : ""}</td>
                <td>{e.ms ?? ""}</td>
                <td>
                  <AuditDetails e={e} s={s} connections={connections.data} connectionsFailed={connections.isError} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}
