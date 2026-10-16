import { useTranslation } from "react-i18next";
import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConnectionProfile, OffsetTarget, ResetPreviewRow } from "../../api/types";
import { tr } from "../../i18n/text";
import type { ResetPrefill } from "../../state/workspace";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { buildResetRequest, defaultResetForm, type ResetForm, targetsFromPreview } from "./reset";

const STRATEGIES = [
  { id: "earliest", labelKey: "groups.reset.strategyEarliest" },
  { id: "latest", labelKey: "groups.reset.strategyLatest" },
  { id: "timestamp", labelKey: "groups.reset.strategyTimestamp" },
  { id: "offset", labelKey: "groups.reset.strategyOffset" },
  { id: "shift", labelKey: "groups.reset.strategyShift" },
] as const satisfies readonly { id: ResetForm["strategy"]; labelKey: string }[];
const SCOPES = [
  { id: "all", labelKey: "groups.reset.scopeAll" },
  { id: "topics", labelKey: "groups.reset.scopeTopics" },
  { id: "partitions", labelKey: "groups.reset.scopePartitions" },
] as const satisfies readonly { id: ResetForm["scope"]; labelKey: string }[];

interface Props {
  connection: ConnectionProfile;
  groupId: string;
  prefill?: ResetPrefill;
  /** Почему применение сейчас невозможно (например, группа не пуста); null — можно. */
  blockedReason?: string | null;
  onApplied: () => void;
  onClose: () => void;
}

export function ResetOffsetsPanel({ connection, groupId, prefill, blockedReason = null, onApplied, onClose }: Props) {
  const { t } = useTranslation();
  const [form, setForm] = useState<ResetForm>(() => defaultResetForm(prefill));
  const [formError, setFormError] = useState<string | null>(null);
  const [confirming, setConfirming] = useState(false);
  const preview = useMutation({
    mutationFn: () => api.previewOffsetReset(connection.id, buildResetRequest(groupId, form)),
  });
  const queryClient = useQueryClient();
  const apply = useMutation({
    mutationFn: ({ targets, confirm }: { targets: OffsetTarget[]; confirm: string | null }) =>
      api.resetOffsets(connection.id, groupId, targets, confirm),
    onSuccess: (results) => {
      setConfirming(false);
      invalidateGroup();
      if (results.every((r) => !r.error)) onApplied();
    },
  });
  const invalidateGroup = () => {
    const c = connection.id;
    for (const key of [["group", c, groupId], ["groupOffsets", c, groupId], ["groups", c], ["groupDescriptions", c], ["groupsLag", c]]) {
      void queryClient.invalidateQueries({ queryKey: key });
    }
  };
  // Любое изменение формы делает предпросмотр недействительным: применяется только показанный план.
  const update = (patch: Partial<ResetForm>) => {
    setForm((f) => ({ ...f, ...patch }));
    preview.reset();
    apply.reset();
    setConfirming(false);
  };
  const runPreview = () => {
    try {
      buildResetRequest(groupId, form);
      setFormError(null);
      apply.reset();
      setConfirming(false);
      preview.mutate();
    } catch (e) {
      setFormError((e as Error).message);
    }
  };
  const rows: ResetPreviewRow[] = preview.data ?? [];
  const targets = targetsFromPreview(rows);

  return (
    <div className="panel">
      <h3>{t("groups.reset.title", { groupId })}</h3>
      <p className="muted">{t("groups.reset.onlyEmpty")}</p>
      <fieldset className="form" disabled={apply.isPending} style={{ border: 0, padding: 0 }}>
        <label htmlFor="rs-strategy">{t("groups.reset.strategy")}</label>
        <select id="rs-strategy" value={form.strategy} onChange={(e) => update({ strategy: e.target.value as ResetForm["strategy"], value: "" })}>
          {STRATEGIES.map((s) => (
            <option key={s.id} value={s.id}>
              {t(s.labelKey)}
            </option>
          ))}
        </select>
        {form.strategy === "timestamp" && (
          <>
            <label htmlFor="rs-value">{t("groups.reset.timestamp")}</label>
            <input id="rs-value" type="datetime-local" step={1} value={form.value} onChange={(e) => update({ value: e.target.value })} />
          </>
        )}
        {(form.strategy === "offset" || form.strategy === "shift") && (
          <>
            <label htmlFor="rs-value">{form.strategy === "offset" ? t("groups.reset.offset") : t("groups.reset.shift")}</label>
            <input id="rs-value" type="number" value={form.value} onChange={(e) => update({ value: e.target.value })} />
          </>
        )}
        <label htmlFor="rs-scope">{t("groups.reset.scope")}</label>
        <select id="rs-scope" value={form.scope} onChange={(e) => update({ scope: e.target.value as ResetForm["scope"] })}>
          {SCOPES.map((s) => (
            <option key={s.id} value={s.id}>
              {t(s.labelKey)}
            </option>
          ))}
        </select>
        {form.scope !== "all" && (
          <>
            <label htmlFor="rs-scope-text">{form.scope === "topics" ? t("groups.reset.scopeTopics") : t("groups.reset.partitionsLabel")}</label>
            <textarea id="rs-scope-text" rows={2} value={form.scopeText} onChange={(e) => update({ scopeText: e.target.value })} />
          </>
        )}
      </fieldset>
      {formError && <p className="warn">{formError}</p>}
      <div className="toolbar">
        <button type="button" className="primary" disabled={preview.isPending || apply.isPending} onClick={runPreview}>
          {t("groups.reset.preview")}
        </button>
        <button type="button" disabled={apply.isPending} onClick={onClose}>
          {t("common.close")}
        </button>
      </div>
      {preview.error && <ErrorBanner error={preview.error} />}
      {preview.data && (
        <>
          {rows.length === 0 && <p className="muted">{t("groups.reset.noPartitions")}</p>}
          <table>
            <thead>
              <tr>
                <th>{t("groups.reset.colTopic")}</th>
                <th>{t("groups.reset.colPartition")}</th>
                <th>{t("groups.reset.colCurrent")}</th>
                <th>{t("groups.reset.colNew")}</th>
                <th>Low / High</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={`${r.topic}:${r.partition}`} className={r.error || r.target === null ? "warn" : undefined}>
                  <td>{r.topic}</td>
                  <td>{r.partition}</td>
                  <td>{r.current ?? <span className="muted" title={t("groups.reset.noCommittedOffset")}>{t("groups.lag.noCommit")}</span>}</td>
                  <td title={r.target === null ? (r.error ? tr(r.error) : t("groups.reset.targetUnknown")) : undefined}>{r.target ?? "—"}</td>
                  <td className="muted">
                    <span title={r.low === null ? t("groups.reset.boundUnknown") : undefined}>{r.low ?? "—"}</span> /{" "}
                    <span title={r.high === null ? t("groups.reset.boundUnknown") : undefined}>{r.high ?? "—"}</span>
                  </td>
                  <td className="warn">{r.error ? tr(r.error) : r.target === null ? t("groups.reset.noTarget") : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {apply.error && <ErrorBanner error={apply.error} />}
          {apply.data && (
            <>
              <h4>{t("groups.reset.result")}</h4>
              <ul>
                {apply.data.map((r) => (
                  <li key={`${r.topic}:${r.partition}`} className={r.error ? "warn" : undefined}>
                    {r.topic}:{r.partition} — {r.error ? t("groups.reset.resultError", { error: tr(r.error) }) : t("groups.reset.resultSet", { offset: r.offset })}
                  </li>
                ))}
              </ul>
            </>
          )}
          {confirming ? (
            <ConfirmDialog
              connection={connection}
              title={t("groups.reset.confirmTitle", { count: targets.length, groupId })}
              confirmLabel={t("groups.details.resetOffsets")}
              busy={apply.isPending}
              onConfirm={(c) => apply.mutate({ targets, confirm: c.confirm })}
              onCancel={() => setConfirming(false)}
            />
          ) : (
            <>
              {blockedReason && <p className="warn">{blockedReason}</p>}
              <button
                type="button"
                className="primary"
                disabled={targets.length === 0 || apply.isPending || apply.isSuccess || blockedReason !== null}
                title={blockedReason ?? undefined}
                onClick={() => (needsConfirmation(connection) ? setConfirming(true) : apply.mutate({ targets, confirm: null }))}
              >
                {t("groups.reset.apply", { count: targets.length })}
              </button>
            </>
          )}
        </>
      )}
    </div>
  );
}
