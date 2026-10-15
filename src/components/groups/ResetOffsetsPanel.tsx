import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConnectionProfile, OffsetTarget, ResetPreviewRow } from "../../api/types";
import type { ResetPrefill } from "../../state/workspace";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { buildResetRequest, defaultResetForm, type ResetForm, targetsFromPreview } from "./reset";

const STRATEGIES: { id: ResetForm["strategy"]; label: string }[] = [
  { id: "earliest", label: "С начала (earliest)" },
  { id: "latest", label: "С конца (latest)" },
  { id: "timestamp", label: "По времени" },
  { id: "offset", label: "Конкретный оффсет" },
  { id: "shift", label: "Сдвиг на ±N" },
];
const SCOPES: { id: ResetForm["scope"]; label: string }[] = [
  { id: "all", label: "Вся группа" },
  { id: "topics", label: "Топики" },
  { id: "partitions", label: "Партиции" },
];

interface Props {
  connection: ConnectionProfile;
  groupId: string;
  prefill?: ResetPrefill;
  onApplied: () => void;
  onClose: () => void;
}

export function ResetOffsetsPanel({ connection, groupId, prefill, onApplied, onClose }: Props) {
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
      <h3>Сброс оффсетов группы «{groupId}»</h3>
      <p className="muted">Сброс возможен только для группы без активных участников (Empty).</p>
      <fieldset className="form" disabled={apply.isPending} style={{ border: 0, padding: 0 }}>
        <label htmlFor="rs-strategy">Стратегия</label>
        <select id="rs-strategy" value={form.strategy} onChange={(e) => update({ strategy: e.target.value as ResetForm["strategy"], value: "" })}>
          {STRATEGIES.map((s) => (
            <option key={s.id} value={s.id}>
              {s.label}
            </option>
          ))}
        </select>
        {form.strategy === "timestamp" && (
          <>
            <label htmlFor="rs-value">Дата и время (локальное)</label>
            <input id="rs-value" type="datetime-local" step={1} value={form.value} onChange={(e) => update({ value: e.target.value })} />
          </>
        )}
        {(form.strategy === "offset" || form.strategy === "shift") && (
          <>
            <label htmlFor="rs-value">{form.strategy === "offset" ? "Оффсет" : "Сдвиг (±N)"}</label>
            <input id="rs-value" type="number" value={form.value} onChange={(e) => update({ value: e.target.value })} />
          </>
        )}
        <label htmlFor="rs-scope">Область</label>
        <select id="rs-scope" value={form.scope} onChange={(e) => update({ scope: e.target.value as ResetForm["scope"] })}>
          {SCOPES.map((s) => (
            <option key={s.id} value={s.id}>
              {s.label}
            </option>
          ))}
        </select>
        {form.scope !== "all" && (
          <>
            <label htmlFor="rs-scope-text">{form.scope === "topics" ? "Топики" : "Партиции (топик:партиция)"}</label>
            <textarea id="rs-scope-text" rows={2} value={form.scopeText} onChange={(e) => update({ scopeText: e.target.value })} />
          </>
        )}
      </fieldset>
      {formError && <p className="warn">{formError}</p>}
      <div className="toolbar">
        <button type="button" className="primary" disabled={preview.isPending || apply.isPending} onClick={runPreview}>
          Предпросмотр
        </button>
        <button type="button" disabled={apply.isPending} onClick={onClose}>
          Закрыть
        </button>
      </div>
      {preview.error && <ErrorBanner error={preview.error} />}
      {preview.data && (
        <>
          {rows.length === 0 && <p className="muted">Нет партиций для сброса</p>}
          <table>
            <thead>
              <tr>
                <th>Топик</th>
                <th>Партиция</th>
                <th>Текущий</th>
                <th>Новый</th>
                <th>Low / High</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={`${r.topic}:${r.partition}`} className={r.error || r.target === null ? "warn" : undefined}>
                  <td>{r.topic}</td>
                  <td>{r.partition}</td>
                  <td>{r.current ?? <span className="muted" title="нет закоммиченного оффсета">нет коммита</span>}</td>
                  <td title={r.target === null ? (r.error ?? "целевой оффсет не вычислен") : undefined}>{r.target ?? "—"}</td>
                  <td className="muted">
                    <span title={r.low === null ? "граница партиции неизвестна" : undefined}>{r.low ?? "—"}</span> /{" "}
                    <span title={r.high === null ? "граница партиции неизвестна" : undefined}>{r.high ?? "—"}</span>
                  </td>
                  <td className="warn">{r.error ?? (r.target === null ? "Нет целевого оффсета — не будет применено" : "")}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {apply.error && <ErrorBanner error={apply.error} />}
          {apply.data && (
            <>
              <h4>Результат применения</h4>
              <ul>
                {apply.data.map((r) => (
                  <li key={`${r.topic}:${r.partition}`} className={r.error ? "warn" : undefined}>
                    {r.topic}:{r.partition} — {r.error ? `ошибка: ${r.error}` : `оффсет ${r.offset} установлен`}
                  </li>
                ))}
              </ul>
            </>
          )}
          {confirming ? (
            <ConfirmDialog
              connection={connection}
              title={`Сбросить оффсеты ${targets.length} партиций группы «${groupId}»?`}
              confirmLabel="Сбросить оффсеты"
              busy={apply.isPending}
              onConfirm={(c) => apply.mutate({ targets, confirm: c.confirm })}
              onCancel={() => setConfirming(false)}
            />
          ) : (
            <button
              type="button"
              className="primary"
              disabled={targets.length === 0 || apply.isPending || apply.isSuccess}
              onClick={() => (needsConfirmation(connection) ? setConfirming(true) : apply.mutate({ targets, confirm: null }))}
            >
              Применить ({targets.length})
            </button>
          )}
        </>
      )}
    </div>
  );
}
