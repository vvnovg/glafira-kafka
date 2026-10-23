import { useTranslation } from "react-i18next";
import { useEffect, useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConfigChange, ConfigEntry, ConnectionProfile } from "../../api/types";
import { ConfirmDialog, needsConfirmation } from "./ConfirmDialog";
import { type ConfigEdit, type DiffRow, diffConfigs, isEditable } from "./configDiff";
import { ErrorBanner } from "./ErrorBanner";
import { Hint } from "./Hint";

interface Props {
  connection: ConnectionProfile;
  topic: string;
  entries: ConfigEntry[];
  onDone: () => void;
  /** Есть несохранённые правки (для блокировки закрытия редактора). */
  onDirtyChange?: (dirty: boolean) => void;
  /** Подсказки к параметрам: имя → ключ словаря; без него значков нет. */
  hints?: Record<string, string>;
  /** Начальные правки из предложения LLM (G3): редактор открывается уже заполненным. */
  initialEdits?: Record<string, ConfigEdit>;
  /** Предложение LLM, из которого открыт редактор: Rust отметит его применённым после успеха. */
  proposalId?: string | null;
}

export function ConfigEditor({ connection, topic, entries, onDone, onDirtyChange, hints, initialEdits, proposalId = null }: Props) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const [edits, setEdits] = useState<Record<string, ConfigEdit>>(() => initialEdits ?? {});
  // Снимок изменений на момент открытия проверки: дальше он не зависит от живых правок/обновления entries.
  const [review, setReview] = useState<{ changes: ConfigChange[]; rows: DiffRow[] } | null>(null);
  const [query, setQuery] = useState("");
  const { changes, invalid } = diffConfigs(entries, edits);
  const invalidSet = new Set(invalid);
  const apply = useMutation({
    mutationFn: (v: { changes: ConfigChange[]; confirm: string | null }) =>
      api.alterTopicConfigs(connection.id, topic, v.changes, v.confirm, proposalId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["topicConfigs", connection.id, topic] });
      void queryClient.invalidateQueries({ queryKey: ["topicPolicies", connection.id] });
      void queryClient.invalidateQueries({ queryKey: ["topicStats", connection.id] });
      void queryClient.invalidateQueries({ queryKey: ["topic", connection.id, topic] });
      setEdits({});
      setReview(null);
      onDone();
    },
  });
  const dirty = Object.keys(edits).length > 0 || review !== null;
  useEffect(() => {
    onDirtyChange?.(dirty);
    return () => onDirtyChange?.(false);
  }, [dirty, onDirtyChange]);
  const q = query.trim().toLowerCase();
  const visible = entries.filter((e) => !q || e.name.toLowerCase().includes(q));
  const valueOf = (e: ConfigEntry) => {
    const edit = edits[e.name];
    if (!edit) return e.value ?? "";
    return edit.kind === "set" ? edit.value : "";
  };

  if (review) {
    return (
      <div className="panel">
        <h3>{t("common.config.reviewTitle", { topic })}</h3>
        <ul>
          {review.rows.map((r) => (
            <li key={r.name}>
              <code>{r.name}</code>:{" "}
              <span>
                {r.before ?? "—"} → {r.after ?? t("common.config.defaultValue")}
              </span>
            </li>
          ))}
        </ul>
        {apply.error && <ErrorBanner error={apply.error} />}
        {needsConfirmation(connection) ? (
          <ConfirmDialog
            connection={connection}
            title={t("common.config.applyTitle")}
            confirmLabel={t("common.config.apply")}
            busy={apply.isPending}
            onConfirm={(c) => apply.mutate({ changes: review.changes, confirm: c.confirm })}
            onCancel={() => setReview(null)}
          />
        ) : (
          <div className="toolbar">
            <button
              type="button"
              className="primary"
              disabled={apply.isPending}
              onClick={() => apply.mutate({ changes: review.changes, confirm: null })}
            >
              {t("common.config.apply")}
            </button>
            <button type="button" disabled={apply.isPending} onClick={() => setReview(null)}>
              {t("common.config.back")}
            </button>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="toolbar">
        <input aria-label={t("common.config.searchParam")} placeholder={t("common.config.searchParam")} value={query} onChange={(e) => setQuery(e.target.value)} />
        <button type="button" className="primary" disabled={changes.length === 0 || invalid.length > 0} onClick={() => {
          const { changes, rows } = diffConfigs(entries, edits);
          setReview({ changes, rows });
        }}>
          {t("common.config.review")}
        </button>
        <Hint k="hints.topics.config" label={t("common.config.review")} />
        <button type="button" disabled={Object.keys(edits).length === 0} onClick={() => setEdits({})}>
          {t("common.config.discard")}
        </button>
        <span className="muted">{t("common.config.changesCount", { count: changes.length })}</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>{t("common.config.colParam")}</th>
            <th>{t("common.config.colValue")}</th>
            <th>{t("common.config.colSource")}</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {visible.map((e) => (
            // Строка с правкой подсвечена, чтобы изменения (в т. ч. из предложения LLM) были видны без поиска.
            <tr key={e.name} className={edits[e.name] ? "edited" : undefined}>
              <td style={{ fontWeight: e.isDefault ? 400 : 600 }}>
                {e.name}
                {hints?.[e.name] && <Hint k={hints[e.name]} label={e.name} />}
              </td>
              <td>
                <input
                  aria-label={t("common.config.valueAria", { name: e.name })}
                  disabled={!isEditable(e)}
                  value={e.isSensitive ? "••••••" : valueOf(e)}
                  placeholder={edits[e.name]?.kind === "reset" ? t("common.config.defaultValue") : undefined}
                  aria-invalid={invalidSet.has(e.name) || undefined}
                  className={edits[e.name] ? "edited" : undefined}
                  onChange={(ev) => setEdits((s) => ({ ...s, [e.name]: { kind: "set", value: ev.target.value } }))}
                />
                {invalidSet.has(e.name) && <div className="warn">{t("common.config.emptyValue")}</div>}
              </td>
              <td className="muted">{e.source}</td>
              <td>
                {isEditable(e) && !e.isDefault && (
                  <button
                    type="button"
                    className="link"
                    aria-label={t("common.config.resetAria", { name: e.name })}
                    onClick={() => setEdits((s) => ({ ...s, [e.name]: { kind: "reset" } }))}
                  >
                    {t("common.config.reset")}
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
