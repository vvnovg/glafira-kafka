import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConfigChange, ConfigEntry, ConnectionProfile } from "../../api/types";
import { ConfirmDialog, needsConfirmation } from "./ConfirmDialog";
import { type ConfigEdit, type DiffRow, diffConfigs, isEditable } from "./configDiff";
import { ErrorBanner } from "./ErrorBanner";

interface Props {
  connection: ConnectionProfile;
  topic: string;
  entries: ConfigEntry[];
  onDone: () => void;
}

export function ConfigEditor({ connection, topic, entries, onDone }: Props) {
  const queryClient = useQueryClient();
  const [edits, setEdits] = useState<Record<string, ConfigEdit>>({});
  // Снимок изменений на момент открытия проверки: дальше он не зависит от живых правок/обновления entries.
  const [review, setReview] = useState<{ changes: ConfigChange[]; rows: DiffRow[] } | null>(null);
  const [query, setQuery] = useState("");
  const { changes } = diffConfigs(entries, edits);
  const apply = useMutation({
    mutationFn: (v: { changes: ConfigChange[]; confirm: string | null }) =>
      api.alterTopicConfigs(connection.id, topic, v.changes, v.confirm),
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
        <h3>Изменения конфигурации «{topic}»</h3>
        <ul>
          {review.rows.map((r) => (
            <li key={r.name}>
              <code>{r.name}</code>:{" "}
              <span>
                {r.before ?? "—"} → {r.after ?? "по умолчанию"}
              </span>
            </li>
          ))}
        </ul>
        {apply.error && <ErrorBanner error={apply.error} />}
        {needsConfirmation(connection) ? (
          <ConfirmDialog
            connection={connection}
            title="Применить изменения?"
            confirmLabel="Применить"
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
              Применить
            </button>
            <button type="button" disabled={apply.isPending} onClick={() => setReview(null)}>
              Назад
            </button>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="panel">
      <div className="toolbar">
        <input aria-label="Поиск параметра" placeholder="Поиск параметра" value={query} onChange={(e) => setQuery(e.target.value)} />
        <button type="button" className="primary" disabled={changes.length === 0} onClick={() => setReview(diffConfigs(entries, edits))}>
          Проверить изменения
        </button>
        <button type="button" disabled={Object.keys(edits).length === 0} onClick={() => setEdits({})}>
          Отменить правки
        </button>
        <span className="muted">изменений: {changes.length}</span>
      </div>
      <table>
        <thead>
          <tr>
            <th>Параметр</th>
            <th>Значение</th>
            <th>Источник</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {visible.map((e) => (
            <tr key={e.name}>
              <td style={{ fontWeight: e.isDefault ? 400 : 600 }}>{e.name}</td>
              <td>
                <input
                  aria-label={`Значение ${e.name}`}
                  disabled={!isEditable(e)}
                  value={e.isSensitive ? "••••••" : valueOf(e)}
                  placeholder={edits[e.name]?.kind === "reset" ? "по умолчанию" : undefined}
                  onChange={(ev) => setEdits((s) => ({ ...s, [e.name]: { kind: "set", value: ev.target.value } }))}
                />
              </td>
              <td className="muted">{e.source}</td>
              <td>
                {isEditable(e) && !e.isDefault && (
                  <button
                    type="button"
                    className="link"
                    aria-label={`Сбросить ${e.name}`}
                    onClick={() => setEdits((s) => ({ ...s, [e.name]: { kind: "reset" } }))}
                  >
                    к default
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
