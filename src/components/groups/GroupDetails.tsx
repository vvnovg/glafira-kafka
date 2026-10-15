import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { Confirmation, GroupDescription, GroupPartitionOffset } from "../../api/types";
import { formatCount } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import { newConsumerKey, type ResetPrefill, tabId, useWorkspace } from "../../state/workspace";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { groupTotalLag } from "./groupLag";
import { ResetOffsetsPanel } from "./ResetOffsetsPanel";

const REFRESH_OPTIONS = [
  { label: "Автообновление: выкл.", value: 0 },
  { label: "каждые 5 с", value: 5_000 },
  { label: "каждые 10 с", value: 10_000 },
];

type Panel = "reset" | "delete" | null;

interface Props {
  connectionId: string;
  groupId: string;
  resetPrefill?: ResetPrefill;
}

export function GroupDetails({ connectionId, groupId, resetPrefill }: Props) {
  const connection = useConnection(connectionId);
  const canMutate = connection !== undefined && !connection.readOnly;
  const readOnly = connection?.readOnly === true;
  const [refreshMs, setRefreshMs] = useState(0);
  const [panel, setPanel] = useState<Panel>(resetPrefill && canMutate ? "reset" : null);
  const queryClient = useQueryClient();
  const closeTab = useWorkspace((s) => s.close);
  const open = useWorkspace((s) => s.open);

  // Новый предзаполненный сброс («Сбросить группу сюда») открывает панель сброса.
  useEffect(() => {
    if (resetPrefill && canMutate) setPanel("reset");
  }, [resetPrefill, canMutate]);

  const desc = useQuery({
    queryKey: ["group", connectionId, groupId],
    queryFn: async (): Promise<GroupDescription> => {
      const d = (await api.describeGroups(connectionId, [groupId]))[0];
      if (!d) throw new Error("Брокер не вернул описание группы");
      return d;
    },
    refetchInterval: refreshMs || false,
  });
  const offsets = useQuery({
    queryKey: ["groupOffsets", connectionId, groupId],
    queryFn: () => api.getGroupOffsets(connectionId, groupId),
    refetchInterval: refreshMs || false,
  });
  const remove = useMutation({
    mutationFn: (c: Confirmation) => api.deleteGroup(connectionId, groupId, c),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["groups", connectionId] });
      void queryClient.invalidateQueries({ queryKey: ["groupDescriptions", connectionId] });
      void queryClient.invalidateQueries({ queryKey: ["groupsLag", connectionId] });
      void queryClient.removeQueries({ queryKey: ["group", connectionId, groupId] });
      void queryClient.removeQueries({ queryKey: ["groupOffsets", connectionId, groupId] });
      closeTab(tabId({ kind: "group", connectionId, groupId }));
    },
  });
  const refresh = () => {
    void desc.refetch();
    void offsets.refetch();
  };
  const d = desc.data;
  // Сброс применим только к группе без участников; окончательная проверка — в бэкенде.
  const resetBlock =
    d?.state === "Empty" || d?.state === "Dead"
      ? null
      : `Сброс возможен только для группы без участников (сейчас: ${d?.state ?? "неизвестно"})`;
  const rows = offsets.data ?? [];

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{groupId}</h2>
        {d && (
          <span className="muted">
            {d.state} · участников: {d.members.length} · assignor: {d.assignor || "—"}
          </span>
        )}
        <select aria-label="Автообновление" value={refreshMs} onChange={(e) => setRefreshMs(Number(e.target.value))}>
          {REFRESH_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <button type="button" disabled={desc.isFetching || offsets.isFetching} onClick={refresh}>
          Обновить
        </button>
        <button
          type="button"
          disabled={readOnly || offsets.isPending}
          title={
            readOnly
              ? "Подключение только для чтения: вход в группу меняет её состояние"
              : offsets.isPending
                ? "Загружаются топики группы"
                : undefined
          }
          onClick={() =>
            open({
              kind: "consumer",
              connectionId,
              key: newConsumerKey(),
              groupId,
              // Топики — из закоммиченных партиций группы.
              topic: [...new Set(rows.map((r) => r.topic))].join(", "),
            })
          }
        >
          Читать как участник
        </button>
      </div>
      {canMutate && (
        <div className="toolbar">
          <button type="button" disabled={resetBlock !== null} title={resetBlock ?? undefined} onClick={() => setPanel("reset")}>
            Сбросить оффсеты
          </button>
          <button
            type="button"
            className="danger"
            disabled={d?.state !== "Empty"}
            title={d?.state !== "Empty" ? "Удалить можно только группу без участников (Empty)" : undefined}
            onClick={() => {
              remove.reset();
              setPanel("delete");
            }}
          >
            Удалить группу
          </button>
        </div>
      )}
      {desc.error && <ErrorBanner error={desc.error} onRetry={refresh} />}
      {d?.error && <p className="warn">{d.error}</p>}
      {panel === "delete" && connection && (
        <>
          {remove.error && <ErrorBanner error={remove.error} />}
          <ConfirmDialog
            connection={connection}
            title={`Удалить группу «${groupId}»?`}
            objectName={groupId}
            objectLabel="id группы"
            confirmLabel="Удалить"
            danger
            busy={remove.isPending}
            onConfirm={(c) => remove.mutate(c)}
            onCancel={() => {
              remove.reset();
              setPanel(null);
            }}
          >
            <p className="muted">Будут удалены все закоммиченные оффсеты группы.</p>
          </ConfirmDialog>
        </>
      )}
      {panel === "reset" && connection && canMutate && (
        <ResetOffsetsPanel
          key={resetPrefill?.nonce ?? 0}
          connection={connection}
          groupId={groupId}
          prefill={resetPrefill}
          blockedReason={resetBlock}
          onApplied={refresh}
          onClose={() => setPanel(null)}
        />
      )}

      <h3>Партиции</h3>
      {offsets.error && <ErrorBanner error={offsets.error} onRetry={refresh} />}
      {offsets.data && (
        <p
          className="muted"
          title={
            groupTotalLag(rows) === null
              ? rows.every((r) => r.committed === null)
                ? "нет коммитов"
                : "лаг неизвестен: ошибка оффсетов партиций"
              : undefined
          }
        >
          Суммарный лаг: {formatCount(groupTotalLag(rows))}
        </p>
      )}
      <OffsetsTable rows={rows} />

      <h3>Участники</h3>
      {d && d.members.length === 0 && <p className="muted">Нет активных участников.</p>}
      {d && d.members.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Client ID</th>
              <th>Хост</th>
              <th>Member ID</th>
              <th>Назначение</th>
            </tr>
          </thead>
          <tbody>
            {d.members.map((m) => (
              <tr key={m.memberId}>
                <td>{m.clientId}</td>
                <td>{m.host}</td>
                <td className="muted">{m.memberId}</td>
                <td>{m.assignment.map((a) => `${a.topic}:${a.partition}`).join(", ")}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

function OffsetsTable({ rows }: { rows: GroupPartitionOffset[] }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Топик</th>
          <th>Партиция</th>
          <th>Committed</th>
          <th>Log end</th>
          <th>Лаг</th>
          <th>Участник</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={`${r.topic}:${r.partition}`}>
            <td>{r.topic}</td>
            <td>{r.partition}</td>
            <td className={r.error ? "warn" : ""} title={r.error}>
              {r.committed !== null ? r.committed : r.error ? "—" : <span className="muted">нет коммита</span>}
            </td>
            <td title={r.logEnd === null ? (r.error ?? "log end неизвестен") : undefined}>{r.logEnd ?? "—"}</td>
            <td title={r.lag === null ? (r.error ?? (r.committed === null ? "нет коммита" : "log end неизвестен")) : undefined}>
              {formatCount(r.lag)}
            </td>
            <td>{r.memberClientId ?? "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
