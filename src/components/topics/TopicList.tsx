import { useMemo, useRef, useState } from "react";
import { useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { api } from "../../api/client";
import { activateProps } from "../../lib/activate";
import { formatCount } from "../../lib/format";
import { useWorkspace } from "../../state/workspace";
import { useConnection } from "../../lib/useConnection";
import { CreateTopicForm } from "./CreateTopicForm";
import { DeleteTopicsPanel } from "./DeleteTopicsPanel";
import { ErrorBanner } from "../common/ErrorBanner";
import { useDebouncedValue } from "../../lib/useDebouncedValue";
import {
  buildCounts,
  filterTopics,
  isInvalidRegexQuery,
  STATS_CHUNK,
  visibleChunks,
} from "./topicFilter";

const ROW_HEIGHT = 30;
const REFRESH_OPTIONS = [
  { label: "Автообновление: выкл.", value: 0 },
  { label: "каждые 10 с", value: 10_000 },
  { label: "каждые 30 с", value: 30_000 },
];

export function TopicList({ connectionId }: { connectionId: string }) {
  const [query, setQuery] = useState("");
  const [showInternal, setShowInternal] = useState(false);
  const [refreshMs, setRefreshMs] = useState(0);
  const debouncedQuery = useDebouncedValue(query, 250);
  const queryClient = useQueryClient();
  const open = useWorkspace((s) => s.open);
  const connection = useConnection(connectionId);
  const canMutate = connection !== undefined && !connection.readOnly;
  const [creating, setCreating] = useState(false);
  const [selecting, setSelecting] = useState(false);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [deleteTargets, setDeleteTargets] = useState<string[] | null>(null);
  const deleting = deleteTargets !== null;
  const toggle = (name: string) =>
    !deleting &&
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });

  const q = useQuery({
    queryKey: ["topics", connectionId],
    queryFn: () => api.listTopics(connectionId),
    refetchInterval: refreshMs || false,
  });
  const rows = useMemo(
    () => filterTopics(q.data ?? [], debouncedQuery, showInternal),
    [q.data, debouncedQuery, showInternal],
  );
  const names = useMemo(() => rows.map((r) => r.name), [rows]);
  // Внутренние топики не выбираются для удаления (backend их тоже отклоняет).
  const internalNames = useMemo(
    () => new Set((q.data ?? []).filter((t) => t.internal).map((t) => t.name)),
    [q.data],
  );

  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });
  const items = virtualizer.getVirtualItems();
  const chunks = visibleChunks(
    items[0]?.index ?? 0,
    items[items.length - 1]?.index ?? -1,
  );

  // Счётчики сообщений грузим лениво: только для чанков по 200 топиков, попавших в видимую область.
  const statsResults = useQueries({
    queries: chunks.map((c) => {
      const slice = names.slice(c * STATS_CHUNK, (c + 1) * STATS_CHUNK);
      return {
        queryKey: ["topicStats", connectionId, slice],
        queryFn: () => api.getTopicStats(connectionId, slice),
        staleTime: 30_000,
        refetchInterval: refreshMs || false,
      };
    }),
  });
  const counts = buildCounts(
    chunks.map((c, i) => ({
      names: names.slice(c * STATS_CHUNK, (c + 1) * STATS_CHUNK),
      data: statsResults[i]?.data,
      isError: statsResults[i]?.isError ?? false,
    })),
  );

  const policyResults = useQueries({
    queries: chunks.map((c) => {
      const slice = names.slice(c * STATS_CHUNK, (c + 1) * STATS_CHUNK);
      return {
        queryKey: ["topicPolicies", connectionId, slice],
        queryFn: () => api.getTopicPolicies(connectionId, slice),
        staleTime: 60_000,
      };
    }),
  });
  const policies = new Map<string, string | null>();
  const policyErrors = new Map<string, string>();
  policyResults.forEach((r, i) => {
    const slice = names.slice(chunks[i] * STATS_CHUNK, (chunks[i] + 1) * STATS_CHUNK);
    if (r.isError && !r.data)
      for (const n of slice) {
        policies.set(n, null);
        policyErrors.set(n, r.error instanceof Error ? r.error.message : String((r.error as { message?: string } | null)?.message ?? r.error));
      }
    for (const p of r.data ?? []) policies.set(p.name, p.cleanupPolicy);
  });

  if (q.isPending) return <p className="empty">Загрузка списка топиков…</p>;
  if (q.error)
    return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>Топики</h2>
        <input
          placeholder="Поиск (или /regex/)"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        {isInvalidRegexQuery(debouncedQuery) && (
          <span className="warn">Некорректное регулярное выражение</span>
        )}
        <label>
          <input
            type="checkbox"
            checked={showInternal}
            onChange={(e) => setShowInternal(e.target.checked)}
          />{" "}
          Внутренние
        </label>
        <select
          value={refreshMs}
          onChange={(e) => setRefreshMs(Number(e.target.value))}
        >
          {REFRESH_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <button
          type="button"
          disabled={q.isFetching}
          onClick={() => {
            void q.refetch();
            void queryClient.invalidateQueries({
              queryKey: ["topicStats", connectionId],
            });
          }}
        >
          Обновить
        </button>
        {canMutate && (
          <>
            <button type="button" onClick={() => setCreating(true)} disabled={creating || deleting}>
              Создать топик
            </button>
            <button
              type="button"
              aria-pressed={selecting}
              disabled={deleting}
              onClick={() => {
                setSelecting((v) => !v);
                setSelected(new Set());
              }}
            >
              {selecting ? "Отменить выбор" : "Выбрать"}
            </button>
            {selecting && (
              <button type="button" className="danger" disabled={selected.size === 0 || deleting || creating}
                onClick={() => setDeleteTargets([...selected].filter((n) => !internalNames.has(n)).sort())}>
                Удалить ({selected.size})
              </button>
            )}
          </>
        )}
        <span className="muted">
          {rows.length} из {q.data.length}
        </span>
      </div>
      {creating && connection && (
        <CreateTopicForm
          connection={connection}
          onCancel={() => setCreating(false)}
          onCreated={(name) => {
            setCreating(false);
            void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
            open({ kind: "topic", connectionId, topic: name });
          }}
        />
      )}
      {deleting && connection && (
        <DeleteTopicsPanel
          connection={connection}
          topics={deleteTargets}
          onCancel={() => setDeleteTargets(null)}
          onDone={(deleted) => {
            setSelected((s) => new Set([...s].filter((n) => !deleted.includes(n))));
            void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
          }}
        />
      )}
      <div className="vlist" ref={parentRef}>
        <div className="row header topic-row" style={{ height: ROW_HEIGHT }}>
          <span />
          <span>Имя</span>
          <span>Партиции</span>
          <span>RF</span>
          <span>Сообщений (≈)</span>
          <span>cleanup.policy</span>
          <span>Under-repl.</span>
        </div>
        <div
          style={{ height: virtualizer.getTotalSize(), position: "relative" }}
        >
          {items.map((item) => {
            const t = rows[item.index];
            return (
              <div
                key={t.name}
                className="row clickable topic-row"
                style={{ top: item.start, height: ROW_HEIGHT }}
                {...activateProps(() =>
                  selecting ? !t.internal && toggle(t.name) : open({ kind: "topic", connectionId, topic: t.name }),
                )}
              >
                <span>
                  {selecting && !t.internal && (
                    <input
                      type="checkbox"
                      aria-label={`Выбрать ${t.name}`}
                      checked={selected.has(t.name)}
                      disabled={deleting}
                      onClick={(e) => e.stopPropagation()}
                      onKeyDown={(e) => e.stopPropagation()}
                      onChange={() => toggle(t.name)}
                    />
                  )}
                </span>
                <span title={t.name}>
                  {t.name}
                  {t.internal && <span className="muted"> (внутр.)</span>}
                </span>
                <span>{t.partitionCount}</span>
                <span>{t.replicationFactor}</span>
                <span>{formatCount(counts.get(t.name))}</span>
                <span title={policies.has(t.name) && policies.get(t.name) == null ? `Не удалось загрузить cleanup.policy: ${policyErrors.get(t.name) ?? "причина неизвестна"}` : undefined}>
                  {policies.has(t.name) ? (policies.get(t.name) ?? "—") : "…"}
                </span>
                <span className={t.underReplicated > 0 ? "warn" : ""}>
                  {t.underReplicated}
                </span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
