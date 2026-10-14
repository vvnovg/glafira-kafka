import { useMemo, useRef, useState } from "react";
import { useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { api } from "../../api/client";
import { activateProps } from "../../lib/activate";
import { formatCount } from "../../lib/format";
import { useWorkspace } from "../../state/workspace";
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
        <span className="muted">
          {rows.length} из {q.data.length}
        </span>
      </div>
      <div className="vlist" ref={parentRef}>
        <div className="row header topic-row" style={{ height: ROW_HEIGHT }}>
          <span>Имя</span>
          <span>Партиции</span>
          <span>RF</span>
          <span>Сообщений (≈)</span>
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
                  open({ kind: "topic", connectionId, topic: t.name }),
                )}
              >
                <span title={t.name}>
                  {t.name}
                  {t.internal && <span className="muted"> (внутр.)</span>}
                </span>
                <span>{t.partitionCount}</span>
                <span>{t.replicationFactor}</span>
                <span>{formatCount(counts.get(t.name))}</span>
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
