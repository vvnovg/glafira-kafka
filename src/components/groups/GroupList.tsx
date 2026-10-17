import { useTranslation } from "react-i18next";
import { useMemo, useRef, useState } from "react";
import { useQueries, useQuery, useQueryClient } from "@tanstack/react-query";
import { useVirtualizer } from "@tanstack/react-virtual";
import { api, toAppError } from "../../api/client";
import type { GroupDescription, GroupLag, LocalizedText } from "../../api/types";
import { i18n } from "../../i18n";
import { tr } from "../../i18n/text";
import { activateProps } from "../../lib/activate";
import { formatCount } from "../../lib/format";
import { useDebouncedValue } from "../../lib/useDebouncedValue";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { visibleChunks } from "../topics/topicFilter";
import { filterGroups, GROUP_CHUNK, groupStates } from "./groupFilter";

const ROW_HEIGHT = 30;
const EMPTY: never[] = [];

export function GroupList({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const [query, setQuery] = useState("");
  const [state, setState] = useState("all");
  const debounced = useDebouncedValue(query, 250);
  const open = useWorkspace((s) => s.open);
  const queryClient = useQueryClient();
  const q = useQuery({ queryKey: ["groups", connectionId], queryFn: () => api.listGroups(connectionId) });
  const all = q.data?.groups ?? EMPTY;
  const rows = useMemo(() => filterGroups(all, debounced, state), [all, debounced, state]);
  const ids = useMemo(() => rows.map((r) => r.groupId), [rows]);

  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });
  const items = virtualizer.getVirtualItems();
  const chunks = visibleChunks(items[0]?.index ?? 0, items[items.length - 1]?.index ?? -1, GROUP_CHUNK);
  const slices = chunks.map((c) => ids.slice(c * GROUP_CHUNK, (c + 1) * GROUP_CHUNK));
  const details = useQueries({
    queries: slices.map((slice) => ({
      queryKey: ["groupDescriptions", connectionId, slice],
      queryFn: () => api.describeGroups(connectionId, slice),
      staleTime: 15_000,
    })),
  });
  const lags = useQueries({
    queries: slices.map((slice) => ({
      queryKey: ["groupsLag", connectionId, slice],
      queryFn: () => api.getGroupsLag(connectionId, slice),
      staleTime: 15_000,
    })),
  });
  // null — чанк не загрузился (ошибка), undefined — ещё грузится.
  const descById = new Map<string, GroupDescription | { error: string }>();
  const lagById = new Map<string, GroupLag | { error: string }>();
  slices.forEach((slice, i) => {
    const d = details[i];
    if (d?.isError && !d.data) for (const id of slice) descById.set(id, { error: tr(toAppError(d.error)) });
    d?.data?.forEach((x) => descById.set(x.groupId, x));
    if (d?.isSuccess) for (const id of slice) if (!descById.has(id)) descById.set(id, { error: i18n.t("groups.list.noData") });
    const l = lags[i];
    if (l?.isError && !l.data) for (const id of slice) lagById.set(id, { error: tr(toAppError(l.error)) });
    l?.data?.forEach((x) => lagById.set(x.groupId, x));
    if (l?.isSuccess) for (const id of slice) if (!lagById.has(id)) lagById.set(id, { error: i18n.t("groups.list.noData") });
  });

  if (q.isPending) return <p className="empty">{t("groups.list.loading")}</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{t("groups.list.title")}</h2>
        <input aria-label={t("groups.list.search")} placeholder={t("groups.list.search")} value={query} onChange={(e) => setQuery(e.target.value)} />
        <select aria-label={t("groups.list.state")} value={state} onChange={(e) => setState(e.target.value)}>
          <option value="all">{t("groups.list.allStates")}</option>
          {groupStates(all).map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <button
          type="button"
          disabled={q.isFetching}
          onClick={() => {
            void q.refetch();
            void queryClient.invalidateQueries({ queryKey: ["groupDescriptions", connectionId] });
            void queryClient.invalidateQueries({ queryKey: ["groupsLag", connectionId] });
          }}
        >
          {t("groups.list.refresh")}
        </button>
        <span className="muted">
          {t("groups.list.shownOf", { shown: rows.length, total: all.length })}
        </span>
      </div>
      {q.data.errors.length > 0 && (
        <p className="warn" role="alert" title={q.data.errors.map(tr).join("\n")}>
          {t("groups.list.incomplete", { count: q.data.errors.length })}
        </p>
      )}
      <div className="vlist" ref={parentRef}>
        <div className="row header group-row" style={{ height: ROW_HEIGHT }}>
          <span>{t("groups.list.colGroup")}</span>
          <span>{t("groups.list.colState")}</span>
          <span>{t("groups.list.colMembers")}</span>
          <span>Assignor</span>
          <span>
            {t("groups.list.colLag")}
            <Hint k="hints.groups.lag" label={t("groups.list.colLag")} />
          </span>
          <span>{t("groups.list.colTopics")}</span>
        </div>
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {items.map((item) => {
            const g = rows[item.index];
            if (!g) return null;
            const d = descById.get(g.groupId);
            const lag = lagById.get(g.groupId);
            const dErr = d?.error;
            const lErr = errorText(lag?.error);
            const full = d && "members" in d ? d : undefined;
            const fullLag = lag && "topics" in lag ? lag : undefined;
            return (
              <div
                key={g.groupId}
                className="row clickable group-row"
                style={{ top: item.start, height: ROW_HEIGHT }}
                {...activateProps(() => open({ kind: "group", connectionId, groupId: g.groupId }))}
              >
                <span title={g.groupId}>{g.groupId}</span>
                <span>{g.state}</span>
                <span title={dErr}>{!d ? "…" : dErr ? "—" : full?.members.length}</span>
                <span title={dErr}>{!d ? "…" : dErr ? "—" : full?.assignor || "—"}</span>
                <span title={lErr ?? lagHint(fullLag)}>{!lag ? "…" : lErr ? "—" : formatCount(fullLag?.totalLag ?? null)}</span>
                <span title={fullLag?.topics.join(", ")}>{fullLag?.topics.join(", ") ?? ""}</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

/** Подсказка для «—» в колонке лага без ошибки запроса. */
function lagHint(l: GroupLag | undefined): string | undefined {
  if (!l || l.totalLag !== null) return undefined;
  return l.topics.length === 0 ? i18n.t("groups.lag.noCommits") : i18n.t("groups.lag.unknown");
}

/** Ошибка лага: текст из Rust (`LocalizedText`) или уже готовая строка интерфейса. */
function errorText(e: string | LocalizedText | undefined): string | undefined {
  return e === undefined || typeof e === "string" ? e : tr(e);
}
