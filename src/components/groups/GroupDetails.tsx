import { useTranslation } from "react-i18next";
import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { Confirmation, GroupDescription, GroupPartitionOffset } from "../../api/types";
import { i18n } from "../../i18n";
import { tr } from "../../i18n/text";
import { formatCount } from "../../lib/format";
import { useConnection } from "../../lib/useConnection";
import { newConsumerKey, type ProposalPrefill, type ResetPrefill, tabId, useWorkspace } from "../../state/workspace";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { HealthBlock } from "../health/HealthBlock";
import { ProposalBanner } from "../health/ProposalBanner";
import { useActivePrefill } from "../health/useActivePrefill";
import { groupTotalLag } from "./groupLag";
import { ResetOffsetsPanel } from "./ResetOffsetsPanel";

const REFRESH_OPTIONS = [
  { labelKey: "groups.details.refreshOff", value: 0 },
  { labelKey: "groups.details.refresh5", value: 5_000 },
  { labelKey: "groups.details.refresh10", value: 10_000 },
] as const;

type Panel = "reset" | "delete" | null;

interface Props {
  connectionId: string;
  groupId: string;
  resetPrefill?: ResetPrefill;
  /** Предложение LLM сбросить оффсеты этой группы (G3): панель открывается заполненной. */
  proposal?: ProposalPrefill;
}

/** Заполнение панели сброса из предложения LLM по этой группе. */
function resetFromProposal(proposal: ProposalPrefill | undefined, groupId: string): ResetPrefill | undefined {
  const c = proposal?.proposal.change;
  if (proposal && c?.kind === "resetOffsets" && c.groupId === groupId) {
    return { strategy: c.strategy, scope: c.scope, proposalId: proposal.proposal.id, nonce: proposal.nonce };
  }
  return undefined;
}

export function GroupDetails({ connectionId, groupId, resetPrefill: fromMessage, proposal }: Props) {
  const { t } = useTranslation();
  const [activeProposal, clearPrefill] = useActivePrefill(proposal);
  // Вкладка с предложением заполняет сброс только из него, «Сбросить группу сюда» из сообщения — без предложения.
  // Снятое (закрытое) предложение не подменяется заполнением из сообщения, иначе панель открылась бы снова.
  // useMemo держит объект стабильным: эффект ниже открывает панель только при новом заполнении.
  const hasProposal = proposal !== undefined;
  const resetPrefill = useMemo(
    () => (hasProposal ? resetFromProposal(activeProposal, groupId) : fromMessage),
    [hasProposal, activeProposal, groupId, fromMessage],
  );
  const forReset = resetPrefill && "proposalId" in resetPrefill ? activeProposal : undefined;
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
      if (!d) throw new Error(i18n.t("groups.details.noDescription"));
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
  // Колонка group.instance.id — только если static membership есть хотя бы у одного участника.
  const staticMembers = d?.members.some((m) => m.groupInstanceId) ?? false;
  // Сброс применим только к группе без участников; окончательная проверка — в бэкенде.
  const resetBlock =
    d?.state === "Empty" || d?.state === "Dead"
      ? null
      : t("groups.details.resetBlocked", { state: d?.state ?? t("groups.details.stateUnknown") });
  const rows = offsets.data ?? [];

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{groupId}</h2>
        {d && (
          <span className="muted">
            {t("groups.details.summary", { state: d.state, count: d.members.length, assignor: d.assignor || "—" })}
          </span>
        )}
        {d?.coordinator && (
          <span className="muted">
            {t("groups.details.coordinator", { id: d.coordinator.id, host: d.coordinator.host, port: d.coordinator.port, type: d.groupType ?? "—" })}
          </span>
        )}
        <select aria-label={t("groups.details.autoRefresh")} value={refreshMs} onChange={(e) => setRefreshMs(Number(e.target.value))}>
          {REFRESH_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {t(o.labelKey)}
            </option>
          ))}
        </select>
        <button type="button" disabled={desc.isFetching || offsets.isFetching} onClick={refresh}>
          {t("groups.details.refresh")}
        </button>
        <button
          type="button"
          disabled={readOnly || offsets.isPending}
          title={readOnly ? t("groups.details.joinReadOnly") : offsets.isPending ? t("groups.details.joinLoading") : undefined}
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
          {t("groups.details.join")}
        </button>
      </div>
      <HealthBlock connectionId={connectionId} match={(x) => x.kind === "group" && x.name === groupId} />
      {canMutate && (
        <div className="toolbar">
          <button type="button" disabled={resetBlock !== null} title={resetBlock ?? undefined} onClick={() => setPanel("reset")}>
            {t("groups.details.resetOffsets")}
          </button>
          <button
            type="button"
            className="danger"
            disabled={d?.state !== "Empty"}
            title={d?.state !== "Empty" ? t("groups.details.deleteOnlyEmpty") : undefined}
            onClick={() => {
              remove.reset();
              setPanel("delete");
            }}
          >
            {t("groups.details.deleteGroup")}
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
            title={t("groups.details.deleteTitle", { groupId })}
            objectName={groupId}
            objectLabel={t("groups.details.deleteObject")}
            confirmLabel={t("common.delete")}
            danger
            busy={remove.isPending}
            onConfirm={(c) => remove.mutate(c)}
            onCancel={() => {
              remove.reset();
              setPanel(null);
            }}
          >
            <p className="muted">{t("groups.details.deleteNote")}</p>
          </ConfirmDialog>
        </>
      )}
      {panel === "reset" && connection && canMutate && (
        <>
          <ProposalBanner prefill={forReset} />
          <ResetOffsetsPanel
            key={resetPrefill?.nonce ?? 0}
            connection={connection}
            groupId={groupId}
            prefill={resetPrefill}
            blockedReason={resetBlock}
            onApplied={refresh}
            onClose={() => {
              setPanel(null);
              clearPrefill();
            }}
          />
        </>
      )}

      <h3>{t("groups.details.partitions")}</h3>
      {offsets.error && <ErrorBanner error={offsets.error} onRetry={refresh} />}
      {offsets.data && (
        <p
          className="muted"
          title={
            groupTotalLag(rows) === null
              ? rows.every((r) => r.committed === null)
                ? t("groups.lag.noCommits")
                : t("groups.lag.unknown")
              : undefined
          }
        >
          {t("groups.details.totalLag", { value: formatCount(groupTotalLag(rows)) })}
        </p>
      )}
      <OffsetsTable rows={rows} />

      <h3>{t("groups.details.members")}</h3>
      {d && d.members.length === 0 && <p className="muted">{t("groups.details.noMembers")}</p>}
      {d && d.members.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Client ID</th>
              <th>{t("groups.details.colHost")}</th>
              {staticMembers && <th>{t("groups.details.colInstance")}</th>}
              <th>Member ID</th>
              <th>{t("groups.details.colAssignment")}</th>
            </tr>
          </thead>
          <tbody>
            {d.members.map((m) => (
              <tr key={m.memberId}>
                <td>{m.clientId}</td>
                <td>{m.host}</td>
                {staticMembers && <td>{m.groupInstanceId ?? ""}</td>}
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
  const { t } = useTranslation();
  return (
    <table>
      <thead>
        <tr>
          <th>{t("groups.details.colTopic")}</th>
          <th>{t("groups.details.colPartition")}</th>
          <th>Committed</th>
          <th>Log end</th>
          <th>{t("groups.details.colLag")}</th>
          <th>{t("groups.details.colMember")}</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => {
          const error = r.error ? tr(r.error) : undefined;
          return (
            <tr key={`${r.topic}:${r.partition}`}>
              <td>{r.topic}</td>
              <td>{r.partition}</td>
              <td className={error ? "warn" : ""} title={error}>
                {r.committed !== null ? r.committed : error ? "—" : <span className="muted">{t("groups.lag.noCommit")}</span>}
              </td>
              <td title={r.logEnd === null ? (error ?? t("groups.lag.logEndUnknown")) : undefined}>{r.logEnd ?? "—"}</td>
              <td title={r.lag === null ? (error ?? (r.committed === null ? t("groups.lag.noCommit") : t("groups.lag.logEndUnknown"))) : undefined}>
                {formatCount(r.lag)}
              </td>
              <td>{r.memberClientId ?? "—"}</td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}
