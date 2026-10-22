import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { ConnectionProfile, TopicDetails } from "../../api/types";
import { i18n } from "../../i18n";
import { tr } from "../../i18n/text";
import type { ProposalPrefill } from "../../state/workspace";
import { ConfirmDialog, needsConfirmation } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";
import { ProposalBanner } from "../health/ProposalBanner";
import { DeleteTopicsPanel } from "./DeleteTopicsPanel";

type Action = "partitions" | "purge" | "delete" | null;

interface Props {
  connection: ConnectionProfile;
  topic: TopicDetails;
  onDeleted: () => void;
  /** Предложение LLM добавить партиции (G3): диалог открывается с его числом. */
  proposal?: ProposalPrefill;
}

function useInvalidateTopic(connectionId: string, topic: string) {
  const queryClient = useQueryClient();
  return () => {
    void queryClient.invalidateQueries({ queryKey: ["topic", connectionId, topic] });
    void queryClient.invalidateQueries({ queryKey: ["topicConfigs", connectionId, topic] });
    void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
    void queryClient.invalidateQueries({ queryKey: ["topicStats", connectionId] });
    void queryClient.invalidateQueries({ queryKey: ["topicPolicies", connectionId] });
  };
}

/** Брокер принимает DeleteRecords, только если в cleanup.policy есть delete. */
export function purgeBlockReason(cleanup: string | null): string | null {
  if (cleanup === null || cleanup.trim() === "") return null;
  const parts = cleanup.split(",").map((x) => x.trim());
  if (parts.includes("delete")) return null;
  return i18n.t("topics.actions.purgeBlocked", { policy: cleanup });
}

export function TopicActions({ connection, topic, onDeleted, proposal }: Props) {
  const { t } = useTranslation();
  const [action, setAction] = useState<Action>(proposal ? "partitions" : null);
  // Предложение LLM добавить партиции (новый nonce) открывает диалог с его числом.
  useEffect(() => {
    if (proposal) setAction("partitions");
  }, [proposal]);
  const policy = useQuery({
    queryKey: ["topicPolicies", connection.id, [topic.name]],
    queryFn: () => api.getTopicPolicies(connection.id, [topic.name]),
  });
  const cleanup = policy.data?.[0]?.cleanupPolicy ?? null;
  const purgeBlock = purgeBlockReason(cleanup);
  return (
    <>
      <div className="toolbar">
        <button type="button" onClick={() => setAction("partitions")}>
          {t("topics.actions.addPartitions")}
        </button>
        <button
          type="button"
          className="danger"
          disabled={purgeBlock !== null}
          title={purgeBlock ?? undefined}
          onClick={() => setAction("purge")}
        >
          {t("topics.actions.purge")}
        </button>
        <button type="button" className="danger" onClick={() => setAction("delete")}>
          {t("topics.actions.deleteTopic")}
        </button>
      </div>
      {purgeBlock && <p className="warn">{purgeBlock}</p>}
      {action === "partitions" && (
        <>
          <ProposalBanner prefill={proposal} />
          <AddPartitions
            key={proposal?.nonce ?? 0}
            connection={connection}
            topic={topic}
            initialTotal={proposal?.proposal.change.kind === "addPartitions" ? proposal.proposal.change.newTotal : undefined}
            proposalId={proposal?.proposal.id ?? null}
            onClose={() => setAction(null)}
          />
        </>
      )}
      {action === "purge" && <Purge connection={connection} topic={topic} cleanup={cleanup} policyError={policy.error} onClose={() => setAction(null)} />}
      {action === "delete" && (
        <DeleteTopicsPanel
          connection={connection}
          topics={[topic.name]}
          onCancel={() => setAction(null)}
          onDone={(deleted) => {
            if (deleted.includes(topic.name)) onDeleted();
          }}
        />
      )}
    </>
  );
}

function AddPartitions({
  connection,
  topic,
  onClose,
  initialTotal,
  proposalId = null,
}: {
  connection: ConnectionProfile;
  topic: TopicDetails;
  onClose: () => void;
  /** Число из предложения LLM (G3). */
  initialTotal?: number;
  proposalId?: string | null;
}) {
  const { t } = useTranslation();
  const invalidate = useInvalidateTopic(connection.id, topic.name);
  const current = topic.partitions.length;
  const [value, setValue] = useState(String(initialTotal ?? current + 1));
  // Снимок цели на момент открытия подтверждения.
  const [confirming, setConfirming] = useState<number | null>(null);
  const n = Number(value);
  const valid = Number.isInteger(n) && n > current;
  const run = useMutation({
    mutationFn: (v: { total: number; confirm: string | null }) => api.addPartitions(connection.id, topic.name, v.total, v.confirm, proposalId),
    onSuccess: () => {
      invalidate();
      onClose();
    },
  });
  return (
    <div className="panel">
      <label>
        {t("topics.actions.newTotal", { current })}
        <input
          aria-label={t("topics.actions.newTotalAria")}
          type="number"
          min={current + 1}
          value={value}
          disabled={confirming !== null || run.isPending}
          onChange={(e) => setValue(e.target.value)}
        />
      </label>
      <p className="muted">
        {t("topics.actions.addHint")}
      </p>
      {run.error && <ErrorBanner error={run.error} />}
      {confirming !== null ? (
        <ConfirmDialog
          connection={connection}
          title={t("topics.actions.addTitle", { name: topic.name, total: confirming })}
          confirmLabel={t("topics.actions.addConfirm")}
          busy={run.isPending}
          onConfirm={(c) => run.mutate({ total: confirming, confirm: c.confirm })}
          onCancel={() => setConfirming(null)}
        />
      ) : (
        <div className="toolbar">
          <button
            type="button"
            className="primary"
            disabled={!valid || run.isPending}
            onClick={() => (needsConfirmation(connection) ? setConfirming(n) : run.mutate({ total: n, confirm: null }))}
          >
            {t("topics.actions.addSubmit")}
          </button>
          <button type="button" onClick={onClose}>
            {t("topics.actions.cancel")}
          </button>
        </div>
      )}
    </div>
  );
}

function Purge({
  connection,
  topic,
  cleanup,
  policyError,
  onClose,
}: {
  connection: ConnectionProfile;
  topic: TopicDetails;
  cleanup: string | null;
  policyError: unknown;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const invalidate = useInvalidateTopic(connection.id, topic.name);
  // Список партиций фиксируется при открытии: обновление топика не меняет цель под подтверждением.
  const [ids] = useState(() => topic.partitions.map((p) => p.id));
  const [selected, setSelected] = useState<Set<number>>(new Set(ids));
  const compact = cleanup?.includes("compact") ?? false;
  const run = useMutation({
    mutationFn: (v: { parts: number[]; c: Parameters<typeof api.purgeTopic>[3] }) =>
      api.purgeTopic(connection.id, topic.name, v.parts, v.c),
    onSettled: invalidate,
  });
  const toggle = (id: number) =>
    setSelected((s) => {
      const next = new Set(s);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });

  if (run.data) {
    return (
      <div className="panel">
        <ul>
          {run.data.map((p) => (
            <li key={p.partition} className={p.error ? "warn" : ""}>
              {p.error
                ? t("topics.actions.partitionError", { id: p.partition, error: tr(p.error) })
                : t("topics.actions.partitionOk", { id: p.partition, low: p.lowWatermark ?? "—" })}
            </li>
          ))}
        </ul>
        <button type="button" onClick={onClose}>
          {t("topics.actions.close")}
        </button>
      </div>
    );
  }

  const sortedSel = [...selected].sort((a, b) => a - b);
  const allSelected = selected.size === ids.length;
  return (
    <div className="panel">
      {compact && (
        <p className="warn">{t("topics.actions.compactWarn", { policy: cleanup })}</p>
      )}
      {policyError != null && (
        <p className="warn" title={tr(toAppError(policyError))}>
          {t("topics.actions.policyUnknown")}
        </p>
      )}
      <div className="toolbar">
        {ids.map((id) => (
          <label key={id}>
            <input type="checkbox" aria-label={t("topics.actions.partitionAria", { id })} checked={selected.has(id)} disabled={run.isPending} onChange={() => toggle(id)} /> {id}
          </label>
        ))}
      </div>
      {run.error && <ErrorBanner error={run.error} />}
      <ConfirmDialog
        key={sortedSel.join(",")}
        connection={connection}
        title={
          allSelected
            ? t("topics.actions.purgeAllTitle", { name: topic.name })
            : t("topics.actions.purgeSomeTitle", { ids: sortedSel.join(", "), name: topic.name })
        }
        objectName={topic.name}
        objectLabel={t("topics.actions.purgeObject")}
        confirmLabel={t("topics.actions.purgeConfirm")}
        danger
        busy={run.isPending || selected.size === 0}
        onConfirm={(c) => run.mutate({ parts: sortedSel, c })}
        onCancel={onClose}
      >
        <p className="muted">{t("topics.actions.purgeIrreversible")}</p>
      </ConfirmDialog>
    </div>
  );
}
