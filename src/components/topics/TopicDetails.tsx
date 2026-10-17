import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { TopicDetails as TopicDetailsDto } from "../../api/types";
import { tr } from "../../i18n/text";
import { formatCount } from "../../lib/format";
import { proHint, useLicense } from "../../lib/useLicense";
import { useConnection } from "../../lib/useConnection";
import { newConsumerKey, tabId, useWorkspace } from "../../state/workspace";
import { BrowsePanel } from "../browse/BrowsePanel";
import { ProLock } from "../common/ProLock";
import { ConfigEditor } from "../common/ConfigEditor";
import { ConfigTable } from "../common/ConfigTable";
import { TOPIC_CONFIG_HINTS } from "../common/topicConfigHints";
import { ErrorBanner } from "../common/ErrorBanner";
import { TopicActions } from "./TopicActions";
import { TopicGroups } from "./TopicGroups";

type SubTab = "messages" | "partitions" | "configs" | "groups";
const SUBTABS: { id: SubTab; labelKey: string }[] = [
  { id: "messages", labelKey: "topics.details.tabMessages" },
  { id: "partitions", labelKey: "topics.details.tabPartitions" },
  { id: "configs", labelKey: "topics.details.tabConfigs" },
  { id: "groups", labelKey: "topics.details.tabGroups" },
];

export function TopicDetails({ connectionId, topic }: { connectionId: string; topic: string }) {
  const { t: tt } = useTranslation();
  const [sub, setSub] = useState<SubTab>("messages");
  const q = useQuery({ queryKey: ["topic", connectionId, topic], queryFn: () => api.getTopic(connectionId, topic) });

  const connection = useConnection(connectionId);
  const canMutate = connection !== undefined && !connection.readOnly;
  const readOnly = connection?.readOnly === true;
  const { locked } = useLicense();
  const queryClient = useQueryClient();
  const closeTab = useWorkspace((s) => s.close);
  const open = useWorkspace((s) => s.open);

  if (q.isPending) return <p className="empty">{tt("topics.details.loading")}</p>;
  if (q.error && !q.data) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const t = q.data;

  return (
    <div className="page">
      {q.error && <ErrorBanner error={q.error} onRetry={() => q.refetch()} />}
      <div className="toolbar">
        <h2>{t.name}</h2>
        <span className="muted">
          {tt("topics.details.summary", { partitions: t.partitions.length, rf: t.replicationFactor, messages: formatCount(t.messageCount) })}
        </span>
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          {tt("topics.details.refresh")}
        </button>
        <button
          type="button"
          disabled={readOnly}
          title={readOnly ? tt("topics.details.produceReadOnly") : undefined}
          onClick={() =>
            open({
              kind: "producer",
              connectionId,
              prefill: {
                message: { topic, partition: null, key: { format: "string", text: "" }, value: { format: "json", text: "" }, headers: [] },
                nonce: Date.now(),
              },
            })
          }
        >
          {tt("topics.details.produce")}
        </button>
        <button
          type="button"
          disabled={readOnly || locked}
          title={readOnly ? tt("topics.details.consumeReadOnly") : locked ? proHint() : undefined}
          onClick={() => open({ kind: "consumer", connectionId, key: newConsumerKey(), topic })}
        >
          {tt("topics.details.consume")}
          {locked && (
            <>
              {" "}
              <ProLock />
            </>
          )}
        </button>
      </div>
      {canMutate && connection && !t.internal && (
        <TopicActions
          connection={connection}
          topic={t}
          onDeleted={() => {
            void queryClient.invalidateQueries({ queryKey: ["topics", connectionId] });
            closeTab(tabId({ kind: "topic", connectionId, topic }));
          }}
        />
      )}
      <div className="subtabs">
        {SUBTABS.map((s) => (
          <button key={s.id} type="button" className={sub === s.id ? "active" : ""} aria-pressed={sub === s.id} onClick={() => setSub(s.id)}>
            {tt(s.labelKey)}
          </button>
        ))}
      </div>
      {/* Сообщения не размонтируем при переключении, чтобы не терять результаты browse. */}
      <div style={{ flex: 1, display: sub === "messages" ? "flex" : "none", flexDirection: "column" }}>
        <BrowsePanel connectionId={connectionId} topic={topic} />
      </div>
      {sub === "partitions" && <PartitionsTable topic={t} />}
      {sub === "configs" && <TopicConfigs connectionId={connectionId} topic={topic} />}
      {sub === "groups" && <TopicGroups connectionId={connectionId} topic={topic} />}
    </div>
  );
}

function PartitionsTable({ topic }: { topic: TopicDetailsDto }) {
  const { t } = useTranslation();
  return (
    <table>
      <thead>
        <tr>
          <th>{t("topics.details.colPartition")}</th>
          <th>{t("topics.details.colLeader")}</th>
          <th>{t("topics.details.colReplicas")}</th>
          <th>ISR</th>
          <th>Low</th>
          <th>High</th>
          <th>{t("topics.details.colMessages")}</th>
        </tr>
      </thead>
      <tbody>
        {topic.partitions.map((p) => (
          <tr key={p.id}>
            <td>{p.id}</td>
            <td className={p.leader < 0 ? "warn" : ""}>{p.leader < 0 ? t("topics.details.noLeader") : p.leader}</td>
            <td>{p.replicas.join(", ")}</td>
            <td className={p.underReplicated ? "warn" : ""}>{p.isr.join(", ")}</td>
            <td>{p.low ?? "—"}</td>
            <td>{p.high ?? "—"}</td>
            <td className={p.offsetsError ? "warn" : ""} title={p.offsetsError && tr(p.offsetsError)}>
              {formatCount(p.messageCount)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function TopicConfigs({ connectionId, topic }: { connectionId: string; topic: string }) {
  const { t } = useTranslation();
  const connection = useConnection(connectionId);
  const [editing, setEditing] = useState(false);
  const [dirty, setDirty] = useState(false);
  const q = useQuery({
    queryKey: ["topicConfigs", connectionId, topic],
    queryFn: () => api.getTopicConfigs(connectionId, topic),
  });
  if (q.isPending) return <p className="empty">{t("topics.details.loading")}</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const canEdit = connection !== undefined && !connection.readOnly;
  return (
    <>
      {canEdit && (
        <div className="toolbar">
          <button
            type="button"
            aria-pressed={editing}
            disabled={editing && dirty}
            title={editing && dirty ? t("topics.details.dirtyTitle") : undefined}
            onClick={() => setEditing((v) => !v)}>
            {editing ? t("topics.details.closeEditor") : t("topics.details.edit")}
          </button>
        </div>
      )}
      {editing && canEdit && connection ? (
        <ConfigEditor connection={connection} topic={topic} entries={q.data} onDone={() => setEditing(false)} onDirtyChange={setDirty} hints={TOPIC_CONFIG_HINTS} />
      ) : (
        <ConfigTable entries={q.data} hints={TOPIC_CONFIG_HINTS} />
      )}
    </>
  );
}
