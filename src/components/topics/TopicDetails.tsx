import { useState } from "react";
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
import { ErrorBanner } from "../common/ErrorBanner";
import { TopicActions } from "./TopicActions";
import { TopicGroups } from "./TopicGroups";

type SubTab = "messages" | "partitions" | "configs" | "groups";
const SUBTABS: { id: SubTab; label: string }[] = [
  { id: "messages", label: "Сообщения" },
  { id: "partitions", label: "Партиции" },
  { id: "configs", label: "Конфигурация" },
  { id: "groups", label: "Consumer groups" },
];

export function TopicDetails({ connectionId, topic }: { connectionId: string; topic: string }) {
  const [sub, setSub] = useState<SubTab>("messages");
  const q = useQuery({ queryKey: ["topic", connectionId, topic], queryFn: () => api.getTopic(connectionId, topic) });

  const connection = useConnection(connectionId);
  const canMutate = connection !== undefined && !connection.readOnly;
  const readOnly = connection?.readOnly === true;
  const { locked } = useLicense();
  const queryClient = useQueryClient();
  const closeTab = useWorkspace((s) => s.close);
  const open = useWorkspace((s) => s.open);

  if (q.isPending) return <p className="empty">Загрузка…</p>;
  if (q.error && !q.data) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const t = q.data;

  return (
    <div className="page">
      {q.error && <ErrorBanner error={q.error} onRetry={() => q.refetch()} />}
      <div className="toolbar">
        <h2>{t.name}</h2>
        <span className="muted">
          партиций: {t.partitions.length} · RF: {t.replicationFactor} · сообщений ≈ {formatCount(t.messageCount)}
        </span>
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          Обновить
        </button>
        <button
          type="button"
          disabled={readOnly}
          title={readOnly ? "Подключение только для чтения: отправка сообщений недоступна" : undefined}
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
          Отправить сообщение
        </button>
        <button
          type="button"
          disabled={readOnly || locked}
          title={readOnly ? "Подключение только для чтения: вход в группу меняет её состояние" : locked ? proHint() : undefined}
          onClick={() => open({ kind: "consumer", connectionId, key: newConsumerKey(), topic })}
        >
          Читать как группа
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
            {s.label}
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
  return (
    <table>
      <thead>
        <tr>
          <th>Партиция</th>
          <th>Лидер</th>
          <th>Реплики</th>
          <th>ISR</th>
          <th>Low</th>
          <th>High</th>
          <th>Сообщений</th>
        </tr>
      </thead>
      <tbody>
        {topic.partitions.map((p) => (
          <tr key={p.id}>
            <td>{p.id}</td>
            <td className={p.leader < 0 ? "warn" : ""}>{p.leader < 0 ? "нет" : p.leader}</td>
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
  const connection = useConnection(connectionId);
  const [editing, setEditing] = useState(false);
  const [dirty, setDirty] = useState(false);
  const q = useQuery({
    queryKey: ["topicConfigs", connectionId, topic],
    queryFn: () => api.getTopicConfigs(connectionId, topic),
  });
  if (q.isPending) return <p className="empty">Загрузка…</p>;
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
            title={editing && dirty ? "Есть несохранённые изменения: примените или отмените правки" : undefined}
            onClick={() => setEditing((v) => !v)}>
            {editing ? "Закрыть редактор" : "Редактировать"}
          </button>
        </div>
      )}
      {editing && canEdit && connection ? (
        <ConfigEditor connection={connection} topic={topic} entries={q.data} onDone={() => setEditing(false)} onDirtyChange={setDirty} />
      ) : (
        <ConfigTable entries={q.data} />
      )}
    </>
  );
}
