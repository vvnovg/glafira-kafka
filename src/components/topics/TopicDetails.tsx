import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { TopicDetails as TopicDetailsDto } from "../../api/types";
import { formatCount } from "../../lib/format";
import { ConfigTable } from "../common/ConfigTable";
import { ErrorBanner } from "../common/ErrorBanner";

type SubTab = "messages" | "partitions" | "configs";
const SUBTABS: { id: SubTab; label: string }[] = [
  { id: "messages", label: "Сообщения" },
  { id: "partitions", label: "Партиции" },
  { id: "configs", label: "Конфигурация" },
];

export function TopicDetails({ connectionId, topic }: { connectionId: string; topic: string }) {
  const [sub, setSub] = useState<SubTab>("messages");
  const q = useQuery({ queryKey: ["topic", connectionId, topic], queryFn: () => api.getTopic(connectionId, topic) });

  if (q.isPending) return <p className="empty">Загрузка…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const t = q.data;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{t.name}</h2>
        <span className="muted">
          партиций: {t.partitions.length} · RF: {t.replicationFactor} · сообщений ≈ {formatCount(t.messageCount)}
        </span>
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          Обновить
        </button>
      </div>
      <div className="subtabs">
        {SUBTABS.map((s) => (
          <button key={s.id} type="button" className={sub === s.id ? "active" : ""} onClick={() => setSub(s.id)}>
            {s.label}
          </button>
        ))}
      </div>
      {/* Сообщения не размонтируем при переключении, чтобы не терять результаты browse. */}
      <div hidden={sub !== "messages"} style={{ flex: 1, display: sub === "messages" ? "flex" : "none", flexDirection: "column" }}>
        <p className="empty">Просмотр сообщений</p>
      </div>
      {sub === "partitions" && <PartitionsTable topic={t} />}
      {sub === "configs" && <TopicConfigs connectionId={connectionId} topic={topic} />}
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
            <td className={p.offsetsError ? "warn" : ""} title={p.offsetsError}>
              {formatCount(p.messageCount)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function TopicConfigs({ connectionId, topic }: { connectionId: string; topic: string }) {
  const q = useQuery({
    queryKey: ["topicConfigs", connectionId, topic],
    queryFn: () => api.getTopicConfigs(connectionId, topic),
  });
  if (q.isPending) return <p className="empty">Загрузка…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  return <ConfigTable entries={q.data} />;
}
