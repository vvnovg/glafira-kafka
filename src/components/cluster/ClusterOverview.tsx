import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import { activateProps } from "../../lib/activate";
import { formatCount } from "../../lib/format";
import { ConfigTable } from "../common/ConfigTable";
import { ErrorBanner } from "../common/ErrorBanner";

export function ClusterOverview({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["cluster", connectionId], queryFn: () => api.getClusterOverview(connectionId) });
  const [brokerId, setBrokerId] = useState<number | null>(null);

  if (q.isPending) return <p className="empty">{t("cluster.overview.loading")}</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  const o = q.data;

  return (
    <div className="page">
      <div className="toolbar">
        <h2>{t("cluster.overview.title")}</h2>
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          {t("cluster.overview.refresh")}
        </button>
      </div>
      <dl className="stats">
        <div>
          <dt>Cluster ID</dt>
          <dd>
            <code>{o.clusterId ?? "—"}</code>
          </dd>
        </div>
        <div>
          <dt>{t("cluster.overview.controller")}</dt>
          <dd>{o.controllerId ?? "—"}</dd>
        </div>
        <div>
          <dt>{t("cluster.overview.brokers")}</dt>
          <dd>{o.brokers.length}</dd>
        </div>
        <div>
          <dt>{t("cluster.overview.topicsInternal")}</dt>
          <dd>
            {formatCount(o.topicCount)} ({o.internalTopicCount})
          </dd>
        </div>
        <div>
          <dt>{t("cluster.overview.partitions")}</dt>
          <dd>{formatCount(o.partitionCount)}</dd>
        </div>
        <div>
          <dt>Under-replicated</dt>
          <dd className={o.underReplicatedPartitions > 0 ? "warn" : ""}>{o.underReplicatedPartitions}</dd>
        </div>
        <div>
          <dt>Offline</dt>
          <dd className={o.offlinePartitions > 0 ? "warn" : ""}>{o.offlinePartitions}</dd>
        </div>
      </dl>

      <h3>{t("cluster.overview.brokers")}</h3>
      <table>
        <thead>
          <tr>
            <th>ID</th>
            <th>{t("cluster.overview.address")}</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {o.brokers.map((b) => (
            <tr
              key={b.id}
              className={b.id === brokerId ? "clickable selected" : "clickable"}
              aria-pressed={b.id === brokerId}
              {...activateProps(() => setBrokerId(b.id))}
            >
              <td>{b.id}</td>
              <td>
                {b.host}:{b.port}
              </td>
              <td>{b.id === o.controllerId && <span className="muted">{t("cluster.overview.controllerBadge")}</span>}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {brokerId !== null && <BrokerConfig connectionId={connectionId} brokerId={brokerId} />}
    </div>
  );
}

function BrokerConfig({ connectionId, brokerId }: { connectionId: string; brokerId: number }) {
  const { t } = useTranslation();
  const q = useQuery({
    queryKey: ["brokerConfig", connectionId, brokerId],
    queryFn: () => api.getBrokerConfig(connectionId, brokerId),
  });
  return (
    <div>
      <h3>{t("cluster.overview.brokerConfig", { id: brokerId })}</h3>
      {q.isPending && <p className="empty">{t("cluster.overview.brokerLoading")}</p>}
      {q.error ? <ErrorBanner error={q.error} onRetry={() => q.refetch()} /> : q.data && <ConfigTable entries={q.data} />}
    </div>
  );
}
