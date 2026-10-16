import { useTranslation } from "react-i18next";
import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { activateProps } from "../../lib/activate";
import { formatCount } from "../../lib/format";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";

export function TopicGroups({ connectionId, topic }: { connectionId: string; topic: string }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const q = useQuery({
    queryKey: ["topicGroups", connectionId, topic],
    queryFn: () => api.getTopicGroups(connectionId, topic),
  });
  if (q.isPending) return <p className="empty">{t("topics.groups.loading")}</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  return (
    <div>
      <div className="toolbar">
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          {t("topics.groups.refresh")}
        </button>
        {q.data.incomplete && (
          <span className="warn">{t("topics.groups.incomplete")}</span>
        )}
      </div>
      {q.data.groups.length === 0 ? (
        <p className="empty">{t("topics.groups.empty")}</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>{t("topics.groups.colGroup")}</th>
              <th>{t("topics.groups.colState")}</th>
              <th>{t("topics.groups.colPartitions")}</th>
              <th>{t("topics.groups.colLag")}</th>
            </tr>
          </thead>
          <tbody>
            {q.data.groups.map((g) => (
              <tr key={g.groupId} className="clickable" {...activateProps(() => open({ kind: "group", connectionId, groupId: g.groupId }))}>
                <td>{g.groupId}</td>
                <td>{g.state}</td>
                <td>{g.partitions}</td>
                <td title={g.lag === null ? t("topics.groups.lagUnknown") : undefined}>
                  {formatCount(g.lag)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
