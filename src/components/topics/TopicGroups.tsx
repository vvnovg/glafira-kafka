import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { activateProps } from "../../lib/activate";
import { formatCount } from "../../lib/format";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "../common/ErrorBanner";

export function TopicGroups({ connectionId, topic }: { connectionId: string; topic: string }) {
  const open = useWorkspace((s) => s.open);
  const q = useQuery({
    queryKey: ["topicGroups", connectionId, topic],
    queryFn: () => api.getTopicGroups(connectionId, topic),
  });
  if (q.isPending) return <p className="empty">Поиск групп с коммитами в топик…</p>;
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  return (
    <div>
      <div className="toolbar">
        <button type="button" disabled={q.isFetching} onClick={() => q.refetch()}>
          Обновить
        </button>
        {q.data.incomplete && (
          <span className="warn">За время запроса проверены не все группы — список может быть неполным.</span>
        )}
      </div>
      {q.data.groups.length === 0 ? (
        <p className="empty">Нет групп с коммитами в этот топик.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Группа</th>
              <th>Состояние</th>
              <th>Партиций с коммитами</th>
              <th>Лаг</th>
            </tr>
          </thead>
          <tbody>
            {q.data.groups.map((g) => (
              <tr key={g.groupId} className="clickable" {...activateProps(() => open({ kind: "group", connectionId, groupId: g.groupId }))}>
                <td>{g.groupId}</td>
                <td>{g.state}</td>
                <td>{g.partitions}</td>
                <td title={g.lag === null ? "Лаг неизвестен: не удалось получить конечные оффсеты партиций" : undefined}>
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
