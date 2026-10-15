import { useQuery } from "@tanstack/react-query";
import { api } from "../../api/client";
import { formatTimestamp } from "../../lib/format";
import { ErrorBanner } from "../common/ErrorBanner";
import { messageToForm, type ProducerForm } from "./form";
import { literalMessage } from "./fromMessage";

export function HistoryList({ connectionId, onRepeat }: { connectionId: string; onRepeat: (form: ProducerForm) => void }) {
  const q = useQuery({ queryKey: ["history", connectionId], queryFn: () => api.listHistory(connectionId) });
  if (q.error) return <ErrorBanner error={q.error} onRetry={() => q.refetch()} />;
  if (!q.data || q.data.length === 0) return <p className="muted">История отправки пуста.</p>;
  return (
    <table>
      <thead>
        <tr>
          <th>Время</th>
          <th>Топик</th>
          <th>P / offset</th>
          <th>Ключ</th>
          <th />
        </tr>
      </thead>
      <tbody>
        {q.data.map((h) => (
          <tr key={h.id}>
            <td>{formatTimestamp(h.sentAt)}</td>
            <td>{h.message.topic}</td>
            <td>
              {h.result.partition} /{" "}
              {h.result.offset === null ? <span title="Брокер не вернул offset (например, при acks=0)">—</span> : h.result.offset}
            </td>
            <td className="muted">{h.message.key.format === "null" ? "null" : h.message.key.text.slice(0, 40)}</td>
            <td>
              <button
                type="button"
                className="link"
                disabled={h.truncated}
                title={h.truncated ? "Значение длиннее 64 КБ сохранено не полностью — повторить нельзя" : undefined}
                // В истории — уже отрисованный текст: повтор отправляет его буквально.
                onClick={() => onRepeat(messageToForm(literalMessage(h.message), h.settings))}
              >
                Повторить
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
