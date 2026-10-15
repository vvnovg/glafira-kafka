import { useMutation } from "@tanstack/react-query";
import { api } from "../../api/client";
import type { ConnectionProfile, TopicDeleteResult } from "../../api/types";
import { ConfirmDialog } from "../common/ConfirmDialog";
import { ErrorBanner } from "../common/ErrorBanner";

interface Props {
  connection: ConnectionProfile;
  topics: string[];
  onDone: (deleted: string[]) => void;
  onCancel: () => void;
}

/** Удаление одного или нескольких топиков: один подтверждается именем, несколько — числом. */
export function DeleteTopicsPanel({ connection, topics, onDone, onCancel }: Props) {
  const single = topics.length === 1;
  const del = useMutation({
    mutationFn: (c: Parameters<typeof api.deleteTopics>[2]) => api.deleteTopics(connection.id, topics, c),
    onSuccess: (results: TopicDeleteResult[]) => onDone(results.filter((r) => !r.error).map((r) => r.name)),
  });
  const failed = del.data?.filter((r) => r.error) ?? [];

  return (
    <div className="panel">
      {failed.length > 0 && (
        <ul className="warn">
          {failed.map((r) => (
            <li key={r.name}>
              {r.name}: {r.error?.message} {r.error?.details && <span className="muted">({r.error.details})</span>}
            </li>
          ))}
        </ul>
      )}
      {del.error && <ErrorBanner error={del.error} />}
      {!del.data && (
        <ConfirmDialog
          connection={connection}
          title={single ? `Удалить топик «${topics[0]}»?` : `Удалить ${topics.length} топиков?`}
          objectName={single ? topics[0] : String(topics.length)}
          objectLabel={single ? "имя топика" : "число топиков"}
          confirmLabel="Удалить"
          danger
          busy={del.isPending}
          onConfirm={(c) => del.mutate(c)}
          onCancel={onCancel}
        >
          {!single && <p className="muted">{topics.join(", ")}</p>}
          <p className="muted">Данные удаляются безвозвратно.</p>
        </ConfirmDialog>
      )}
      {del.data && (
        <button type="button" onClick={onCancel}>
          Закрыть
        </button>
      )}
    </div>
  );
}
