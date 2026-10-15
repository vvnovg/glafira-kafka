import type { RebalanceRecord, TopicPartitionRef } from "../../api/types";
import { formatTimestamp } from "../../lib/format";
import { partitionKey } from "./state";

interface Props {
  assignment: TopicPartitionRef[];
  paused: string[];
  log: RebalanceRecord[];
  disabled: boolean;
  onPause: (p: TopicPartitionRef) => void;
  onResume: (p: TopicPartitionRef) => void;
}

const LOG_TEXT = { assigned: "назначены", revoked: "отозваны", error: "ошибка ребаланса" } as const;

/** Пауза/продолжение предлагаются только для партиций текущего назначения (для остальных бэкенд вернёт INVALID_INPUT). */
export function AssignmentPanel({ assignment, paused, log, disabled, onPause, onResume }: Props) {
  const sorted = [...assignment].sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition);
  return (
    <div className="assignment">
      <h4>Назначение ({assignment.length})</h4>
      {sorted.length === 0 && <p className="muted">Партиции не назначены.</p>}
      <ul>
        {sorted.map((p) => {
          const isPaused = paused.includes(partitionKey(p));
          return (
            <li key={partitionKey(p)}>
              <code>{partitionKey(p)}</code> {isPaused && <span className="warn">пауза</span>}{" "}
              <button
                type="button"
                className="link"
                disabled={disabled}
                aria-label={`${isPaused ? "Продолжить" : "Пауза"} ${partitionKey(p)}`}
                onClick={() => (isPaused ? onResume(p) : onPause(p))}
              >
                {isPaused ? "продолжить" : "пауза"}
              </button>
            </li>
          );
        })}
      </ul>
      <h4>Журнал ребалансов</h4>
      {log.length === 0 && <p className="muted">Ребалансов не было.</p>}
      <ul className="muted">
        {[...log].reverse().map((r, i) => (
          <li key={log.length - i}>
            {formatTimestamp(r.at)} — {LOG_TEXT[r.kind]}
            {r.kind === "error" ? `: ${r.message}` : `: ${r.partitions.map(partitionKey).join(", ") || "—"}`}
          </li>
        ))}
      </ul>
    </div>
  );
}
