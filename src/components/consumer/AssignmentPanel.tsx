import { useTranslation } from "react-i18next";
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

/** Пауза/продолжение предлагаются только для партиций текущего назначения (для остальных бэкенд вернёт INVALID_INPUT). */
export function AssignmentPanel({ assignment, paused, log, disabled, onPause, onResume }: Props) {
  const { t } = useTranslation();
  const sorted = [...assignment].sort((a, b) => a.topic.localeCompare(b.topic) || a.partition - b.partition);
  return (
    <div className="assignment">
      <h4>{t("consumer.assignment.title", { count: assignment.length })}</h4>
      {sorted.length === 0 && <p className="muted">{t("consumer.assignment.none")}</p>}
      <ul>
        {sorted.map((p) => {
          const isPaused = paused.includes(partitionKey(p));
          return (
            <li key={partitionKey(p)}>
              <code>{partitionKey(p)}</code> {isPaused && <span className="warn">{t("consumer.assignment.paused")}</span>}{" "}
              <button
                type="button"
                className="link"
                disabled={disabled}
                aria-label={t(isPaused ? "consumer.assignment.resumeAria" : "consumer.assignment.pauseAria", { partition: partitionKey(p) })}
                onClick={() => (isPaused ? onResume(p) : onPause(p))}
              >
                {isPaused ? t("consumer.assignment.resume") : t("consumer.assignment.pause")}
              </button>
            </li>
          );
        })}
      </ul>
      <h4>{t("consumer.assignment.logTitle")}</h4>
      {log.length === 0 && <p className="muted">{t("consumer.assignment.logEmpty")}</p>}
      <ul className="muted">
        {[...log].reverse().map((r, i) => (
          <li key={log.length - i}>
            {r.kind === "error"
              ? t("consumer.assignment.logError", { time: formatTimestamp(r.at), message: r.message })
              : t(r.kind === "assigned" ? "consumer.assignment.logAssigned" : "consumer.assignment.logRevoked", {
                  time: formatTimestamp(r.at),
                  partitions: r.partitions.map(partitionKey).join(", ") || "—",
                })}
          </li>
        ))}
      </ul>
    </div>
  );
}
