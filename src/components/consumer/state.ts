import type { AppError, BrowsedMessage, ConsumeEvent, OffsetTarget, RebalanceRecord, TopicPartitionRef } from "../../api/types";
import { appendMessages } from "../browse/state";

export const partitionKey = (p: TopicPartitionRef) => `${p.topic}:${p.partition}`;
const LOG_LIMIT = 200;

export interface ConsumerState {
  status: "idle" | "running" | "finished" | "error";
  sessionId: string | null;
  messages: BrowsedMessage[];
  consumed: number;
  matched: number;
  /** Текущее назначение. Пауза/продолжение возможны только для этих партиций. */
  assignment: TopicPartitionRef[];
  /** partitionKey приостановленных партиций (подмножество assignment). */
  paused: string[];
  log: RebalanceRecord[];
  /** Последний подтверждённый сессией коммит (событие committed). */
  lastCommit: OffsetTarget[] | null;
  reconnecting: string | null;
  error: AppError | null;
  limit: number;
}

export const initialConsumerState: ConsumerState = {
  status: "idle",
  sessionId: null,
  messages: [],
  consumed: 0,
  matched: 0,
  assignment: [],
  paused: [],
  log: [],
  lastCommit: null,
  reconnecting: null,
  error: null,
  limit: 5000,
};

export type ConsumerAction =
  | { type: "start"; limit: number }
  | { type: "started"; sessionId: string }
  | { type: "event"; event: ConsumeEvent }
  | { type: "failed"; error: AppError }
  /** Пользователь нажал «Отключиться»: UI не ждёт терминального события (его может не быть). */
  | { type: "stopped" };

const without = (list: TopicPartitionRef[], remove: TopicPartitionRef[]) => {
  const keys = new Set(remove.map(partitionKey));
  return list.filter((p) => !keys.has(partitionKey(p)));
};

/** Сессия завершена: назначения и паузы больше не действуют. */
const ended = { assignment: [], paused: [], reconnecting: null };

export function consumerReducer(s: ConsumerState, a: ConsumerAction): ConsumerState {
  switch (a.type) {
    case "start":
      return { ...initialConsumerState, status: "running", limit: a.limit };
    case "started":
      return { ...s, sessionId: a.sessionId };
    case "failed":
      return { ...s, ...ended, status: "error", error: a.error };
    case "stopped":
      return s.status === "running" ? { ...s, ...ended, status: "finished" } : s;
    case "event": {
      const e = a.event;
      switch (e.type) {
        case "messages":
          return { ...s, messages: appendMessages(s.messages, e.messages, s.limit) };
        case "progress":
          return { ...s, consumed: e.consumed, matched: e.matched };
        case "rebalance": {
          const log = [...s.log, e.record].slice(-LOG_LIMIT);
          const r = e.record;
          if (r.kind === "assigned") {
            // Eager-протокол присылает полное назначение после revoke, cooperative — прирост: объединяем.
            // Новые назначенные партиции начинают без паузы.
            const added = new Set(r.partitions.map(partitionKey));
            return {
              ...s,
              log,
              assignment: [...without(s.assignment, r.partitions), ...r.partitions],
              paused: s.paused.filter((k) => !added.has(k)),
            };
          }
          if (r.kind === "revoked") {
            const removed = new Set(r.partitions.map(partitionKey));
            return { ...s, log, assignment: without(s.assignment, r.partitions), paused: s.paused.filter((k) => !removed.has(k)) };
          }
          return { ...s, log };
        }
        case "committed":
          return { ...s, lastCommit: e.offsets };
        case "paused": {
          const assigned = new Set(s.assignment.map(partitionKey));
          const keys = e.partitions.map(partitionKey).filter((k) => assigned.has(k));
          return { ...s, paused: [...new Set([...s.paused, ...keys])] };
        }
        case "resumed": {
          const keys = new Set(e.partitions.map(partitionKey));
          return { ...s, paused: s.paused.filter((k) => !keys.has(k)) };
        }
        case "reconnecting":
          return { ...s, reconnecting: e.message };
        case "recovered":
          return { ...s, reconnecting: null };
        case "finished":
          return { ...s, ...ended, status: "finished", consumed: e.consumed, matched: e.matched };
        case "error":
          return { ...s, ...ended, status: "error", error: e.error };
      }
    }
  }
}
