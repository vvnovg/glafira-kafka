import type { AppError, BatchError, BatchEvent } from "../../api/types";

export interface BatchState {
  status: "idle" | "running" | "finished" | "error";
  sent: number;
  failed: number;
  total: number | null;
  /** Сообщения, отменённые до отправки */
  notSent: number;
  errors: BatchError[];
  cancelled: boolean;
  error: AppError | null;
}

export const initialBatchState: BatchState = {
  status: "idle",
  sent: 0,
  failed: 0,
  total: null,
  notSent: 0,
  errors: [],
  cancelled: false,
  error: null,
};

export type BatchAction =
  | { type: "start" }
  | { type: "event"; event: BatchEvent }
  | { type: "failed"; error: AppError }
  /** Пользователь нажал «Отменить»: UI не ждёт терминального события (его может не быть). */
  | { type: "stopped" };

export function batchReducer(s: BatchState, a: BatchAction): BatchState {
  switch (a.type) {
    case "start":
      return { ...initialBatchState, status: "running" };
    case "failed":
      return { ...s, status: "error", error: a.error };
    case "stopped":
      return s.status === "running" ? { ...s, status: "finished", cancelled: true } : s;
    case "event": {
      const e = a.event;
      switch (e.type) {
        case "progress":
          return { ...s, sent: e.sent, failed: e.failed, total: e.total };
        case "errors":
          return { ...s, errors: e.errors };
        case "finished":
          return { ...s, status: "finished", sent: e.sent, failed: e.failed, notSent: e.notSent, cancelled: e.cancelled };
        case "error":
          return { ...s, status: "error", error: e.error };
      }
    }
  }
}

export function fileFormatOf(path: string): "jsonl" | "csv" | null {
  const p = path.toLowerCase();
  if (p.endsWith(".jsonl") || p.endsWith(".ndjson")) return "jsonl";
  if (p.endsWith(".csv")) return "csv";
  return null;
}
