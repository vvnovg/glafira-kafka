import type { AppError, BrowseEvent, BrowsedMessage, FinishReason } from "../../api/types";

export interface BrowseState {
  status: "idle" | "running" | "finished" | "error";
  sessionId: string | null;
  messages: BrowsedMessage[];
  scanned: number;
  matched: number;
  reachedEnd: boolean;
  finishReason: FinishReason | null;
  error: AppError | null;
  limit: number;
  /** Связь с брокером временно потеряна (идёт переподключение): причина, иначе null. */
  reconnecting: string | null;
}

export const initialBrowseState: BrowseState = {
  status: "idle",
  sessionId: null,
  messages: [],
  scanned: 0,
  matched: 0,
  reachedEnd: false,
  finishReason: null,
  error: null,
  limit: 500,
  reconnecting: null,
};

export type BrowseAction =
  | { type: "start"; limit: number }
  | { type: "started"; sessionId: string }
  | { type: "event"; event: BrowseEvent }
  | { type: "failed"; error: AppError }
  | { type: "stopped" };

export function browseReducer(s: BrowseState, a: BrowseAction): BrowseState {
  switch (a.type) {
    case "start":
      return { ...initialBrowseState, status: "running", limit: a.limit };
    case "started":
      return { ...s, sessionId: a.sessionId };
    case "failed":
      return { ...s, status: "error", error: a.error, reconnecting: null };
    case "stopped":
      // Пользователь остановил сессию: backend-событие finished{cancelled} отфильтровано токеном.
      return s.status === "running" ? { ...s, status: "finished", finishReason: "cancelled", reconnecting: null } : s;
    case "event": {
      const e = a.event;
      switch (e.type) {
        case "messages": {
          const merged = s.messages.concat(e.messages);
          const messages = merged.length > s.limit ? merged.slice(merged.length - s.limit) : merged;
          return { ...s, messages };
        }
        case "progress":
          return { ...s, scanned: e.scanned, matched: e.matched };
        case "reachedEnd":
          return { ...s, reachedEnd: true };
        case "reconnecting":
          return { ...s, reconnecting: e.message };
        case "recovered":
          return { ...s, reconnecting: null };
        case "finished":
          return {
            ...s,
            status: "finished",
            scanned: e.scanned,
            matched: e.matched,
            finishReason: e.reason,
            reconnecting: null,
          };
        case "error":
          return { ...s, status: "error", error: e.error, reconnecting: null };
      }
    }
  }
}
