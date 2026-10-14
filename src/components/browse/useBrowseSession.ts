import { useCallback, useEffect, useReducer, useRef } from "react";
import { api, toAppError } from "../../api/client";
import type { BrowseParams } from "../../api/types";
import { browseReducer, initialBrowseState } from "./state";

export function useBrowseSession() {
  const [state, dispatch] = useReducer(browseReducer, initialBrowseState);
  const sessionRef = useRef<string | null>(null);
  // Токен запуска: события от предыдущей (остановленной) сессии игнорируются.
  const tokenRef = useRef(0);

  const stop = useCallback(async () => {
    // Инвалидируем ещё не завершённый startBrowse: поздняя сессия будет остановлена в start().
    tokenRef.current++;
    const id = sessionRef.current;
    sessionRef.current = null;
    if (id) await api.stopSession(id).catch(() => false);
  }, []);

  const start = useCallback(
    async (params: BrowseParams) => {
      const stopping = stop();
      const gen = tokenRef.current;
      await stopping;
      // За время остановки был stop/unmount/новый start — этот запуск устарел.
      if (tokenRef.current !== gen) return;
      const token = ++tokenRef.current;
      dispatch({ type: "start", limit: params.limit });
      try {
        const id = await api.startBrowse(params, (event) => {
          if (tokenRef.current === token) dispatch({ type: "event", event });
        });
        if (tokenRef.current !== token) {
          await api.stopSession(id).catch(() => false);
          return;
        }
        sessionRef.current = id;
        dispatch({ type: "started", sessionId: id });
      } catch (e) {
        if (tokenRef.current === token) dispatch({ type: "failed", error: toAppError(e) });
      }
    },
    [stop],
  );

  useEffect(() => () => void stop(), [stop]);

  return { state, start, stop };
}
