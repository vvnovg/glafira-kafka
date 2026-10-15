import { useCallback, useEffect, useReducer, useRef } from "react";
import { api, toAppError } from "../../api/client";
import type { ConsumerParams } from "../../api/types";
import { consumerReducer, initialConsumerState } from "./state";

/** Протокол токенов как у useBrowseSession: события остановленной сессии игнорируются, поздний ответ запуска останавливает сессию. */
export function useConsumerSession(limit: number) {
  const [state, dispatch] = useReducer(consumerReducer, initialConsumerState);
  const sessionRef = useRef<string | null>(null);
  const tokenRef = useRef(0);

  const stopInternal = useCallback(async () => {
    // Инвалидируем ещё не завершённый startConsumer: поздняя сессия будет остановлена в start().
    tokenRef.current++;
    const id = sessionRef.current;
    sessionRef.current = null;
    if (id) await api.stopSession(id).catch(() => false);
  }, []);

  // «Отключиться» всегда сбрасывает UI: терминальное событие может не прийти.
  const stop = useCallback(async () => {
    dispatch({ type: "stopped" });
    await stopInternal();
  }, [stopInternal]);

  const start = useCallback(
    async (params: ConsumerParams, confirm: string | null) => {
      const stopping = stopInternal();
      const gen = tokenRef.current;
      await stopping;
      // За время остановки был stop/unmount/новый start — этот запуск устарел.
      if (tokenRef.current !== gen) return;
      const token = ++tokenRef.current;
      dispatch({ type: "start", limit });
      try {
        const id = await api.startConsumer(
          params,
          (event) => {
            if (tokenRef.current === token) dispatch({ type: "event", event });
          },
          confirm,
        );
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
    [stopInternal, limit],
  );

  // Закрытие вкладки — выход из группы.
  useEffect(() => () => void stopInternal(), [stopInternal]);

  return { state, start, stop };
}
