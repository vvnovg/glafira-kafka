import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../../api/client";
import { useWorkspace } from "../../state/workspace";

/** Проверка обновлений один раз при запуске. Ошибки молча игнорируются — подробности в «О программе». */
export function UpdateNotice() {
  const q = useQuery({ queryKey: ["update-check"], queryFn: api.checkUpdate, staleTime: Infinity, retry: false });
  const open = useWorkspace((s) => s.open);
  const [hidden, setHidden] = useState(false);
  if (hidden || q.data?.status !== "available") return null;
  return (
    <div className="notice" role="status">
      <span>Доступна версия {q.data.version}.</span>
      <button type="button" className="link" onClick={() => open({ kind: "about" })}>
        Подробнее
      </button>
      <button type="button" className="icon" aria-label="Скрыть уведомление" onClick={() => setHidden(true)}>
        ×
      </button>
    </div>
  );
}
