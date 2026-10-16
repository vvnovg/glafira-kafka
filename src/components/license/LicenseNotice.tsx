import { useState } from "react";
import { useLicense } from "../../lib/useLicense";
import { useWorkspace } from "../../state/workspace";

export function LicenseNotice() {
  const { status } = useLicense();
  const open = useWorkspace((s) => s.open);
  const [hidden, setHidden] = useState(false);
  if (hidden || !status || (status.state !== "expiring" && status.state !== "expired")) return null;
  return (
    <div className="notice" role="status">
      <span>
        {status.state === "expiring"
          ? `Лицензия Pro истекает через ${status.daysLeft} дн.`
          : `Лицензия Pro истекла ${status.expires}: функции Pro отключены.`}
      </span>
      <button type="button" className="link" onClick={() => open({ kind: "license" })}>
        Подробнее
      </button>
      <button type="button" className="icon" aria-label="Скрыть уведомление" onClick={() => setHidden(true)}>
        ×
      </button>
    </div>
  );
}
