import { toAppError } from "../../api/client";
import { useWorkspace } from "../../state/workspace";

export function ErrorBanner({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const e = toAppError(error);
  const openTab = useWorkspace((s) => s.open);
  return (
    <div className="error-banner" role="alert">
      <div className="error-title">
        <strong>{e.message}</strong> <span className="muted">({e.code})</span>
        {onRetry && (
          <button type="button" onClick={onRetry}>
            Повторить
          </button>
        )}
        {e.code === "PRO_REQUIRED" && (
          <button type="button" onClick={() => openTab({ kind: "license" })}>
            Открыть «Лицензия»
          </button>
        )}
      </div>
      {e.details && (
        <details>
          <summary>Подробности</summary>
          <pre>{e.details}</pre>
        </details>
      )}
    </div>
  );
}
