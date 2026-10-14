import { toAppError } from "../../api/client";

export function ErrorBanner({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  const e = toAppError(error);
  return (
    <div className="error-banner" role="alert">
      <div className="error-title">
        <strong>{e.message}</strong> <span className="muted">({e.code})</span>
        {onRetry && (
          <button type="button" onClick={onRetry}>
            Повторить
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
