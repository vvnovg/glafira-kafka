import type { Environment } from "../../api/types";
import { ENV_COLORS, ENV_LABELS } from "../../lib/env";

export function EnvBadge({ environment, readOnly }: { environment: Environment; readOnly: boolean }) {
  return (
    <>
      <span className="env-badge" style={{ background: ENV_COLORS[environment] }}>
        {ENV_LABELS[environment]}
      </span>
      {readOnly && (
        <span role="img" aria-label="Только чтение" title="Только чтение">
          🔒
        </span>
      )}
    </>
  );
}
