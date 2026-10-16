import { proHint } from "../../lib/useLicense";

export function ProLock() {
  return (
    <span className="pro-lock" title={proHint()}>
      🔒 Pro
    </span>
  );
}
