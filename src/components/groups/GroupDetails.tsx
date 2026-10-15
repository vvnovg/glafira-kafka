import type { ResetPrefill } from "../../state/workspace";

export function GroupDetails({ groupId }: { connectionId: string; groupId: string; resetPrefill?: ResetPrefill }) {
  return (
    <div className="page">
      <h2>{groupId}</h2>
    </div>
  );
}
