import type { GroupPartitionOffset } from "../../api/types";

/** Как total_lag в Rust: null, если хоть одна партиция с ошибкой; партиции без коммита не суммируются. */
export function groupTotalLag(rows: GroupPartitionOffset[]): number | null {
  if (rows.some((r) => r.error)) return null;
  return rows.reduce((sum, r) => sum + (r.lag ?? 0), 0);
}
